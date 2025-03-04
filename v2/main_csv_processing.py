import csv
import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple
from collections import OrderedDict, defaultdict
from enum import Enum
from main_config import INPUT_FOLDER, OUTPUT_FOLDER, ARCHIVE_FOLDER, canonical_fields
from main_file_utils import save_checkpoint
from business import process_claim
from services import FinancialServicesFacade
from evaluation_report_builder import EvaluationReportBuilder
from evaluation_report_director import EvaluationReportDirector

logger = logging.getLogger('main_csv_processing')

current_csv = None
current_line = 0
skipped_records = defaultdict(list)
error_records = defaultdict(list)  # New: Track rows with processing errors

class SkipScenario(Enum):
    NO_NAME = "Missing both first and last names"
    NO_EMPLOYEE_NUMBER = "Missing employee number"
    NO_ORG_IDENTIFIERS = "Missing all organization identifiers (crd_number, organization_crd, organization_name)"

def generate_reference_id(crd_number: str = None) -> str:
    if crd_number and crd_number.strip():
        return crd_number
    return f"DEF-{random.randint(100000000000, 999999999999)}"

def resolve_headers(fieldnames: List[str]) -> Dict[str, str]:
    resolved_headers = {}
    logger.info(f"Raw fieldnames from CSV: {fieldnames}")
    for header in fieldnames:
        if not header.strip():
            logger.warning("Empty header name encountered")
            continue
        header_lower = header.lower().strip()
        logger.info(f"Processing header: '{header}' (lowercase: '{header_lower}')")
        for canonical, variants in canonical_fields.items():
            variants_lower = [v.lower().strip() for v in variants]
            logger.info(f"Checking against canonical '{canonical}' variants: {variants_lower}")
            if header_lower in variants_lower:
                resolved_headers[header] = canonical
                logger.info(f"Mapped header '{header}' to '{canonical}'")
                break
        else:
            logger.warning(f"Unmapped CSV column: '{header}'")
    logger.info(f"Resolved headers: {json.dumps(resolved_headers, indent=2)}")
    unmapped_canonicals = set(canonical_fields.keys()) - set(resolved_headers.values())
    if unmapped_canonicals:
        logger.info(f"Canonical fields not found in CSV headers: {unmapped_canonicals}")
    logger.info(f"Finished resolving headers, returning: {resolved_headers}")
    return resolved_headers

def validate_row(claim: Dict[str, str]) -> Tuple[bool, List[str]]:
    issues = []
    first_name = claim.get('first_name', '').strip()
    last_name = claim.get('last_name', '').strip()
    if not (first_name and last_name):
        issues.append(SkipScenario.NO_NAME.value)
    employee_number = claim.get('employee_number', '').strip()
    if not employee_number:
        issues.append(SkipScenario.NO_EMPLOYEE_NUMBER.value)
    crd_number = claim.get('crd_number', '').strip()
    org_crd = claim.get('organization_crd', '').strip()
    org_name = claim.get('organization_name', '').strip()
    if not (crd_number or org_crd or org_name):
        issues.append(SkipScenario.NO_ORG_IDENTIFIERS.value)
    return (len(issues) == 0, issues)

def process_csv(csv_file_path: str, start_line: int, facade: FinancialServicesFacade, config: Dict[str, bool], wait_time: float):
    global current_csv, current_line
    current_csv = os.path.basename(csv_file_path)
    current_line = 0
    logger.info(f"Starting to process {csv_file_path} from line {start_line}")

    try:
        with open(csv_file_path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            resolved_headers = resolve_headers(reader.fieldnames)
            logger.info(f"Resolved headers (post-processing): {resolved_headers}")

            for i, row in enumerate(reader, start=2):
                if i <= start_line:
                    logger.info(f"Skipping line {i} (before start_line {start_line})")
                    continue
                logger.info(f"Processing {current_csv}, line {i}, row: {dict(row)}")
                current_line = i
                try:
                    process_row(row, resolved_headers, facade, config, wait_time)
                except Exception as e:
                    logger.error(f"Error processing {current_csv}, line {i}: {str(e)}", exc_info=True)
                    error_records[current_csv].append({"row_data": dict(row)})  # Capture row on error
                save_checkpoint(current_csv, current_line)
                time.sleep(wait_time)
    except Exception as e:
        logger.error(f"Error reading {csv_file_path}: {str(e)}", exc_info=True)
        error_records[current_csv].append({"row_data": {"file_error": f"Unable to read file: {str(e)}"}})
    finally:
        write_error_records()  # Write errors after each file

def process_row(row: Dict[str, str], resolved_headers: Dict[str, str], facade: FinancialServicesFacade, config: Dict[str, bool], wait_time: float):
    global current_csv
    reference_id_header = next((k for k, v in resolved_headers.items() if v == 'reference_id'), 'reference_id')
    reference_id = row.get(reference_id_header, '').strip() or generate_reference_id(row.get(resolved_headers.get('crd_number', 'crd_number'), ''))

    try:
        # Safely handle non-string values
        raw_row = {}
        for header in row:
            value = row.get(header, '')
            if isinstance(value, str):
                raw_row[header] = value.strip()
            elif isinstance(value, list):
                raw_row[header] = ''.join(str(v).strip() for v in value if v)  # Join list elements into a string
            else:
                raw_row[header] = str(value).strip()  # Fallback: convert to string

        logger.info(f"Raw CSV row for reference_id='{reference_id}': {json.dumps(raw_row, indent=2)}")

        claim = {}
        for header, canonical in resolved_headers.items():
            value = raw_row.get(header, '')  # Use raw_row to ensure cleaned values
            claim[canonical] = value
            logger.info(f"Mapping field - canonical: '{canonical}', header: '{header}', value: '{value}'")

        claim['individual_name'] = f"{claim.get('first_name', '').strip()} {claim.get('last_name', '').strip()}"
        employee_number_header = next((k for k, v in resolved_headers.items() if v == 'employee_number'), 'employee_number')
        employee_number = raw_row.get(employee_number_header, '').strip()
        claim['employee_number'] = employee_number
        logger.info(f"Employee number header: '{employee_number_header}', value: '{employee_number}'")

        is_valid, issues = validate_row(claim)
        logger.info(f"Validation for reference_id='{reference_id}': valid={is_valid}, issues={issues}")

        if not is_valid:
            for issue in issues:
                logger.warning(f"Row skipped - {issue} for reference_id='{reference_id}'")
            builder = EvaluationReportBuilder(reference_id)
            director = EvaluationReportDirector(builder)
            extracted_info = {
                "search_evaluation": {
                    "source": "Validation",
                    "basic_result": None,
                    "detailed_result": None,
                    "search_strategy": "none",
                    "crd_number": None,
                    "compliance": False,
                    "compliance_explanation": f"Insufficient data: {', '.join(issues)}"
                },
                "skip_reasons": issues,
                "individual": {},
                "fetched_name": "",
                "other_names": [],
                "bc_scope": "NotInScope",
                "ia_scope": "NotInScope",
                "exams": [],
                "disclosures": [],
                "disciplinary_evaluation": {"actions": [], "due_diligence": {"status": "Skipped due to insufficient data"}},
                "arbitration_evaluation": {"actions": [], "due_diligence": {"status": "Skipped due to insufficient data"}},
                "regulatory_evaluation": {"actions": [], "due_diligence": {"status": "Skipped due to insufficient data"}}
            }
            report = director.construct_evaluation_report(claim, extracted_info)
        else:
            unmapped_fields = set(row.keys()) - set(resolved_headers.keys())
            if unmapped_fields:
                logger.warning(f"Unmapped fields in row for reference_id='{reference_id}': {unmapped_fields}")
            
            logger.info(f"Canonical claim for reference_id='{reference_id}': {json.dumps(claim, indent=2)}")

            report = process_claim(
                claim,
                facade,
                employee_number,
                skip_disciplinary=config.get('skip_disciplinary', False),
                skip_arbitration=config.get('skip_arbitration', False),
                skip_regulatory=config.get('skip_regulatory', False)
            )
            if report is None:
                logger.error(f"process_claim returned None for reference_id='{reference_id}'")
                builder = EvaluationReportBuilder(reference_id)
                director = EvaluationReportDirector(builder)
                extracted_info = {
                    "search_evaluation": {
                        "search_strategy": "unknown",
                        "search_outcome": "process_claim returned None",
                        "search_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "compliance": False,
                        "compliance_explanation": "Processing failed: process_claim returned None"
                    },
                    "skip_reasons": ["Processing failure"],
                    "individual": {},
                    "fetched_name": "",
                    "other_names": [],
                    "bc_scope": "NotInScope",
                    "ia_scope": "NotInScope",
                    "exams": [],
                    "disclosures": [],
                    "disciplinary_evaluation": {"actions": [], "due_diligence": {"status": "Skipped due to processing failure"}},
                    "arbitration_evaluation": {"actions": [], "due_diligence": {"status": "Skipped due to processing failure"}},
                    "regulatory_evaluation": {"actions": [], "due_diligence": {"status": "Skipped due to processing failure"}}
                }
                report = director.construct_evaluation_report(claim, extracted_info)

        save_evaluation_report(report, employee_number, reference_id)

    except AttributeError as e:
        logger.warning(f"Skipping row due to invalid data type: {str(e)}. Row: {dict(row)}")
        error_records[current_csv].append({"row_data": dict(row)})
        return  # Skip this row
    except Exception as e:
        logger.error(f"Unexpected error processing row for reference_id='{reference_id}': {str(e)}", exc_info=True)
        error_records[current_csv].append({"row_data": dict(row)})
        return  # Skip this row

def save_evaluation_report(report: Dict[str, Any], employee_number: str, reference_id: str):
    report_path = os.path.join(OUTPUT_FOLDER, f"{reference_id}.json")
    logger.info(f"Saving report to {report_path}")
    try:
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        compliance = report.get('final_evaluation', {}).get('overall_compliance', False)
        logger.info(f"Processed {reference_id}, overall_compliance: {compliance}")
    except Exception as e:
        logger.error(f"Error saving report to {report_path}: {str(e)}", exc_info=True)

def write_skipped_records():
    date_str = datetime.now().strftime("%m-%d-%Y")
    archive_subfolder = os.path.join(ARCHIVE_FOLDER, date_str)
    skipped_csv_path = os.path.join(archive_subfolder, "skipped.csv")
    try:
        os.makedirs(archive_subfolder, exist_ok=True)
        with open(skipped_csv_path, 'a', newline='') as f:
            file_exists = os.path.exists(skipped_csv_path) and os.path.getsize(skipped_csv_path) > 0
            writer = csv.DictWriter(f, fieldnames=None)
            
            total_skipped = 0
            for csv_file, records in skipped_records.items():
                for record in records:
                    raw_row = record['row_data']
                    if not file_exists:
                        writer.fieldnames = raw_row.keys()
                        writer.writeheader()
                        file_exists = True
                    writer.writerow(raw_row)
                    total_skipped += 1
            
            logger.info(f"Appended {total_skipped} skipped records to {skipped_csv_path}")
    except Exception as e:
        logger.error(f"Error writing skipped records to {skipped_csv_path}: {str(e)}", exc_info=True)
    finally:
        skipped_records.clear()

def write_error_records():
    date_str = datetime.now().strftime("%m-%d-%Y")
    archive_subfolder = os.path.join(ARCHIVE_FOLDER, date_str)
    errors_csv_path = os.path.join(archive_subfolder, "errors.csv")
    try:
        os.makedirs(archive_subfolder, exist_ok=True)
        with open(errors_csv_path, 'a', newline='') as f:
            file_exists = os.path.exists(errors_csv_path) and os.path.getsize(errors_csv_path) > 0
            writer = csv.DictWriter(f, fieldnames=None)
            
            total_errors = 0
            for csv_file, records in error_records.items():
                for record in records:
                    raw_row = record['row_data']
                    if not file_exists:
                        writer.fieldnames = raw_row.keys()
                        writer.writeheader()
                        file_exists = True
                    writer.writerow(raw_row)
                    total_errors += 1
            
            logger.info(f"Appended {total_errors} error records to {errors_csv_path}")
    except Exception as e:
        logger.error(f"Error writing error records to {errors_csv_path}: {str(e)}", exc_info=True)
    finally:
        error_records.clear()  # Clear after writing, even if writing fails