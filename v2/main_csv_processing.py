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

logger = logging.getLogger('main_csv_processing')

current_csv = None
current_line = 0
skipped_records = defaultdict(list)

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
                save_checkpoint(current_csv, current_line)
                time.sleep(wait_time)
    except Exception as e:
        logger.error(f"Error reading {csv_file_path}: {str(e)}", exc_info=True)

def process_row(row: Dict[str, str], resolved_headers: Dict[str, str], facade: FinancialServicesFacade, config: Dict[str, bool], wait_time: float):
    global current_csv
    reference_id_header = next((k for k, v in resolved_headers.items() if v == 'reference_id'), 'reference_id')
    reference_id = row.get(reference_id_header, '').strip() or generate_reference_id(row.get(resolved_headers.get('crd_number', 'crd_number'), ''))

    # Log the raw CSV row
    raw_row = {header: row.get(header, '').strip() for header in row}
    logger.info(f"Raw CSV row for reference_id='{reference_id}': {json.dumps(raw_row, indent=2)}")

    # Build the claim first
    claim = {}
    for header, canonical in resolved_headers.items():
        value = row.get(header, '').strip()
        claim[canonical] = value
        logger.info(f"Mapping field - canonical: '{canonical}', header: '{header}', value: '{value}'")

    # Derive individual_name from first_name and last_name
    first_name = claim.get('first_name', '').strip()
    last_name = claim.get('last_name', '').strip()
    claim['individual_name'] = f"{first_name} {last_name}".strip()

    # Ensure employee_number is populated
    employee_number_header = next((k for k, v in resolved_headers.items() if v == 'employee_number'), 'employee_number')
    employee_number = row.get(employee_number_header, '').strip()
    claim['employee_number'] = employee_number
    logger.info(f"Employee number header: '{employee_number_header}', value: '{employee_number}'")

    # Validate using the claim
    is_valid, issues = validate_row(claim)
    
    if not is_valid:
        for issue in issues:
            logger.warning(f"Skipping row - {issue} for reference_id='{reference_id}'")
        logger.info(f"Skipped claim for reference_id='{reference_id}': {json.dumps(raw_row, indent=2)}")
        
        skipped_records[current_csv].append({
            'reference_id': reference_id,
            'row_data': raw_row,
            'skip_reasons': issues,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        report = OrderedDict([
            ("reference_id", reference_id),
            ("claim", raw_row),
            ("processing_status", "skipped"),
            ("skip_reasons", issues),
            ("timestamp", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        ])
        save_evaluation_report(report, employee_number or "unknown", reference_id)
        return

    unmapped_fields = set(row.keys()) - set(resolved_headers.keys())
    if unmapped_fields:
        logger.warning(f"Unmapped fields in row for reference_id='{reference_id}': {unmapped_fields}")
    
    logger.info(f"Canonical claim for reference_id='{reference_id}': {json.dumps(claim, indent=2)}")

    try:
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
            return
        logger.info(f"Raw report from process_claim: {json.dumps(report, indent=2)}")
        
        if 'disclosure_review' in report and 'disclosure_evaluation' not in report:
            report['disclosure_evaluation'] = {
                "compliance": report['disclosure_review'].get('compliance', True),
                "compliance_explanation": report['disclosure_review'].get('compliance_explanation', "No disclosures evaluated"),
                "alerts": report['disclosure_review'].get('alerts', [])
            }
        save_evaluation_report(report, employee_number, reference_id)
    except Exception as e:
        logger.error(f"Error processing claim for reference_id='{reference_id}': {str(e)}", exc_info=True)
        report = OrderedDict([
            ("reference_id", reference_id),
            ("claim", claim),
            ("search_evaluation", {
                "search_strategy": "unknown",
                "search_outcome": str(e),
                "search_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "alerts": [{"alert_type": "Processing Error", "severity": "High", "metadata": {}, "description": str(e)}],
                "source": "Unknown"
            })
        ])
        save_evaluation_report(report, employee_number, reference_id)

    time.sleep(wait_time)

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
        # Open in append mode to preserve existing rows
        with open(skipped_csv_path, 'a', newline='') as f:
            file_exists = os.path.exists(skipped_csv_path) and os.path.getsize(skipped_csv_path) > 0
            writer = csv.DictWriter(f, fieldnames=None)  # We'll set fieldnames dynamically
            
            total_skipped = 0
            for csv_file, records in skipped_records.items():
                for record in records:
                    raw_row = record['row_data']  # The original row as a dict
                    if not file_exists:
                        # Write headers from the first skipped row
                        writer.fieldnames = raw_row.keys()
                        writer.writeheader()
                        file_exists = True
                    writer.writerow(raw_row)  # Write the raw row as-is
                    total_skipped += 1
            
            logger.info(f"Appended {total_skipped} skipped records to {skipped_csv_path}")
    except Exception as e:
        logger.error(f"Error writing skipped records to {skipped_csv_path}: {str(e)}", exc_info=True)
    finally:
        # Clear skipped_records after writing
        skipped_records.clear()