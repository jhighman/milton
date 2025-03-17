import csv
import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple
from collections import defaultdict
from enum import Enum
from main_config import INPUT_FOLDER, OUTPUT_FOLDER, ARCHIVE_FOLDER, canonical_fields
from main_file_utils import save_checkpoint
from business import process_claim
from services import FinancialServicesFacade
from evaluation_report_builder import EvaluationReportBuilder
from evaluation_report_director import EvaluationReportDirector

logger = logging.getLogger('main_csv_processing')

class SkipScenario(Enum):
    NO_NAME = "Missing both first/last names and individual name"
    NO_EMPLOYEE_NUMBER = "Missing employee number"
    NO_ORG_IDENTIFIERS = "Missing all organization identifiers (crd_number, organization_crd, organization_name)"

class CSVProcessor:
    def __init__(self):
        self.current_csv = None
        self.current_line = 0
        self.skipped_records = defaultdict(list)
        self.error_records = defaultdict(list)

    def generate_reference_id(self, crd_number: str = None) -> str:
        if crd_number and crd_number.strip():
            return crd_number
        return f"DEF-{random.randint(100000000000, 999999999999)}"

    def resolve_headers(self, fieldnames: List[str]) -> Dict[str, str]:
        resolved_headers = {}
        logger.info(f"Raw fieldnames from CSV: {fieldnames}")
        for header in fieldnames:
            if not header.strip():
                logger.warning("Empty header name encountered")
                continue
            header_lower = header.lower().strip()
            logger.debug(f"Processing header: '{header}' (lowercase: '{header_lower}')")
            for canonical, variants in canonical_fields.items():
                variants_lower = [v.lower().strip() for v in variants]
                logger.debug(f"Checking against canonical '{canonical}' variants: {variants_lower}")
                if header_lower in variants_lower:
                    resolved_headers[header] = canonical
                    logger.debug(f"Mapped header '{header}' to '{canonical}'")
                    break
            else:
                logger.warning(f"Unmapped CSV column: '{header}' will be included as-is")
                resolved_headers[header] = header
        logger.debug(f"Resolved headers: {json.dumps(resolved_headers, indent=2)}")
        unmapped_canonicals = set(canonical_fields.keys()) - set(resolved_headers.values())
        if unmapped_canonicals:
            logger.debug(f"Canonical fields not found in CSV headers: {unmapped_canonicals}")
        return resolved_headers

    def validate_row(self, claim: Dict[str, str]) -> Tuple[bool, List[str]]:
        issues = []
        first_name = claim.get('first_name', '').strip()
        last_name = claim.get('last_name', '').strip()
        individual_name = claim.get('individual_name', '').strip()
        if not (first_name and last_name) and not individual_name:
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

    def process_csv(self, csv_file_path: str, start_line: int, facade: FinancialServicesFacade, config: Dict[str, bool], wait_time: float):
        self.current_csv = os.path.basename(csv_file_path)
        self.current_line = 0
        logger.info(f"Starting to process {csv_file_path} from line {start_line}")

        try:
            with open(csv_file_path, 'r', newline='') as f:
                reader = csv.DictReader(f)
                resolved_headers = self.resolve_headers(reader.fieldnames)
                logger.debug(f"Resolved headers (post-processing): {resolved_headers}")

                for i, row in enumerate(reader, start=2):
                    if i <= start_line:
                        logger.debug(f"Skipping line {i} (before start_line {start_line})")
                        continue
                    logger.info(f"Processing {self.current_csv}, line {i}, row: {dict(row)}")
                    self.current_line = i
                    try:
                        self.process_row(row, resolved_headers, facade, config, wait_time)
                    except Exception as e:
                        logger.error(f"Error processing {self.current_csv}, line {i}: {str(e)}", exc_info=True)
                        self.error_records[self.current_csv].append({"row_data": dict(row), "error": str(e)})
                    save_checkpoint(self.current_csv, self.current_line)
                    time.sleep(wait_time)
        except Exception as e:
            logger.error(f"Error reading {csv_file_path}: {str(e)}", exc_info=True)
            self.error_records[self.current_csv].append({"row_data": {}, "error": f"File read error: {str(e)}"})
        finally:
            self.write_error_records()
            self.write_skipped_records()

    def process_row(self, row: Dict[str, str], resolved_headers: Dict[str, str], facade: FinancialServicesFacade, config: Dict[str, bool], wait_time: float):
        reference_id_header = next((k for k, v in resolved_headers.items() if v == 'reference_id'), 'reference_id')
        reference_id = row.get(reference_id_header, '').strip() or self.generate_reference_id(row.get(resolved_headers.get('crd_number', 'crd_number'), ''))

        try:
            raw_row = {}
            for header in row:
                value = row.get(header, '')
                if isinstance(value, str):
                    raw_row[header] = value.strip()
                elif isinstance(value, list):
                    raw_row[header] = ''.join(str(v).strip() for v in value if v)
                else:
                    raw_row[header] = str(value).strip()

            logger.info(f"Raw CSV row for reference_id='{reference_id}': {json.dumps(raw_row, indent=2)}")

            claim = {}
            for header, canonical in resolved_headers.items():
                value = raw_row.get(header, '')
                claim[canonical] = value
                logger.debug(f"Mapping field - canonical: '{canonical}', header: '{header}', value: '{value}'")

            claim['individual_name'] = " ".join(
                filter(None, [
                    claim.get('first_name', '').strip(),
                    claim.get('middle_name', '').strip(),
                    claim.get('last_name', '').strip(),
                    claim.get('suffix', '').strip()
                ])
            )

            employee_number_header = next((k for k, v in resolved_headers.items() if v == 'employee_number'), 'employee_number')
            employee_number = raw_row.get(employee_number_header, '').strip()
            claim['employee_number'] = employee_number
            logger.info(f"Employee number header: '{employee_number_header}', value: '{employee_number}'")

            is_valid, issues = self.validate_row(claim)
            logger.info(f"Validation for reference_id='{reference_id}': valid={is_valid}, issues={issues}")

            if not is_valid:
                for issue in issues:
                    logger.warning(f"Row skipped - {issue} for reference_id='{reference_id}'")
                    self.skipped_records[self.current_csv].append({"row_data": raw_row})
                builder = EvaluationReportBuilder()
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
                process_claim(
                    claim,
                    facade,
                    employee_number,
                    skip_disciplinary=config.get('skip_disciplinary', False),
                    skip_arbitration=config.get('skip_arbitration', False),
                    skip_regulatory=config.get('skip_regulatory', False)
                )
                self._save_report(report, employee_number, reference_id)
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
                    builder = EvaluationReportBuilder()
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
                    facade.save_compliance_report(report, employee_number)

                self._save_report(report, employee_number, reference_id)

        except Exception as e:
            logger.error(f"Unexpected error processing row for reference_id='{reference_id}': {str(e)}", exc_info=True)
            self.error_records[self.current_csv].append({"row_data": raw_row if 'raw_row' in locals() else dict(row), "error": str(e)})

    def _save_report(self, report: Dict[str, Any], employee_number: str, reference_id: str):
        report_path = os.path.join(OUTPUT_FOLDER, f"{reference_id}.json")
        logger.info(f"Saving report to {report_path} (output folder)")
        try:
            # Convert Alert objects to dictionaries
            def convert_to_serializable(obj):
                if hasattr(obj, '__dict__'):
                    return {k: convert_to_serializable(v) for k, v in obj.__dict__.items()}
                elif isinstance(obj, list):
                    return [convert_to_serializable(item) for item in obj]
                elif isinstance(obj, dict):
                    return {k: convert_to_serializable(v) for k, v in obj.items()}
                return obj

            serializable_report = convert_to_serializable(report)
            with open(report_path, 'w') as f:
                json.dump(serializable_report, f, indent=2)
            compliance = report.get('final_evaluation', {}).get('overall_compliance', False)
            logger.info(f"Processed {reference_id}, overall_compliance: {compliance}, saved to output/")
        except Exception as e:
            logger.error(f"Error saving report to {report_path}: {str(e)}", exc_info=True)

        logger.info(f"Report for reference_id='{reference_id}' also saved to cache/{employee_number}/ by process_claim")

    def _write_records(self, records: defaultdict, output_file: str, record_type: str):
        date_str = datetime.now().strftime("%m-%d-%Y")
        archive_subfolder = os.path.join(ARCHIVE_FOLDER, date_str)
        csv_path = os.path.join(archive_subfolder, output_file)
        try:
            os.makedirs(archive_subfolder, exist_ok=True)
            with open(csv_path, 'a', newline='') as f:
                file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0
                writer = csv.DictWriter(f, fieldnames=None)
                
                total_records = 0
                for csv_file, record_list in records.items():
                    for record in record_list:
                        row_data = record['row_data']
                        if not file_exists:
                            fieldnames = list(row_data.keys())
                            if "error" in record:
                                fieldnames.append("error")
                            writer.fieldnames = fieldnames
                            writer.writeheader()
                            file_exists = True
                        writer.writerow({**row_data, **({"error": record["error"]} if "error" in record else {})})
                        total_records += 1
                
                logger.info(f"Appended {total_records} {record_type} records to {csv_path}")
        except Exception as e:
            logger.error(f"Error writing {record_type} records to {csv_path}: {str(e)}", exc_info=True)
        finally:
            records.clear()

    def write_skipped_records(self):
        self._write_records(self.skipped_records, "skipped.csv", "skipped")

    def write_error_records(self):
        self._write_records(self.error_records, "errors.csv", "error")

if __name__ == "__main__":
    facade = FinancialServicesFacade()
    config = {"skip_disciplinary": True, "skip_arbitration": True, "skip_regulatory": True}
    processor = CSVProcessor()
    processor.process_csv(
        os.path.join(INPUT_FOLDER, "sample.csv"),
        start_line=0,
        facade=facade,
        config=config,
        wait_time=0.1
    )