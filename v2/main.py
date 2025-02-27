import argparse
import csv
import json
import logging
import os
import shutil
import signal
import time
from datetime import datetime
from typing import Dict, Optional, Any
from collections import OrderedDict
import random

from business import process_claim
from services import FinancialServicesFacade
from logger_config import setup_logging

logger = logging.getLogger('main')

canonical_fields = {
    'reference_id': ['referenceId', 'Reference ID', 'reference_id', 'ref_id', 'RefID'],
    'crd_number': ['CRD', 'crd_number', 'crd', 'CRD Number', 'CRDNumber', 'crdnumber'],
    'first_name': ['firstName', 'First Name', 'first_name', 'fname', 'FirstName', 'first'],
    'middle_name': ['middle_name', 'Middle Name', 'middlename', 'MiddleName', 'middle', 'middleName'],
    'last_name': ['lastName', 'Last Name', 'last_name', 'lname', 'LastName', 'last'],
    'employee_number': ['employeeNumber', 'Employee Number', 'employee_number', 'emp_id', 'employeenumber'],
    'license_type': ['license_type', 'License Type', 'licensetype', 'LicenseType', 'license'],
    'organization_name': ['orgName', 'Organization Name', 'organization_name', 'firm_name', 'organizationname', 'OrganizationName', 'organization'],
    'organization_crd': ['orgCRD', 'Organization CRD', 'org_crd_number', 'firm_crd', 'organizationCRD', 'organization_crd_number', 'organization_crd', 'organizationCrdNumber'],
    'suffix': ['suffix', 'Suffix'],
    'ssn': ['ssn', 'SSN', 'Social Security Number', 'social_security_number'],
    'dob': ['dob', 'DOB', 'Date of Birth', 'date_of_birth', 'birthDate', 'birth_date'],
    'address_line1': ['addressLine1', 'Address Line 1', 'address_line1', 'addressLineOne'],
    'address_line2': ['addressLine2', 'Address Line 2', 'address_line2', 'addressLineTwo'],
    'city': ['city', 'City'],
    'county': ['county', 'County'],
    'state': ['state', 'State', 'state_code', 'stateCode'],
    'zip': ['zip', 'Zip', 'zipcode', 'postalCode', 'postal_code'],
    'country': ['country', 'Country'],
    'gender': ['gender', 'Gender', 'sex'],
    'role': ['role', 'Role', 'jobRole', 'job_role'],
    'title': ['title', 'Title', 'jobTitle', 'job_title'],
    'department_number': ['departmentNumber', 'Department Number', 'department_number'],
    'division_name': ['divisionName', 'Division Name', 'division_name'],
    'division_code': ['divisionCode', 'Division Code', 'division_code'],
    'business_unit': ['businessUnit', 'Business Unit', 'business_unit'],
    'location': ['location', 'Location', 'workLocation', 'work_location'],
    'original_hire_date': ['originalHireDate', 'Original Hire Date', 'original_hire_date'],
    'last_hire_date': ['lastHireDate', 'Last Hire Date', 'last_hire_date'],
    'email': ['email', 'Email', 'emailAddress', 'email_address'],
    'phone': ['phone', 'Phone', 'phoneNumber', 'phone_number'],
    'city_of_birth': ['cityofBirth', 'City of Birth', 'city_of_birth'],
    'state_of_birth': ['stateofBirth', 'State of Birth', 'state_of_birth'],
    'county_of_birth': ['countyofBirth', 'County of Birth', 'county_of_birth'],
    'employee_status': ['employeeStatus', 'Employee Status', 'employee_status'],
    'employment_type': ['employmentType', 'Employment Type', 'employment_type'],
    'professional_license_number': ['professionalLicenseNumber1', 'Professional License Number', 'licenseNumber', 'license_number'],
    'professional_license_industry': ['professionalLicenseIndustry1', 'Professional License Industry', 'licenseIndustry', 'license_industry'],
    'professional_license_category': ['professionalLicenseCategory1', 'Professional License Category', 'licenseCategory', 'license_category'],
    'professional_license_speciality': ['professionalLicenseSpeciality1', 'Professional License Speciality', 'licenseSpeciality', 'license_speciality'],
    'professional_license_name': ['professionalLicenseName1', 'Professional License Name', 'licenseName', 'license_name'],
    'professional_license_state': ['professionalLicenseState1', 'Professional License State', 'licenseState', 'license_state'],
    'professional_license_issued_date': ['professionalLicenseIssuedDate1', 'Professional License Issued Date', 'licenseIssuedDate', 'license_issued_date'],
    'professional_license_exp_date': ['professionalLicenseExpDate1', 'Professional License Exp Date', 'licenseExpDate', 'license_exp_date'],
    'driving_license_number': ['drivingLicenseNumber', 'Driving License Number', 'driversLicenseNumber', 'driving_license_number'],
    'driving_license_state': ['drivingLicenseState', 'Driving License State', 'driversLicenseState', 'driving_license_state'],
    'driving_license_issue_date': ['drivingLicenseIssueDate', 'Driving License Issue Date', 'driversLicenseIssueDate', 'driving_license_issue_date'],
    'driving_license_expiry_date': ['drivingLicenseExpiryDate', 'Driving License Expiry Date', 'driversLicenseExpiryDate', 'driving_license_expiry_date'],
    'driving_license_class_code': ['drivingLicenseClassCode', 'Driving License Class Code', 'driversLicenseClassCode', 'driving_license_class_code'],
    'driving_license_restriction_code': ['drivingLicenseRestrictionCode', 'Driving License Restriction Code', 'driversLicenseRestrictionCode', 'driving_license_restriction_code']
}

DEFAULT_CONFIG = {
    "evaluate_name": True,
    "evaluate_license": True,
    "evaluate_exams": True,
    "evaluate_disclosures": True,
    "skip_disciplinary": True,  # Default: Skip disciplinary
    "skip_arbitration": True,   # Default: Skip arbitration
    "skip_regulatory": True     # Default: Skip regulatory
}

DISCIPLINARY_ENABLED = True
ARBITRATION_ENABLED = True

INPUT_FOLDER = "drop"
OUTPUT_FOLDER = "output"
ARCHIVE_FOLDER = "archive"
CHECKPOINT_FILE = os.path.join(OUTPUT_FOLDER, "checkpoint.json")
CONFIG_FILE = "config.json"  # Explicitly defined for clarity

current_csv = None
current_line = 0

def load_config(config_path: str = CONFIG_FILE) -> Dict[str, bool]:
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            return {**DEFAULT_CONFIG, **config}
    except FileNotFoundError:
        logger.warning("Config file not found, using defaults")
        return DEFAULT_CONFIG
    except Exception as e:
        logger.error(f"Error loading config file {config_path}: {str(e)}")
        return DEFAULT_CONFIG

def save_config(config: Dict[str, bool], config_path: str = CONFIG_FILE):
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        logger.info(f"Saved settings to {config_path}: {config}")
    except Exception as e:
        logger.error(f"Error saving config to {config_path}: {str(e)}")

def generate_reference_id(crd_number: str = None) -> str:
    if crd_number and crd_number.strip():
        return crd_number
    return f"DEF-{random.randint(100000000000, 999999999999)}"

def setup_folders():
    for folder in [INPUT_FOLDER, OUTPUT_FOLDER, ARCHIVE_FOLDER]:
        try:
            os.makedirs(folder, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create folder {folder}: {str(e)}")
            raise

def load_checkpoint() -> Optional[Dict[str, Any]]:
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.error(f"Error loading checkpoint: {str(e)}")
        return None

def save_checkpoint(csv_file: str, line_number: int):
    if not csv_file or line_number is None:
        logger.error(f"Cannot save checkpoint: csv_file={csv_file}, line_number={line_number}")
        return
    try:
        checkpoint_path = str(CHECKPOINT_FILE)
        with open(checkpoint_path, 'w') as f:
            json.dump({"csv_file": csv_file, "line": line_number}, f)
        logger.debug(f"Checkpoint saved: {csv_file}, line {line_number}")
    except Exception as e:
        logger.error(f"Error saving checkpoint: {str(e)}")

def signal_handler(sig, frame):
    if current_csv and current_line > 0:
        logger.info(f"Signal received ({signal.Signals(sig).name}), saving checkpoint: {current_csv}, line {current_line}")
        save_checkpoint(current_csv, current_line)
    logger.info("Exiting due to signal")
    exit(0)

def get_csv_files() -> list[str]:
    try:
        files = sorted([f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith('.csv')])
        logger.debug(f"Found CSV files: {files}")
        return files
    except Exception as e:
        logger.error(f"Error listing CSV files in {INPUT_FOLDER}: {str(e)}")
        return []

def archive_file(csv_file_path: str):
    date_str = datetime.now().strftime("%m-%d-%Y")
    archive_subfolder = os.path.join(ARCHIVE_FOLDER, date_str)
    try:
        os.makedirs(archive_subfolder, exist_ok=True)
        dest_path = os.path.join(archive_subfolder, os.path.basename(csv_file_path))
        shutil.move(csv_file_path, dest_path)
        logger.info(f"Archived {csv_file_path} to {dest_path}")
    except Exception as e:
        logger.error(f"Error archiving {csv_file_path}: {str(e)}")

def resolve_headers(fieldnames: list[str]) -> Dict[str, str]:
    resolved_headers = {}
    for header in fieldnames:
        if not header.strip():
            logger.warning("Empty header name encountered")
            continue
        header_lower = header.lower()
        for canonical, variants in canonical_fields.items():
            if header_lower in [variant.lower() for variant in variants]:
                resolved_headers[header] = canonical
                logger.debug(f"Mapped header '{header}' to '{canonical}'")
                break
        else:
            logger.warning(f"Unmapped CSV column: {header}")
    unmapped_canonicals = set(canonical_fields.keys()) - set(resolved_headers.values())
    if unmapped_canonicals:
        logger.debug(f"Canonical fields not found in CSV headers: {unmapped_canonicals}")
    return resolved_headers

def process_csv(csv_file_path: str, start_line: int, facade: FinancialServicesFacade, config: Dict[str, bool], wait_time: float):
    global current_csv, current_line
    current_csv = os.path.basename(csv_file_path)
    current_line = 0
    logger.info(f"Starting to process {csv_file_path} from line {start_line}")

    try:
        with open(csv_file_path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            resolved_headers = resolve_headers(reader.fieldnames)
            logger.debug(f"Resolved headers: {resolved_headers}")

            for i, row in enumerate(reader, start=2):
                if i <= start_line:
                    logger.debug(f"Skipping line {i} (before start_line {start_line})")
                    continue
                logger.debug(f"Processing {current_csv}, line {i}, row: {dict(row)}")
                current_line = i
                try:
                    process_row(row, resolved_headers, facade, config)
                except Exception as e:
                    logger.error(f"Error processing {current_csv}, line {i}: {str(e)}", exc_info=True)
                save_checkpoint(current_csv, current_line)
                time.sleep(wait_time)
    except Exception as e:
        logger.error(f"Error reading {csv_file_path}: {str(e)}", exc_info=True)

def process_row(row: Dict[str, str], resolved_headers: Dict[str, str], facade: FinancialServicesFacade, config: Dict[str, bool]):
    reference_id_header = next((k for k, v in resolved_headers.items() if v == 'reference_id'), 'reference_id')
    reference_id = row.get(reference_id_header, '').strip() or generate_reference_id(row.get(resolved_headers.get('crd_number', 'crd_number'), ''))

    employee_number_header = next((k for k, v in resolved_headers.items() if v == 'employee_number'), 'employee_number')
    employee_number = row.get(employee_number_header, '').strip()
    logger.debug(f"Employee number header: '{employee_number_header}', value: '{employee_number}'")

    if not employee_number:
        logger.error(f"Skipping row - missing or blank employee_number for reference_id='{reference_id}'")
        return

    logger.debug(f"Reference ID: {reference_id}, Employee Number: {employee_number}")

    claim = {}
    for header, canonical in resolved_headers.items():
        value = row.get(header, '').strip()
        claim[canonical] = value
        logger.debug(f"Mapping field - canonical: '{canonical}', header: '{header}', value: '{value}'")
    
    first_name = claim.get('first_name', '')
    last_name = claim.get('last_name', '')
    claim['individual_name'] = f"{first_name} {last_name}".strip() if first_name or last_name else ""
    claim['employee_number'] = employee_number
    
    unmapped_fields = set(row.keys()) - set(resolved_headers.keys())
    if unmapped_fields:
        logger.warning(f"Unmapped fields in row for reference_id='{reference_id}': {unmapped_fields}")
    
    logger.debug(f"Claim built: {claim}")

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
        logger.debug(f"Raw report from process_claim: {json.dumps(report, indent=2)}")
        
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

    time.sleep(7)

def save_evaluation_report(report: Dict[str, Any], employee_number: str, reference_id: str):
    report_path = os.path.join(OUTPUT_FOLDER, f"{reference_id}.json")
    logger.debug(f"Saving report to {report_path}")
    try:
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        compliance = report.get('final_evaluation', {}).get('overall_compliance', False)
        logger.info(f"Processed {reference_id}, overall_compliance: {compliance}")
    except Exception as e:
        logger.error(f"Error saving report to {report_path}: {str(e)}", exc_info=True)

def run_batch_processing(facade: FinancialServicesFacade, config: Dict[str, bool], wait_time: float):
    print("\nRunning batch processing...")
    checkpoint = load_checkpoint()
    csv_files = get_csv_files()
    if not csv_files:
        logger.warning(f"No CSV files found in {INPUT_FOLDER}")
        print(f"No CSV files found in {INPUT_FOLDER}")
        return

    start_file = checkpoint["csv_file"] if checkpoint else None
    start_line = checkpoint["line"] if checkpoint else 0

    processed_files = 0
    processed_records = 0

    for csv_file in csv_files:
        csv_path = os.path.join(INPUT_FOLDER, csv_file)
        if start_file and csv_file < start_file:
            logger.debug(f"Skipping {csv_file} - before start_file {start_file}")
            continue
        logger.info(f"Processing {csv_path} from line {start_line}")
        process_csv(csv_path, start_line, facade, config, wait_time)
        try:
            with open(csv_path, 'r') as f:
                processed_records += sum(1 for _ in csv.reader(f) if _.strip()) - 1
        except Exception as e:
            logger.error(f"Error counting records in {csv_path}: {str(e)}", exc_info=True)
        archive_file(csv_path)
        processed_files += 1
        start_line = 0

    logger.info(f"Processed {processed_files} files, {processed_records} records")
    if os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
            logger.debug(f"Removed checkpoint file: {CHECKPOINT_FILE}")
        except Exception as e:
            logger.error(f"Error removing checkpoint file {CHECKPOINT_FILE}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Compliance CSV Processor")
    parser.add_argument('--diagnostic', action='store_true', help="Enable verbose debug logging")
    parser.add_argument('--wait-time', type=float, default=7.0, help="Seconds to wait between API calls")
    parser.add_argument('--skip-disciplinary', action='store_true', help="Skip disciplinary review for all claims")
    parser.add_argument('--skip-arbitration', action='store_true', help="Skip arbitration review for all claims")
    parser.add_argument('--skip-regulatory', action='store_true', help="Skip regulatory review for all claims")
    parser.add_argument('--headless', action='store_true', help="Run in headless mode with specified settings")
    args = parser.parse_args()

    loggers = setup_logging(args.diagnostic)
    global logger
    logger = loggers['main']

    logger.info("=== Starting application ===")
    logger.debug("Debug logging is enabled" if args.diagnostic else "Debug logging is disabled")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    setup_folders()

    try:
        facade = FinancialServicesFacade()
    except Exception as e:
        logger.error(f"Failed to initialize FinancialServicesFacade: {str(e)}", exc_info=True)
        return

    if args.headless:
        config = {
            "evaluate_name": True,
            "evaluate_license": True,
            "evaluate_exams": True,
            "evaluate_disclosures": True,
            "skip_disciplinary": args.skip_disciplinary,
            "skip_arbitration": args.skip_arbitration,
            "skip_regulatory": args.skip_regulatory
        }
        if not (args.skip_disciplinary or args.skip_arbitration or args.skip_regulatory):
            config.update(load_config())
        run_batch_processing(facade, config, args.wait_time)
        return

    skip_disciplinary = True
    skip_arbitration = True
    skip_regulatory = True

    while True:
        print("\nCompliance CSV Processor Menu:")
        print("1. Run batch processing")
        print(f"2. Toggle disciplinary review (currently: {'skipped' if skip_disciplinary else 'enabled'})")
        print(f"3. Toggle arbitration review (currently: {'skipped' if skip_arbitration else 'enabled'})")
        print(f"4. Toggle regulatory review (currently: {'skipped' if skip_regulatory else 'enabled'})")
        print("5. Save settings")
        print("6. Exit")
        choice = input("Enter your choice (1-6): ").strip()

        if choice == "1":
            config = {
                "evaluate_name": True,
                "evaluate_license": True,
                "evaluate_exams": True,
                "evaluate_disclosures": True,
                "skip_disciplinary": skip_disciplinary,
                "skip_arbitration": skip_arbitration,
                "skip_regulatory": skip_regulatory
            }
            logger.info(f"Running batch with config: {config}")
            run_batch_processing(facade, config, args.wait_time)
        elif choice == "2":
            skip_disciplinary = not skip_disciplinary
            logger.info(f"Disciplinary review {'skipped' if skip_disciplinary else 'enabled'}")
            print(f"Disciplinary review is now {'skipped' if skip_disciplinary else 'enabled'}")
        elif choice == "3":
            skip_arbitration = not skip_arbitration
            logger.info(f"Arbitration review {'skipped' if skip_arbitration else 'enabled'}")
            print(f"Arbitration review is now {'skipped' if skip_arbitration else 'enabled'}")
        elif choice == "4":
            skip_regulatory = not skip_regulatory
            logger.info(f"Regulatory review {'skipped' if skip_regulatory else 'enabled'}")
            print(f"Regulatory review is now {'skipped' if skip_regulatory else 'enabled'}")
        elif choice == "5":
            config = {
                "evaluate_name": True,
                "evaluate_license": True,
                "evaluate_exams": True,
                "evaluate_disclosures": True,
                "skip_disciplinary": skip_disciplinary,
                "skip_arbitration": skip_arbitration,
                "skip_regulatory": skip_regulatory
            }
            save_config(config)
            print(f"Settings saved to {CONFIG_FILE}")
        elif choice == "6":
            logger.info("User chose to exit")
            print("Exiting...")
            break
        else:
            logger.warning(f"Invalid menu choice: {choice}")
            print("Invalid choice. Please enter 1-6.")

if __name__ == "__main__":
    main()