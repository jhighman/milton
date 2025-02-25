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

# Setup logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.FileHandler('logs/app.log'), logging.StreamHandler()]
)
logger = logging.getLogger('main')

# Canonical field mappings (expanded with organization_crd)
canonical_fields = {
    'reference_id': ['referenceId', 'Reference ID', 'reference_id', 'ref_id', 'RefID'],
    'crd_number': ['CRD', 'crd_number', 'crd', 'CRD Number', 'CRDNumber', 'crdnumber'],
    'first_name': ['firstName', 'First Name', 'first_name', 'fname', 'FirstName', 'first'],
    'middle_name': ['middle_name', 'Middle Name', 'middlename', 'MiddleName', 'middle', 'middleName'],
    'last_name': ['lastName', 'Last Name', 'last_name', 'lname', 'LastName', 'last'],
    'employee_number': ['employeeNumber', 'Employee Number', 'employee_number', 'emp_id', 'employeenumber'],
    'license_type': ['license_type', 'License Type', 'licensetype', 'LicenseType', 'license'],
    'organization_name': ['orgName', 'Organization Name', 'organization_name', 'firm_name', 'organizationname', 'OrganizationName', 'organization'],
    'organization_crd': ['orgCRD', 'Organization CRD', 'org_crd_number', 'firm_crd', 'organizationCRD', 'organization_crd_number', 'organization_crd'],
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

# Configurable evaluation flags
DEFAULT_CONFIG = {
    "evaluate_name": True,
    "evaluate_license": True,
    "evaluate_exams": True,
    "evaluate_disclosures": True
}

# Local flags for additional checks
DISCIPLINARY_ENABLED = True
ARBITRATION_ENABLED = True

# Folder paths
INPUT_FOLDER = "drop"
OUTPUT_FOLDER = "output"
ARCHIVE_FOLDER = "archive"
CHECKPOINT_FILE = os.path.join(OUTPUT_FOLDER, "checkpoint.json")

# Global state for signal handling
current_csv = None
current_line = 0

def load_config(config_path: str = "config.json") -> Dict[str, bool]:
    """Load configuration from config.json or use defaults."""
    try:
        with open(config_path, 'r') as f:
            return {**DEFAULT_CONFIG, **json.load(f)}
    except FileNotFoundError:
        logger.warning("Config file not found, using defaults")
        return DEFAULT_CONFIG

def generate_reference_id(crd_number: str = None) -> str:
    """Generate a unique reference ID, using CRD if provided."""
    if crd_number and crd_number.strip():
        return crd_number
    return f"DEF-{random.randint(100000000000, 999999999999)}"

def setup_folders():
    """Ensure all required folders exist."""
    for folder in [INPUT_FOLDER, OUTPUT_FOLDER, ARCHIVE_FOLDER]:
        os.makedirs(folder, exist_ok=True)

def load_checkpoint() -> Optional[Dict[str, Any]]:
    """Load the last processed file and line from checkpoint.json."""
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def save_checkpoint(csv_file: str, line_number: int):
    """Save the current file and line to checkpoint.json."""
    if csv_file is None or line_number is None:
        logger.error(f"Cannot save checkpoint: csv_file={csv_file}, line_number={line_number}")
        return
    checkpoint_path = str(CHECKPOINT_FILE)
    with open(checkpoint_path, 'w') as f:
        json.dump({"csv_file": csv_file, "line": line_number}, f)
    logger.debug(f"Checkpoint saved: {csv_file}, line {line_number}")

def signal_handler(sig, frame):
    """Handle SIGINT/SIGTERM by saving checkpoint and exiting."""
    if current_csv and current_line > 0:
        logger.info(f"Signal received, saving checkpoint: {current_csv}, line {current_line}")
        save_checkpoint(current_csv, current_line)
    exit(0)

def get_csv_files() -> list[str]:
    """List all CSV files in the drop folder."""
    files = sorted([f for f in os.listdir(INPUT_FOLDER) if f.endswith('.csv')])
    logger.debug(f"Found CSV files: {files}")
    return files

def archive_file(csv_file_path: str):
    """Move processed CSV to archive with date-based subfolder."""
    date_str = datetime.now().strftime("%m-%d-%Y")
    archive_subfolder = os.path.join(ARCHIVE_FOLDER, date_str)
    os.makedirs(archive_subfolder, exist_ok=True)
    dest_path = os.path.join(archive_subfolder, os.path.basename(csv_file_path))
    shutil.move(csv_file_path, dest_path)
    logger.info(f"Archived {csv_file_path} to {dest_path}")

def resolve_headers(fieldnames: list[str]) -> Dict[str, str]:
    """Map CSV headers to canonical fields."""
    resolved_headers = {}
    for header in fieldnames:
        for canonical, variants in canonical_fields.items():
            if header in variants:
                resolved_headers[header] = canonical
                logger.debug(f"Mapped header '{header}' to '{canonical}'")
                break
        else:
            logger.warning(f"Unmapped CSV column: {header}")
    for canonical in canonical_fields:
        if canonical not in resolved_headers.values():
            logger.debug(f"Canonical field '{canonical}' not found in CSV headers")
    return resolved_headers

def process_csv(csv_file_path: str, start_line: int, facade: FinancialServicesFacade, config: Dict[str, bool], wait_time: float):
    """Process a CSV file starting from the given line."""
    global current_csv, current_line
    current_csv = os.path.basename(csv_file_path)
    current_line = 0
    logger.info(f"Starting to process {csv_file_path} from line {start_line}")

    with open(csv_file_path, 'r') as f:
        reader = csv.DictReader(f)
        resolved_headers = resolve_headers(reader.fieldnames)
        logger.debug(f"Resolved headers: {resolved_headers}")

        for i, row in enumerate(reader, start=2):
            if i <= start_line:
                logger.debug(f"Skipping line {i} (before start_line {start_line})")
                continue
            logger.debug(f"Processing {current_csv}, line {i}, row: {row}")
            current_line = i
            try:
                process_row(row, resolved_headers, facade, config)
            except Exception as e:
                logger.error(f"Error processing {current_csv}, line {i}: {str(e)}")
            save_checkpoint(current_csv, current_line)
            time.sleep(wait_time)

def process_row(row: Dict[str, str], resolved_headers: Dict[str, str], facade: FinancialServicesFacade, config: Dict[str, bool]):
    """Process a single CSV row and save the evaluation report from process_claim."""
    logger.debug(f"Processing row: {row}")
    reference_id_key = resolved_headers.get('reference_id', 'reference_id')
    reference_id = row.get(reference_id_key, '').strip() or generate_reference_id(row.get(resolved_headers.get('crd_number', 'crd_number'), ''))

    employee_number_key = resolved_headers.get('employee_number', 'employee_number')
    employee_number = row.get(employee_number_key, '').strip()
    if not employee_number:
        logger.error(f"Skipping row - missing or blank employee_number for reference_id='{reference_id}'")
        return

    logger.debug(f"Reference ID: {reference_id}, Employee Number: {employee_number}")

    # Build claim dictionary with expanded fields
    claim = {}
    for header, canonical in resolved_headers.items():
        claim[canonical] = row.get(header, '').strip()
    first_name = claim.get('first_name', '')
    last_name = claim.get('last_name', '')
    claim['individual_name'] = f"{first_name} {last_name}".strip() if first_name or last_name else ""
    claim['employee_number'] = employee_number
    logger.debug(f"Claim built: {claim}")

    # Process claim and save the report directly
    try:
        report = process_claim(claim, facade, employee_number)
        logger.debug(f"Raw report from process_claim: {json.dumps(report, indent=2)}")
        if report is None:
            logger.error(f"process_claim returned None for reference_id='{reference_id}'")
            return
        save_evaluation_report(report, employee_number, reference_id)
    except Exception as e:
        logger.error(f"Error processing claim for reference_id='{reference_id}': {str(e)}")
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

    # Throttle with a 7-second delay after processing each row
    time.sleep(7)

def save_evaluation_report(report: Dict[str, Any], employee_number: str, reference_id: str):
    """Save the evaluation report as a JSON file."""
    report_path = os.path.join(OUTPUT_FOLDER, f"{reference_id}.json")
    logger.debug(f"Saving report to {report_path}")
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    compliance = report.get('final_evaluation', {}).get('overall_compliance', False)
    logger.info(f"Processed {reference_id}, overall_compliance: {compliance}")

def main():
    parser = argparse.ArgumentParser(description="Compliance CSV Processor")
    parser.add_argument('--diagnostic', action='store_true', help="Enable verbose debug logging")
    parser.add_argument('--wait-time', type=float, default=7.0, help="Seconds to wait between API calls")
    args = parser.parse_args()

    if args.diagnostic:
        logger.setLevel(logging.DEBUG)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    setup_folders()
    config = load_config()
    facade = FinancialServicesFacade()

    print("\nCompliance CSV Processor Menu:")
    print("1. Run batch processing")
    print("2. Exit")
    choice = input("Enter your choice (1-2): ").strip()

    if choice == "1":
        print("\nRunning batch processing...")
        checkpoint = load_checkpoint()
        csv_files = get_csv_files()
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
            process_csv(csv_path, start_line, facade, config, args.wait_time)
            with open(csv_path, 'r') as f:
                processed_records += sum(1 for _ in csv.reader(f)) - 1  # Minus header
            archive_file(csv_path)
            processed_files += 1
            start_line = 0

        logger.info(f"Processed {processed_files} files, {processed_records} records")
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
    elif choice == "2":
        print("Exiting...")
    else:
        print("Invalid choice. Please enter 1 or 2.")

if __name__ == "__main__":
    main()