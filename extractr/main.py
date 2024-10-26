import os
import csv
import json
import time
import shutil
import logging
import argparse
import signal
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional

# Import the ApiClient from api_client.py
from api_client import ApiClient, RateLimitExceeded

from evaluation_library import (
    evaluate_name,
    evaluate_license,
    evaluate_exams,
    evaluate_registration_status,
    evaluate_disclosures,
    get_passed_exams,
    Alert,
    AlertSeverity,
)

# Define folder paths
folder_path = './'
input_folder = os.path.join(folder_path, 'drop')
output_folder = os.path.join(folder_path, 'output')
archive_folder = os.path.join(folder_path, 'archive')
cache_folder = os.path.join(folder_path, 'cache')
checkpoint_file = os.path.join(output_folder, 'checkpoint.json')
log_file_path = os.path.join(output_folder, 'unresolved_crd_cases.csv')  # CSV for unresolved cases

# Initialize counters
files_processed = 0
records_written = 0
current_csv_file = None
last_processed_line = -1

# Canonical field mappings for CSV header flexibility
canonical_fields = {
    'crd': ['CRD', 'crd', 'CRDNumber', 'crd_number'],
    'first_name': ['firstName', 'first_name', 'FirstName'],
    'middle_name': ['middleName', 'middle_name', 'MiddleName'],
    'last_name': ['lastName', 'last_name', 'LastName'],
    'suffix': ['suffix', 'Suffix'],
    'ssn': ['ssn', 'SSN'],
    'dob': ['dob', 'DOB', 'dateOfBirth'],
    'gender': ['gender', 'Gender'],
    'address_line1': ['addressLine1', 'address1', 'Address1'],
    'address_line2': ['addressLine2', 'address2', 'Address2'],
    'city': ['city', 'City'],
    'county': ['county', 'County'],
    'state': ['state', 'State'],
    'zip': ['zip', 'ZipCode', 'PostalCode'],
    'country': ['country', 'Country'],
    'email': ['email', 'EmailAddress'],
    'phone': ['phone', 'PhoneNumber'],
    'employee_number': ['employeeNumber', 'employeeID', 'EmployeeID'],
    'role': ['role', 'Role'],
    'title': ['title', 'Title'],
    'department_number': ['departmentNumber', 'DeptNumber', 'DepartmentID'],
    'division_name': ['divisionName', 'Division'],
    'division_code': ['divisionCode', 'DivisionCode'],
    'business_unit': ['businessUnit', 'BusinessUnit'],
    'location': ['location', 'Location'],
    'original_hire_date': ['originalHireDate', 'OriginalHireDate'],
    'last_hire_date': ['lastHireDate', 'LastHireDate'],
    'employee_status': ['employeeStatus', 'EmploymentStatus'],
    'employment_type': ['employmentType', 'EmploymentType'],
    'organization_name': ['organizationName', 'OrgName', 'Organization'],
    'professional_license_number1': ['professionalLicenseNumber1', 'LicenseNumber'],
    'professional_license_industry1': ['professionalLicenseIndustry1', 'LicenseIndustry'],
    'professional_license_category1': ['professionalLicenseCategory1', 'LicenseCategory'],
    'professional_license_speciality1': ['professionalLicenseSpeciality1', 'LicenseSpeciality'],
    'professional_license_name1': ['professionalLicenseName1', 'LicenseName'],
    'professional_license_state1': ['professionalLicenseState1', 'LicenseState'],
    'professional_license_issued_date1': ['professionalLicenseIssuedDate1', 'LicenseIssuedDate'],
    'professional_license_exp_date1': ['professionalLicenseExpDate1', 'LicenseExpDate'],
    'driving_license_number': ['drivingLicenseNumber', 'DriverLicenseNumber'],
    'driving_license_state': ['drivingLicenseState', 'DriverLicenseState'],
    'driving_license_issue_date': ['drivingLicenseIssueDate', 'DriverLicenseIssuedDate'],
    'driving_license_expiry_date': ['drivingLicenseExpiryDate', 'DriverLicenseExpDate'],
    'driving_license_class_code': ['drivingLicenseClassCode', 'DriverLicenseClass'],
    'driving_license_restriction_code': ['drivingLicenseRestrictionCode', 'LicenseRestrictionCode'],
    'city_of_birth': ['cityofBirth', 'BirthCity'],
    'state_of_birth': ['stateofBirth', 'BirthState'],
    'county_of_birth': ['countyofBirth', 'BirthCounty']
}

# Load configuration file
def load_config():
    with open('config.json') as config_file:
        return json.load(config_file)

config = load_config()

# Resolve headers to canonical model
def resolve_headers(headers):
    """Map CSV headers to a canonical model based on predefined aliases."""
    resolved_fields = {}
    for canon, aliases in canonical_fields.items():
        for alias in aliases:
            normalized_alias = alias.lstrip('\ufeff')  # Remove BOM if present
            if normalized_alias in headers:
                resolved_fields[canon] = normalized_alias
                break
    # Log any missing mappings for easier debugging
    missing_mappings = [canon for canon in canonical_fields if canon not in resolved_fields]
    if missing_mappings:
        logging.warning(f"Unmapped canonical fields: {missing_mappings}")
    return resolved_fields


# Parse command-line arguments
parser = argparse.ArgumentParser(description='Evaluation Framework')
parser.add_argument('--diagnostic', action='store_true', help='Enable diagnostic mode')
parser.add_argument('--wait-time', type=int, default=7, help='Wait time between requests in seconds (default: 7)')
args = parser.parse_args()

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_diagnostic(message):
    """Log diagnostic information if diagnostic mode is enabled."""
    if args.diagnostic:
        logging.info(f"[DIAGNOSTIC] {message}")

# Save checkpoint
def save_checkpoint():
    """Save the current processing state to a checkpoint file."""
    checkpoint_data = {
        'current_csv_file': current_csv_file,
        'last_processed_line': last_processed_line
    }
    temp_checkpoint_file = checkpoint_file + '.tmp'
    with open(temp_checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f)
    os.replace(temp_checkpoint_file, checkpoint_file)
    log_diagnostic("Checkpoint saved.")

# Load checkpoint
def load_checkpoint():
    """Load the last saved processing state from the checkpoint file."""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            checkpoint_data = json.load(f)
        log_diagnostic("Checkpoint loaded.")
        return checkpoint_data.get('current_csv_file'), checkpoint_data.get('last_processed_line')
    else:
        return None, -1

# Signal handling for safe exit
def signal_handler(sig, frame):
    """Handle interrupts to save the checkpoint and exit gracefully."""
    logging.info('Interrupt received, saving checkpoint...')
    save_checkpoint()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Initialize API client
logger = logging.getLogger(__name__)
api_client = ApiClient(cache_folder=cache_folder, wait_time=args.wait_time, logger=logger)

# Log unresolved CRD cases to a CSV file
def log_unresolved_crd(row, resolved_fields):
    """Log unresolved CRD cases to a CSV file using canonical headers."""
    unresolved_file = log_file_path
    write_headers = not os.path.exists(unresolved_file)  # Check if headers are needed
    unresolved_data = {field: row.get(resolved_fields.get(field, ''), '') for field in canonical_fields.keys()}
    with open(unresolved_file, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=list(canonical_fields.keys()))
        if write_headers:
            writer.writeheader()
        writer.writerow(unresolved_data)

# Process each CSV file
def process_csv(csv_file_path, start_line):
    """Process each CSV file, iterating through rows and handling checkpoints."""
    global last_processed_line, current_csv_file
    current_csv_file = os.path.basename(csv_file_path)
    last_processed_line = start_line
    with open(csv_file_path, 'r', encoding='utf-8-sig') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        resolved_fields = resolve_headers(csv_reader.fieldnames)
        for index, row in enumerate(csv_reader):
            if index <= last_processed_line:
                continue
            process_row(row, resolved_fields)
            save_checkpoint()

def process_row(row, resolved_fields):
    """Process a single row of data, performing validation and evaluation tasks."""
    global records_written, last_processed_line

    # Retrieve and validate CRD value
    crd_value = row.get(resolved_fields.get('crd', ''), None)
    if crd_value and crd_value.isdigit() and int(crd_value) > 0:
        crd_number = int(crd_value)
        first_name = row.get(resolved_fields['first_name'], '').strip()
        last_name = row.get(resolved_fields['last_name'], '').strip()
        name = f"{first_name} {last_name}"
        license_type = row.get(resolved_fields.get('license_type', ''), '')

        # Initialize the evaluation report with employee_number first, then crd_number
        evaluation_report = {}
        
        # Add employee_number if it exists
        employee_number = row.get(resolved_fields.get('employee_number', ''), '').strip()
        if employee_number:
            evaluation_report['employee_number'] = employee_number

        # Add remaining fields
        evaluation_report['crd_number'] = crd_number
        evaluation_report['data_source'] = None  # To be set after data source determination
        evaluation_report['name'] = {
            'expected_name': name,
        }

        log_diagnostic(f"Processing CRD {crd_number}")

        # Attempt to retrieve individual information from BrokerCheck and SEC
        try:
            basic_info = api_client.get_individual_basic_info(crd_number)
            detailed_info = api_client.get_individual_detailed_info(crd_number)
            basic_info_sec = api_client.get_individual_basic_info_from_sec(crd_number)
            detailed_info_sec = api_client.get_individual_detailed_info_from_sec(crd_number)
        except RateLimitExceeded as e:
            logging.error(str(e))
            logging.info(f"Processed {records_written} records before rate limiting.")
            save_checkpoint()
            sys.exit(1)

        # Determine data source based on availability
        if basic_info and basic_info.get('hits', {}).get('hits', []):
            selected_basic_info = basic_info
            selected_detailed_info = detailed_info
            evaluation_report['data_source'] = "BrokerCheck"
        elif basic_info_sec and basic_info_sec.get('hits', {}).get('hits', []):
            selected_basic_info = basic_info_sec
            selected_detailed_info = detailed_info_sec
            evaluation_report['data_source'] = "SEC"
        else:
            logging.warning(f"No data available from either BrokerCheck or SEC for CRD {crd_number}.")
            log_unresolved_crd(row, resolved_fields)
            return  # Skip this record if no data available

        # Extract details from selected data source
        individual = selected_basic_info['hits']['hits'][0]['_source']
        fetched_name = f"{individual.get('ind_firstname', '')} {individual.get('ind_middlename', '')} {individual.get('ind_lastname', '')}".strip()
        other_names = individual.get('ind_other_names', [])
        bc_scope = individual.get('ind_bc_scope', '')
        ia_scope = individual.get('ind_ia_scope', '')

        alerts = []

        # Name Evaluation
        if config.get('evaluate_name', True):
            name_match, name_alert = evaluate_name(name, fetched_name, other_names)
            evaluation_report['name'].update({
                'fetched_name': fetched_name,
                'name_match': name_match,
                'name_match_explanation': "" if name_match else "Expected name did not match fetched name."
            })
            if name_alert:
                alerts.append(name_alert)
            log_diagnostic(f"Name evaluation: {'PASSED' if name_match else 'FAILED'}")
        else:
            evaluation_report['name'] = {'evaluation_skipped': True}
            log_diagnostic("Name evaluation skipped as per configuration.")

        # License Compliance Evaluation
        if config.get('evaluate_license', True):
            license_compliant, license_alert = evaluate_license(license_type, bc_scope, ia_scope, name)
            evaluation_report['license_verification'] = {
                'license_compliance': license_compliant,
                'license_compliance_explanation': "The individual holds an active license." if license_compliant else "License compliance failed."
            }
            if license_alert:
                alerts.append(license_alert)
            log_diagnostic(f"License evaluation: {'PASSED' if license_compliant else 'FAILED'}")
        else:
            evaluation_report['license_verification'] = {'evaluation_skipped': True}
            log_diagnostic("License evaluation skipped as per configuration.")

        # Registration Status Evaluation
        if config.get('evaluate_registration_status', True):
            status_ok, status_alerts = evaluate_registration_status(individual)
            evaluation_report['registration_status'] = {
                'status_alerts': not status_ok,
                'status_summary': "Active registration found." if status_ok else "Registration status is concerning."
            }
            alerts.extend(status_alerts)
            log_diagnostic(f"Registration status evaluation: {'PASSED' if status_ok else 'FAILED'}")
        else:
            evaluation_report['registration_status'] = {'evaluation_skipped': True}
            log_diagnostic("Registration status evaluation skipped as per configuration.")

        # Exam Compliance Evaluation
        if config.get('evaluate_exams', True):
            detailed_content = selected_detailed_info['hits']['hits'][0]['_source'].get('content', '{}')
            detailed_data = json.loads(detailed_content)
            exams = detailed_data.get('stateExamCategory', []) + detailed_data.get('productExamCategory', [])
            passed_exams = get_passed_exams(exams)
            exam_compliant, exam_alert = evaluate_exams(passed_exams, license_type, name)
            evaluation_report['exam_evaluation'] = {
                'exam_compliance': exam_compliant,
                'exam_compliance_explanation': "The individual has passed all required exams." if exam_compliant else "Exam compliance failed."
            }
            if exam_alert:
                alerts.append(exam_alert)
            log_diagnostic(f"Exam evaluation: {'PASSED' if exam_compliant else 'FAILED'}")
        else:
            evaluation_report['exam_evaluation'] = {'evaluation_skipped': True}
            log_diagnostic("Exam evaluation skipped as per configuration.")

        # Disclosures Review
        if config.get('evaluate_disclosures', True):
            disclosures = detailed_data.get('disclosures', [])
            disclosure_alerts, disclosure_summary = evaluate_disclosures(disclosures, name)
            evaluation_report['disclosure_review'] = {
                'disclosure_alerts': bool(disclosure_alerts),
                'disclosure_review_summary': disclosure_summary if disclosure_summary else "No disclosures found."
            }
            alerts.extend(disclosure_alerts)
            log_diagnostic(f"Disclosure evaluation: {'FOUND' if disclosure_alerts else 'NONE'}")
        else:
            evaluation_report['disclosure_review'] = {'evaluation_skipped': True}
            log_diagnostic("Disclosure evaluation skipped as per configuration.")

        # Final Evaluation Summary
        evaluations_performed = []
        if config.get('evaluate_name', True):
            evaluations_performed.append(evaluation_report['name']['name_match'])
        if config.get('evaluate_license', True):
            evaluations_performed.append(evaluation_report['license_verification']['license_compliance'])
        if config.get('evaluate_exams', True):
            evaluations_performed.append(evaluation_report['exam_evaluation']['exam_compliance'])
        if config.get('evaluate_registration_status', True):
            evaluations_performed.append(not evaluation_report['registration_status']['status_alerts'])

        overall_compliance = all(evaluations_performed)

        # Determine overall risk level based on alerts
        if any(alert.severity == AlertSeverity.HIGH for alert in alerts):
            overall_risk_level = "High"
        elif any(alert.severity == AlertSeverity.MEDIUM for alert in alerts):
            overall_risk_level = "Medium"
        else:
            overall_risk_level = "Low"

        # Recommendations based on risk level
        if overall_risk_level == "High":
            recommendations = "Immediate action required due to critical compliance issues."
        elif overall_risk_level == "Medium":
            recommendations = "Further review recommended due to potential compliance issues."
        else:
            recommendations = "No action needed."

        evaluation_report['final_evaluation'] = {
            'overall_compliance': overall_compliance,
            'overall_risk_level': overall_risk_level,
            'recommendations': recommendations,
            'alerts': [alert.to_dict() for alert in alerts]
        }

        # Save evaluation report to a JSON file
        os.makedirs(output_folder, exist_ok=True)
        output_file_path = os.path.join(output_folder, f"{crd_number}.json")
        with open(output_file_path, 'w') as json_file:
            json.dump(evaluation_report, json_file, indent=2)

        # Increment counters and update checkpoint
        records_written += 1
        last_processed_line += 1
    else:
        # Log unresolved CRD cases if validation fails
        log_unresolved_crd(row, resolved_fields)


# Main function to manage file processing and archiving
def main():
    """Main entry point for handling CSV processing, checkpointing, and archiving."""
    global files_processed, last_processed_line, current_csv_file

    # Ensure required directories exist
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(archive_folder, exist_ok=True)
    os.makedirs(input_folder, exist_ok=True)
    os.makedirs(cache_folder, exist_ok=True)

    # Load checkpoint if available
    checkpoint_csv_file, checkpoint_line = load_checkpoint()

    # Gather CSV files in the input folder
    csv_files = sorted([f for f in os.listdir(input_folder) if f.endswith('.csv')])

    if not csv_files:
        logging.info("No CSV files found in the input folder.")
        return

    # Adjust for checkpoint if resuming
    if checkpoint_csv_file and checkpoint_csv_file in csv_files:
        csv_files = csv_files[csv_files.index(checkpoint_csv_file):]
        last_processed_line = checkpoint_line
    else:
        last_processed_line = -1

    # Process each CSV file
    for csv_file in csv_files:
        current_csv_file = csv_file
        csv_file_path = os.path.join(input_folder, csv_file)
        process_csv(csv_file_path, last_processed_line)
        files_processed += 1
        last_processed_line = -1  # Reset for next file

        # Archive processed files
        archive_subfolder = os.path.join(archive_folder, datetime.now().strftime("%m-%d-%Y"))
        os.makedirs(archive_subfolder, exist_ok=True)
        shutil.move(csv_file_path, os.path.join(archive_subfolder, csv_file))

    # Completion log
    logging.info(f"Processing complete! Files processed: {files_processed}, Records written: {records_written}")

if __name__ == "__main__":
    main()
