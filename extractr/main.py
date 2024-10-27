# main.py

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

# Import evaluation functions and classes from evaluation_library.py
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
    'crd': ['crd', 'CRD', 'CRDNumber', 'crd_number', 'crdnumber', 'CRD Number'],
    'first_name': ['first_name', 'First Name', 'firstname', 'FirstName', 'first'],
    'last_name': ['last_name', 'Last Name', 'lastname', 'LastName', 'last'],
    'middle_name': ['middle_name', 'Middle Name', 'middlename', 'MiddleName', 'middle'],
    'license_type': ['license_type', 'License Type', 'licensetype', 'LicenseType', 'license'],
    'employee_number': ['employee_number', 'Employee Number', 'employeenumber', 'EmployeeNumber'],
    # Add other canonical fields and their variations as needed
}

# Load configuration file
def load_config():
    with open('config.json') as config_file:
        return json.load(config_file)

config = load_config()

# Resolve headers to canonical model
def resolve_headers(headers):
    resolved_headers = {}
    unmapped_canonical_fields = set(canonical_fields.keys())
    header_map = {header.lower().strip(): header for header in headers}  # Map lowercase headers to original headers
    for canonical, variations in canonical_fields.items():
        for variation in variations:
            variation_lower = variation.lower().strip()
            if variation_lower in header_map:
                resolved_headers[canonical] = header_map[variation_lower]
                unmapped_canonical_fields.discard(canonical)
                break
    if unmapped_canonical_fields:
        logging.warning(f"Unmapped canonical fields: {unmapped_canonical_fields}")
    return resolved_headers

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Evaluation Framework')
parser.add_argument('--diagnostic', action='store_true', help='Enable diagnostic mode')
parser.add_argument('--wait-time', type=int, default=7, help='Wait time between requests in seconds (default: 7)')
args = parser.parse_args()

# Logging setup
if args.diagnostic:
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_diagnostic(message):
    """Log diagnostic information if diagnostic mode is enabled."""
    logging.debug(message)

# Save checkpoint
def save_checkpoint():
    """Save the current processing state to a checkpoint file."""
    checkpoint_data = {
        'csv_file': current_csv_file,
        'line': last_processed_line
    }
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f)
    log_diagnostic(f"Checkpoint saved: {checkpoint_data}")

# Load checkpoint
def load_checkpoint():
    """Load the last saved processing state from the checkpoint file."""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            checkpoint_data = json.load(f)
        log_diagnostic(f"Checkpoint loaded: {checkpoint_data}")
        return checkpoint_data.get('csv_file'), checkpoint_data.get('line', -1)
    return None, -1  # Return default values if no checkpoint exists

# Signal handling for safe exit
def signal_handler(sig, frame):
    """Handle interrupts to save the checkpoint and exit gracefully."""
    logging.info("Interrupt received. Saving checkpoint and exiting...")
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
    os.makedirs(output_folder, exist_ok=True)
    file_exists = os.path.isfile(log_file_path)
    with open(log_file_path, 'a', newline='', encoding='utf-8-sig') as csvfile:
        fieldnames = resolved_fields.values()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({field: row.get(field, '') for field in fieldnames})

# Process each CSV file
def process_csv(csv_file_path, start_line):
    """Process each CSV file, iterating through rows and handling checkpoints."""
    global last_processed_line, current_csv_file
    current_csv_file = os.path.basename(csv_file_path)
    last_processed_line = start_line
    with open(csv_file_path, 'r', encoding='utf-8-sig') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        resolved_headers = resolve_headers(csv_reader.fieldnames)
        for index, row in enumerate(csv_reader):
            if index <= last_processed_line:
                continue
            log_diagnostic(f"Processing line {index} in file {current_csv_file}")
            process_row(row, resolved_headers)
            last_processed_line = index
            save_checkpoint()

def process_row(row, resolved_fields):
    """Process a single row of data, performing validation and evaluation tasks."""
    global records_written, last_processed_line

    # Retrieve and validate CRD value
    crd_value = row.get(resolved_fields.get('crd', ''), '').strip()
    if crd_value and crd_value.isdigit() and int(crd_value) > 0:
        crd_number = int(crd_value)
        first_name = row.get(resolved_fields.get('first_name', ''), '').strip()
        last_name = row.get(resolved_fields.get('last_name', ''), '').strip()
        name = f"{first_name} {last_name}".strip()
        license_type = row.get(resolved_fields.get('license_type', ''), '').strip()

        # Initialize the evaluation report
        evaluation_report = {}

        # Add employee_number if it exists
        employee_number = row.get(resolved_fields.get('employee_number', ''), '').strip()
        if employee_number:
            evaluation_report['employee_number'] = employee_number

        # Add crd_number
        evaluation_report['crd_number'] = crd_number

        # Populate the "claim" object with all canonical fields
        claim = {}
        for canonical_field in canonical_fields:
            value = row.get(resolved_fields.get(canonical_field, ''), '').strip()
            if value:
                claim[canonical_field] = value
        evaluation_report['claim'] = claim  # Add the "claim" object to the report

        # Add remaining fields to evaluation_report
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
            status_compliant, status_alerts = evaluate_registration_status(individual)
            evaluation_report['registration_status'] = {
                'status_compliance': status_compliant,
                'status_compliance_explanation': "Registration status is active and acceptable." if status_compliant else "Registration status is concerning."
            }
            alerts.extend(status_alerts)
            log_diagnostic(f"Registration status evaluation: {'PASSED' if status_compliant else 'FAILED'}")
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
            disclosure_compliance, disclosure_summary, disclosure_alerts = evaluate_disclosures(disclosures, name)
            evaluation_report['disclosure_review'] = {
                'disclosure_compliance': disclosure_compliance,
                'disclosure_compliance_explanation': disclosure_summary
            }
            alerts.extend(disclosure_alerts)
            log_diagnostic(f"Disclosure evaluation: {'PASSED' if disclosure_compliance else 'FAILED'}")
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
            evaluations_performed.append(evaluation_report['registration_status']['status_compliance'])
        if config.get('evaluate_disclosures', True):
            evaluations_performed.append(evaluation_report['disclosure_review']['disclosure_compliance'])

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
        log_diagnostic(f"Evaluation report saved to {output_file_path}")

        # Increment counters and update checkpoint
        records_written += 1
        last_processed_line += 1

    else:
        # Log unresolved CRD cases if validation fails
        line_number = last_processed_line + 1
        logging.warning(f"Invalid or missing CRD value at line {line_number}: '{crd_value}'. Row data: {row}")
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
        log_diagnostic(f"Resuming from checkpoint: {checkpoint_csv_file} at line {last_processed_line}")
    else:
        last_processed_line = -1

    # Process each CSV file
    for csv_file in csv_files:
        current_csv_file = csv_file
        csv_file_path = os.path.join(input_folder, csv_file)
        logging.info(f"Processing file: {csv_file}")
        process_csv(csv_file_path, last_processed_line)
        files_processed += 1
        last_processed_line = -1  # Reset for next file

        # Archive processed files
        archive_subfolder = os.path.join(archive_folder, datetime.now().strftime("%m-%d-%Y"))
        os.makedirs(archive_subfolder, exist_ok=True)
        shutil.move(csv_file_path, os.path.join(archive_subfolder, csv_file))
        logging.info(f"Archived processed file: {csv_file}")

    # Completion log
    logging.info(f"Processing complete! Files processed: {files_processed}, Records written: {records_written}")

if __name__ == "__main__":
    main()
