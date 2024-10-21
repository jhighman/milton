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
import requests

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

# Initialize counters
files_processed = 0
records_written = 0
current_csv_file = None
last_processed_line = -1

# Custom exception for rate limiting
class RateLimitExceeded(Exception):
    """Exception raised when the API rate limit is exceeded."""
    pass

# Load configuration file
def load_config():
    with open('config.json') as config_file:
        return json.load(config_file)

config = load_config()

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Evaluation Framework')
parser.add_argument('--diagnostic', action='store_true', help='Enable diagnostic mode')
parser.add_argument('--wait-time', type=int, default=7, help='Wait time between requests in seconds (default: 7)')
args = parser.parse_args()

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_diagnostic(message):
    if args.diagnostic:
        logging.info(f"[DIAGNOSTIC] {message}")

# Save checkpoint
def save_checkpoint():
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
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            checkpoint_data = json.load(f)
        log_diagnostic("Checkpoint loaded.")
        return checkpoint_data.get('current_csv_file'), checkpoint_data.get('last_processed_line')
    else:
        return None, -1

# Handle signal interruptions
def signal_handler(sig, frame):
    logging.info('Interrupt received, saving checkpoint...')
    save_checkpoint()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)  # Handle SIGTERM as well

# Cache helper functions
def read_from_cache(crd_number: str, operation: str) -> Optional[Dict]:
    cache_file = os.path.join(cache_folder, f"{crd_number}_{operation}.json")
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            return json.load(f)
    return None

def write_to_cache(crd_number: str, operation: str, data: Dict):
    os.makedirs(cache_folder, exist_ok=True)
    cache_file = os.path.join(cache_folder, f"{crd_number}_{operation}.json")
    with open(cache_file, 'w') as f:
        json.dump(data, f, indent=2)

# Function to fetch basic info from API with caching
def get_individual_basic_info(crd_number):
    cached_data = read_from_cache(crd_number, "basic_info")
    if cached_data:
        log_diagnostic(f"Loaded basic info for CRD {crd_number} from cache.")
        return cached_data

    try:
        url = 'https://api.brokercheck.finra.org/search/individual'
        params = {
            'query': crd_number,
            'filter': 'active=true,prev=true,bar=true,broker=true,ia=true,brokeria=true',
            'includePrevious': 'true',
            'hl': 'true',
            'nrows': '12',
            'start': '0',
            'r': '25',
            'sort': 'score+desc',
            'wt': 'json'
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            time.sleep(args.wait_time)
            data = response.json()
            write_to_cache(crd_number, "basic_info", data)
            return data
        elif response.status_code == 403:
            raise RateLimitExceeded(f"403 Forbidden error encountered when fetching basic info for CRD {crd_number}.")
        else:
            logging.error(f"Error fetching basic info for CRD {crd_number}: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception for CRD {crd_number}: {e}")
        return None

# Function to fetch detailed info from API with caching
def get_individual_detailed_info(crd_number):
    cached_data = read_from_cache(crd_number, "detailed_info")
    if cached_data:
        log_diagnostic(f"Loaded detailed info for CRD {crd_number} from cache.")
        return cached_data

    try:
        url = f'https://api.brokercheck.finra.org/search/individual/{crd_number}'
        params = {
            'hl': 'true',
            'includePrevious': 'true',
            'nrows': '12',
            'query': 'john',
            'r': '25',
            'sort': 'bc_lastname_sort asc,bc_firstname_sort asc,bc_middlename_sort asc,score desc',
            'wt': 'json'
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            time.sleep(args.wait_time)
            data = response.json()
            write_to_cache(crd_number, "detailed_info", data)
            return data
        elif response.status_code == 403:
            raise RateLimitExceeded(f"403 Forbidden error encountered when fetching detailed info for CRD {crd_number}.")
        else:
            logging.error(f"Error fetching detailed info for CRD {crd_number}: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception for CRD {crd_number}: {e}")
        return None

# Process each CSV file
def process_csv(csv_file_path, start_line):
    global records_written, last_processed_line, current_csv_file
    current_csv_file = os.path.basename(csv_file_path)
    last_processed_line = start_line

    with open(csv_file_path, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        records = list(csv_reader)
        total_records = len(records)

        for index, row in enumerate(records):
            if index <= last_processed_line:
                continue  # Skip already processed records

            alerts = []
            evaluation_report = {}
            try:
                crd_number = row['crd_number']
                last_name = row['last_name']
                first_name = row['first_name']
                name = f"{first_name} {last_name}"
                license_type = row.get('license_type', '')
            except KeyError as e:
                missing_key = str(e).strip("'")
                logging.warning(f"Missing key '{missing_key}' in row: {row}")
                continue  # Skip this row

            log_diagnostic(f"Processing CRD {crd_number}")

            try:
                basic_info = get_individual_basic_info(crd_number)
                detailed_info = get_individual_detailed_info(crd_number)
            except RateLimitExceeded as e:
                logging.error(str(e))
                logging.info(f"Processed {records_written} records before rate limiting.")
                save_checkpoint()
                sys.exit(1)

            if basic_info and detailed_info:
                # Parse basic info
                if basic_info['hits']['hits']:
                    individual = basic_info['hits']['hits'][0]['_source']
                    fetched_name = f"{individual.get('ind_firstname', '')} {individual.get('ind_middlename', '')} {individual.get('ind_lastname', '')}".strip()
                    other_names = individual.get('ind_other_names', [])
                    bc_scope = individual.get('ind_bc_scope', '')
                    ia_scope = individual.get('ind_ia_scope', '')

                    # Name Verification
                    if config.get('evaluate_name', True):
                        name_match, name_alert = evaluate_name(name, fetched_name, other_names)
                        evaluation_report['name'] = {
                            'expected_name': name,
                            'fetched_name': fetched_name,
                            'name_match': name_match,
                            'name_match_explanation': "" if name_match else "Expected name did not match fetched name."
                        }
                        if name_alert:
                            alerts.append(name_alert)
                        log_diagnostic(f"Name evaluation: {'PASSED' if name_match else 'FAILED'}")
                    else:
                        evaluation_report['name'] = {'evaluation_skipped': True}
                        log_diagnostic("Name evaluation skipped as per configuration.")

                    # License Compliance
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

                    # Registration Status Check
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

                    # Exam Evaluation
                    if config.get('evaluate_exams', True):
                        detailed_content = detailed_info['hits']['hits'][0]['_source'].get('content', '{}')
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
                        detailed_content = detailed_info['hits']['hits'][0]['_source'].get('content', '{}')
                        detailed_data = json.loads(detailed_content)
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

                    # Final Evaluation
                    evaluations_performed = []
                    if config.get('evaluate_name', True):
                        evaluations_performed.append(name_match)
                    if config.get('evaluate_license', True):
                        evaluations_performed.append(license_compliant)
                    if config.get('evaluate_exams', True):
                        evaluations_performed.append(exam_compliant)
                    if config.get('evaluate_registration_status', True):
                        evaluations_performed.append(status_ok)

                    overall_compliance = all(evaluations_performed)

                    # Determine overall risk level based on alerts
                    if any(alert.severity == AlertSeverity.HIGH for alert in alerts):
                        overall_risk_level = "High"
                    elif any(alert.severity == AlertSeverity.MEDIUM for alert in alerts):
                        overall_risk_level = "Medium"
                    else:
                        overall_risk_level = "Low"

                    # Recommendations
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

                    # Write the evaluation report to a JSON file named using CRD number
                    os.makedirs(output_folder, exist_ok=True)
                    output_file_path = os.path.join(output_folder, f"{crd_number}.json")
                    with open(output_file_path, 'w') as json_file:
                        json.dump({'crd_number': crd_number, **evaluation_report}, json_file, indent=2)

                    # Increment the record counter
                    records_written += 1
                    last_processed_line = index
                    save_checkpoint()

                else:
                    logging.warning(f"No basic information available for CRD number {crd_number}.")
            else:
                logging.error(f"Failed to fetch information for CRD number {crd_number}")
                last_processed_line = index - 1  # Since this record failed
                save_checkpoint()
                sys.exit(1)  # Terminate the program

def main():
    global current_csv_file, last_processed_line, files_processed

    # Ensure directories exist
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(archive_folder, exist_ok=True)
    os.makedirs(input_folder, exist_ok=True)
    os.makedirs(cache_folder, exist_ok=True)

    # Load checkpoint if exists
    checkpoint_csv_file, checkpoint_line = load_checkpoint()

    # Get list of CSV files in input_folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
    if not csv_files:
        logging.info("No CSV files found in the input folder.")
        return

    # Sort the list of CSV files
    csv_files.sort()

    # If resuming, adjust the list of files to process
    if checkpoint_csv_file:
        if checkpoint_csv_file in csv_files:
            csv_files = csv_files[csv_files.index(checkpoint_csv_file):]
            last_processed_line = checkpoint_line
        else:
            logging.warning(f"Checkpoint file {checkpoint_csv_file} not found in input folder.")
            last_processed_line = -1  # Start from the beginning
    else:
        last_processed_line = -1  # Start from the beginning

    # Process each CSV file
    for csv_file in csv_files:
        current_csv_file = csv_file
        csv_file_path = os.path.join(input_folder, csv_file)
        # Process the CSV file
        process_csv(csv_file_path, last_processed_line)
        last_processed_line = -1  # Reset for the next file

        # Move the processed CSV file to the archive with date subfolder
        current_date = datetime.now().strftime("%m-%d-%Y")
        archive_subfolder = os.path.join(archive_folder, current_date)
        os.makedirs(archive_subfolder, exist_ok=True)
        dest_path = os.path.join(archive_subfolder, csv_file)
        shutil.move(csv_file_path, dest_path)

        # Increment the file counter
        files_processed += 1

        # Remove checkpoint file after successful file processing
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            log_diagnostic("Checkpoint removed.")

    # Print the summary
    logging.info(f"Processing complete! Files processed: {files_processed}, Records written: {records_written}")

if __name__ == "__main__":
    main()
