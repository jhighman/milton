"""
main.py

This program processes CSV files from the "drop" folder. For each record, it:
  1. Resolves CSV headers to a canonical form.
  2. Constructs a claim dictionary.
  3. Performs a search via the API client.
  4. If a matching record is found, extracts additional data using DataSourceHandler.
  5. Uses the EvaluationReportBuilder and EvaluationReportDirector to construct a complete evaluation report.
  6. Saves the report in the "output" folder and archives the processed CSV.
"""

import os
import csv
import json
import time
import shutil
import logging
import argparse
import signal
import sys
import random
from datetime import datetime
from collections import OrderedDict

# Import our new Builder and Director for evaluation reports
from evaluation_report_builder import EvaluationReportBuilder
from evaluation_report_director import EvaluationReportDirector

# Import DataSourceHandler to extract individual data (BrokerCheck/IAPD)
from data_source_handler import DataSourceHandler

# Import the API client and exception class
from api_client import ApiClient, RateLimitExceeded

# Folder paths
folder_path = './'
input_folder = os.path.join(folder_path, 'drop')
output_folder = os.path.join(folder_path, 'output')
archive_folder = os.path.join(folder_path, 'archive')
cache_folder = os.path.join(folder_path, 'cache')
checkpoint_file = os.path.join(output_folder, 'checkpoint.json')

# Canonical field mappings (modify or extend as needed)
canonical_fields = {
    'reference_id': ['referenceId', 'reference_id', 'Reference ID', 'ReferenceId'],
    'crd_number': ['crd', 'CRD', 'CRDNumber', 'crd_number', 'crdnumber', 'CRD Number'],
    'first_name': ['first_name', 'First Name', 'firstname', 'FirstName', 'first', 'firstName'],
    'middle_name': ['middle_name', 'Middle Name', 'middlename', 'MiddleName', 'middle', 'middleName'],
    'last_name': ['last_name', 'Last Name', 'lastname', 'LastName', 'last', 'lastName'],
    'employee_number': ['employee_number', 'Employee Number', 'employeenumber', 'EmployeeNumber', 'employeeNumber'],
    'license_type': ['license_type', 'License Type', 'licensetype', 'LicenseType', 'license'],
    'organization_name': ['organization_name', 'Organization Name', 'organizationname', 'OrganizationName', 'organization']
    # ... (other fields can be added as needed)
}

# Global state variables
files_processed = 0
records_written = 0
current_csv_file = None
last_processed_line = -1

# Load configuration from file
def load_config():
    with open('config.json') as f:
        return json.load(f)
config = load_config()

# Set up argument parser and logging
parser = argparse.ArgumentParser(description='Evaluation Framework')
parser.add_argument('--diagnostic', action='store_true', help='Enable diagnostic mode')
parser.add_argument('--wait-time', type=int, default=7, help='Wait time between API requests (default: 7 seconds)')
args = parser.parse_args()

if args.diagnostic:
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_diagnostic(message):
    logging.debug(message)

# Checkpoint functions to resume processing if needed
def save_checkpoint():
    checkpoint_data = {'csv_file': current_csv_file, 'line': last_processed_line}
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f)
    log_diagnostic(f"Checkpoint saved: {checkpoint_data}")

def load_checkpoint():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            checkpoint_data = json.load(f)
        log_diagnostic(f"Checkpoint loaded: {checkpoint_data}")
        return checkpoint_data.get('csv_file'), checkpoint_data.get('line', -1)
    return None, -1

def signal_handler(sig, frame):
    logging.info("Interrupt received. Saving checkpoint and exiting...")
    save_checkpoint()
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Create the API client instance.
logger = logging.getLogger(__name__)
api_client = ApiClient(cache_folder=cache_folder, wait_time=args.wait_time, logger=logger, webdriver_enabled=True)

# Generate a unique reference_id if missing.
def generate_reference_id(prefix="DEF-") -> str:
    sponsor_id = ''.join(random.choices("0123456789", k=12))
    return f"{prefix}{sponsor_id}"

# Resolve CSV headers to canonical fields.
def resolve_headers(headers):
    resolved = {}
    unmapped = set(canonical_fields.keys())
    header_map = {header.lower().strip(): header for header in headers}
    for canonical, variations in canonical_fields.items():
        for variation in variations:
            if variation.lower().strip() in header_map:
                resolved[canonical] = header_map[variation.lower().strip()]
                unmapped.discard(canonical)
                break
    if unmapped:
        logging.warning(f"Unmapped canonical fields: {unmapped}")
    return resolved

# Save the evaluation report to the output folder.
def save_evaluation_report(report: dict, employee_number: str, reference_id: str):
    if not reference_id:
        logging.warning("Missing reference_id; using 'unknown' as filename.")
        reference_id = "unknown"
    output_path = os.path.join(output_folder, f"{reference_id}.json")
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)
    log_diagnostic(f"Report saved to {output_path}")

# Harvested search logic from the original program.
def perform_search(claim: dict, api_client: ApiClient) -> dict:
    from collections import OrderedDict
    search_eval = OrderedDict([
        ('compliance', False),
        ('compliance_explanation', ''),
        ('search_strategy', None),
        ('search_outcome', None),
        ('search_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
        ('cache_files', {}),
    ])
    # Determine search strategy (assume you have a determine_search_strategy function)
    from your_strategy_module import determine_search_strategy  # Adjust as needed.
    search_strategy = determine_search_strategy(claim, api_client)
    search_eval['search_strategy'] = search_strategy['strategy']
    employee_number = claim.get('employee_number')
    
    try:
        individual, detailed_info = None, None
        if search_strategy['strategy'] == 'basic_info':
            crd_number = claim.get('crd_number', '').strip()
            search_eval['crd_number'] = crd_number
            if crd_number and crd_number.isdigit() and int(crd_number) > 0:
                crd_int = int(crd_number)
                log_diagnostic(f"Processing CRD {crd_int}")
                basic_info, basic_info_cache = api_client.get_individual_basic_info(
                    crd_int, return_cache_filename=True, employee_number=employee_number
                )
                search_eval['data_source'] = "BrokerCheck"
                search_eval['cache_files']['basic_info'] = basic_info_cache
                total_hits = basic_info.get('hits', {}).get('total', 0) if basic_info else 0
                if total_hits == 1:
                    individual = basic_info['hits']['hits'][0]['_source']
                    detailed_info, detailed_info_cache = api_client.get_individual_detailed_info(
                        crd_int, return_cache_filename=True, employee_number=employee_number
                    )
                    search_eval['cache_files']['detailed_info'] = detailed_info_cache
                    search_eval['search_outcome'] = "Record found"
                else:
                    search_eval['search_outcome'] = "Record not found" if total_hits == 0 else f"Multiple records found ({total_hits})"
            else:
                search_eval['search_outcome'] = "Invalid or missing CRD value"
        elif search_strategy['strategy'] == 'correlated_firm_info':
            individual_name = claim.get('name', '').strip()
            firm_crd = search_strategy['firm_crd']
            crd_number = claim.get('crd_number', '').strip()
            search_eval['individual_name'] = individual_name
            search_eval['firm_crd'] = firm_crd
            search_eval['crd_number'] = crd_number
            log_diagnostic(f"Processing correlated search for '{individual_name}' with firm CRD {firm_crd}")
            if crd_number and crd_number.isdigit() and int(crd_number) > 0:
                crd_int = int(crd_number)
                basic_info, basic_info_cache = api_client.get_individual_basic_info(
                    crd_int, return_cache_filename=True, employee_number=employee_number
                )
                search_eval['data_source'] = "BrokerCheck"
                search_eval['cache_files']['basic_info'] = basic_info_cache
                total_hits = basic_info.get('hits', {}).get('total', 0) if basic_info else 0
                if total_hits == 1:
                    individual = basic_info['hits']['hits'][0]['_source']
                    detailed_info, detailed_info_cache = api_client.get_individual_detailed_info(
                        crd_int, service='sec', return_cache_filename=True, employee_number=employee_number
                    )
                    search_eval['cache_files']['detailed_info'] = detailed_info_cache
                    search_eval['search_outcome'] = "Record found"
                else:
                    search_eval['search_outcome'] = "No records found" if total_hits == 0 else f"Multiple records found ({total_hits})"
            else:
                basic_info, basic_info_cache = api_client.get_individual_correlated_firm_info(
                    individual_name, firm_crd, return_cache_filename=True, employee_number=employee_number
                )
                search_eval['data_source'] = "IAPD"
                search_eval['cache_files']['basic_info'] = basic_info_cache
                total_hits = basic_info.get('hits', {}).get('total', 0) if basic_info else 0
                if total_hits == 1:
                    individual = basic_info['hits']['hits'][0]['_source']
                    individual_id = individual.get('ind_source_id')
                    if individual_id:
                        detailed_info, detailed_info_cache = api_client.get_individual_detailed_info(
                            individual_id, service='sec', return_cache_filename=True, employee_number=employee_number
                        )
                        search_eval['cache_files']['detailed_info'] = detailed_info_cache
                        search_eval['search_outcome'] = "Record found"
                    else:
                        search_eval['search_outcome'] = "individualId not found"
                else:
                    search_eval['search_outcome'] = "No matching individual found" if total_hits == 0 else f"Multiple records found ({total_hits})"
        else:
            search_eval['search_outcome'] = f"Unsupported search strategy: {search_strategy['strategy']}"
    except RateLimitExceeded as e:
        logging.error(str(e))
        save_checkpoint()
        sys.exit(1)
    except Exception as e:
        logging.exception("Error during search.")
        search_eval['search_outcome'] = "Search failed due to an error"
    
    # Set compliance flag based on search outcome.
    search_eval['compliance'] = (search_eval.get('search_outcome') == "Record found")
    if not search_eval.get('compliance_explanation'):
        search_eval['compliance_explanation'] = search_eval.get('search_outcome', 'No explanation available')
    if search_eval['compliance']:
        search_eval['individual'] = individual
        search_eval['detailed_info'] = detailed_info
        search_eval['data_source'] = search_eval.get('data_source', 'Unknown')
    return search_eval

def process_row(row, resolved_headers):
    global records_written, last_processed_line

    # 1. Build claim dictionary from CSV row using canonical fields.
    claim = {}
    for field, variations in canonical_fields.items():
        header = resolved_headers.get(field)
        claim[field] = row.get(header, '').strip() if header else None

    # Set full name (if not already set) and default employee_number.
    first_name = claim.get('first_name', '')
    last_name = claim.get('last_name', '')
    claim['name'] = f"{first_name} {last_name}".strip()
    if not claim.get('employee_number'):
        claim['employee_number'] = generate_reference_id()

    # 2. Perform search based on claim.
    search_eval = perform_search(claim, api_client)

    # 3. Initialize a basic report with claim and search evaluation.
    base_report = OrderedDict()
    base_report['reference_id'] = claim.get('reference_id', generate_reference_id())
    base_report['claim'] = claim
    base_report['search_evaluation'] = search_eval

    # 4. If search failed, build minimal final evaluation and save.
    if not search_eval.get('compliance'):
        # For simplicity, just add the search evaluation alerts.
        base_report['final_evaluation'] = {
            "overall_compliance": False,
            "overall_risk_level": "High",
            "recommendations": "Search failed; manual review required.",
            "alerts": search_eval.get('alerts', [])
        }
        save_evaluation_report(base_report, claim.get('employee_number'), claim.get('reference_id'))
        records_written += 1
        return

    # 5. If a record is found, extract additional info using DataSourceHandler.
    data_source = search_eval.get('data_source', 'Unknown')
    handler = DataSourceHandler(data_source)
    extracted_info = handler.extract_individual_info(search_eval.get('individual'), search_eval.get('detailed_info'))
    # Also include any search_evaluation details.
    extracted_info["search_evaluation"] = search_eval

    # 6. Use Builder and Director to construct the complete evaluation report.
    reference_id = claim.get('reference_id', generate_reference_id())
    builder = EvaluationReportBuilder(reference_id)
    director = EvaluationReportDirector(builder)
    final_report = director.construct_evaluation_report(claim, extracted_info)

    # 7. Save the final report.
    save_evaluation_report(final_report, claim.get('employee_number'), reference_id)
    records_written += 1

def process_csv(csv_file_path, start_line):
    global last_processed_line, current_csv_file
    current_csv_file = os.path.basename(csv_file_path)
    last_processed_line = start_line
    with open(csv_file_path, 'r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        resolved_headers = resolve_headers(reader.fieldnames)
        for idx, row in enumerate(reader):
            if idx <= last_processed_line:
                continue
            log_diagnostic(f"Processing line {idx} in file {current_csv_file}")
            try:
                process_row(row, resolved_headers)
            except Exception as e:
                logging.exception(f"Error processing row {idx} in {current_csv_file}: {e}")
            last_processed_line = idx
            save_checkpoint()

def main():
    global files_processed, last_processed_line, current_csv_file, records_written
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(archive_folder, exist_ok=True)
    os.makedirs(input_folder, exist_ok=True)
    os.makedirs(cache_folder, exist_ok=True)
    
    checkpoint_csv, checkpoint_line = load_checkpoint()
    csv_files = sorted([f for f in os.listdir(input_folder) if f.endswith('.csv')])
    if not csv_files:
        logging.info("No CSV files found in input folder.")
        return
    if checkpoint_csv and checkpoint_csv in csv_files:
        csv_files = csv_files[csv_files.index(checkpoint_csv):]
        last_processed_line = checkpoint_line
        log_diagnostic(f"Resuming from checkpoint: {checkpoint_csv} at line {last_processed_line}")
    else:
        last_processed_line = -1

    for csv_file in csv_files:
        current_csv_file = csv_file
        csv_path = os.path.join(input_folder, csv_file)
        logging.info(f"Processing file: {csv_file}")
        process_csv(csv_path, last_processed_line)
        files_processed += 1
        last_processed_line = -1
        # Archive the processed CSV file.
        archive_subfolder = os.path.join(archive_folder, datetime.now().strftime("%m-%d-%Y"))
        os.makedirs(archive_subfolder, exist_ok=True)
        shutil.move(csv_path, os.path.join(archive_subfolder, csv_file))
        logging.info(f"Archived processed file: {csv_file}")

    logging.info(f"Processing complete! Files processed: {files_processed}, Records written: {records_written}")

if __name__ == "__main__":
    main()
