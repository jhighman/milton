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
from typing import Dict, List
from collections import OrderedDict
from selenium.webdriver.support.ui import Select

# Import the ApiClient from api_client.py
from api_client import ApiClient, RateLimitExceeded

# Import evaluation functions and classes from evaluation_library.py
from evaluation_library import (
    evaluate_name,
    evaluate_license,
    evaluate_exams,
    evaluate_registration_status,
    evaluate_disclosures,
    evaluate_arbitration,
    evaluate_disciplinary,
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

# Canonical field mappings
canonical_fields = {
    'reference_id': ['referenceId', 'reference_id', 'Reference ID', 'ReferenceId'],
    'crd': ['crd', 'CRD', 'CRDNumber', 'crd_number', 'crdnumber', 'CRD Number'],
    'first_name': ['first_name', 'First Name', 'firstname', 'FirstName', 'first', 'firstName'],
    'middle_name': ['middle_name', 'Middle Name', 'middlename', 'MiddleName', 'middle', 'middleName'],
    'last_name': ['last_name', 'Last Name', 'lastname', 'LastName', 'last', 'lastName'],
    'employee_number': ['employee_number', 'Employee Number', 'employeenumber', 'EmployeeNumber', 'employeeNumber'],
    'license_type': ['license_type', 'License Type', 'licensetype', 'LicenseType', 'license'],
    'organization_name': ['organization_name', 'Organization Name', 'organizationname', 'OrganizationName', 'organization']
}

class DataSourceHandler:
    """Handles data extraction based on the data source."""
    def __init__(self, data_source: str):
        self.data_source = data_source

    def extract_individual_info(self, individual: dict, detailed_info: dict) -> dict:
        if individual is None:
            logging.warning("No individual data available to extract.")
            return {}

        extracted_info = {
            'individual': individual
        }

        if self.data_source == "BrokerCheck":
            extracted_info['fetched_name'] = f"{individual.get('ind_firstname', '')} {individual.get('ind_middlename', '')} {individual.get('ind_lastname', '')}".strip()
            extracted_info['other_names'] = individual.get('ind_other_names', [])
            extracted_info['bc_scope'] = individual.get('ind_bc_scope', '')
            extracted_info['ia_scope'] = individual.get('ind_ia_scope', '')
            extracted_info['individual'] = individual

        elif self.data_source == "IAPD":
            basic_info = individual
            extracted_info['fetched_name'] = f"{basic_info.get('ind_firstname', '')} {basic_info.get('ind_middlename', '')} {basic_info.get('ind_lastname', '')}".strip()
            extracted_info['other_names'] = basic_info.get('ind_other_names', [])
            extracted_info['ia_scope'] = basic_info.get('ind_ia_scope', '')
            extracted_info['bc_scope'] = basic_info.get('ind_bc_scope', '')

            extracted_info['current_ia_employments'] = [
                {
                    'firm_id': emp.get('firmId'),
                    'firm_name': emp.get('firmName'),
                    'registration_begin_date': emp.get('registrationBeginDate'),
                    'branch_offices': [
                        {
                            'street': office.get('street1'),
                            'city': office.get('city'),
                            'state': office.get('state'),
                            'zip_code': office.get('zipCode')
                        }
                        for office in emp.get('branchOfficeLocations', [])
                    ]
                }
                for emp in json.loads(individual.get('iacontent', '{}')).get('currentIAEmployments', [])
            ]

            extracted_info['individual'] = individual
            iacontent_str = detailed_info['hits']['hits'][0]['_source']['iacontent']
            logging.debug(f"Extracted iacontent as string: {iacontent_str}")

            iacontent_data = json.loads(iacontent_str)
            logging.debug(f"Parsed iacontent JSON: {iacontent_data}")
            state_exams = iacontent_data.get('stateExamCategory', [])
            principal_exams = iacontent_data.get('principalExamCategory', [])
            product_exams = iacontent_data.get('productExamCategory', [])
            all_exams = state_exams + principal_exams + product_exams
            logging.debug(f"All combined exams: {all_exams}")

            extracted_info['exams'] = all_exams
            extracted_info['disclosures'] = iacontent_data.get('disclosures', [])
            extracted_info['arbitrations'] = iacontent_data.get('arbitrations', [])
        else:
            raise ValueError(f"Unknown data source: {self.data_source}")

        return extracted_info

    def extract_exam_info(self, individual: dict, detailed_info: dict) -> list:
        if individual is None:
            logging.warning("No individual data available to extract exam info.")
            return []

        if self.data_source == "BrokerCheck":
            return detailed_info.get('stateExamCategory', [])
        elif self.data_source == "IAPD":
            try:
                ia_content = json.loads(individual['_source']['iacontent'])
                state_exams = ia_content.get('stateExamCategory', [])
                principal_exams = ia_content.get('principalExamCategory', [])
                product_exams = ia_content.get('productExamCategory', [])
                return state_exams + principal_exams + product_exams
            except (KeyError, json.JSONDecodeError) as e:
                logging.error(f"Failed to parse IAPD exam data: {str(e)}")
                return []
        else:
            raise ValueError(f"Unknown data source: {self.data_source}")

# Initialize counters and state variables
files_processed = 0
records_written = 0
current_csv_file = None
last_processed_line = -1

def load_config():
    with open('config.json') as config_file:
        return json.load(config_file)

config = load_config()

parser = argparse.ArgumentParser(description='Evaluation Framework')
parser.add_argument('--diagnostic', action='store_true', help='Enable diagnostic mode')
parser.add_argument('--wait-time', type=int, default=7, help='Wait time between requests in seconds (default: 7)')
args = parser.parse_args()

if args.diagnostic:
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_diagnostic(message):
    logging.debug(message)

checkpoint_file = os.path.join(output_folder, 'checkpoint.json')

def save_checkpoint():
    checkpoint_data = {
        'csv_file': current_csv_file,
        'line': last_processed_line
    }
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

logger = logging.getLogger(__name__)
api_client = ApiClient(cache_folder=cache_folder, wait_time=args.wait_time, logger=logger, webdriver_enabled=True)

def log_unresolved_crd(row, resolved_fields):
    os.makedirs(output_folder, exist_ok=True)
    file_exists = os.path.isfile(log_file_path)
    with open(log_file_path, 'a', newline='', encoding='utf-8-sig') as csvfile:
        fieldnames = resolved_fields.values()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({field: row.get(field, '') for field in fieldnames})

def resolve_headers(headers):
    resolved_headers = {}
    unmapped_canonical_fields = set(canonical_fields.keys())
    header_map = {header.lower().strip(): header for header in headers}
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

def determine_search_strategy(claim, api_client: ApiClient):
    crd_number = claim.get('crd_number', '').strip()
    organization_name = claim.get('organization_name', '').strip()
    name = claim.get('name', '').strip()
    individual_name = f"{name}".strip()

    if crd_number:
        logging.info(f"Search strategy selected: 'basic_info' using CRD '{crd_number}' for individual '{individual_name}'")
        return {
            "strategy": "basic_info",
            "crd_number": crd_number,
            "individual_name": individual_name
        }
    elif organization_name and individual_name:
        firm_crd = api_client.get_firm_crd(organization_name)
        if firm_crd:
            logging.info(f"Search strategy selected: 'correlated_firm_info' using org '{organization_name}' with CRD '{firm_crd}' for '{individual_name}'")
            return {
                "strategy": "correlated_firm_info",
                "firm_crd": firm_crd,
                "individual_name": individual_name,
                "crd_number": crd_number
            }
        else:
            logging.info(f"Firm CRD not found for organization '{organization_name}', defaulting to 'basic_info' strategy")
    logging.info(f"Search strategy selected: 'basic_info' with no CRD or organization for '{individual_name}'")
    return {
        "strategy": "basic_info",
        "crd_number": crd_number,
        "individual_name": individual_name
    }

def save_evaluation_report(evaluation_report: dict, employee_number: str, reference_id: str):
    if not reference_id:
        logging.warning("Reference ID is missing. Using 'unknown' as the filename.")
        reference_id = "unknown"

    output_file_name = f"{reference_id}.json"
    output_file_path = os.path.join(output_folder, output_file_name)
    with open(output_file_path, 'w') as json_file:
        json.dump(evaluation_report, json_file, indent=2)
    log_diagnostic(f"Evaluation report saved to {output_file_path}")


def build_final_evaluation(evaluation_report: dict, alerts: List[Alert]):
    evaluations_performed = [evaluation_report['search_evaluation']['search_compliance']]
    if 'name' in evaluation_report and 'name_match' in evaluation_report['name']:
        evaluations_performed.append(evaluation_report['name']['name_match'])
    if 'license_verification' in evaluation_report and 'license_compliance' in evaluation_report['license_verification']:
        evaluations_performed.append(evaluation_report['license_verification']['license_compliance'])
    if 'exam_evaluation' in evaluation_report and 'exam_compliance' in evaluation_report['exam_evaluation']:
        evaluations_performed.append(evaluation_report['exam_evaluation']['exam_compliance'])
    if 'registration_status' in evaluation_report and 'status_compliance' in evaluation_report['registration_status']:
        evaluations_performed.append(evaluation_report['registration_status']['status_compliance'])
    if 'disclosure_review' in evaluation_report and 'disclosure_compliance' in evaluation_report['disclosure_review']:
        evaluations_performed.append(evaluation_report['disclosure_review']['disclosure_compliance'])
    if 'arbitration_evaluation' in evaluation_report and 'arbitration_compliance' in evaluation_report['arbitration_evaluation']:
        evaluations_performed.append(evaluation_report['arbitration_evaluation']['arbitration_compliance'])

    overall_compliance = all(evaluations_performed) if evaluations_performed else True

    if any(alert.severity == AlertSeverity.HIGH for alert in alerts):
        overall_risk_level = "High"
    elif any(alert.severity == AlertSeverity.MEDIUM for alert in alerts):
        overall_risk_level = "Medium"
    else:
        overall_risk_level = "Low"

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

def perform_search(claim: dict, api_client: ApiClient) -> dict:
    search_evaluation = {}
    search_strategy = determine_search_strategy(claim, api_client)
    search_evaluation['search_strategy'] = search_strategy['strategy']
    search_evaluation['search_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    search_evaluation['cache_files'] = {}

    employee_number = claim.get('employee_number')

    try:
        individual, detailed_info = None, None

        if search_strategy['strategy'] == 'basic_info':
            crd_number = claim.get('crd_number', '').strip()
            search_evaluation['crd_number'] = crd_number

            if crd_number and crd_number.isdigit() and int(crd_number) > 0:
                crd_number = int(crd_number)
                log_diagnostic(f"Processing CRD {crd_number}")
                
                basic_info, basic_info_cache_file = api_client.get_individual_basic_info(crd_number, return_cache_filename=True, employee_number=employee_number)
                search_evaluation['data_source'] = "BrokerCheck"
                search_evaluation['cache_files']['basic_info'] = basic_info_cache_file
                
                total_hits = basic_info.get('hits', {}).get('total', 0) if basic_info else 0
                if total_hits == 1:
                    individual = basic_info['hits']['hits'][0]['_source']
                    detailed_info, detailed_info_cache_file = api_client.get_individual_detailed_info(crd_number, return_cache_filename=True, employee_number=employee_number)
                    search_evaluation['cache_files']['detailed_info'] = detailed_info_cache_file
                    search_evaluation['search_outcome'] = "Record found"
                elif total_hits == 0:
                    search_evaluation['search_outcome'] = "No records found"
                else:
                    search_evaluation['search_outcome'] = f"Multiple records found ({total_hits})"
            else:
                search_evaluation['search_outcome'] = "Invalid or missing CRD value"

        elif search_strategy['strategy'] == 'correlated_firm_info':
            individual_name = claim.get('name', '').strip()
            firm_crd = search_strategy['firm_crd']
            crd_number = claim.get('crd_number', '').strip()

            search_evaluation['individual_name'] = individual_name
            search_evaluation['firm_crd'] = firm_crd
            search_evaluation['crd_number'] = crd_number

            log_diagnostic(f"Processing individual '{individual_name}' with CRD '{crd_number}' at firm CRD {firm_crd}")

            if crd_number and crd_number.isdigit() and int(crd_number) > 0:
                crd_number = int(crd_number)
                basic_info, basic_info_cache_file = api_client.get_individual_basic_info(crd_number, return_cache_filename=True, employee_number=employee_number)
                search_evaluation['data_source'] = "BrokerCheck"
                search_evaluation['cache_files']['basic_info'] = basic_info_cache_file
                
                total_hits = basic_info.get('hits', {}).get('total', 0) if basic_info else 0
                if total_hits == 1:
                    individual = basic_info['hits']['hits'][0]['_source']
                    detailed_info, detailed_info_cache_file = api_client.get_individual_detailed_info(crd_number, service='sec', return_cache_filename=True, employee_number=employee_number)
                    search_evaluation['cache_files']['detailed_info'] = detailed_info_cache_file
                    search_evaluation['search_outcome'] = "Record found"
                elif total_hits == 0:
                    search_evaluation['search_outcome'] = "No records found"
                else:
                    search_evaluation['search_outcome'] = f"Multiple records found ({total_hits})"
            else:
                basic_info, basic_info_cache_file = api_client.get_individual_correlated_firm_info(individual_name, firm_crd, return_cache_filename=True, employee_number=employee_number)
                search_evaluation['data_source'] = "IAPD"
                search_evaluation['cache_files']['basic_info'] = basic_info_cache_file
                
                total_hits = basic_info.get('hits', {}).get('total', 0) if basic_info else 0
                if total_hits == 1:
                    individual = basic_info['hits']['hits'][0]['_source']
                    individual_id = individual.get('ind_source_id') 
                    if individual_id:
                        detailed_info, detailed_info_cache_file = api_client.get_individual_detailed_info(individual_id, service='sec', return_cache_filename=True, employee_number=employee_number)
                        search_evaluation['cache_files']['detailed_info'] = detailed_info_cache_file
                        search_evaluation['search_outcome'] = "Record found"
                    else:
                        search_evaluation['search_outcome'] = "individualId not found in API response"
                elif total_hits == 0:
                    search_evaluation['search_outcome'] = "No matching individual found"
                else:
                    search_evaluation['search_outcome'] = f"Multiple records found ({total_hits})"
        else:
            search_evaluation['search_outcome'] = f"Unsupported search strategy: {search_strategy['strategy']}"

    except RateLimitExceeded as e:
        logging.error(str(e))
        logging.info(f"Processed records before rate limiting.")
        save_checkpoint()
        sys.exit(1)
    except Exception as e:
        logging.exception("An unexpected error occurred during search.")
        search_evaluation['search_outcome'] = "Search failed due to an error"

    search_compliance = search_evaluation.get('search_outcome') == "Record found"
    search_evaluation['search_compliance'] = search_compliance

    if search_compliance:
        search_evaluation['individual'] = individual    
        search_evaluation['detailed_info'] = detailed_info
        search_evaluation['data_source'] = search_evaluation.get('data_source', 'Unknown')

    return search_evaluation

def process_csv(csv_file_path, start_line):
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
            try:
                process_row(row, resolved_headers)
            except Exception as e:
                logging.exception(f"Error processing row {index} in file {current_csv_file}: {e}")
            last_processed_line = index
            save_checkpoint()

def perform_evaluations(evaluation_report: dict, extracted_info: dict, claim: dict, alerts: list):
    # Check if we have individual data before proceeding with status evaluation
    if not extracted_info.get('individual'):
        logging.warning("No individual data available for status evaluation")
        evaluation_report['status_evaluation'] = {
            'status_compliant': False,
            'alerts': ['No individual data available for status evaluation']
        }
        return

    # Registration Status Evaluation
    status_compliant, status_alerts = evaluate_registration_status(extracted_info['individual'])
    evaluation_report['status_evaluation'] = {
        'status_compliance': status_compliant,
        'alerts': status_alerts
    }
    alerts.extend(status_alerts)

    # Name Evaluation
    name = f"{claim.get('first_name', '')} {claim.get('last_name', '')}".strip()
    evaluation_report['name'] = {
        'expected_name': name,
    }

    if config.get('evaluate_name', True):
        name_match, name_alert = evaluate_name(name, extracted_info['fetched_name'], extracted_info['other_names'])
        fetched_name_lower = extracted_info['fetched_name'].lower()
        expected_name_lower = name.lower()
        evaluation_report['name'].update({
            'fetched_name': extracted_info['fetched_name'],
            'name_match': fetched_name_lower == expected_name_lower,
            'name_match_explanation': "" if fetched_name_lower == expected_name_lower 
                                   else f"Expected name '{name}' did not match fetched name '{extracted_info['fetched_name']}'"
        })
        if name_alert:
            alerts.append(name_alert)
    else:
        evaluation_report['name'] = {'evaluation_skipped': True}

    # License Evaluation
    if config.get('evaluate_license', True):
        license_type = claim.get('license_type', '')
        bc_scope = extracted_info.get('bc_scope', '')
        ia_scope = extracted_info.get('ia_scope', '')
        license_compliant, license_alert = evaluate_license(license_type, bc_scope, ia_scope, name)
        evaluation_report['license_verification'] = {
            'license_compliance': license_compliant,
            'license_compliance_explanation': "The individual holds an active license." if license_compliant else "License compliance failed."
        }
        if license_alert:
            alerts.append(license_alert)
    else:
        evaluation_report['license_verification'] = {'evaluation_skipped': True}

    # Exam Evaluation
    if config.get('evaluate_exams', True):
        exams = extracted_info.get('exams', [])
        if exams:
            try:
                passed_exams = get_passed_exams(exams)
                license_type = claim.get('license_type', '')
                exam_compliant, exam_alert = evaluate_exams(passed_exams, license_type, name)
                evaluation_report['exam_evaluation'] = {
                    'exam_compliance': exam_compliant,
                    'exam_compliance_explanation': "The individual has passed all required exams." if exam_compliant else "Exam compliance failed."
                }
                if exam_alert:
                    alerts.append(exam_alert)
            except Exception as e:
                logging.warning(f"Failed to evaluate exams: {e}")
                evaluation_report['exam_evaluation'] = {
                    'evaluation_skipped': True,
                    'reason': 'Failed to evaluate exams.'
                }
        else:
            evaluation_report['exam_evaluation'] = {
                'evaluation_skipped': True,
                'reason': 'Exams information not available.'
            }
    else:
        evaluation_report['exam_evaluation'] = {'evaluation_skipped': True}

    # Disclosures Review
    if config.get('evaluate_disclosures', True):
        disclosures = extracted_info.get('disclosures', [])
        if disclosures:
            try:
                disclosure_compliance, disclosure_summary, disclosure_alerts = evaluate_disclosures(disclosures, name)
                evaluation_report['disclosure_review'] = {
                    'disclosure_compliance': disclosure_compliance,
                    'disclosure_compliance_explanation': disclosure_summary
                }
                alerts.extend(disclosure_alerts)
            except Exception as e:
                logging.warning(f"Failed to evaluate disclosures: {e}")
                evaluation_report['disclosure_review'] = {
                    'evaluation_skipped': True,
                    'reason': 'Failed to evaluate disclosures.'
                }
        else:
            evaluation_report['disclosure_review'] = {
                'evaluation_skipped': True,
                'reason': 'Disclosures information not available.'
            }
    else:
        evaluation_report['disclosure_review'] = {'evaluation_skipped': True}


def process_row(row, resolved_headers):
    global records_written

    # Extract claim information from the row
    reference_id = row.get(resolved_headers.get('reference_id', ''), '').strip()
    crd_number = row.get(resolved_headers.get('crd', ''), '').strip()
    first_name = row.get(resolved_headers.get('first_name', ''), '').strip()
    middle_name = row.get(resolved_headers.get('middle_name', ''), '').strip()
    last_name = row.get(resolved_headers.get('last_name', ''), '').strip()
    name = f"{first_name} {last_name}".strip()
    license_type = row.get(resolved_headers.get('license_type', ''), '').strip()
    employee_number = row.get(resolved_headers.get('employee_number', ''), '').strip()

     # If employee_number is missing, use reference_id as fallback
    if not employee_number and reference_id:
        logging.info(f"Employee number missing; using reference ID '{reference_id}' as employee number.")
        employee_number = reference_id
    organization_name_field = resolved_headers.get('organization_name', '')
    organization_name = row.get(organization_name_field, '').strip()

    claim = {
        'crd_number': crd_number,
        'first_name': first_name,
        'middle_name': middle_name,
        'last_name': last_name,
        'name': name,
        'organization_name': organization_name,
        'license_type': license_type,
        'employee_number': employee_number if employee_number else None
    }

    # Perform initial search
    search_evaluation = perform_search(claim, api_client)

    evaluation_report = OrderedDict()
    evaluation_report['reference_id'] = reference_id  
    evaluation_report['claim'] = claim
    evaluation_report['search_evaluation'] = search_evaluation

    if not search_evaluation['search_compliance']:
        alerts = []
        build_final_evaluation(evaluation_report, alerts)
        save_evaluation_report(evaluation_report, claim.get('employee_number', 'unknown'), reference_id)
        records_written += 1
        return

    individual = search_evaluation['individual']
    data_source = search_evaluation['data_source']
    detailed_info = search_evaluation.get('detailed_info')

    alerts = []

    # Extract individual information
    data_handler = DataSourceHandler(data_source)
    extracted_info = data_handler.extract_individual_info(individual, detailed_info)

    # Test the result set 
    disciplinary_records_full = api_client.get_finra_disciplinary_actions(
        employee_number=claim.get('employee_number'),
        first_name=claim.get('first_name', ''),
        last_name=claim.get('last_name', ''),
        alternate_names=extracted_info.get('other_names', [])
    )
    # Assuming the results are structured as a dictionary with disciplinary data keyed by name variation
    disciplinary_records = [value["data"] for value in disciplinary_records_full.values()]


    disciplinary_compliance, disciplinary_explanation, disciplinary_alerts = evaluate_disciplinary(disciplinary_records, name)
    evaluation_report['disciplinary_evaluation'] = {
        'disciplinary_compliance': disciplinary_compliance,
        'disciplinary_compliance_explanation': disciplinary_explanation,
        'disciplinary_records': disciplinary_records,
        'alerts': [alert.to_dict() for alert in disciplinary_alerts]
    }
    alerts.extend(disciplinary_alerts)

    # Perform other evaluations
    perform_evaluations(evaluation_report, extracted_info, claim, alerts)

    # Arbitration evaluation
    arbitrations = extracted_info.get('arbitrations', [])
    arbitration_compliance, arbitration_explanation, arbitration_alerts = evaluate_arbitration(arbitrations, name)
    evaluation_report['arbitration_evaluation'] = {
        'arbitration_compliance': arbitration_compliance,
        'arbitration_compliance_explanation': arbitration_explanation,
        'arbitrations': arbitrations,
    }
    alerts.extend(arbitration_alerts)

    # Build the final evaluation report
    build_final_evaluation(evaluation_report, alerts)
    save_evaluation_report(evaluation_report, "unknown", reference_id)
    records_written += 1

def main():
    global files_processed, last_processed_line, current_csv_file

    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(archive_folder, exist_ok=True)
    os.makedirs(input_folder, exist_ok=True)
    os.makedirs(cache_folder, exist_ok=True)

    checkpoint_csv_file, checkpoint_line = load_checkpoint()
    csv_files = sorted([f for f in os.listdir(input_folder) if f.endswith('.csv')])

    if not csv_files:
        logging.info("No CSV files found in the input folder.")
        return

    if checkpoint_csv_file and checkpoint_csv_file in csv_files:
        csv_files = csv_files[csv_files.index(checkpoint_csv_file):]
        last_processed_line = checkpoint_line
        log_diagnostic(f"Resuming from checkpoint: {checkpoint_csv_file} at line {last_processed_line}")
    else:
        last_processed_line = -1

    for csv_file in csv_files:
        current_csv_file = csv_file
        csv_file_path = os.path.join(input_folder, csv_file)
        logging.info(f"Processing file: {csv_file}")
        process_csv(csv_file_path, last_processed_line)
        files_processed += 1
        last_processed_line = -1

        archive_subfolder = os.path.join(archive_folder, datetime.now().strftime("%m-%d-%Y"))
        os.makedirs(archive_subfolder, exist_ok=True)
        shutil.move(csv_file_path, os.path.join(archive_subfolder, csv_file))
        logging.info(f"Archived processed file: {csv_file}")

    logging.info(f"Processing complete! Files processed: {files_processed}, Records written: {records_written}")

if __name__ == "__main__":
    main()

# Re-initialize API client if needed
logger = logging.getLogger(__name__)
# If you previously had a line re-initializing api_client, add it here if required:
# api_client = ApiClient(cache_folder=cache_folder, wait_time=args.wait_time, logger=logger, webdriver_enabled=True)
