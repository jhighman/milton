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
from typing import Dict, List
from collections import OrderedDict
from selenium.webdriver.support.ui import Select
# main.py
from evaluation_library import determine_alert_category

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
    'crd_number': ['crd', 'CRD', 'CRDNumber', 'crd_number', 'crdnumber', 'CRD Number'],
    'first_name': ['first_name', 'First Name', 'firstname', 'FirstName', 'first', 'firstName'],
    'middle_name': ['middle_name', 'Middle Name', 'middlename', 'MiddleName', 'middle', 'middleName'],
    'last_name': ['last_name', 'Last Name', 'lastname', 'LastName', 'last', 'lastName'],
    'employee_number': ['employee_number', 'Employee Number', 'employeenumber', 'EmployeeNumber', 'employeeNumber'],
    'license_type': ['license_type', 'License Type', 'licensetype', 'LicenseType', 'license'],
    'organization_name': ['organization_name', 'Organization Name', 'organizationname', 'OrganizationName', 'organization'],

    # Newly added fields
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
        # Parse the "content" JSON from detailed_info
            # (Make sure you check for None or empty hits)
            if detailed_info and 'hits' in detailed_info:
                hits_list = detailed_info['hits'].get('hits', [])
                if len(hits_list) > 0:
                    content_str = hits_list[0]['_source'].get('content', '')
                    try:
                        content_json = json.loads(content_str)
                    except json.JSONDecodeErrorcd as e:
                        logging.warning(f"Error parsing 'content' JSON: {e}")
                        content_json = {}

                    # Now stash the disclosures
                    extracted_info['disclosures'] = content_json.get('disclosures', [])
                    # (You can parse more fields if needed)

            extracted_info['individual'] = individual
            # done        

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


def generate_reference_id(prefix="DEF-") -> str:
    """
    Generates a reference_id with the specified prefix followed by a 
    12-digit randomly generated sponsor_id.

    Args:
        prefix (str): The prefix for the reference_id. Defaults to "DEF-".

    Returns:
        str: The generated reference_id.
    """
    # Generate a 12-digit random sponsor_id
    sponsor_id = ''.join(random.choices("0123456789", k=12))
    # Concatenate the prefix and sponsor_id
    return f"{prefix}{sponsor_id}"


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

def extract_organization_name(input_string):
    """
    Extracts the organization name by checking for a concatenated identifier separated by a dash.
    If no concatenation is detected, returns the input string as is.
    
    Args:
        input_string (str): The raw organization name field.
    
    Returns:
        str: The extracted organization name with concatenated identifier removed, if present.
    """
    # Split the input string on the first occurrence of a dash
    parts = input_string.split(" - ", 1)
    
    # Return the second part if a dash is found; otherwise, return the original input
    return parts[1] if len(parts) > 1 else input_string



def determine_search_strategy(claim, api_client: ApiClient):
    """
    Determines the best search strategy based on available claim data.
    """
    # Extract key fields, ensuring we handle None values
    crd_number = claim.get('crd_number')
    crd_number = crd_number.strip() if crd_number else ''
    
    organization_name = claim.get('organization_name')
    organization_name = organization_name.strip() if organization_name else ''
    
    name = claim.get('name')
    name = name.strip() if name else ''

    # 1) If we have a valid CRD number, that's our best option
    if crd_number and crd_number.isdigit() and int(crd_number) > 0:
        logging.info(f"Search strategy selected: 'basic_info' using CRD '{crd_number}'")
        return {
            "strategy": "basic_info",
            "crd_number": crd_number,
            "individual_name": name
        }

    # 2) If both CRD and organization name are missing,
    #    try to look up a known CRD from firms.json
    if not crd_number and not organization_name:
        potential_firm_name = claim.get('firm_lookup_key', '')
        derived_firm_crd = api_client.get_organization_crd(potential_firm_name) if potential_firm_name else None

        if derived_firm_crd == "NOT_FOUND":
            logging.warning(f"Firm not found in index for firm lookup key: '{potential_firm_name}'")
            return {
                "strategy": "unknown_org",
                "error": "Firm not found in organization index",
                "individual_name": name,
                "crd_number": crd_number
            }
        elif not derived_firm_crd:
            logging.error("Failed to load firms cache or invalid firm lookup key")
            return {
                "strategy": "unknown_org",
                "error": "Failed to load firms cache or invalid firm lookup key",
                "individual_name": name,
                "crd_number": crd_number
            }

        logging.info(f"Derived CRD from known firm reference: {derived_firm_crd}")
        return {
            "strategy": "correlated_firm_info",
            "firm_crd": derived_firm_crd,
            "individual_name": name,
            "crd_number": crd_number
        }

    # 3) If organization name is provided (and possibly no CRD),
    #    try to get the firm's CRD via the API client
    if organization_name:
        firm_crd = api_client.get_organization_crd(organization_name)
        if firm_crd == "NOT_FOUND":
            logging.warning(f"Organization '{organization_name}' not found in index")
            return {
                "strategy": "unknown_org",
                "error": f"Organization '{organization_name}' not found in index",
                "individual_name": name,
                "crd_number": crd_number
            }
        elif not firm_crd:
            logging.error("Failed to load firms cache")
            return {
                "strategy": "unknown_org",
                "error": "Failed to load firms cache",
                "individual_name": name,
                "crd_number": crd_number
            }

        logging.info(f"Search strategy selected: 'correlated_firm_info' using org '{organization_name}' with CRD '{firm_crd}' for '{name}'")
        return {
            "strategy": "correlated_firm_info",
            "firm_crd": firm_crd,
            "individual_name": name,
            "crd_number": crd_number
        }

    # 4) If no valid CRD or organization, fallback to 'unknown_org'
    logging.warning("Insufficient data to determine search strategy")
    return {
        "strategy": "unknown_org",
        "error": "Insufficient data to determine search strategy",
        "individual_name": name,
        "crd_number": crd_number
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


def build_final_evaluation(evaluation_report: Dict, alerts: List[Alert]) -> None:
    """
    Builds the final evaluation section of the report.
    If ANY evaluation shows non-compliance, overall_compliance should be False.
    """
    # Check all evaluation sections for any non-compliance
    evaluation_sections = [
        'search_evaluation',
        'status_evaluation', 
        'name_evaluation',
        'license_evaluation',
        'exam_evaluation',
        'disclosure_review',
        'disciplinary_evaluation',
        'arbitration_review'
    ]
    
    # If any evaluation section has compliance = False, overall is False
    overall_compliance = all(
        evaluation_report.get(section, {}).get('compliance', True)
        for section in evaluation_sections
    )

    # Set risk level based on alerts
    risk_level = "Low"
    if any(alert.severity == AlertSeverity.HIGH for alert in alerts):
        risk_level = "High"
    elif any(alert.severity == AlertSeverity.MEDIUM for alert in alerts):
        risk_level = "Medium"

    # Add alert categories
    for alert in alerts:
        if not alert.alert_category:
            alert.alert_category = determine_alert_category(alert.alert_type)

    evaluation_report['final_evaluation'] = {
        'overall_compliance': overall_compliance,
        'overall_risk_level': risk_level,
        'recommendations': 'Immediate action required due to critical compliance issues.' if not overall_compliance else 'No immediate action required.',
        'alerts': [alert.to_dict() for alert in alerts]
    }


def perform_search(claim: dict, api_client: ApiClient) -> dict:
    """
    Executes the search based on the determined strategy. If the strategy
    is 'unknown_org', or if an error occurs, it returns a failed compliance
    state with appropriate alerts, always placing 'compliance' and 
    'compliance_explanation' first in the result.
    """
    # Start by building an OrderedDict with placeholders for compliance fields
    search_evaluation = OrderedDict([
        ('compliance', False),                  # default
        ('compliance_explanation', ''),         # will fill in based on outcome
        ('search_strategy', None),
        ('search_outcome', None),
        ('search_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
        ('cache_files', {}),
        # We will fill in or overwrite these fields as we discover info
    ])

    # Determine search strategy
    search_strategy = determine_search_strategy(claim, api_client)
    search_evaluation['search_strategy'] = search_strategy['strategy']

    employee_number = claim.get('employee_number')

    # 1) Handle the 'unknown_org' strategy or error from determine_search_strategy
    if search_strategy['strategy'] == 'unknown_org':
        error_message = search_strategy.get('error', 'Unknown error occurred while determining search strategy.')
        search_evaluation['search_outcome'] = error_message
        search_evaluation['compliance'] = False
        search_evaluation['compliance_explanation'] = error_message
        search_evaluation['alerts'] = [
            {
                "alert_type": "SearchStrategyError",
                "message": error_message,
                "severity": "HIGH",
                "alert_category": "SearchEvaluation"
            }
        ]
        return search_evaluation

    try:
        individual, detailed_info = None, None

        # 2) Strategy: basic_info
        if search_strategy['strategy'] == 'basic_info':
            crd_number = claim.get('crd_number', '').strip()
            search_evaluation['crd_number'] = crd_number

            if crd_number and crd_number.isdigit() and int(crd_number) > 0:
                crd_number = int(crd_number)
                log_diagnostic(f"Processing CRD {crd_number}")

                basic_info, basic_info_cache_file = api_client.get_individual_basic_info(
                    crd_number,
                    return_cache_filename=True,
                    employee_number=employee_number
                )
                search_evaluation['data_source'] = "BrokerCheck"
                search_evaluation['cache_files']['basic_info'] = basic_info_cache_file

                total_hits = basic_info.get('hits', {}).get('total', 0) if basic_info else 0
                if total_hits == 1:
                    individual = basic_info['hits']['hits'][0]['_source']
                    detailed_info, detailed_info_cache_file = api_client.get_individual_detailed_info(
                        crd_number,
                        return_cache_filename=True,
                        employee_number=employee_number
                    )
                    search_evaluation['cache_files']['detailed_info'] = detailed_info_cache_file
                    search_evaluation['search_outcome'] = "Record found"
                elif total_hits == 0:
                    search_evaluation['search_outcome'] = "No records found"
                else:
                    search_evaluation['search_outcome'] = f"Multiple records found ({total_hits})"
            else:
                search_evaluation['search_outcome'] = "Invalid or missing CRD value"

        # 3) Strategy: correlated_firm_info
        elif search_strategy['strategy'] == 'correlated_firm_info':
            individual_name = claim.get('name', '').strip()
            firm_crd = search_strategy['firm_crd']
            crd_number = claim.get('crd_number', '').strip()

            search_evaluation['individual_name'] = individual_name
            search_evaluation['firm_crd'] = firm_crd
            search_evaluation['crd_number'] = crd_number

            log_diagnostic(f"Processing individual '{individual_name}' with CRD '{crd_number}' at firm CRD {firm_crd}")

            # If CRD is numeric, treat it like a BrokerCheck search
            if crd_number and crd_number.isdigit() and int(crd_number) > 0:
                crd_number = int(crd_number)
                basic_info, basic_info_cache_file = api_client.get_individual_basic_info(
                    crd_number,
                    return_cache_filename=True,
                    employee_number=employee_number
                )
                search_evaluation['data_source'] = "BrokerCheck"
                search_evaluation['cache_files']['basic_info'] = basic_info_cache_file

                total_hits = basic_info.get('hits', {}).get('total', 0) if basic_info else 0
                if total_hits == 1:
                    individual = basic_info['hits']['hits'][0]['_source']
                    detailed_info, detailed_info_cache_file = api_client.get_individual_detailed_info(
                        crd_number,
                        service='sec',
                        return_cache_filename=True,
                        employee_number=employee_number
                    )
                    search_evaluation['cache_files']['detailed_info'] = detailed_info_cache_file
                    search_evaluation['search_outcome'] = "Record found"
                elif total_hits == 0:
                    search_evaluation['search_outcome'] = "No records found"
                else:
                    search_evaluation['search_outcome'] = f"Multiple records found ({total_hits})"

            # Otherwise, treat it like an IAPD search using the correlated firm CRD
            else:
                basic_info, basic_info_cache_file = api_client.get_individual_correlated_firm_info(
                    individual_name,
                    firm_crd,
                    return_cache_filename=True,
                    employee_number=employee_number
                )
                search_evaluation['data_source'] = "IAPD"
                search_evaluation['cache_files']['basic_info'] = basic_info_cache_file

                total_hits = basic_info.get('hits', {}).get('total', 0) if basic_info else 0
                if total_hits == 1:
                    individual = basic_info['hits']['hits'][0]['_source']
                    individual_id = individual.get('ind_source_id') 
                    if individual_id:
                        detailed_info, detailed_info_cache_file = api_client.get_individual_detailed_info(
                            individual_id,
                            service='sec',
                            return_cache_filename=True,
                            employee_number=employee_number
                        )
                        search_evaluation['cache_files']['detailed_info'] = detailed_info_cache_file
                        search_evaluation['search_outcome'] = "Record found"
                    else:
                        search_evaluation['search_outcome'] = "individualId not found in API response"
                elif total_hits == 0:
                    search_evaluation['search_outcome'] = "No matching individual found"
                else:
                    search_evaluation['search_outcome'] = f"Multiple records found ({total_hits})"

        # 4) Any unrecognized strategy
        else:
            search_evaluation['search_outcome'] = f"Unsupported search strategy: {search_strategy['strategy']}"

    except RateLimitExceeded as e:
        logging.error(str(e))
        logging.info("Processed records before rate limiting.")
        save_checkpoint()
        sys.exit(1)
    except Exception as e:
        logging.exception("An unexpected error occurred during search.")
        search_evaluation['search_outcome'] = "Search failed due to an error"

    # 5) Determine if record was found
    #    (We only consider "Record found" as a True compliance)
    search_compliance = (search_evaluation.get('search_outcome') == "Record found")
    search_evaluation['compliance'] = search_compliance

    # Provide a final compliance_explanation if none has been set
    # or simply reuse the 'search_outcome' as the explanation.
    if not search_evaluation['compliance_explanation']:
        explanation = search_evaluation.get('search_outcome', 'No explanation available')
        search_evaluation['compliance_explanation'] = explanation

    # If record is found, stash the data we retrieved
    if search_compliance:
        search_evaluation['individual'] = individual
        search_evaluation['detailed_info'] = detailed_info
        # If data_source wasn't set earlier, default to 'Unknown'
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
    """
    Performs various compliance checks (status, name, license, exam, disclosures)
    and updates evaluation_report accordingly. Uses OrderedDict to ensure
    'compliance' and 'compliance_explanation' appear first.
    """

    alternate_names = extracted_info.get('other_names', [])

    # 1) REGISTRATION STATUS EVALUATION
    if not extracted_info.get('individual'):
        logging.warning("No individual data available for status evaluation")
        evaluation_report['status_evaluation'] = OrderedDict([
            ('compliance', False),
            ('compliance_explanation', "No individual data available for status evaluation."),
            ('alerts', ["No data found for status evaluation"]),
        ])
        return  # Stop further checks if we have no data at all

    status_compliant, status_alerts = evaluate_registration_status(extracted_info['individual'])
    evaluation_report['status_evaluation'] = OrderedDict([
        ('compliance', status_compliant),
        ('compliance_explanation',
            "The individual was found and registration status is valid."
            if status_compliant
            else "The individual was found, but registration status check failed."
        ),
        ('alerts', status_alerts),
    ])
    alerts.extend(status_alerts)

    # 2) NAME EVALUATION (renamed key to "name_evaluation")
    name = f"{claim.get('first_name', '')} {claim.get('last_name', '')}".strip()

    if config.get('evaluate_name', True):
        evaluation_details, name_alert = evaluate_name(
            name,
            extracted_info.get('fetched_name', ''),
            extracted_info.get('other_names', [])
        )
        # Determine compliance from evaluation_details (e.g., name_match)
        name_compliance = evaluation_details.get('name_match', False)
        name_explanation = (
            "Name matches the fetched record."
            if name_compliance
            else "Name mismatch detected."
        )

        # Build an OrderedDict with compliance info first using "name_evaluation"
        evaluation_report['name_evaluation'] = OrderedDict([
            ('compliance', name_compliance),
            ('compliance_explanation', name_explanation),
            ('expected_name', name),
        ])
        # Preserve any additional fields from evaluation_details
        for k, v in evaluation_details.items():
            if k not in ('name_match'):
                evaluation_report['name_evaluation'][k] = v

        if name_alert:
            alerts.append(name_alert)
    else:
        # If name evaluation is skipped by configuration, mark compliance as True.
        evaluation_report['name_evaluation'] = OrderedDict([
            ('compliance', True),
            ('compliance_explanation', "Name evaluation was skipped by configuration."),
            ('evaluation_skipped', True),
            ('expected_name', name),
        ])

    # 3) LICENSE EVALUATION
    if config.get('evaluate_license', True):
        license_type = claim.get('license_type', '')
        bc_scope = extracted_info.get('bc_scope', '')
        ia_scope = extracted_info.get('ia_scope', '')
        license_compliant, license_alert = evaluate_license(license_type, bc_scope, ia_scope, name)

        evaluation_report['license_evaluation'] = OrderedDict([
            ('compliance', license_compliant),
            ('compliance_explanation',
             "The individual holds an active license." if license_compliant
             else "License compliance failed."),
        ])
        if license_alert:
            alerts.append(license_alert)
    else:
        # If license evaluation is skipped, mark compliance as True.
        evaluation_report['license_evaluation'] = OrderedDict([
            ('compliance', True),
            ('compliance_explanation', "License evaluation was skipped by configuration."),
            ('evaluation_skipped', True),
        ])

    # 4) EXAM EVALUATION
    if config.get('evaluate_exams', True):
        exams = extracted_info.get('exams', [])
        if exams:
            try:
                passed_exams = get_passed_exams(exams)
                license_type = claim.get('license_type', '')
                exam_compliant, exam_alert = evaluate_exams(passed_exams, license_type, name)

                evaluation_report['exam_evaluation'] = OrderedDict([
                    ('compliance', exam_compliant),
                    ('compliance_explanation',
                     "The individual has passed all required exams." if exam_compliant
                     else "Exam compliance failed."),
                ])
                if exam_alert:
                    alerts.append(exam_alert)

            except Exception as e:
                logging.warning(f"Failed to evaluate exams: {e}")
                evaluation_report['exam_evaluation'] = OrderedDict([
                    # When evaluation is skipped due to error, mark as compliant.
                    ('compliance', True),
                    ('compliance_explanation', "Failed to fully evaluate exams due to an error."),
                    ('evaluation_skipped', True),
                    ('reason', 'Failed to evaluate exams.'),
                ])
        else:
            # If no exam data is available, mark evaluation as skipped and compliant.
            evaluation_report['exam_evaluation'] = OrderedDict([
                ('compliance', True),
                ('compliance_explanation', "No exam information available."),
                ('evaluation_skipped', True),
                ('reason', 'Exams information not available.'),
            ])
    else:
        # If exam evaluation is off in config, mark as skipped and compliant.
        evaluation_report['exam_evaluation'] = OrderedDict([
            ('compliance', True),
            ('compliance_explanation', "Exam evaluation was skipped by configuration."),
            ('evaluation_skipped', True),
        ])

    # 5) DISCLOSURE REVIEW
    if config.get('evaluate_disclosures', True):
        disclosures = extracted_info.get('disclosures', [])
        if disclosures:
            try:
                disclosure_compliance, disclosure_summary, disclosure_alerts = evaluate_disclosures(disclosures, name)
                evaluation_report['disclosure_review'] = OrderedDict([
                    ('compliance', disclosure_compliance),
                    ('compliance_explanation', disclosure_summary if disclosure_summary else "No disclosure summary provided."),
                ])
                alerts.extend(disclosure_alerts)
            except Exception as e:
                logging.warning(f"Failed to evaluate disclosures: {e}")
                evaluation_report['disclosure_review'] = OrderedDict([
                    ('compliance', True),
                    ('compliance_explanation', "Failed to evaluate disclosures due to an error."),
                    ('evaluation_skipped', True),
                    ('reason', 'Failed to evaluate disclosures.'),
                ])
        else:
            evaluation_report['disclosure_review'] = OrderedDict([
                ('compliance', True),
                ('compliance_explanation', "No disclosures information available."),
                ('evaluation_skipped', True),
                ('reason', 'Disclosures information not available.'),
            ])
    else:
        evaluation_report['disclosure_review'] = OrderedDict([
            ('compliance', True),
            ('compliance_explanation', "Disclosure review was skipped by configuration."),
            ('evaluation_skipped', True),
        ])


def process_row(row, resolved_headers):
    global records_written

    # ------------------------------
    # Feature flags for toggling checks
    # ------------------------------
    DISCIPLINARY_ENABLED = True
    ARBITRATION_ENABLED = True

    # 1) Extract the reference_id separately
    reference_id = row.get(resolved_headers.get('reference_id', ''), '').strip()

    # 2) If reference_id is missing, generate one
    if not reference_id:
        reference_id = generate_reference_id()

    # 3) Initialize the claim dictionary
    claim = {}

    # 4) Populate claim fields dynamically based on the canonical model
    for canonical_field, variations in canonical_fields.items():
        # Find the header for this canonical field in the resolved_headers
        header = resolved_headers.get(canonical_field)
        # Set the value in the claim, or default to None if not found
        claim[canonical_field] = row.get(header, '').strip() if header else None

    first_name = claim.get('first_name', '').strip()
    last_name = claim.get('last_name', '').strip()
    claim['name'] = f"{first_name} {last_name}".strip()

    # 5) Ensure employee_number defaults to the reference_id if missing
    if not claim.get('employee_number'):
        claim['employee_number'] = reference_id

    # 6) Perform the search based on the strategy
    search_evaluation = perform_search(claim, api_client)

    # 7) Prepare our evaluation report as an OrderedDict
    evaluation_report = OrderedDict()
    evaluation_report['reference_id'] = reference_id
    evaluation_report['claim'] = claim
    evaluation_report['search_evaluation'] = search_evaluation

    alerts = []

    # 8) If the strategy is unknown_org, generate a HIGH-severity alert
    if search_evaluation.get('search_strategy') == 'unknown_org':
        unknown_org_alert = Alert(
            alert_type="OrganizationNotIndexed",
            severity=AlertSeverity.HIGH,
            alert_category="SearchEvaluation",
            metadata={},
            description="Neither CRD nor organization was provided, and no CRD was found in firms.json."
        )
        alerts.append(unknown_org_alert)

    # 9) If we failed to find a record ("search_compliance" == False), skip further evaluations
    if not search_evaluation['compliance']:
        build_final_evaluation(evaluation_report, alerts)
        save_evaluation_report(evaluation_report, claim.get('employee_number', 'unknown'), reference_id)
        records_written += 1
        return

    # 10) If we did find a record, continue with further evaluations
    individual = search_evaluation['individual']
    data_source = search_evaluation['data_source']
    detailed_info = search_evaluation.get('detailed_info')

    # Extract additional individual info
    data_handler = DataSourceHandler(data_source)
    extracted_info = data_handler.extract_individual_info(individual, detailed_info)

    # -----------------------------------------------------------------------
    # Disciplinary checks (toggle with DISCIPLINARY_ENABLED)
    # -----------------------------------------------------------------------
    if DISCIPLINARY_ENABLED:
        disciplinary_records_full = api_client.get_finra_disciplinary_actions(
            employee_number=claim.get('employee_number'),
            first_name=claim.get('first_name', ''),
            last_name=claim.get('last_name', ''),
            alternate_names=extracted_info.get('other_names', [])
        )
        disciplinary_records = [value["data"] for value in disciplinary_records_full.values()]

        disciplinary_compliance, disciplinary_explanation, disciplinary_alerts = evaluate_disciplinary(
            disciplinary_records,
            claim.get('name')
        )

        # Convert disciplinary_evaluation to OrderedDict
        evaluation_report['disciplinary_evaluation'] = OrderedDict([
            ('compliance', disciplinary_compliance),
            ('compliance_explanation', disciplinary_explanation),
            ('disciplinary_records', disciplinary_records),
            ('alerts', [alert.to_dict() for alert in disciplinary_alerts]),
        ])
        alerts.extend(disciplinary_alerts)
    else:
        # If disciplinary checks are disabled, just note it in the report
        evaluation_report['disciplinary_evaluation'] = OrderedDict([
            ('compliance', True),
            ('compliance_explanation', "Disciplinary checks were skipped by configuration."),
            ('disciplinary_records', []),
            ('alerts', []),
        ])

    # Perform standard evaluations (name, license, exam, disclosures, etc.)
    perform_evaluations(evaluation_report, extracted_info, claim, alerts)

    # -----------------------------------------------------------------------
    # Arbitration checks (toggle with ARBITRATION_ENABLED)
    # -----------------------------------------------------------------------
    if ARBITRATION_ENABLED:
        arbitration_records_full = api_client.get_finra_arbitrations(
            employee_number=claim.get('employee_number'),
            first_name=claim.get('first_name', ''),
            last_name=claim.get('last_name', ''),
            alternate_names=extracted_info.get('other_names', [])
        )
        # Flatten the returned data
        arbitration_records = [value["data"] for value in arbitration_records_full.values()]

        # Evaluate these arbitration records
        arbitration_compliance, arbitration_explanation, arbitration_alerts = evaluate_arbitration(
            arbitration_records,
            claim.get('name')
        )

    if ARBITRATION_ENABLED:
        arbitration_records_full = api_client.get_finra_arbitrations(
            employee_number=claim.get('employee_number'),
            first_name=claim.get('first_name', ''),
            last_name=claim.get('last_name', ''),
            alternate_names=extracted_info.get('other_names', [])
        )
        # Flatten the returned data
        arbitration_records = [value["data"] for value in arbitration_records_full.values()]

        # Evaluate these arbitration records
        arbitration_compliance, arbitration_explanation, arbitration_alerts = evaluate_arbitration(
            arbitration_records,
            claim.get('name')
        )

        # -------- NEW ARBITRATION_REVIEW BLOCK (like disclosure_review) --------
        evaluation_report['arbitration_review'] = OrderedDict([
            ('compliance', arbitration_compliance),
            ('compliance_explanation', arbitration_explanation),
            ('arbitrations', arbitration_records),  # The raw data or partial data
            ('alerts', [alert.to_dict() for alert in arbitration_alerts]),
        ])
        alerts.extend(arbitration_alerts)
    else:
        # If arbitration checks are disabled, just note it in the report
        evaluation_report['arbitration_review'] = OrderedDict([
            ('compliance', True),
            ('compliance_explanation', "Arbitration checks were skipped by configuration."),
            ('arbitrations', []),
            ('alerts', []),
        ])
        evaluation_report['arbitration_evaluation'] = OrderedDict([
            ('compliance', True),
            ('compliance_explanation', "Arbitration checks were skipped by configuration."),
            ('arbitrations', []),
        ])


    # 11) Build the final evaluation, then save the report
    build_final_evaluation(evaluation_report, alerts)
    save_evaluation_report(evaluation_report, claim['employee_number'], reference_id)
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
