"""
Main configuration module.

This module defines the main configuration settings for the application.
"""

import json
import logging
import os
from typing import Dict, Any, Optional

logger = logging.getLogger('main_config')

DEFAULT_WAIT_TIME = 7.0

DEFAULT_CONFIG = {
    "evaluate_name": True,
    "skip_disciplinary": False,
    "skip_arbitration": False,
    "skip_regulatory": False,
    "storage": {
        "mode": "local",
        "local": {
            "input_folder": "drop",
            "output_folder": "output",
            "archive_folder": "archive",
            "cache_folder": "cache"
        },
        "s3": {
            "aws_region": "us-east-1",
            "input_bucket": "",
            "input_prefix": "input/",
            "output_bucket": "",
            "output_prefix": "output/",
            "archive_bucket": "",
            "archive_prefix": "archive/",
            "cache_bucket": "",
            "cache_prefix": "cache/"
        }
    }
}

canonical_fields = {
    'reference_id': ['referenceId', 'Reference ID', 'reference_id', 'ref_id', 'RefID'],  # Removed 'workProductNumber'
    'work_product_number': ['workProductNumber', 'Work Product Number', 'work_product_number', 'workProductNo'],  # New field
    'crd_number': ['CRD', 'crd_number', 'crd', 'CRD Number', 'CRDNumber', 'crdnumber', 'individualCRDNumber'],
    'first_name': ['firstName', 'First Name', 'first_name', 'fname', 'FirstName', 'first'],
    'middle_name': ['middle_name', 'Middle Name', 'middlename', 'MiddleName', 'middle', 'middleName'],
    'last_name': ['lastName', 'Last Name', 'last_name', 'lname', 'LastName', 'last'],
    'employee_number': ['employeeNumber', 'Employee Number', 'employee_number', 'emp_id', 'employeenumber'],
    'license_type': ['license_type', 'License Type', 'licensetype', 'LicenseType', 'license'],
    'organization_name': ['orgName', 'Organization Name', 'organization_name', 'firm_name', 'organizationname', 'OrganizationName', 'organization', 'organizationName'],
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

DISCIPLINARY_ENABLED = True
ARBITRATION_ENABLED = True

INPUT_FOLDER = "drop"
OUTPUT_FOLDER = "output"
ARCHIVE_FOLDER = "archive"
CHECKPOINT_FILE = os.path.join(OUTPUT_FOLDER, "checkpoint.json")
CONFIG_FILE = "config.json"

def deep_merge(base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two dictionaries.
    
    Args:
        base: Base dictionary
        update: Dictionary to merge on top of base
        
    Returns:
        Merged dictionary
    """
    result = base.copy()
    for key, value in update.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result

def load_config(config_path: str = CONFIG_FILE) -> Dict[str, Any]:
    """
    Load configuration from file.
    
    Args:
        config_path: Path to the configuration file.
        
    Returns:
        Dictionary containing the configuration.
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            return deep_merge(DEFAULT_CONFIG, config)
    except FileNotFoundError:
        logger.warning("Config file not found, using defaults")
        return DEFAULT_CONFIG.copy()
    except Exception as e:
        logger.error(f"Error loading config file {config_path}: {str(e)}")
        return DEFAULT_CONFIG.copy()

def save_config(config: Dict[str, Any], config_path: str = CONFIG_FILE):
    """
    Save configuration to file.
    
    Args:
        config: Configuration dictionary to save.
        config_path: Path to save the configuration to.
    """
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        logger.info(f"Saved settings to {config_path}: {config}")
    except Exception as e:
        logger.error(f"Error saving config to {config_path}: {str(e)}")

def get_storage_config(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Get storage configuration from the main config.
    
    Args:
        config: Optional configuration dictionary. If not provided,
               loads from config file or uses defaults.
               
    Returns:
        Dictionary containing storage configuration
    """
    if config is None:
        config = load_config()
    
    logger.debug(f"Getting storage config from: {json.dumps(config, indent=2)}")
    storage_config = config.get('storage', DEFAULT_CONFIG['storage'])
    logger.debug(f"Retrieved storage config: {json.dumps(storage_config, indent=2)}")
    
    # Ensure the storage config has all required sections
    if 'mode' not in storage_config:
        storage_config['mode'] = DEFAULT_CONFIG['storage']['mode']
    if 'local' not in storage_config:
        storage_config['local'] = DEFAULT_CONFIG['storage']['local']
    if 's3' not in storage_config:
        storage_config['s3'] = DEFAULT_CONFIG['storage']['s3']
        
    # Ensure local config has all required fields
    local_config = storage_config['local']
    for key in ['input_folder', 'output_folder', 'archive_folder', 'cache_folder']:
        if key not in local_config:
            local_config[key] = DEFAULT_CONFIG['storage']['local'][key]
            
    # Ensure S3 config has all required fields
    s3_config = storage_config['s3']
    for key in ['aws_region', 'input_bucket', 'input_prefix', 'output_bucket', 'output_prefix',
                'archive_bucket', 'archive_prefix', 'cache_bucket', 'cache_prefix']:
        if key not in s3_config:
            s3_config[key] = DEFAULT_CONFIG['storage']['s3'][key]
            
    return storage_config