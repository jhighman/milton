#!/usr/bin/env python3

import os
import json
import csv
import argparse
import logging
from typing import List, Dict, Any

def setup_logger():
    """
    Sets up the logging configuration.
    """
    logger = logging.getLogger('Aggregator')
    logger.setLevel(logging.INFO)

    # Create console handler and set level to info
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Add formatter to ch
    ch.setFormatter(formatter)

    # Add ch to logger
    logger.addHandler(ch)

    return logger

def aggregate_reports(output_folder: str, csv_report_path: str):
    """
    Aggregates JSON evaluation reports into a single CSV file.

    Args:
        output_folder (str): Path to the directory containing JSON files.
        csv_report_path (str): Path where the CSV report will be saved.
    """
    logger = logging.getLogger('Aggregator')

    # Define the base CSV headers in the specified order
    base_headers = [
        'employee_number', 'crd', 'organization_name', 'search_strategy', 'search_compliance', 
        'overall_compliance', 'name_match', 'license_compliance', 'exam_compliance',
        'status_compliance', 'disclosure_compliance', 'alerts_count', 'license_type', 
        'license_issued_date', 'license_expiration_date', 'license_status', 'license_scope', 
        'required_exams', 'passed_exams', 'exam_issues', 'current_registration_status', 
        'registration_dates', 'registration_type', 'total_disclosures', 'highest_alert_severity', 
        'alert_details', 'data_source', 'evaluation_date', 'evaluator_id', 'evaluation_notes'
    ]

    disclosure_types = set()  # To dynamically collect all unique disclosure types

    # Step 1: Identify all unique disclosure types across all JSON files
    try:
        json_files = [file for file in os.listdir(output_folder) if file.endswith('.json')]
        logger.info(f"Found {len(json_files)} JSON files in '{output_folder}'")
    except Exception as e:
        logger.error(f"Error accessing output folder '{output_folder}': {e}")
        return

    # First pass through JSON files to gather all disclosure types
    for json_file in json_files:
        json_file_path = os.path.join(output_folder, json_file)
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            disclosures = data.get('disclosure_review', {}).get('disclosures', [])
            for disclosure in disclosures:
                disclosure_type = disclosure.get('type')
                if disclosure_type:
                    disclosure_types.add(disclosure_type)  # Add to the set of disclosure types
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in file '{json_file}': {e}")
        except Exception as e:
            logger.error(f"Error processing file '{json_file}': {e}")

    # Finalize CSV headers including dynamically collected disclosure fields
    csv_headers = base_headers + sorted(disclosure_types)

    # Store rows to ensure they are ordered by employee_number
    rows = []

    # Step 2: Collect all data rows
    for json_file in json_files:
        employee_number = os.path.splitext(json_file)[0]  # Extract employee_number from filename
        json_file_path = os.path.join(output_folder, json_file)
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Extract data from claim and search_evaluation sections
            claim = data.get('claim', {})
            search_evaluation = data.get('search_evaluation', {})

            # Determine the CRD number and organization name
            crd_number = claim.get('crd', '')  # Default to claim CRD
            if search_evaluation.get('search_strategy') == 'correlated_firm_info':
                crd_number = search_evaluation.get('firm_crd', crd_number)  # Override with search_evaluation CRD if present
            organization_name = search_evaluation.get('organization_name', claim.get('organization_name', ''))

            # Extract remaining main evaluation data points
            final_evaluation = data.get('final_evaluation', {})
            overall_compliance = final_evaluation.get('overall_compliance', '')
            search_strategy = search_evaluation.get('search_strategy', '')
            search_compliance = search_evaluation.get('search_compliance', '')

            name_match = data.get('name', {}).get('name_match', '')
            license_compliance = data.get('license_verification', {}).get('license_compliance', '')
            exam_compliance = data.get('exam_evaluation', {}).get('exam_compliance', '')
            status_compliance = data.get('registration_status', {}).get('status_compliance', '')
            disclosure_compliance = data.get('disclosure_review', {}).get('disclosure_compliance', '')

            # Count the number of alerts
            alerts = final_evaluation.get('alerts', [])
            alerts_count = len(alerts) if isinstance(alerts, list) else 0
            highest_alert_severity = max((alert.get('severity', 'Low') for alert in alerts), default='Low')
            alert_details = ", ".join(set(alert.get('description', 'Unknown') for alert in alerts))

            # Initialize all disclosure fields to False
            disclosure_flat_record = {disclosure_type: False for disclosure_type in disclosure_types}
            
            # Update disclosure fields to True if they are present in the JSON report
            disclosures = data.get('disclosure_review', {}).get('disclosures', [])
            for disclosure in disclosures:
                disclosure_type = disclosure.get('type')
                if disclosure_type in disclosure_flat_record:
                    disclosure_flat_record[disclosure_type] = True  # Set to True if present
                else:
                    logger.warning(f"Unexpected disclosure type '{disclosure_type}' found in file '{json_file}'")

            # Log any missing disclosures for this report
            missing_disclosures = [d for d, present in disclosure_flat_record.items() if not present]
            if missing_disclosures:
                logger.info(f"File '{json_file}' is missing the following disclosures: {', '.join(missing_disclosures)}")

            # Prepare the row with all data fields
            row = {
                'employee_number': employee_number,
                'crd': crd_number,
                'organization_name': organization_name,
                'search_strategy': search_strategy,
                'search_compliance': search_compliance,
                'overall_compliance': overall_compliance,
                'name_match': name_match,
                'license_compliance': license_compliance,
                'exam_compliance': exam_compliance,
                'status_compliance': status_compliance,
                'disclosure_compliance': disclosure_compliance,
                'alerts_count': alerts_count,
                'highest_alert_severity': highest_alert_severity,
                'alert_details': alert_details,
                'license_type': data.get('license_verification', {}).get('license_type', ''),
                'license_issued_date': data.get('license_verification', {}).get('license_issued_date', ''),
                'license_expiration_date': data.get('license_verification', {}).get('license_expiration_date', ''),
                'license_status': data.get('license_verification', {}).get('license_status', ''),
                'license_scope': data.get('license_verification', {}).get('license_scope', ''),
                'required_exams': ", ".join(data.get('exam_evaluation', {}).get('required_exams', [])),
                'passed_exams': ", ".join(data.get('exam_evaluation', {}).get('passed_exams', [])),
                'exam_issues': data.get('exam_evaluation', {}).get('exam_issues', False),
                'current_registration_status': data.get('registration_status', {}).get('current_registration_status', ''),
                'registration_dates': data.get('registration_status', {}).get('registration_dates', ''),
                'registration_type': data.get('registration_status', {}).get('registration_type', ''),
                'total_disclosures': len(disclosures),
                'data_source': data.get('data_source', ''),
                'evaluation_date': data.get('evaluation_date', ''),
                'evaluator_id': data.get('evaluator_id', ''),
                'evaluation_notes': data.get('evaluation_notes', ''),
                **disclosure_flat_record  # Add the flattened disclosure fields
            }

            rows.append(row)
            logger.info(f"Processed CRD {crd_number} from file '{json_file}'")

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in file '{json_file}': {e}")
        except Exception as e:
            logger.error(f"Error processing file '{json_file}': {e}")

    # Sort rows by employee_number (derived from filename)
    rows.sort(key=lambda x: x['employee_number'])

    # Step 3: Write all rows to the CSV file
    try:
        with open(csv_report_path, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
            writer.writeheader()
            writer.writerows(rows)

        logger.info(f"Aggregated CSV report created at '{csv_report_path}'")

    except Exception as e:
        logger.error(f"Error writing CSV report to '{csv_report_path}': {e}")

def parse_arguments():
    """
    Parses command-line arguments.
    """
    parser = argparse.ArgumentParser(description='Aggregate JSON evaluation reports into a CSV file.')
    parser.add_argument('--output-folder', type=str, default='./output', help='Path to the output folder containing JSON files (default: ./output)')
    parser.add_argument('--csv-report', type=str, default='./aggregated_report.csv', help='Path to save the aggregated CSV report (default: ./aggregated_report.csv)')
    return parser.parse_args()

if __name__ == "__main__":
    # Set up logger
    logger = setup_logger()

    # Parse command-line arguments
    args = parse_arguments()

    # Run the aggregator
    aggregate_reports(output_folder=args.output_folder, csv_report_path=args.csv_report)
