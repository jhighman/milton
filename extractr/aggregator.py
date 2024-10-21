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

    # Define the CSV headers
    csv_headers = [
        'crd_number',
        'overall_compliance',
        'disclosure_alerts',
        'exam_compliance',
        'license_compliance',
        'alerts_count'
    ]

    # Get a list of all JSON files in the output directory
    try:
        json_files = [file for file in os.listdir(output_folder) if file.endswith('.json')]
        logger.info(f"Found {len(json_files)} JSON files in '{output_folder}'")
    except Exception as e:
        logger.error(f"Error accessing output folder '{output_folder}': {e}")
        return

    if not json_files:
        logger.warning(f"No JSON files found in the output folder '{output_folder}'")
        return

    # Open the CSV file for writing
    try:
        with open(csv_report_path, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
            writer.writeheader()

            # Process each JSON file
            for json_file in json_files:
                json_file_path = os.path.join(output_folder, json_file)
                try:
                    with open(json_file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    # Extract the required data with validation
                    crd_number = data.get('crd_number', '')
                    if not crd_number:
                        logger.warning(f"CRD number missing in file '{json_file}'")
                        continue  # Skip this record

                    # Extract evaluations with default values if missing
                    final_evaluation = data.get('final_evaluation', {})
                    overall_compliance = final_evaluation.get('overall_compliance', '')
                    if overall_compliance == '':
                        logger.warning(f"Overall compliance missing for CRD {crd_number} in file '{json_file}'")

                    disclosure_review = data.get('disclosure_review', {})
                    disclosure_alerts = disclosure_review.get('disclosure_alerts', '')
                    if disclosure_alerts == '':
                        logger.warning(f"Disclosure alerts missing for CRD {crd_number} in file '{json_file}'")

                    exam_evaluation = data.get('exam_evaluation', {})
                    exam_compliance = exam_evaluation.get('exam_compliance', '')
                    if exam_compliance == '':
                        logger.warning(f"Exam compliance missing for CRD {crd_number} in file '{json_file}'")

                    license_verification = data.get('license_verification', {})
                    license_compliance = license_verification.get('license_compliance', '')
                    if license_compliance == '':
                        logger.warning(f"License compliance missing for CRD {crd_number} in file '{json_file}'")

                    # Count the number of alerts
                    alerts = final_evaluation.get('alerts', [])
                    if not isinstance(alerts, list):
                        logger.warning(f"Alerts format invalid for CRD {crd_number} in file '{json_file}'")
                        alerts_count = 0
                    else:
                        alerts_count = len(alerts)

                    # Create a row for the CSV
                    row = {
                        'crd_number': crd_number,
                        'overall_compliance': overall_compliance,
                        'disclosure_alerts': disclosure_alerts,
                        'exam_compliance': exam_compliance,
                        'license_compliance': license_compliance,
                        'alerts_count': alerts_count
                    }

                    # Write the row to the CSV file
                    writer.writerow(row)
                    logger.info(f"Processed CRD {crd_number} from file '{json_file}'")

                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error in file '{json_file}': {e}")
                except Exception as e:
                    logger.error(f"Error processing file '{json_file}': {e}")

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
