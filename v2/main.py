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

# Canonical field mappings
canonical_fields = {
    'reference_id': ['referenceId', 'Reference ID', 'reference_id', 'ref_id', 'RefID'],
    'crd_number': ['CRD', 'crd_number', 'crd', 'CRD Number'],
    'first_name': ['firstName', 'First Name', 'first_name', 'fname', 'FirstName'],
    'last_name': ['lastName', 'Last Name', 'last_name', 'lname', 'LastName'],
    'organization_name': ['orgName', 'Organization Name', 'organization_name', 'firm_name'],
    'organization_crd_number': ['orgCRD', 'Organization CRD', 'org_crd_number', 'firm_crd'],
    'employee_number': ['employeeNumber', 'Employee Number', 'employee_number', 'emp_id']
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
    checkpoint_path = str(CHECKPOINT_FILE)  # Force string to avoid Path/None issues
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
            logger.warning(f"Canonical field '{canonical}' not found in CSV headers")
    return resolved_headers

def process_csv(csv_file_path: str, start_line: int, facade: FinancialServicesFacade, config: Dict[str, bool], wait_time: float):
    """Process a CSV file starting from the given line."""
    global current_csv, current_line
    current_csv = os.path.basename(csv_file_path)
    current_line = 0  # Reset to avoid None creep
    logger.info(f"Starting to process {csv_file_path} from line {start_line}")

    with open(csv_file_path, 'r') as f:
        reader = csv.DictReader(f)
        resolved_headers = resolve_headers(reader.fieldnames)
        logger.debug(f"Resolved headers: {resolved_headers}")

        for i, row in enumerate(reader, start=2):  # Start at 2 to account for header
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
    """Process a single CSV row and generate an evaluation report."""
    logger.debug(f"Processing row: {row}")
    # Extract reference_id
    reference_id_key = resolved_headers.get('reference_id', 'reference_id')
    reference_id = row.get(reference_id_key, '').strip()
    if not reference_id:
        logger.error(f"Skipping row - missing or blank reference_id")
        return

    # Extract employee_number
    employee_number_key = resolved_headers.get('employee_number', 'employee_number')
    employee_number = row.get(employee_number_key, '').strip()
    if not employee_number:
        logger.error(f"Skipping row - missing or blank employee_number for reference_id='{reference_id}'")
        return

    logger.debug(f"Reference ID: {reference_id}, Employee Number: {employee_number}")

    # Build claim dictionary
    claim = {}
    for header, canonical in resolved_headers.items():
        claim[canonical] = row.get(header, '').strip()
    logger.debug(f"Claim built: {claim}")

    # Add individual_name from first_name and last_name
    first_name = claim.get('first_name', '')
    last_name = claim.get('last_name', '')
    claim['individual_name'] = f"{first_name} {last_name}".strip() if first_name or last_name else ""
    claim['employee_number'] = employee_number
    logger.debug(f"Updated claim with individual_name: {claim['individual_name']}")

    # Perform search via business.py
    try:
        detailed_info = process_claim(claim, facade, employee_number)
        if detailed_info is None:
            logger.error(f"process_claim returned None for reference_id='{reference_id}'")
            return
        logger.debug(f"Detailed info from process_claim: {detailed_info}")
        evaluation_report = OrderedDict([
            ("reference_id", reference_id),
            ("claim", claim),
            ("search_evaluation", {
                **detailed_info["search_evaluation"],
                "detailed_info": detailed_info
            })
        ])
    except ValueError as e:
        logger.error(f"Unresolved CRD for {reference_id}: {str(e)}")
        evaluation_report = OrderedDict([
            ("reference_id", reference_id),
            ("claim", claim),
            ("search_evaluation", {
                "compliance": False,
                "search_strategy": "unknown",
                "search_outcome": str(e),
                "detailed_info": {}
            })
        ])
        save_evaluation_report(evaluation_report, employee_number, reference_id)
        return

    # Additional evaluations (stubs)
    alerts = []
    if config["evaluate_license"]:
        license_status = "Passed Series 7"  # Stub
        evaluation_report["license_status"] = license_status
        logger.debug("Added license_status stub")

    if DISCIPLINARY_ENABLED:
        disciplinary = "None"  # Stub
        if disciplinary != "None":
            alerts.append("Disciplinary action found")
        evaluation_report["disciplinary"] = disciplinary
        logger.debug("Added disciplinary stub")

    if ARBITRATION_ENABLED:
        arbitration = "None"  # Stub
        if arbitration != "None":
            alerts.append("Arbitration issue found")
        evaluation_report["arbitration"] = arbitration
        logger.debug("Added arbitration stub")

    # Final evaluation
    evaluation_report["overall_compliance"] = evaluation_report["search_evaluation"]["compliance"] and not alerts
    evaluation_report["alerts"] = alerts or [evaluation_report["search_evaluation"]["search_outcome"] if not evaluation_report["search_evaluation"]["compliance"] else ""]
    evaluation_report["risk_level"] = "High" if alerts else "Low" if evaluation_report["overall_compliance"] else "Medium"
    logger.debug(f"Evaluation report built: {evaluation_report}")

    save_evaluation_report(evaluation_report, employee_number, reference_id)

def save_evaluation_report(report: Dict[str, Any], employee_number: str, reference_id: str):
    """Save the evaluation report as a JSON file."""
    report_path = os.path.join(OUTPUT_FOLDER, f"{reference_id}.json")
    logger.debug(f"Saving report to {report_path}")
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    compliance = report.get('search_evaluation', {}).get('compliance', False)
    logger.info(f"Processed {reference_id}, compliance: {compliance}")

def main():
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(description="Compliance CSV Processor")
    parser.add_argument('--diagnostic', action='store_true', help="Enable verbose debug logging")
    parser.add_argument('--wait-time', type=float, default=7.0, help="Seconds to wait between API calls")
    args = parser.parse_args()

    if args.diagnostic:
        logger.setLevel(logging.DEBUG)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    setup_folders()
    config = load_config()
    facade = FinancialServicesFacade()

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
        # Count records before archiving
        with open(csv_path, 'r') as f:
            processed_records += sum(1 for _ in csv.reader(f)) - 1  # Minus header
        archive_file(csv_path)
        processed_files += 1
        start_line = 0

    logger.info(f"Processed {processed_files} files, {processed_records} records")
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

if __name__ == "__main__":
    main()