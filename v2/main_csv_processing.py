"""
CSV processing module.

This module handles CSV file processing and data extraction.
"""

import csv
import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple, Set
from collections import defaultdict
from enum import Enum
from main_config import INPUT_FOLDER, OUTPUT_FOLDER, ARCHIVE_FOLDER, canonical_fields
from main_file_utils import save_checkpoint
from business import process_claim
from services import FinancialServicesFacade
from evaluation_report_builder import EvaluationReportBuilder
from evaluation_report_director import EvaluationReportDirector
from evaluation_processor import Alert
from storage_manager import StorageManager

logger = logging.getLogger('csv_processor')

class AlertEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Alert objects."""
    def default(self, obj):
        if isinstance(obj, Alert):
            return obj.to_dict()
        return super().default(obj)

def json_dumps_with_alerts(obj: Any, **kwargs) -> str:
    """Helper function to serialize objects that may contain Alert instances."""
    return json.dumps(obj, cls=AlertEncoder, **kwargs)

class SkipScenario(Enum):
    NO_NAME = "Missing both first/last names and individual name"
    NO_EMPLOYEE_NUMBER = "Missing employee number"
    NO_ORG_IDENTIFIERS = "Missing all organization identifiers (crd_number, organization_crd, organization_name)"

class CSVProcessor:
    """Handles CSV file processing and data extraction."""
    
    def __init__(self):
        """Initialize the CSV processor."""
        self.current_csv = None
        self.current_line = 0
        self.skipped_records: Set[str] = set()
        self.error_records = defaultdict(list)
        self.storage_manager = None

    def set_storage_manager(self, storage_manager: StorageManager):
        """Set the storage manager instance."""
        self.storage_manager = storage_manager

    def generate_reference_id(self, crd_number: str = None) -> str:
        if crd_number and crd_number.strip():
            return crd_number
        return f"DEF-{random.randint(100000000000, 999999999999)}"

    def resolve_headers(self, fieldnames: List[str]) -> Dict[str, str]:
        resolved_headers = {}
        logger.info(f"Raw fieldnames from CSV: {fieldnames}")
        for header in fieldnames:
            if not header.strip():
                logger.warning("Empty header name encountered")
                continue
            header_lower = header.lower().strip()
            logger.info(f"Processing header: '{header}' (lowercase: '{header_lower}')")
            for canonical, variants in canonical_fields.items():
                variants_lower = [v.lower().strip() for v in variants]
                logger.info(f"Checking against canonical '{canonical}' variants: {variants_lower}")
                if header_lower in variants_lower:
                    resolved_headers[header] = canonical
                    logger.info(f"Mapped header '{header}' to '{canonical}'")
                    break
            else:
                logger.warning(f"Unmapped CSV column: '{header}' will be included as-is")
                resolved_headers[header] = header
        logger.info(f"Resolved headers: {json_dumps_with_alerts(resolved_headers, indent=2)}")
        unmapped_canonicals = set(canonical_fields.keys()) - set(resolved_headers.values())
        if unmapped_canonicals:
            logger.info(f"Canonical fields not found in CSV headers: {unmapped_canonicals}")
        return resolved_headers

    def validate_row(self, claim: Dict[str, str]) -> Tuple[bool, List[str]]:
        issues = []
        first_name = claim.get('first_name', '').strip()
        last_name = claim.get('last_name', '').strip()
        individual_name = claim.get('individual_name', '').strip()
        if not (first_name and last_name) and not individual_name:
            issues.append(SkipScenario.NO_NAME.value)
        employee_number = claim.get('employee_number', '').strip()
        if not employee_number:
            issues.append(SkipScenario.NO_EMPLOYEE_NUMBER.value)
        crd_number = claim.get('crd_number', '').strip()
        org_crd = claim.get('organization_crd', '').strip()
        org_name = claim.get('organization_name', '').strip()
        if not (crd_number or org_crd or org_name):
            issues.append(SkipScenario.NO_ORG_IDENTIFIERS.value)
        return (len(issues) == 0, issues)

    def process_csv(self, csv_file: str, start_line: int = 0, facade: Any = None, config: Dict[str, Any] = None, wait_time: float = 0.0):
        """
        Process a CSV file.
        
        Args:
            csv_file: Path to the CSV file.
            start_line: Line number to start processing from.
            facade: Financial services facade instance.
            config: Configuration dictionary.
            wait_time: Time to wait between records.
        """
        if not self.storage_manager:
            raise ValueError("Storage manager not set")
        
        self.current_csv = csv_file
        self.current_line = start_line
        
        try:
            # Read CSV file
            content = self.storage_manager.read_file(csv_file, storage_type='input')
            reader = csv.DictReader(content.decode('utf-8').splitlines())
            
            # Skip to start line
            for _ in range(start_line):
                next(reader, None)
            
            # Process each row
            for row in reader:
                try:
                    self.process_row(row, facade, config, wait_time)
                    self.current_line += 1
                except Exception as e:
                    logger.error(f"Error processing row {self.current_line}: {str(e)}")
                    self.skipped_records.add(str(self.current_line))
                    continue
                
        except Exception as e:
            logger.error(f"Error processing CSV file {csv_file}: {str(e)}")
            raise

    def process_row(self, row: Dict[str, str], facade: Any, config: Dict[str, Any], wait_time: float):
        """
        Process a single row from the CSV file.
        
        Args:
            row: Dictionary containing row data.
            facade: Financial services facade instance.
            config: Configuration dictionary.
            wait_time: Time to wait between records.
        """
        # Extract data from row
        data = self.extract_data(row)
        
        # Process the data
        result = facade.process_record(data, config)
        
        # Save result
        self.save_result(result)
        
        # Wait between records
        if wait_time > 0:
            time.sleep(wait_time)

    def extract_data(self, row: Dict[str, str]) -> Dict[str, Any]:
        """
        Extract data from a CSV row.
        
        Args:
            row: Dictionary containing row data.
            
        Returns:
            Dictionary containing extracted data.
        """
        # Implementation depends on your data structure
        return row

    def save_result(self, result: Dict[str, Any]):
        """
        Save processing result.
        
        Args:
            result: Dictionary containing processing result.
        """
        if not self.storage_manager:
            raise ValueError("Storage manager not set")
        
        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"result_{timestamp}.json"
        
        # Save result
        self.storage_manager.write_file(
            output_file,
            json.dumps(result, indent=2),
            storage_type='output'
        )

    def _write_records(self, records: defaultdict, output_file: str, record_type: str):
        date_str = datetime.now().strftime("%m-%d-%Y")
        archive_subfolder = os.path.join(ARCHIVE_FOLDER, date_str)
        csv_path = os.path.join(archive_subfolder, output_file)
        try:
            os.makedirs(archive_subfolder, exist_ok=True)
            with open(csv_path, 'a', newline='') as f:
                file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0
                writer = csv.DictWriter(f, fieldnames=None)
                
                total_records = 0
                for csv_file, record_list in records.items():
                    for record in record_list:
                        row_data = record['row_data']
                        if not file_exists:
                            fieldnames = list(row_data.keys())
                            if "error" in record:
                                fieldnames.append("error")
                            writer.fieldnames = fieldnames
                            writer.writeheader()
                            file_exists = True
                        writer.writerow({**row_data, **({"error": record["error"]} if "error" in record else {})})
                        total_records += 1
                
                logger.info(f"Appended {total_records} {record_type} records to {csv_path}")
        except Exception as e:
            logger.error(f"Error writing {record_type} records to {csv_path}: {str(e)}", exc_info=True)
        finally:
            records.clear()

    def write_skipped_records(self):
        self._write_records(self.error_records, "errors.csv", "error")

if __name__ == "__main__":
    facade = FinancialServicesFacade()
    config = {"skip_disciplinary": True, "skip_arbitration": True, "skip_regulatory": True}
    processor = CSVProcessor()
    processor.process_csv(
        os.path.join(INPUT_FOLDER, "sample.csv"),
        start_line=0,
        facade=facade,
        config=config,
        wait_time=0.1
    )