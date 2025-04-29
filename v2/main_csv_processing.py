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
            
            # Handle both string and bytes content
            if isinstance(content, bytes):
                content_str = content.decode('utf-8')
            else:
                content_str = content
                
            reader = csv.DictReader(content_str.splitlines())
            
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
        
        # Import the process_claim function from business module
        from business import process_claim
        
        # Process the data using process_claim
        skip_disciplinary = config.get('skip_disciplinary', False)
        skip_arbitration = config.get('skip_arbitration', False)
        skip_regulatory = config.get('skip_regulatory', False)
        
        result = process_claim(
            data,
            facade,
            employee_number=data.get('employee_number'),
            skip_disciplinary=skip_disciplinary,
            skip_arbitration=skip_arbitration,
            skip_regulatory=skip_regulatory
        )
        
        # Save result
        self.save_result(result)
        
        # Wait between records
        if wait_time > 0:
            time.sleep(wait_time)

    def extract_data(self, row: Dict[str, str]) -> Dict[str, Any]:
        """
        Extract data from a CSV row and map to canonical field names.
        
        Args:
            row: Dictionary containing row data.
            
        Returns:
            Dictionary containing extracted data with canonical field names.
        """
        from main_config import canonical_fields
        
        # Create a new dictionary for the extracted data
        extracted = {}
        
        # Map fields to their canonical names
        for canonical_name, aliases in canonical_fields.items():
            # Check if any of the aliases exist in the row
            for alias in aliases:
                if alias in row and row[alias]:
                    extracted[canonical_name] = row[alias]
                    break
        
        # Add any remaining fields that don't have canonical mappings
        for key, value in row.items():
            # Check if this key is already mapped to a canonical name
            is_mapped = False
            for aliases in canonical_fields.values():
                if key in aliases:
                    is_mapped = True
                    break
            
            # If not mapped and has a value, add it to the extracted data
            if not is_mapped and value:
                extracted[key] = value
        
        # Special handling for reference_id if it's not already set
        if 'reference_id' not in extracted and 'referenceId' in row:
            extracted['reference_id'] = row['referenceId']
        
        # Special handling for crdNumber -> crd_number mapping
        if 'crd_number' not in extracted and 'crdNumber' in row and row['crdNumber']:
            extracted['crd_number'] = row['crdNumber']
            logger.debug(f"Mapped crdNumber '{row['crdNumber']}' to crd_number")
        
        # Special handling for organizationCRD -> organization_crd mapping
        if 'organization_crd' not in extracted and 'organizationCRD' in row and row['organizationCRD']:
            extracted['organization_crd'] = row['organizationCRD']
            logger.debug(f"Mapped organizationCRD '{row['organizationCRD']}' to organization_crd")
        
        # Log the mapping for debugging
        logger.debug(f"Mapped row fields: {row.keys()} to canonical fields: {extracted.keys()}")
        
        return extracted

    def save_result(self, result: Dict[str, Any]):
        """
        Save processing result.
        
        Args:
            result: Dictionary containing processing result.
        """
        if not self.storage_manager:
            raise ValueError("Storage manager not set")
        
        # Extract reference ID from the result or use a timestamp if not available
        reference_id = None
        
        # Try to get reference_id from different possible locations in the result
        if isinstance(result, dict):
            # Try to get it directly from the result
            reference_id = result.get('reference_id')
            
            # If not found, try to get it from the claim data if present
            if not reference_id and 'claim' in result:
                reference_id = result['claim'].get('reference_id')
                
            # If still not found, try to get it from search_evaluation if present
            if not reference_id and 'search_evaluation' in result:
                if isinstance(result['search_evaluation'], dict):
                    reference_id = result['search_evaluation'].get('reference_id')
        
        # If reference_id is still not found, use a timestamp
        if not reference_id:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"result_{timestamp}.json"
        else:
            # Clean the reference_id to make it a valid filename
            reference_id = reference_id.replace('/', '_').replace('\\', '_').replace(':', '_')
            output_file = f"{reference_id}.json"
        
        logger.info(f"Saving result to output file: {output_file}")
        
        # Get the output path from the storage manager
        output_path = os.path.join(self.storage_manager.get_output_path(), output_file)
        
        # Save result to the output folder
        try:
            # Make sure the output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Write the file to the output directory
            with open(output_path, 'w') as f:
                f.write(json.dumps(result, indent=2))
            
            logger.debug(f"Successfully saved file: {output_path}")
        except Exception as e:
            logger.error(f"Error saving result to {output_path}: {str(e)}")
            raise

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