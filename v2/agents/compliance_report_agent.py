import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from typing import Dict, Any, Optional
import json
import logging
from logging import Logger
from datetime import datetime
from evaluation_processor import Alert
import os

logger = logging.getLogger(__name__)

CACHE_FOLDER = Path(__file__).parent.parent / "cache"  # Define locally, no marshaller dependency
DATE_FORMAT = "%Y%m%d"

def convert_to_serializable(obj: Any) -> Any:
    """
    Recursively convert an object to a JSON-serializable format.
    
    Args:
        obj: Any Python object
        
    Returns:
        JSON-serializable version of the object
    """
    try:
        if isinstance(obj, Alert):
            logger.debug(f"Converting Alert object to dict using to_dict()")
            return obj.to_dict()
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        elif isinstance(obj, dict):
            return {key: convert_to_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [convert_to_serializable(item) for item in obj]
        elif hasattr(obj, '__dict__'):
            logger.debug(f"Converting object of type {type(obj)} to dict")
            result = {}
            for key, value in obj.__dict__.items():
                try:
                    result[key] = convert_to_serializable(value)
                except Exception as e:
                    logger.warning(f"Failed to convert attribute {key} of {type(obj)}: {str(e)}")
                    result[key] = str(value)
            return result
        elif hasattr(obj, '__slots__'):
            logger.debug(f"Converting object with slots of type {type(obj)} to dict")
            return {slot: convert_to_serializable(getattr(obj, slot)) for slot in obj.__slots__}
        else:
            logger.warning(f"Converting unknown type {type(obj)} to string representation")
            return str(obj)
    except Exception as e:
        logger.error(f"Error converting object of type {type(obj)}: {str(e)}", exc_info=True)
        return str(obj)

def log_alert_structure(alert: Any, index: int):
    """Helper function to log detailed alert structure"""
    logger.debug(f"Alert {index} details:")
    logger.debug(f"  Type: {type(alert)}")
    if hasattr(alert, '__dict__'):
        logger.debug(f"  Dict attributes: {list(alert.__dict__.keys())}")
        for key, value in alert.__dict__.items():
            logger.debug(f"    {key}: {type(value)}")
            if isinstance(value, (list, dict)):
                logger.debug(f"    {key} length: {len(value) if isinstance(value, (list, dict)) else 'N/A'}")
    elif hasattr(alert, '__slots__'):
        logger.debug(f"  Slots: {alert.__slots__}")
        for slot in alert.__slots__:
            value = getattr(alert, slot)
            logger.debug(f"    {slot}: {type(value)}")
    else:
        logger.debug(f"  String representation: {str(alert)}")

def has_significant_changes(new_report: Dict[str, Any], old_report: Dict[str, Any]) -> bool:
    """
    Compare two compliance reports to determine if significant changes warrant a new version.

    Args:
        new_report (Dict[str, Any]): The new report to save.
        old_report (Dict[str, Any]): The latest cached report for comparison.

    Returns:
        bool: True if compliance flags or alert count differ, False otherwise.
    """
    logger.debug(f"Comparing reports for changes. New report keys: {list(new_report.keys())}")
    logger.debug(f"Old report keys: {list(old_report.keys())}")

    compliance_fields = [
        "overall_compliance",
        ("search_evaluation", "compliance"),
        ("status_evaluation", "compliance"),
        ("name_evaluation", "compliance"),
        ("license_evaluation", "compliance"),
        ("exam_evaluation", "compliance"),
        ("disclosure_review", "compliance"),
        ("disciplinary_evaluation", "compliance"),
        ("arbitration_review", "compliance"),
        ("regulatory_evaluation", "compliance")
    ]

    for field in compliance_fields:
        if isinstance(field, tuple):
            new_value = new_report.get(field[0], {}).get(field[1], None)
            old_value = old_report.get(field[0], {}).get(field[1], None)
            logger.debug(f"Checking nested field {field[0]}.{field[1]}: new={new_value}, old={old_value}")
        else:
            new_value = new_report.get("final_evaluation", {}).get(field, None)
            old_value = old_report.get("final_evaluation", {}).get(field, None)
            logger.debug(f"Checking field {field}: new={new_value}, old={old_value}")
        if new_value != old_value:
            logger.info(f"Change detected in {field}: {old_value} -> {new_value}")
            return True

    new_alerts = len(new_report.get("final_evaluation", {}).get("alerts", []))
    old_alerts = len(old_report.get("final_evaluation", {}).get("alerts", []))
    logger.debug(f"Comparing alert counts: new={new_alerts}, old={old_alerts}")
    if new_alerts != old_alerts:
        logger.info(f"Change detected in alert count: {old_alerts} -> {new_alerts}")
        return True

    return False

class AlertEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Alert objects."""
    def default(self, obj):
        if isinstance(obj, Alert):
            return obj.to_dict()
        return super().default(obj)

def json_dumps_with_alerts(obj: Any, **kwargs) -> str:
    """Helper function to serialize objects that may contain Alert instances."""
    return json.dumps(obj, cls=AlertEncoder, **kwargs)

def save_compliance_report(report: Dict[str, Any], employee_number: Optional[str] = None, logger: Logger = logger) -> bool:
    """
    Saves a compliance report to the cache folder under employee_number, with a filename based on
    reference_id and a version number, only if significant changes are detected.

    Args:
        report (Dict[str, Any]): The compliance report to save, must contain 'reference_id'.
        employee_number (Optional[str]): Identifier for the cache subfolder (e.g., "EMP001"). Defaults to "Unknown".
        logger (Logger): Logger instance for structured logging. Defaults to module logger.

    Returns:
        bool: True if the save was successful, False otherwise.
    """
    logger.debug(f"Starting save_compliance_report with report keys: {list(report.keys())}")
    
    if not report or not isinstance(report, dict):
        logger.error("Invalid report data", extra={"employee_number": employee_number})
        return False

    reference_id = report.get("reference_id")
    if not reference_id:
        logger.error("Report missing reference_id", extra={"employee_number": employee_number})
        return False

    if not employee_number:
        employee_number = report.get("claim", {}).get("employee_number", "Unknown")
        logger.debug(f"Using employee_number from claim: {employee_number}")

    logger.info("Processing compliance report save", extra={"reference_id": reference_id, "employee_number": employee_number})

    try:
        # Define cache path directly as cache/<employee_number>/
        cache_path = CACHE_FOLDER / employee_number
        logger.debug(f"Creating cache directory: {cache_path}")
        cache_path.mkdir(parents=True, exist_ok=True)
        date = datetime.now().strftime(DATE_FORMAT)
        logger.debug(f"Using date: {date}")

        # Find existing files for this reference_id and date
        pattern = f"ComplianceReportAgent_{reference_id}_v*_{date}.json"
        logger.debug(f"Looking for existing files with pattern: {pattern}")
        existing_files = sorted(cache_path.glob(pattern))
        logger.debug(f"Found existing files: {[f.name for f in existing_files]}")
        latest_file = existing_files[-1] if existing_files else None

        # Load latest file for comparison, if it exists
        if latest_file:
            logger.debug(f"Loading latest file for comparison: {latest_file}")
            try:
                with latest_file.open("r") as f:
                    old_report = json.load(f)
                logger.debug(f"Successfully loaded old report with keys: {list(old_report.keys())}")
                needs_new_version = has_significant_changes(report, old_report)
                version = len(existing_files) + 1 if needs_new_version else None
                logger.debug(f"Version decision: needs_new_version={needs_new_version}, version={version}")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse existing report: {str(e)}", exc_info=True)
                version = 1  # Force new version if old file is corrupted
        else:
            version = 1  # First version if no prior file
            logger.debug("No existing files found, creating version 1")

        if version:
            file_name = f"ComplianceReportAgent_{reference_id}_v{version}_{date}.json"
            file_path = cache_path / file_name
            logger.debug(f"Attempting to save new version to: {file_path}")
            
            # Log the structure of the report before saving
            logger.debug("Report structure before saving:")
            logger.debug(f"Keys in final_evaluation: {list(report.get('final_evaluation', {}).keys())}")
            alerts = report.get('final_evaluation', {}).get('alerts', [])
            logger.debug(f"Number of alerts: {len(alerts)}")
            
            # Log detailed structure of each alert
            for i, alert in enumerate(alerts):
                log_alert_structure(alert, i)
            
            try:
                # Convert the report to a serializable format
                logger.debug("Starting conversion to serializable format...")
                serializable_report = convert_to_serializable(report)
                logger.debug("Successfully converted report to serializable format")
                
                # Test serialization before writing to file
                logger.debug("Testing JSON serialization...")
                json_str = json_dumps_with_alerts(serializable_report, indent=2)
                logger.debug("JSON serialization test successful")
                
                with file_path.open("w") as f:
                    f.write(json_str)
                logger.info("New version of compliance report saved", 
                            extra={"reference_id": reference_id, "employee_number": employee_number, 
                                   "file_path": str(file_path)})
            except TypeError as e:
                logger.error(f"JSON serialization error: {str(e)}", exc_info=True)
                logger.error("Problematic data structure:", extra={"report": str(report)})
                raise
        else:
            logger.info("No significant changes detected; no new version saved", 
                        extra={"reference_id": reference_id, "employee_number": employee_number})

        return True

    except Exception as e:
        logger.error("Failed to save compliance report", 
                     extra={"reference_id": reference_id, "employee_number": employee_number, "error": str(e)},
                     exc_info=True)
        return False

__all__ = ['save_compliance_report']