import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from typing import Dict, Any, Optional
import json
import logging
from logging import Logger
from datetime import datetime

logger = logging.getLogger(__name__)

CACHE_FOLDER = Path(__file__).parent.parent / "cache"  # Define locally, no marshaller dependency
DATE_FORMAT = "%Y%m%d"

def has_significant_changes(new_report: Dict[str, Any], old_report: Dict[str, Any]) -> bool:
    """
    Compare two compliance reports to determine if significant changes warrant a new version.

    Args:
        new_report (Dict[str, Any]): The new report to save.
        old_report (Dict[str, Any]): The latest cached report for comparison.

    Returns:
        bool: True if compliance flags or alert count differ, False otherwise.
    """
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
        else:
            new_value = new_report.get("final_evaluation", {}).get(field, None)
            old_value = old_report.get("final_evaluation", {}).get(field, None)
        if new_value != old_value:
            logger.debug(f"Change detected in {field}: {old_value} -> {new_value}")
            return True

    new_alerts = len(new_report.get("final_evaluation", {}).get("alerts", []))
    old_alerts = len(old_report.get("final_evaluation", {}).get("alerts", []))
    if new_alerts != old_alerts:
        logger.debug(f"Change detected in alert count: {old_alerts} -> {new_alerts}")
        return True

    return False

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
    if not report or not isinstance(report, dict):
        logger.error("Invalid report data", extra={"employee_number": employee_number})
        return False

    reference_id = report.get("reference_id")
    if not reference_id:
        logger.error("Report missing reference_id", extra={"employee_number": employee_number})
        return False

    if not employee_number:
        employee_number = report.get("claim", {}).get("employee_number", "Unknown")

    logger.info("Processing compliance report save", extra={"reference_id": reference_id, "employee_number": employee_number})

    try:
        # Define cache path directly as cache/<employee_number>/
        cache_path = CACHE_FOLDER / employee_number
        cache_path.mkdir(parents=True, exist_ok=True)
        date = datetime.now().strftime(DATE_FORMAT)

        # Find existing files for this reference_id and date
        existing_files = sorted(cache_path.glob(f"ComplianceReportAgent_{reference_id}_v*_{date}.json"))
        latest_file = existing_files[-1] if existing_files else None

        # Load latest file for comparison, if it exists
        if latest_file:
            with latest_file.open("r") as f:
                old_report = json.load(f)
            needs_new_version = has_significant_changes(report, old_report)
            version = len(existing_files) + 1 if needs_new_version else None
        else:
            version = 1  # First version if no prior file

        if version:
            file_name = f"ComplianceReportAgent_{reference_id}_v{version}_{date}.json"
            file_path = cache_path / file_name
            with file_path.open("w") as f:
                json.dump(report, f, indent=2)
            logger.info("New version of compliance report saved", 
                        extra={"reference_id": reference_id, "employee_number": employee_number, 
                               "file_path": str(file_path)})
        else:
            logger.info("No significant changes detected; no new version saved", 
                        extra={"reference_id": reference_id, "employee_number": employee_number})

        return True

    except Exception as e:
        logger.error("Failed to save compliance report", 
                     extra={"reference_id": reference_id, "employee_number": employee_number, "error": str(e)})
        return False

__all__ = ['save_compliance_report']