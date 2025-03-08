"""
==============================================
📌 COMPLIANCE HANDLER MODULE OVERVIEW
==============================================

🗂 PURPOSE
This module provides the `ComplianceHandler` class to manage compliance report-specific operations.
It handles retrieval and listing of compliance reports stored in the cache, with support for versioning
and pagination.

🗂 USAGE
Initialize and use for compliance operations:
    from cache_manager.compliance_handler import ComplianceHandler
    handler = ComplianceHandler()
    print(handler.get_latest_compliance_report("LD-107-Dev-3A"))

🗂 FEATURES INCLUDED
✔️ Retrieve the latest compliance report with versioning awareness
✔️ Retrieve compliance report by reference ID
✔️ List all compliance reports with only the latest revision per reference ID
✔️ Pagination support for listing reports
✔️ All results returned as JSON strings

🗂 CACHE FOLDER STRUCTURE
Compliance reports are stored directly under employee folders:
cache/
└── LD-107-Dev-3A/
    ├── ComplianceReportAgent_EN-53_v1_20250308.json

🗂 TROUBLESHOOTING
If methods like `list_compliance_reports` return no results:
- Verify the CACHE_FOLDER path matches your directory structure.
- Check logs for warnings or errors (`logging.WARNING` level).
- Ensure read permissions with `ls -ld <cache_folder>` or equivalent.
- Confirm file naming matches expected pattern (e.g., `ComplianceReportAgent_*_v*.json`).
==============================================
"""

import json
from pathlib import Path
from typing import Optional, Dict, Any

from .config import DEFAULT_CACHE_FOLDER
from .file_handler import FileHandler
import logging
logger = logging.getLogger("ComplianceHandler")

class ComplianceHandler:
    """
    Manages compliance report operations for regulatory data.

    Provides methods to retrieve and list compliance reports, handling versioning and pagination.
    All results are returned as JSON strings for consistency and downstream processing.

    Attributes:
        cache_folder (Path): Directory where cache data is stored (default: `cache/`).
        file_handler (FileHandler): Helper class for filesystem operations.

    Methods:
        get_latest_compliance_report: Retrieves the latest compliance report with versioning.
        get_compliance_report_by_ref: Retrieves a compliance report by reference ID.
        list_compliance_reports: Lists all compliance reports with latest revision per reference ID.
    """

    def __init__(self, cache_folder: Path = DEFAULT_CACHE_FOLDER):
        """
        Initializes the ComplianceHandler with a cache folder.

        Args:
            cache_folder (Path): Directory for cached data (default: DEFAULT_CACHE_FOLDER).
        """
        self.cache_folder = cache_folder
        self.file_handler = FileHandler(cache_folder)
        if not self.cache_folder.exists():
            logger.warning(f"Cache folder does not exist: {self.cache_folder}")

    def get_latest_compliance_report(self, employee_number: str) -> str:
        """
        Retrieves the latest compliance report for an employee, considering versioning.

        Args:
            employee_number (str): Employee identifier (e.g., "LD-107-Dev-3A").

        Returns:
            str: JSON-formatted result with the latest report or a warning.

        Example Output:
            {
              "employee_number": "LD-107-Dev-3A",
              "status": "success",
              "message": "Retrieved latest compliance report: ComplianceReportAgent_EN-53_v1_20250308.json",
              "report": {
                "reference_id": "EN-53",
                "final_evaluation": {"overall_compliance": "Compliant", "alerts": []}
              }
            }
        """
        emp_path = self.cache_folder / employee_number
        result = {"employee_number": employee_number, "status": "success", "message": "", "report": None}
        
        if not emp_path.exists():
            result["status"] = "warning"
            result["message"] = f"No cache found for {employee_number} at {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)
        
        report_files = self.file_handler.list_files(emp_path, "ComplianceReportAgent_*_v*.json")
        if not report_files:
            result["status"] = "warning"
            result["message"] = f"No compliance reports found for {employee_number} at {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)
        
        try:
            latest_file = max(report_files, key=lambda f: (
                f.name.split("_")[-1].split(".")[0],  # Date (e.g., 20250308)
                int(f.name.split("_v")[1].split("_")[0])  # Version number
            ))
            
            result["report"] = self.file_handler.read_json(latest_file)
            result["message"] = f"Retrieved latest compliance report: {latest_file.name}"
        except Exception as e:
            result["status"] = "error"
            result["message"] = f"Failed to retrieve latest compliance report: {str(e)}"
            logger.error(result["message"])
        return json.dumps(result, indent=2)

    def get_compliance_report_by_ref(self, employee_number: str, reference_id: str) -> str:
        """
        Retrieves the latest compliance report by reference ID for an employee.

        Args:
            employee_number (str): Employee identifier (e.g., "LD-107-Dev-3A").
            reference_id (str): Reference ID of the report (e.g., "EN-53").

        Returns:
            str: JSON-formatted result with the report or a warning.

        Example Output:
            {
              "employee_number": "LD-107-Dev-3A",
              "reference_id": "EN-53",
              "status": "success",
              "message": "Retrieved compliance report: ComplianceReportAgent_EN-53_v1_20250308.json",
              "report": {
                "reference_id": "EN-53",
                "final_evaluation": {"overall_compliance": "Compliant", "alerts": []}
              }
            }
        """
        emp_path = self.cache_folder / employee_number
        result = {"employee_number": employee_number, "reference_id": reference_id, "status": "success", "message": "", "report": None}
        
        if not emp_path.exists():
            result["status"] = "warning"
            result["message"] = f"No cache found for {employee_number} at {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)
        
        report_files = self.file_handler.list_files(emp_path, f"ComplianceReportAgent_{reference_id}_v*.json")
        if not report_files:
            result["status"] = "warning"
            result["message"] = f"No compliance report found for reference_id {reference_id} under {employee_number}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)
        
        try:
            latest_file = max(report_files, key=lambda f: int(f.name.split("_v")[1].split("_")[0]))
            
            result["report"] = self.file_handler.read_json(latest_file)
            result["message"] = f"Retrieved compliance report: {latest_file.name}"
        except Exception as e:
            result["status"] = "error"
            result["message"] = f"Failed to retrieve compliance report: {str(e)}"
            logger.error(result["message"])
        return json.dumps(result, indent=2)

    def list_compliance_reports(self, employee_number: Optional[str] = None, page: int = 1, page_size: int = 10) -> str:
        """
        Lists all compliance reports, returning only the latest revision for each reference ID, with pagination.
        If no employee is specified, lists reports for all employees in the cache.

        Args:
            employee_number (Optional[str]): Employee identifier (e.g., "LD-107-Dev-3A"). If None, lists all.
            page (int): Page number to retrieve (default: 1).
            page_size (int): Number of items per page (default: 10).

        Returns:
            str: JSON-formatted result with reports and pagination metadata.

        Example Output (Specific Employee):
            {
              "employee_number": "LD-107-Dev-3A",
              "status": "success",
              "message": "Listed 1 compliance reports for LD-107-Dev-3A",
              "reports": [
                {
                  "reference_id": "EN-53",
                  "file_name": "ComplianceReportAgent_EN-53_v1_20250308.json",
                  "last_modified": "2025-03-08 14:30:22"
                }
              ],
              "pagination": {
                "total_items": 1,
                "total_pages": 1,
                "current_page": 1,
                "page_size": 10
              }
            }

        Example Output (All Employees):
            {
              "status": "success",
              "message": "Listed compliance reports for 1 employees",
              "reports": {
                "LD-107-Dev-3A": [
                  {
                    "reference_id": "EN-53",
                    "file_name": "ComplianceReportAgent_EN-53_v1_20250308.json",
                    "last_modified": "2025-03-08 14:30:22"
                  }
                ]
              },
              "pagination": {
                "total_items": 1,
                "total_pages": 1,
                "current_page": 1,
                "page_size": 10
              }
            }

        Troubleshooting:
            - If no reports are listed, ensure files match the pattern `ComplianceReportAgent_*_v*.json`.
            - Check pagination parameters; `page` or `page_size` < 1 are adjusted to 1.
        """
        result = {
            "status": "success",
            "message": "",
            "reports": {} if employee_number is None else [],
            "pagination": {
                "total_items": 0,
                "total_pages": 0,
                "current_page": max(1, page),
                "page_size": max(1, page_size)
            }
        }

        if employee_number is None:
            if not self.cache_folder.exists():
                result["status"] = "warning"
                result["message"] = f"Cache folder not found at {self.cache_folder}"
                logger.warning(result["message"])
                return json.dumps(result, indent=2)
            
            emp_dirs = self.file_handler.list_files(self.cache_folder, "*")
            if not emp_dirs:
                result["status"] = "warning"
                result["message"] = "No directories found in cache folder"
                logger.warning(result["message"])
                return json.dumps(result, indent=2)

            full_reports: Dict[str, list] = {}
            for emp_path in sorted(emp_dirs):
                if emp_path.is_dir():
                    report_files = self.file_handler.list_files(emp_path, "ComplianceReportAgent_*.json")
                    if report_files:
                        latest_reports: Dict[str, Dict[str, Any]] = {}
                        for file in report_files:
                            try:
                                parts = file.name.split("_")
                                if len(parts) < 4:
                                    continue
                                reference_id = parts[1]
                                version = int(parts[2].split("v")[1].split("_")[0])
                                last_modified = self.file_handler.get_last_modified(file)
                                
                                if reference_id not in latest_reports or version > latest_reports[reference_id]["version"]:
                                    latest_reports[reference_id] = {
                                        "reference_id": reference_id,
                                        "version": version,
                                        "file_name": file.name,
                                        "last_modified": last_modified
                                    }
                            except (IndexError, ValueError) as e:
                                logger.warning(f"Failed to parse file {file.name}: {str(e)}")
                                continue
                        full_reports[emp_path.name] = [
                            {"reference_id": data["reference_id"], "file_name": data["file_name"], "last_modified": data["last_modified"]}
                            for data in latest_reports.values()
                        ]

            total_items = len(full_reports)
            page_size = max(1, page_size)
            total_pages = (total_items + page_size - 1) // page_size
            current_page = max(1, min(page, total_pages))
            start_idx = (current_page - 1) * page_size
            end_idx = start_idx + page_size

            result["pagination"].update({
                "total_items": total_items,
                "total_pages": total_pages,
                "current_page": current_page,
                "page_size": page_size
            })
            employee_keys = sorted(full_reports.keys())[start_idx:end_idx]
            result["reports"] = {key: full_reports[key] for key in employee_keys}
            result["message"] = f"Listed compliance reports for {len(result['reports'])} employees (page {current_page} of {total_pages})"
            return json.dumps(result, indent=2)

        emp_path = self.cache_folder / employee_number
        result["employee_number"] = employee_number
        
        if not emp_path.exists():
            result["status"] = "warning"
            result["message"] = f"No compliance reports found for {employee_number} at {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)

        full_reports = []
        report_files = self.file_handler.list_files(emp_path, "ComplianceReportAgent_*.json")
        for file in report_files:
            try:
                parts = file.name.split("_")
                if len(parts) < 4:
                    continue
                reference_id = parts[1]
                version = int(parts[2].split("v")[1].split("_")[0])
                last_modified = self.file_handler.get_last_modified(file)
                full_reports.append({
                    "reference_id": reference_id,
                    "version": version,
                    "file_name": file.name,
                    "last_modified": last_modified
                })
            except (IndexError, ValueError) as e:
                logger.warning(f"Failed to parse file {file.name}: {str(e)}")
                continue

        total_items = len(full_reports)
        page_size = max(1, page_size)
        total_pages = (total_items + page_size - 1) // page_size
        current_page = max(1, min(page, total_pages))
        start_idx = (current_page - 1) * page_size
        end_idx = start_idx + page_size

        result["pagination"].update({
            "total_items": total_items,
            "total_pages": total_pages,
            "current_page": current_page,
            "page_size": page_size
        })
        result["reports"] = full_reports[start_idx:end_idx]
        result["message"] = f"Listed {len(result['reports'])} compliance reports for {employee_number} (page {current_page} of {total_pages})"
        return json.dumps(result, indent=2)