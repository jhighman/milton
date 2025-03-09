import os
import json
import shutil
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple

"""
==============================================
📌 CACHE MANAGER OVERVIEW
==============================================

🗂 PURPOSE
This module provides a `CacheManager` class to handle cached data for regulatory and compliance reporting.
It organizes cache by employee numbers (e.g., `EMP001`, `LD-107-Dev-3A`) and supports various agents, with special handling for compliance reports.

🗂 CACHE FOLDER STRUCTURE
📍 Cached data is stored under `cache/` with the following structure:

Example for `LD-107-Dev-3A`:
cache/
└── LD-107-Dev-3A/
    ├── SEC_IAPD_Agent/
    │   ├── search_individual/
    │   │   ├── SEC_IAPD_Agent_LD-107-Dev-3A_search_individual_20240307.json
    │   │   ├── manifest.txt
    ├── FINRA_BrokerCheck_Agent/
    ├── ComplianceReportAgent_EN-53_v1_20250308.json  # Compliance reports can be directly here
    ├── request_log.txt

📌 FEATURES INCLUDED:
✔️ Clear cache for an employee (excluding ComplianceReportAgent)
✔️ Clear ComplianceReportAgent cache separately
✔️ Clear cache for a specific agent
✔️ List cached files (all employees or specific employee)
✔️ Automatic cleanup of stale cache older than 90 days
✔️ Retrieve the latest compliance report with versioning awareness
✔️ Retrieve compliance report by reference ID
✔️ List all compliance reports with only the latest revision per reference ID (for one or all employees)
✔️ Generate compliance summary with report-level and subsection-level data
✔️ All results returned as JSON with pagination support

📌 TROUBLESHOOTING
If methods like `list_compliance_reports` return no results:
- Verify the CACHE_FOLDER path matches your directory structure.
- Check the logs for warnings or errors.
- Ensure the script has read permissions for the cache directory.
- Run `ls -ld <cache_folder>` to check permissions.
- Run `ls -l <cache_folder>` to verify contents.
- Check for symbolic links with `ls -l <cache_folder>`.

==============================================
"""

# Cache Configuration
DEFAULT_CACHE_FOLDER = Path(__file__).parent / "cache"  # Default to 'cache' subdirectory of script location
CACHE_TTL_DAYS = 90  # Cache expiration in days
DATE_FORMAT = "%Y%m%d"  # Standardized date format for filenames
MANIFEST_FILE = "manifest.txt"  # File tracking last cache update per agent

# Logging Setup (disabled for clean CLI output, only warnings/errors remain)
import logging
logging.basicConfig(level=logging.WARNING, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("CacheManager")

class AgentName:
    """
    Enumerates valid agent names to ensure consistency and prevent typos in cache operations.

    Attributes:
        SEC_IAPD (str): SEC Investment Adviser Public Disclosure agent.
        FINRA_BROKERCHECK (str): FINRA BrokerCheck agent.
        SEC_ARBITRATION (str): SEC Arbitration agent.
        FINRA_DISCIPLINARY (str): FINRA Disciplinary agent.
        NFA_BASIC (str): NFA BASIC agent.
        FINRA_ARBITRATION (str): FINRA Arbitration agent.
        SEC_DISCIPLINARY (str): SEC Disciplinary agent.
        COMPLIANCE_REPORT (str): Compliance report agent (special handling).
    """
    SEC_IAPD = "SEC_IAPD_Agent"
    FINRA_BROKERCHECK = "FINRA_BrokerCheck_Agent"
    SEC_ARBITRATION = "SEC_Arbitration_Agent"
    FINRA_DISCIPLINARY = "FINRA_Disciplinary_Agent"
    NFA_BASIC = "NFA_Basic_Agent"
    FINRA_ARBITRATION = "FINRA_Arbitration_Agent"
    SEC_DISCIPLINARY = "SEC_Disciplinary_Agent"
    COMPLIANCE_REPORT = "ComplianceReportAgent"

class CacheManager:
    """
    Manages cache operations for regulatory and compliance data.

    Provides methods to clear, list, and clean up cache, as well as retrieve compliance reports.
    All results are returned as JSON strings for consistency and downstream processing.

    Attributes:
        cache_folder (Path): Directory where cache data is stored (default: `cache/` relative to script).
        ttl_days (int): Time-to-live for cache files in days (default: 90).

    Methods:
        clear_cache: Clears all agent caches except ComplianceReportAgent.
        clear_compliance_cache: Clears only ComplianceReportAgent cache.
        clear_agent_cache: Clears cache for a specific agent.
        list_cache: Lists cached files for all or a specific employee.
        cleanup_stale_cache: Removes stale cache files older than ttl_days.
        get_latest_compliance_report: Retrieves the latest compliance report with versioning.
        get_compliance_report_by_ref: Retrieves a compliance report by reference ID.
        list_compliance_reports: Lists all compliance reports with only the latest revision per reference ID, with pagination.
        generate_compliance_summary: Generates a summary of compliance reports with report-level and subsection-level data.
    """

    def __init__(self, cache_folder: Path = DEFAULT_CACHE_FOLDER, ttl_days: int = CACHE_TTL_DAYS):
        """
        Initializes the CacheManager with a cache folder and TTL.

        Args:
            cache_folder (Path): Directory for cached data (default: DEFAULT_CACHE_FOLDER).
            ttl_days (int): Cache expiration period in days (default: CACHE_TTL_DAYS).
        """
        self.cache_folder = cache_folder
        self.ttl_days = ttl_days
        if not self.cache_folder.exists():
            logger.warning(f"Cache folder does not exist: {self.cache_folder}")
        elif not self.cache_folder.is_dir():
            logger.warning(f"Cache folder is not a directory: {self.cache_folder}")
        else:
            try:
                os.access(self.cache_folder, os.R_OK)
            except Exception as e:
                logger.warning(f"Cache folder is not readable: {self.cache_folder}, error: {str(e)}")

    def clear_cache(self, employee_number: str) -> str:
        """
        Clears all cache for an employee, excluding ComplianceReportAgent.

        Args:
            employee_number (str): Employee identifier (e.g., "LD-107-Dev-3A").

        Returns:
            str: JSON-formatted result with status, cleared agents, and message.

        Example Output:
            {
              "employee_number": "LD-107-Dev-3A",
              "cleared_agents": ["SEC_IAPD_Agent", "FINRA_BrokerCheck_Agent"],
              "status": "success",
              "message": "Cleared cache for 2 agents"
            }
        """
        emp_path = self.cache_folder / employee_number
        result = {"employee_number": employee_number, "cleared_agents": [], "status": "success", "message": ""}
        
        if not emp_path.exists():
            result["status"] = "warning"
            result["message"] = f"No cache found for {employee_number} at {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)
        
        for agent_folder in emp_path.iterdir():
            if agent_folder.is_dir() and agent_folder.name != AgentName.COMPLIANCE_REPORT:
                shutil.rmtree(agent_folder)
                result["cleared_agents"].append(agent_folder.name)
        result["message"] = f"Cleared cache for {len(result['cleared_agents'])} agents"
        return json.dumps(result, indent=2)

    def clear_compliance_cache(self, employee_number: str) -> str:
        """
        Clears only the ComplianceReportAgent cache for an employee.

        Args:
            employee_number (str): Employee identifier (e.g., "LD-107-Dev-3A").

        Returns:
            str: JSON-formatted result with status and message.

        Example Output:
            {
              "employee_number": "LD-107-Dev-3A",
              "status": "success",
              "message": "Cleared ComplianceReportAgent cache for LD-107-Dev-3A"
            }
        """
        emp_path = self.cache_folder / employee_number
        result = {"employee_number": employee_number, "status": "success", "message": ""}
        
        if not emp_path.exists():
            result["status"] = "warning"
            result["message"] = f"No cache found for {employee_number} at {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)
        
        # Check for compliance files directly in the employee folder
        compliance_files = [f for f in emp_path.glob("ComplianceReportAgent_*.json")]
        if compliance_files:
            for file in compliance_files:
                try:
                    file.unlink()
                except Exception as e:
                    logger.warning(f"Failed to delete {file}: {str(e)}")
            result["message"] = f"Cleared ComplianceReportAgent cache for {employee_number}"
        else:
            result["status"] = "warning"
            result["message"] = f"No ComplianceReportAgent cache found for {employee_number} at {emp_path}"
            logger.warning(result["message"])
        return json.dumps(result, indent=2)

    def clear_agent_cache(self, employee_number: str, agent_name: str) -> str:
        """
        Clears cache for a specific agent under an employee.

        Args:
            employee_number (str): Employee identifier (e.g., "LD-107-Dev-3A").
            agent_name (str): Name of the agent (e.g., "SEC_IAPD_Agent").

        Returns:
            str: JSON-formatted result with status and message.

        Example Output:
            {
              "employee_number": "LD-107-Dev-3A",
              "agent_name": "SEC_IAPD_Agent",
              "status": "success",
              "message": "Cleared cache for agent SEC_IAPD_Agent under LD-107-Dev-3A"
            }
        """
        agent_path = self.cache_folder / employee_number / agent_name
        result = {"employee_number": employee_number, "agent_name": agent_name, "status": "success", "message": ""}
        
        if agent_path.exists():
            shutil.rmtree(agent_path)
            result["message"] = f"Cleared cache for agent {agent_name} under {employee_number}"
        else:
            result["status"] = "warning"
            result["message"] = f"No cache found for {agent_name} under {employee_number} at {agent_path}"
            logger.warning(result["message"])
        return json.dumps(result, indent=2)

    def list_cache(self, employee_number: Optional[str] = None) -> str:
        """
        Lists all cached files for either all employees or a specific employee.

        Args:
            employee_number (Optional[str]): Employee identifier (e.g., "LD-107-Dev-3A"). If None or "ALL", lists all employees.

        Returns:
            str: JSON-formatted result with cache contents.

        Example Output (All Employees):
            {
              "status": "success",
              "message": "Listed all employees with cache",
              "cache": {
                "employees": ["LD-107-Dev-3A", "EMP002"]
              }
            }

        Example Output (Specific Employee):
            {
              "status": "success",
              "message": "Cache contents for LD-107-Dev-3A",
              "cache": {
                "LD-107-Dev-3A": {
                  "SEC_IAPD_Agent": [
                    {"file_name": "SEC_IAPD_Agent_LD-107-Dev-3A_search_individual_20240307.json", "last_modified": "2024-03-07 10:12:45"}
                  ],
                  "ComplianceReportAgent": [
                    {"file_name": "ComplianceReportAgent_EN-53_v1_20250308.json", "last_modified": "2025-03-08 14:30:22"}
                  ]
                }
              }
            }
        """
        result = {"status": "success", "message": "", "cache": {}}
        
        if employee_number is None or employee_number.upper() == "ALL":
            result["cache"]["employees"] = []
            if not self.cache_folder.exists():
                result["status"] = "warning"
                result["message"] = f"Cache folder not found at {self.cache_folder}"
                logger.warning(result["message"])
                return json.dumps(result, indent=2)
            
            try:
                emp_dirs = list(self.cache_folder.iterdir())
                for emp_path in sorted(emp_dirs):
                    if emp_path.is_dir():
                        result["cache"]["employees"].append(emp_path.name)
            except Exception as e:
                result["status"] = "error"
                result["message"] = f"Failed to list cache folder contents: {str(e)}"
                logger.error(result["message"])
                return json.dumps(result, indent=2)
            
            result["message"] = "Listed all employees with cache"
            return json.dumps(result, indent=2)

        emp_path = self.cache_folder / employee_number
        if not emp_path.exists():
            result["status"] = "warning"
            result["message"] = f"No cache found for {employee_number} at {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)
        
        result["cache"][employee_number] = {}
        try:
            for item in emp_path.iterdir():
                if item.is_dir():
                    files = []
                    for file in sorted(item.glob("*.json")):
                        try:
                            last_modified = datetime.fromtimestamp(file.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                            files.append({"file_name": file.name, "last_modified": last_modified})
                        except Exception as e:
                            logger.warning(f"Failed to read file {file}: {str(e)}")
                            continue
                    result["cache"][employee_number][item.name] = files
                elif item.name.startswith("ComplianceReportAgent_") and item.name.endswith(".json"):
                    files = []
                    try:
                        last_modified = datetime.fromtimestamp(item.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                        files.append({"file_name": item.name, "last_modified": last_modified})
                    except Exception as e:
                        logger.warning(f"Failed to read file {item}: {str(e)}")
                        continue
                    result["cache"][employee_number]["ComplianceReportAgent"] = files
        except Exception as e:
            result["status"] = "error"
            result["message"] = f"Failed to list cache for {employee_number}: {str(e)}"
            logger.error(result["message"])
            return json.dumps(result, indent=2)
        
        result["message"] = f"Cache contents for {employee_number}"
        return json.dumps(result, indent=2)

    def cleanup_stale_cache(self) -> str:
        """
        Deletes cache files older than ttl_days, excluding ComplianceReportAgent.

        Args:
            None

        Returns:
            str: JSON-formatted result with deleted files.

        Example Output:
            {
              "status": "success",
              "message": "Deleted 2 stale cache files",
              "deleted_files": [
                "cache/LD-107-Dev-3A/SEC_IAPD_Agent/search_individual/SEC_IAPD_Agent_LD-107-Dev-3A_search_individual_20231107.json",
                "cache/EMP002/FINRA_BrokerCheck_Agent/search_individual/FINRA_BrokerCheck_Agent_EMP002_search_individual_20231105.json"
              ]
            }
        """
        cutoff_date = datetime.now() - timedelta(days=self.ttl_days)
        result = {"status": "success", "message": "", "deleted_files": []}
        
        if not self.cache_folder.exists():
            result["status"] = "warning"
            result["message"] = f"Cache folder not found at {self.cache_folder}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)
        
        try:
            for emp_path in self.cache_folder.iterdir():
                if emp_path.is_dir():
                    for agent_folder in emp_path.iterdir():
                        if agent_folder.name == AgentName.COMPLIANCE_REPORT:
                            continue
                        for file in agent_folder.glob("*.json"):
                            try:
                                if datetime.fromtimestamp(file.stat().st_mtime) < cutoff_date:
                                    file.unlink()
                                    result["deleted_files"].append(str(file))
                            except Exception as e:
                                logger.warning(f"Failed to process file {file}: {str(e)}")
                                continue
                    # Check for compliance files directly in the employee folder
                    for file in emp_path.glob("ComplianceReportAgent_*.json"):
                        try:
                            if datetime.fromtimestamp(file.stat().st_mtime) < cutoff_date:
                                file.unlink()
                                result["deleted_files"].append(str(file))
                        except Exception as e:
                            logger.warning(f"Failed to process file {file}: {str(e)}")
                            continue
        except Exception as e:
            result["status"] = "error"
            result["message"] = f"Failed to cleanup stale cache: {str(e)}"
            logger.error(result["message"])
            return json.dumps(result, indent=2)
        
        result["message"] = f"Deleted {len(result['deleted_files'])} stale cache files"
        return json.dumps(result, indent=2)

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
        
        report_files = sorted(emp_path.glob("ComplianceReportAgent_*_v*.json"))
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
            
            with latest_file.open("r") as f:
                result["report"] = json.load(f)
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
        
        report_files = sorted(emp_path.glob(f"ComplianceReportAgent_{reference_id}_v*.json"))
        if not report_files:
            result["status"] = "warning"
            result["message"] = f"No compliance report found for reference_id {reference_id} under {employee_number}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)
        
        try:
            latest_file = max(report_files, key=lambda f: int(f.name.split("_v")[1].split("_")[0]))
            
            with latest_file.open("r") as f:
                result["report"] = json.load(f)
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
            employee_number (Optional[str]): Employee identifier (e.g., "LD-107-Dev-3A"). If None, lists all employees.
            page (int): Page number to retrieve (default: 1).
            page_size (int): Number of items per page (default: 10).

        Returns:
            str: JSON-formatted result with a list of the latest reports per reference ID and pagination metadata.

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
        """
        result = {
            "status": "success",
            "message": "",
            "reports": {} if employee_number is None else [],
            "pagination": {
                "total_items": 0,
                "total_pages": 0,
                "current_page": max(1, page),  # Ensure page is at least 1
                "page_size": max(1, page_size)  # Ensure page_size is at least 1
            }
        }

        if employee_number is None:
            # List reports for all employees with pagination
            if not self.cache_folder.exists():
                result["status"] = "warning"
                result["message"] = f"Cache folder not found at {self.cache_folder}"
                logger.warning(result["message"])
                return json.dumps(result, indent=2)
            
            if not self.cache_folder.is_dir():
                result["status"] = "warning"
                result["message"] = f"Cache folder is not a directory: {self.cache_folder}"
                logger.warning(result["message"])
                return json.dumps(result, indent=2)

            # Fallback using os.listdir
            try:
                dir_contents = os.listdir(self.cache_folder)
            except Exception as e:
                result["status"] = "error"
                result["message"] = f"Failed to list directory contents with os.listdir: {str(e)}"
                logger.error(result["message"])
                return json.dumps(result, indent=2)

            # Main method using pathlib
            emp_dirs = []
            try:
                emp_dirs = list(self.cache_folder.iterdir())
            except Exception as e:
                result["status"] = "error"
                result["message"] = f"Failed to scan cache folder with pathlib: {str(e)}"
                logger.error(result["message"])
                return json.dumps(result, indent=2)

            # If pathlib fails, try constructing paths from os.listdir
            if not emp_dirs and dir_contents:
                emp_dirs = [self.cache_folder / name for name in dir_contents]

            # Additional fallback: manually construct paths and check existence
            if not emp_dirs:
                known_emp = self.cache_folder / "LD-107-Dev-3A"
                if known_emp.exists() and known_emp.is_dir():
                    emp_dirs.append(known_emp)

            if not emp_dirs:
                result["status"] = "warning"
                result["message"] = "No directories found in cache folder"
                logger.warning(result["message"])
                return json.dumps(result, indent=2)

            # Build full reports dictionary
            full_reports = {}
            employee_count = 0
            for emp_path in sorted(emp_dirs):
                if emp_path.is_dir():
                    # Check for symbolic link
                    if emp_path.is_symlink():
                        emp_path = emp_path.resolve()

                    # Check for compliance files directly in the employee folder
                    report_files = sorted(emp_path.glob("ComplianceReportAgent_*.json"))
                    if report_files:
                        latest_reports: Dict[str, Dict[str, Any]] = {}
                        for file in report_files:
                            try:
                                parts = file.name.split("_")
                                if len(parts) < 4:
                                    continue
                                reference_id = parts[1]  # e.g., "EN-53"
                                version_part = parts[2]  # e.g., "v1_20250308"
                                version_str = version_part.split("v")[1].split("_")[0]  # Extract "1"
                                version = int(version_str)
                                last_modified = datetime.fromtimestamp(file.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                                
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
                            except Exception as e:
                                logger.error(f"Unexpected error processing file {file.name}: {str(e)}")
                                continue
                        
                        if latest_reports:
                            full_reports[emp_path.name] = [
                                {"reference_id": data["reference_id"], "file_name": data["file_name"], "last_modified": data["last_modified"]}
                                for data in latest_reports.values()
                            ]
                            employee_count += 1
                    else:
                        logger.warning(f"No compliance files found in {emp_path}")
                else:
                    logger.warning(f"Skipping non-directory: {emp_path.name}")

            # Apply pagination to the employees
            total_items = len(full_reports)
            page_size = max(1, page_size)
            total_pages = (total_items + page_size - 1) // page_size  # Ceiling division
            current_page = max(1, min(page, total_pages))  # Ensure page is within bounds
            start_idx = (current_page - 1) * page_size
            end_idx = start_idx + page_size

            # Update pagination metadata
            result["pagination"]["total_items"] = total_items
            result["pagination"]["total_pages"] = total_pages
            result["pagination"]["current_page"] = current_page
            result["pagination"]["page_size"] = page_size

            # Slice the reports dictionary
            employee_keys = sorted(full_reports.keys())[start_idx:end_idx]
            result["reports"] = {key: full_reports[key] for key in employee_keys}

            if not result["reports"]:
                result["status"] = "warning"
                result["message"] = "No compliance reports found in cache"
                logger.warning(result["message"])
            else:
                result["message"] = f"Listed compliance reports for {len(result['reports'])} employees (page {current_page} of {total_pages})"
            return json.dumps(result, indent=2)

        # List reports for a specific employee with pagination
        emp_path = self.cache_folder / employee_number
        result["employee_number"] = employee_number
        
        if not emp_path.exists():
            result["status"] = "warning"
            result["message"] = f"No compliance reports found for {employee_number} at {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)
        
        if not emp_path.is_dir():
            result["status"] = "warning"
            result["message"] = f"Employee path is not a directory: {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)

        full_reports: List[Dict[str, Any]] = []
        report_files = sorted(emp_path.glob("ComplianceReportAgent_*.json"))
        for file in report_files:
            try:
                parts = file.name.split("_")
                if len(parts) < 4:
                    continue
                reference_id = parts[1]  # e.g., "EN-53"
                version_part = parts[2]  # e.g., "v1_20250308"
                version_str = version_part.split("v")[1].split("_")[0]  # Extract "1"
                version = int(version_str)
                last_modified = datetime.fromtimestamp(file.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                
                full_reports.append({
                    "reference_id": reference_id,
                    "version": version,
                    "file_name": file.name,
                    "last_modified": last_modified
                })
            except (IndexError, ValueError) as e:
                logger.warning(f"Failed to parse file {file.name}: {str(e)}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error processing file {file.name}: {str(e)}")
                continue

        # Apply pagination to the reports list
        total_items = len(full_reports)
        page_size = max(1, page_size)
        total_pages = (total_items + page_size - 1) // page_size  # Ceiling division
        current_page = max(1, min(page, total_pages))  # Ensure page is within bounds
        start_idx = (current_page - 1) * page_size
        end_idx = start_idx + page_size

        # Update pagination metadata
        result["pagination"]["total_items"] = total_items
        result["pagination"]["total_pages"] = total_pages
        result["pagination"]["current_page"] = current_page
        result["pagination"]["page_size"] = page_size

        # Slice the reports list
        result["reports"] = full_reports[start_idx:end_idx]
        result["message"] = f"Listed {len(result['reports'])} compliance reports for {employee_number} (page {current_page} of {total_pages})"
        return json.dumps(result, indent=2)

    def _load_compliance_reports(self, emp_path: Path) -> List[Dict[str, Any]]:
        """
        Load all ComplianceReportAgent JSON files from the employee folder and return a list of dictionaries.

        Args:
            emp_path (Path): Path to the employee folder.

        Returns:
            List[Dict[str, Any]]: List of report dictionaries.
        """
        reports = []
        for file in emp_path.glob("ComplianceReportAgent_*.json"):
            try:
                with file.open("r") as f:
                    data = json.load(f)
                    data["file_name"] = file.name  # Add file name for reference
                    reports.append(data)
            except json.JSONDecodeError as e:
                logger.warning(f"Skipping invalid JSON file: {file}, error: {str(e)}")
            except Exception as e:
                logger.error(f"Error reading file {file}: {str(e)}")
        return reports

    def _extract_compliance_data(self, reports: List[Dict[str, Any]], employee_number: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Extract compliance data into report-level and subsection-level lists for a given employee.

        Args:
            reports (List[Dict[str, Any]]): List of compliance report dictionaries.
            employee_number (str): Employee identifier (e.g., "LD-107-Dev-3A").

        Returns:
            Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]: Report-level and subsection-level data.
        """
        report_data = []
        subsection_data = []

        for report in reports:
            # Extract employee number from claim, fallback to directory name if not found
            emp_num = report.get('claim', {}).get('employee_number', employee_number)
            ref_id = report.get('reference_id', 'UNKNOWN')
            file_name = report.get('file_name', 'UNKNOWN')

            # Handle overall_compliance as a string and convert to boolean
            overall_compliance_str = report.get('final_evaluation', {}).get('overall_compliance', 'UNKNOWN')
            overall_compliance = overall_compliance_str.lower() == 'true' if isinstance(overall_compliance_str, str) else overall_compliance_str

            # Report-level data
            report_entry = {
                'employee_number': emp_num,
                'reference_id': ref_id,
                'file_name': file_name,
                'overall_compliance': overall_compliance,
                'risk_level': report.get('final_evaluation', {}).get('overall_risk_level', 'N/A'),
                'alert_count': len(report.get('final_evaluation', {}).get('alerts', []))
            }
            report_data.append(report_entry)

            # Subsection-level data (only for non-compliant reports)
            if overall_compliance is False:
                subsections = [
                    ('search_evaluation', report.get('search_evaluation', {})),
                    ('status_evaluation', report.get('status_evaluation', {})),
                    ('name_evaluation', report.get('name_evaluation', {})),
                    ('license_evaluation', report.get('license_evaluation', {})),
                    ('exam_evaluation', report.get('exam_evaluation', {})),
                    ('disclosure_review', report.get('disclosure_review', {})),
                    ('disciplinary_evaluation', report.get('disciplinary_evaluation', {})),
                    ('arbitration_review', report.get('arbitration_review', {})),
                    ('regulatory_evaluation', report.get('regulatory_evaluation', {}))
                ]
                for section_name, section_data in subsections:
                    subsection_entry = {
                        'employee_number': emp_num,
                        'reference_id': ref_id,
                        'file_name': file_name,
                        'subsection': section_name,
                        'compliance': section_data.get('compliance', None),
                        'alert_count': len(section_data.get('alerts', [])) if section_data.get('alerts') is not None else 0,
                        'explanation': section_data.get('compliance_explanation', 'N/A')
                    }
                    subsection_data.append(subsection_entry)

        return report_data, subsection_data

    def generate_compliance_summary(self, employee_number: Optional[str] = None, page: int = 1, page_size: int = 10) -> str:
        """
        Generates a compliance summary with report-level and subsection-level data, with pagination.
        If no employee is specified, summarizes reports for all employees in the cache, paginating over employees.

        Args:
            employee_number (Optional[str]): Employee identifier (e.g., "LD-107-Dev-3A"). If None, summarizes all employees.
            page (int): Page number to retrieve (default: 1).
            page_size (int): Number of employees per page (default: 10).

        Returns:
            str: JSON-formatted result with report-level and subsection-level summaries and pagination metadata.

        Example Output (Specific Employee):
            {
              "employee_number": "LD-107-Dev-3A",
              "status": "success",
              "message": "Generated compliance summary for LD-107-Dev-3A",
              "report_summary": [
                {
                  "employee_number": "EN-017901",
                  "reference_id": "UNKNOWN",
                  "file_name": "ComplianceReportAgent_EN-53_v1_20250308.json",
                  "overall_compliance": false,
                  "risk_level": "High",
                  "alert_count": 1
                }
              ],
              "subsection_summary": [
                {
                  "employee_number": "EN-017901",
                  "reference_id": "UNKNOWN",
                  "file_name": "ComplianceReportAgent_EN-53_v1_20250308.json",
                  "subsection": "status_evaluation",
                  "compliance": false,
                  "alert_count": 1,
                  "explanation": "Individual not found: Individual not found: No name data retrieved from search."
                },
                {
                  "employee_number": "EN-017901",
                  "reference_id": "UNKNOWN",
                  "file_name": "ComplianceReportAgent_EN-53_v1_20250308.json",
                  "subsection": "name_evaluation",
                  "compliance": false,
                  "alert_count": 0,
                  "explanation": "Name evaluation skipped due to search failure: Individual not found: No name data retrieved from search."
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
              "message": "Generated compliance summary for 2 employees",
              "report_summary": [
                {
                  "employee_number": "EN-016314",
                  "reference_id": "UNKNOWN",
                  "file_name": "ComplianceReportAgent_UNKNOWN_v1_20250307.json",
                  "overall_compliance": false,
                  "risk_level": "High",
                  "alert_count": 1
                },
                {
                  "employee_number": "EN-017901",
                  "reference_id": "UNKNOWN",
                  "file_name": "ComplianceReportAgent_UNKNOWN_v1_20250307.json",
                  "overall_compliance": false,
                  "risk_level": "High",
                  "alert_count": 1
                }
              ],
              "subsection_summary": [
                {
                  "employee_number": "EN-016314",
                  "reference_id": "UNKNOWN",
                  "file_name": "ComplianceReportAgent_UNKNOWN_v1_20250307.json",
                  "subsection": "status_evaluation",
                  "compliance": false,
                  "alert_count": 1,
                  "explanation": "Individual not found: Individual not found: No name data retrieved from search."
                },
                {
                  "employee_number": "EN-016314",
                  "reference_id": "UNKNOWN",
                  "file_name": "ComplianceReportAgent_UNKNOWN_v1_20250307.json",
                  "subsection": "name_evaluation",
                  "compliance": false,
                  "alert_count": 0,
                  "explanation": "Name evaluation skipped due to search failure: Individual not found: No name data retrieved from search."
                },
                {
                  "employee_number": "EN-017901",
                  "reference_id": "UNKNOWN",
                  "file_name": "ComplianceReportAgent_UNKNOWN_v1_20250307.json",
                  "subsection": "status_evaluation",
                  "compliance": false,
                  "alert_count": 1,
                  "explanation": "Individual not found: Individual not found: No name data retrieved from search."
                },
                {
                  "employee_number": "EN-017901",
                  "reference_id": "UNKNOWN",
                  "file_name": "ComplianceReportAgent_UNKNOWN_v1_20250307.json",
                  "subsection": "name_evaluation",
                  "compliance": false,
                  "alert_count": 0,
                  "explanation": "Name evaluation skipped due to search failure: Individual not found: No name data retrieved from search."
                }
              ],
              "pagination": {
                "total_items": 1096,
                "total_pages": 548,
                "current_page": 1,
                "page_size": 2
              }
            }
        """
        result = {
            "status": "success",
            "message": "",
            "report_summary": [],
            "subsection_summary": [],
            "pagination": {
                "total_items": 0,
                "total_pages": 0,
                "current_page": max(1, page),  # Ensure page is at least 1
                "page_size": max(1, page_size)  # Ensure page_size is at least 1
            }
        }

        if employee_number is None:
            # Summarize reports for all employees with pagination over employees
            if not self.cache_folder.exists():
                result["status"] = "warning"
                result["message"] = f"Cache folder not found at {self.cache_folder}"
                logger.warning(result["message"])
                return json.dumps(result, indent=2)
            
            if not self.cache_folder.is_dir():
                result["status"] = "warning"
                result["message"] = f"Cache folder is not a directory: {self.cache_folder}"
                logger.warning(result["message"])
                return json.dumps(result, indent=2)

            # Fallback using os.listdir
            try:
                dir_contents = os.listdir(self.cache_folder)
            except Exception as e:
                result["status"] = "error"
                result["message"] = f"Failed to list directory contents with os.listdir: {str(e)}"
                logger.error(result["message"])
                return json.dumps(result, indent=2)

            # Main method using pathlib
            emp_dirs = []
            try:
                emp_dirs = list(self.cache_folder.iterdir())
            except Exception as e:
                result["status"] = "error"
                result["message"] = f"Failed to scan cache folder with pathlib: {str(e)}"
                logger.error(result["message"])
                return json.dumps(result, indent=2)

            # If pathlib fails, try constructing paths from os.listdir
            if not emp_dirs and dir_contents:
                emp_dirs = [self.cache_folder / name for name in dir_contents]

            # Additional fallback: manually construct paths and check existence
            if not emp_dirs:
                known_emp = self.cache_folder / "LD-107-Dev-3A"
                if known_emp.exists() and known_emp.is_dir():
                    emp_dirs.append(known_emp)

            if not emp_dirs:
                result["status"] = "warning"
                result["message"] = "No directories found in cache folder"
                logger.warning(result["message"])
                return json.dumps(result, indent=2)

            # Build a dictionary of reports by employee
            employee_reports = {}
            for emp_path in sorted(emp_dirs):
                if emp_path.is_dir():
                    # Check for symbolic link
                    if emp_path.is_symlink():
                        emp_path = emp_path.resolve()

                    # Load compliance reports for this employee
                    reports = self._load_compliance_reports(emp_path)
                    if reports:
                        emp_report_data, emp_subsection_data = self._extract_compliance_data(reports, emp_path.name)
                        employee_reports[emp_path.name] = (emp_report_data, emp_subsection_data)
                    else:
                        logger.warning(f"No compliance files found in {emp_path}")
                else:
                    logger.warning(f"Skipping non-directory: {emp_path.name}")

            # Apply pagination over employees
            employee_keys = sorted(employee_reports.keys())
            total_items = len(employee_keys)  # Total number of employees with reports
            page_size = max(1, page_size)
            total_pages = (total_items + page_size - 1) // page_size  # Ceiling division
            current_page = max(1, min(page, total_pages))  # Ensure page is within bounds
            start_idx = (current_page - 1) * page_size
            end_idx = start_idx + page_size

            # Update pagination metadata
            result["pagination"]["total_items"] = total_items
            result["pagination"]["total_pages"] = total_pages
            result["pagination"]["current_page"] = current_page
            result["pagination"]["page_size"] = page_size

            # Collect reports and subsections for the current page of employees
            page_employee_keys = employee_keys[start_idx:end_idx]
            for emp_key in page_employee_keys:
                emp_report_data, emp_subsection_data = employee_reports[emp_key]
                result["report_summary"].extend(emp_report_data)
                result["subsection_summary"].extend(emp_subsection_data)

            if not result["report_summary"]:
                result["status"] = "warning"
                result["message"] = "No compliance reports found in cache"
                logger.warning(result["message"])
            else:
                result["message"] = f"Generated compliance summary for {len(page_employee_keys)} employees (page {current_page} of {total_pages})"
            return json.dumps(result, indent=2)

        # Summarize reports for a specific employee with pagination over reports
        emp_path = self.cache_folder / employee_number
        result["employee_number"] = employee_number
        
        if not emp_path.exists():
            result["status"] = "warning"
            result["message"] = f"No compliance reports found for {employee_number} at {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)
        
        if not emp_path.is_dir():
            result["status"] = "warning"
            result["message"] = f"Employee path is not a directory: {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)

        # Load and process reports for this employee
        reports = self._load_compliance_reports(emp_path)
        if not reports:
            result["status"] = "warning"
            result["message"] = f"No compliance reports found for {employee_number} at {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)

        full_report_summary, full_subsection_summary = self._extract_compliance_data(reports, employee_number)

        # Apply pagination to the reports list for this employee
        total_items = len(full_report_summary)
        page_size = max(1, page_size)
        total_pages = (total_items + page_size - 1) // page_size  # Ceiling division
        current_page = max(1, min(page, total_pages))  # Ensure page is within bounds
        start_idx = (current_page - 1) * page_size
        end_idx = start_idx + page_size

        # Update pagination metadata
        result["pagination"]["total_items"] = total_items
        result["pagination"]["total_pages"] = total_pages
        result["pagination"]["current_page"] = current_page
        result["pagination"]["page_size"] = page_size

        # Slice the summaries
        result["report_summary"] = full_report_summary[start_idx:end_idx]
        result["subsection_summary"] = [
            entry for entry in full_subsection_summary
            if any(report["reference_id"] == entry["reference_id"] for report in result["report_summary"])
        ]
        result["message"] = f"Generated compliance summary for {employee_number} with {len(result['report_summary'])} reports (page {current_page} of {total_pages})"
        return json.dumps(result, indent=2)

# CLI Implementation
if __name__ == "__main__":
    """
    Command-line interface for CacheManager operations.

    Usage Examples:
        python cache_manager.py --clear-cache LD-107-Dev-3A
        python cache_manager.py --list-cache LD-107-Dev-3A
        python cache_manager.py --get-latest-compliance LD-107-Dev-3A
        python cache_manager.py --get-compliance-by-ref LD-107-Dev-3A EN-53
        python cache_manager.py --list-compliance-reports LD-107-Dev-3A --page 1 --page-size 5
        python cache_manager.py --list-compliance-reports --page 1 --page-size 5
        python cache_manager.py --generate-compliance-summary LD-107-Dev-3A --page 1 --page-size 5
        python cache_manager.py --generate-compliance-summary --page 1 --page-size 5
        python cache_manager.py --cache-folder /custom/path/to/cache --list-compliance-reports
    """
    try:
        parser = argparse.ArgumentParser(description="Cache Manager CLI for managing regulatory and compliance cache.")
        parser.add_argument("--cache-folder", help="Override the default cache folder location (default: 'cache' subdirectory of script location)")
        parser.add_argument("--clear-cache", help="Clear all cache (except ComplianceReportAgent) for an employee")
        parser.add_argument("--clear-compliance", help="Clear only ComplianceReportAgent cache for an employee")
        parser.add_argument("--clear-agent", nargs=2, metavar=("EMPLOYEE_NUMBER", "AGENT_NAME"), help="Clear cache for a specific agent")
        parser.add_argument("--list-cache", nargs="?", const="ALL", help="List all cached files (or specify an employee)")
        parser.add_argument("--cleanup-stale", action="store_true", help="Delete stale cache older than CACHE_TTL_DAYS")
        parser.add_argument("--get-latest-compliance", help="Get the latest compliance report for an employee")
        parser.add_argument("--get-compliance-by-ref", nargs=2, metavar=("EMPLOYEE_NUMBER", "REFERENCE_ID"), help="Get compliance report by reference ID")
        parser.add_argument("--page", type=int, default=1, help="Page number for paginated results (default: 1)")
        parser.add_argument("--page-size", type=int, default=10, help="Number of items per page (default: 10)")

        # Create a mutually exclusive group for report listing and summary generation
        report_group = parser.add_mutually_exclusive_group()
        report_group.add_argument("--list-compliance-reports", nargs="?", const=None, help="List all compliance reports with latest revision (or specify an employee)")
        report_group.add_argument("--generate-compliance-summary", action="store_true", help="Generate a compliance summary with report-level and subsection-level data")

        args = parser.parse_args()
        cache_folder = Path(args.cache_folder) if args.cache_folder else DEFAULT_CACHE_FOLDER
        cache_manager = CacheManager(cache_folder=cache_folder)

        if args.clear_cache:
            print(cache_manager.clear_cache(args.clear_cache))
        elif args.clear_compliance:
            print(cache_manager.clear_compliance_cache(args.clear_compliance))
        elif args.clear_agent:
            employee, agent = args.clear_agent
            print(cache_manager.clear_agent_cache(employee, agent))
        elif hasattr(args, 'list_cache') and args.list_cache is not None:
            print(cache_manager.list_cache(args.list_cache))
        elif args.cleanup_stale:
            print(cache_manager.cleanup_stale_cache())
        elif args.get_latest_compliance:
            print(cache_manager.get_latest_compliance_report(args.get_latest_compliance))
        elif args.get_compliance_by_ref:
            employee, ref_id = args.get_compliance_by_ref
            print(cache_manager.get_compliance_report_by_ref(employee, ref_id))
        elif args.list_compliance_reports is not None:
            print(cache_manager.list_compliance_reports(args.list_compliance_reports, page=args.page, page_size=args.page_size))
        elif args.generate_compliance_summary:
            print(cache_manager.generate_compliance_summary(None, page=args.page, page_size=args.page_size))
        else:
            parser.print_help()
    except Exception as e:
        logger.error(f"Unexpected error in CLI execution: {str(e)}")