# cache_operations.py
"""
==============================================
📌 CACHE OPERATIONS MODULE OVERVIEW
==============================================
🗂 PURPOSE
This module provides the `CacheManager` class for general cache operations related to regulatory
and compliance data. It handles clearing, listing, and cleaning up stale cache, excluding
compliance-specific logic, which is delegated to `compliance_handler.py`.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from .config import DEFAULT_CACHE_FOLDER, CACHE_TTL_DAYS
from .agents import AgentName
from .file_handler import FileHandler
import logging
logger = logging.getLogger("CacheOperations")

class CacheManager:
    """
    Manages general cache operations for regulatory data.

    Attributes:
        cache_folder (Path): Directory where cache data is stored (default: `cache/`).
        ttl_days (int): Time-to-live for cache files in days (default: 90).
        file_handler (FileHandler): Helper class for filesystem operations.
    """

    def __init__(self, cache_folder: Path = DEFAULT_CACHE_FOLDER, ttl_days: int = CACHE_TTL_DAYS):
        self.cache_folder = cache_folder
        self.ttl_days = ttl_days
        self.file_handler = FileHandler(cache_folder)
        if not self.cache_folder.exists():
            logger.warning(f"Cache folder does not exist: {self.cache_folder}")

    def clear_cache(self, employee_number: str) -> str:
        """
        Clear all cache except ComplianceReportAgent for a specific employee.

        Args:
            employee_number (str): The employee identifier.

        Returns:
            str: JSON-formatted result of the operation.
        """
        emp_path = self.cache_folder / employee_number
        result = {"employee_number": employee_number, "cleared_agents": [], "status": "success", "message": ""}
        if not emp_path.exists():
            result["status"] = "warning"
            result["message"] = f"No cache found for {employee_number} at {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)
        for agent_folder in self.file_handler.list_files(emp_path, "*"):
            if agent_folder.is_dir() and agent_folder.name != AgentName.COMPLIANCE_REPORT:
                self.file_handler.delete_path(agent_folder)
                result["cleared_agents"].append(agent_folder.name)
        result["message"] = f"Cleared cache for {len(result['cleared_agents'])} agents"
        logger.info(result["message"])
        return json.dumps(result, indent=2)

    def clear_all_cache(self) -> str:
        """
        Clear all cache except ComplianceReportAgent files across all employees.

        Returns:
            str: JSON-formatted result of the operation.
        """
        result = {"cleared_employees": [], "total_cleared_agents": 0, "status": "success", "message": ""}
        if not self.cache_folder.exists():
            result["status"] = "warning"
            result["message"] = f"No employee cache folders found at {self.cache_folder}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)

        emp_dirs = self.file_handler.list_files(self.cache_folder, "*")
        for emp_path in emp_dirs:
            if emp_path.is_dir():
                emp_num = emp_path.name
                cleared_agents = []
                for agent_folder in self.file_handler.list_files(emp_path, "*"):
                    if agent_folder.is_dir() and agent_folder.name != AgentName.COMPLIANCE_REPORT:
                        self.file_handler.delete_path(agent_folder)
                        cleared_agents.append(agent_folder.name)
                if cleared_agents:
                    result["cleared_employees"].append({"employee_number": emp_num, "cleared_agents": cleared_agents})
                    result["total_cleared_agents"] += len(cleared_agents)

        if not result["cleared_employees"]:
            result["status"] = "warning"
            result["message"] = f"No cache found to clear in {self.cache_folder}"
        else:
            result["message"] = f"Cleared cache for {len(result['cleared_employees'])} employees, {result['total_cleared_agents']} agents total"
        logger.info(result["message"])
        return json.dumps(result, indent=2)

    def clear_agent_cache(self, employee_number: str, agent_name: str) -> str:
        """
        Clear cache for a specific agent under an employee.

        Args:
            employee_number (str): The employee identifier.
            agent_name (str): The agent name (e.g., SEC_IAPD_Agent).

        Returns:
            str: JSON-formatted result of the operation.
        """
        agent_path = self.cache_folder / employee_number / agent_name
        result = {"employee_number": employee_number, "agent_name": agent_name, "status": "success", "message": ""}
        if agent_path.exists():
            self.file_handler.delete_path(agent_path)
            result["message"] = f"Cleared cache for agent {agent_name} under {employee_number}"
            logger.info(result["message"])
        else:
            result["status"] = "warning"
            result["message"] = f"No cache found for {agent_name} under {employee_number} at {agent_path}"
            logger.warning(result["message"])
        return json.dumps(result, indent=2)

    def list_cache(self, employee_number: Optional[str] = None, page: int = 1, page_size: int = 10) -> str:
        """
        Lists all cached files for either all employees or a specific employee, with pagination.

        Args:
            employee_number (Optional[str]): Employee identifier (e.g., "LD-107-Dev-3A"). If None or "ALL", lists all employees.
            page (int): Page number to retrieve (default: 1).
            page_size (int): Number of items per page (default: 10).

        Returns:
            str: JSON-formatted result with cache contents and pagination info.

        Example Output (All Employees):
            {
              "status": "success",
              "message": "Listed all employees with cache (page 1 of 2)",
              "cache": {
                "employees": ["EN-016314", "EN-017901"]
              },
              "pagination": {
                "total_items": 15,
                "total_pages": 2,
                "current_page": 1,
                "page_size": 10
              }
            }

        Example Output (Specific Employee):
            {
              "status": "success",
              "message": "Cache contents for EN-016314",
              "cache": {
                "EN-016314": {
                  "SEC_IAPD_Agent": [
                    {"file_name": "SEC_IAPD_Agent_EN-016314_search_individual_20240307.json", "last_modified": "2024-03-07 10:12:45"}
                  ]
                }
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
            "cache": {},
            "pagination": {
                "total_items": 0,
                "total_pages": 0,
                "current_page": max(1, page),
                "page_size": max(1, page_size)
            }
        }

        if employee_number is None or employee_number.upper() == "ALL":
            if not self.cache_folder.exists():
                result["status"] = "warning"
                result["message"] = f"Cache folder not found at {self.cache_folder}"
                logger.warning(result["message"])
                return json.dumps(result, indent=2)

            try:
                emp_dirs = self.file_handler.list_files(self.cache_folder, "*")
                employee_list = [emp_path.name for emp_path in sorted(emp_dirs) if emp_path.is_dir()]
                total_items = len(employee_list)
                page_size = max(1, page_size)
                total_pages = (total_items + page_size - 1) // page_size
                current_page = max(1, min(page, total_pages))
                start_idx = (current_page - 1) * page_size
                end_idx = start_idx + page_size

                result["cache"]["employees"] = employee_list[start_idx:end_idx]
                result["pagination"].update({
                    "total_items": total_items,
                    "total_pages": total_pages,
                    "current_page": current_page,
                    "page_size": page_size
                })
                result["message"] = f"Listed all employees with cache (page {current_page} of {total_pages})"
            except Exception as e:
                result["status"] = "error"
                result["message"] = f"Failed to list cache folder contents: {str(e)}"
                logger.error(result["message"])
            return json.dumps(result, indent=2)

        emp_path = self.cache_folder / employee_number
        if not emp_path.exists():
            result["status"] = "warning"
            result["message"] = f"No cache found for {employee_number} at {emp_path}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)

        result["cache"][employee_number] = {}
        try:
            for item in self.file_handler.list_files(emp_path, "*"):
                if item.is_dir():
                    files = [
                        {"file_name": file.name, "last_modified": self.file_handler.get_last_modified(file)}
                        for file in self.file_handler.list_files(item, "*.json")
                    ]
                    total_items = len(files)
                    page_size = max(1, page_size)
                    total_pages = (total_items + page_size - 1) // page_size
                    current_page = max(1, min(page, total_pages))
                    start_idx = (current_page - 1) * page_size
                    end_idx = start_idx + page_size

                    result["cache"][employee_number][item.name] = files[start_idx:end_idx]
                    result["pagination"].update({
                        "total_items": total_items,
                        "total_pages": total_pages,
                        "current_page": current_page,
                        "page_size": page_size
                    })
        except Exception as e:
            result["status"] = "error"
            result["message"] = f"Failed to list cache for {employee_number}: {str(e)}"
            logger.error(result["message"])
        result["message"] = f"Cache contents for {employee_number} (page {result['pagination']['current_page']} of {result['pagination']['total_pages']})"
        return json.dumps(result, indent=2)

    def cleanup_stale_cache(self) -> str:
        """
        Delete cache files older than ttl_days, excluding ComplianceReportAgent.

        Returns:
            str: JSON-formatted result of the cleanup operation.
        """
        cutoff_date = datetime.now() - timedelta(days=self.ttl_days)
        result = {"status": "success", "message": "", "deleted_files": []}
        if not self.cache_folder.exists():
            result["status"] = "warning"
            result["message"] = f"Cache folder not found at {self.cache_folder}"
            logger.warning(result["message"])
            return json.dumps(result, indent=2)
        try:
            for emp_path in self.file_handler.list_files(self.cache_folder, "*"):
                if emp_path.is_dir():
                    for agent_folder in self.file_handler.list_files(emp_path, "*"):
                        if agent_folder.name == AgentName.COMPLIANCE_REPORT:
                            continue
                        for file in self.file_handler.list_files(agent_folder, "*.json"):
                            if datetime.fromtimestamp(file.stat().st_mtime) < cutoff_date:
                                self.file_handler.delete_path(file)
                                result["deleted_files"].append(str(file))
        except Exception as e:
            result["status"] = "error"
            result["message"] = f"Failed to cleanup stale cache: {str(e)}"
            logger.error(result["message"])
        result["message"] = f"Deleted {len(result['deleted_files'])} stale cache files"
        logger.info(result["message"])
        return json.dumps(result, indent=2)