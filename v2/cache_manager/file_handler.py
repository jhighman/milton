"""
==============================================
📌 FILE HANDLER MODULE OVERVIEW
==============================================

🗂 PURPOSE
This module provides a `FileHandler` class to encapsulate low-level filesystem operations
like reading, writing, and deleting files, used by other CacheManager components.

🗂 USAGE
Initialize with a base path and use methods:
    from cache_manager.file_handler import FileHandler
    handler = FileHandler(Path("cache"))
    files = handler.list_files(Path("cache/EMP001"))

🗂 FEATURES
- Lists files with pattern matching
- Reads JSON files with error handling
- Deletes files or directories
- Provides last modified timestamps

🗂 TROUBLESHOOTING
- If `list_files` returns empty, check path existence with `path.exists()`.
- Ensure proper permissions with `os.access(path, os.R_OK)`.
==============================================
"""

import os
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

import logging
logger = logging.getLogger("FileHandler")

class FileHandler:
    """
    Handles filesystem operations for the CacheManager package.

    Attributes:
        base_path (Path): Base directory for file operations.

    Methods:
        list_files: Lists files matching a pattern in a directory.
        read_json: Reads a JSON file with error handling.
        delete_path: Deletes a file or directory.
        get_last_modified: Retrieves the last modified timestamp of a file.
    """

    def __init__(self, base_path: Path):
        """Initialize with a base directory."""
        self.base_path = base_path

    def list_files(self, path: Path, pattern: str = "*.json") -> List[Path]:
        """
        Lists files in a directory matching a pattern.

        Args:
            path (Path): Directory to scan.
            pattern (str): Glob pattern (default: "*.json").

        Returns:
            List[Path]: Sorted list of file paths.

        Example:
            >>> handler.list_files(Path("cache/EMP001"), "*.json")
            [Path("cache/EMP001/SEC_IAPD_Agent/file.json")]
        """
        try:
            return sorted(path.glob(pattern))
        except Exception as e:
            logger.error(f"Failed to list files in {path}: {str(e)}")
            return []

    def read_json(self, file_path: Path) -> Optional[Dict]:
        """
        Reads a JSON file and returns its contents.

        Args:
            file_path (Path): Path to the JSON file.

        Returns:
            Optional[Dict]: Parsed JSON data or None if reading fails.
        """
        try:
            with file_path.open("r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read JSON from {file_path}: {str(e)}")
            return None

    def delete_path(self, path: Path) -> bool:
        """Deletes a file or directory. Returns True on success."""
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            return True
        except Exception as e:
            logger.warning(f"Failed to delete {path}: {str(e)}")
            return False

    def get_last_modified(self, file_path: Path) -> str:
        """Returns the last modified timestamp as a string."""
        try:
            return datetime.fromtimestamp(file_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return "Unknown"