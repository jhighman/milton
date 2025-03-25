"""
Local storage provider implementation.

This module implements the StorageProvider interface for local file system operations.
"""

import os
import glob
import shutil
from typing import List, Optional, Union, BinaryIO
from pathlib import Path
import logging

from .base import StorageProvider

logger = logging.getLogger(__name__)

class LocalStorageProvider(StorageProvider):
    """Implementation of StorageProvider for local file system operations."""
    
    def __init__(self, base_path: Union[str, Path]):
        """
        Initialize the local storage provider.
        
        Args:
            base_path: The base path for all file operations.
        """
        self.base_path = Path(base_path)
        logger.info(f"Initialized LocalStorageProvider with base path: {base_path}")
    
    def _get_full_path(self, path: str) -> Path:
        """
        Get the full path by joining with the base path.
        
        Args:
            path: The relative path.
            
        Returns:
            The full path as a Path object.
        """
        return self.base_path / path
    
    def read_file(self, path: str) -> bytes:
        """Read a file and return its contents as bytes."""
        full_path = self._get_full_path(path)
        try:
            with open(full_path, 'rb') as f:
                return f.read()
        except FileNotFoundError:
            logger.error(f"File not found: {full_path}")
            raise
        except IOError as e:
            logger.error(f"Error reading file {full_path}: {e}")
            raise
    
    def write_file(self, path: str, content: Union[str, bytes, BinaryIO]) -> bool:
        """Write content to a file."""
        full_path = self._get_full_path(path)
        try:
            # Create parent directories if they don't exist
            full_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Handle different content types
            if isinstance(content, str):
                content = content.encode('utf-8')
            elif isinstance(content, BinaryIO):
                content = content.read()
            
            with open(full_path, 'wb') as f:
                f.write(content)
            return True
        except IOError as e:
            logger.error(f"Error writing file {full_path}: {e}")
            return False
    
    def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
        """List files in a directory, optionally filtered by pattern."""
        full_path = self._get_full_path(directory)
        try:
            if pattern:
                # Use glob for pattern matching
                files = glob.glob(str(full_path / pattern))
            else:
                # Recursively walk through directory
                files = []
                for root, _, filenames in os.walk(full_path):
                    for filename in filenames:
                        file_path = Path(root) / filename
                        files.append(str(file_path))
            
            # Convert to relative paths
            return [str(Path(f).relative_to(self.base_path)) for f in files]
        except FileNotFoundError:
            logger.error(f"Directory not found: {full_path}")
            raise
        except IOError as e:
            logger.error(f"Error listing directory {full_path}: {e}")
            raise
    
    def delete_file(self, path: str) -> bool:
        """Delete a file."""
        full_path = self._get_full_path(path)
        try:
            if full_path.exists():
                full_path.unlink()
                return True
            return False
        except IOError as e:
            logger.error(f"Error deleting file {full_path}: {e}")
            return False
    
    def move_file(self, source: str, destination: str) -> bool:
        """Move a file from source to destination."""
        full_source = self._get_full_path(source)
        full_dest = self._get_full_path(destination)
        try:
            # Create parent directories if they don't exist
            full_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(full_source), str(full_dest))
            return True
        except FileNotFoundError:
            logger.error(f"Source file not found: {full_source}")
            return False
        except IOError as e:
            logger.error(f"Error moving file from {full_source} to {full_dest}: {e}")
            return False
    
    def file_exists(self, path: str) -> bool:
        """Check if a file exists."""
        full_path = self._get_full_path(path)
        try:
            return full_path.is_file()
        except IOError as e:
            logger.error(f"Error checking file existence {full_path}: {e}")
            return False
    
    def create_directory(self, path: str) -> bool:
        """Create a directory."""
        full_path = self._get_full_path(path)
        try:
            full_path.mkdir(parents=True, exist_ok=True)
            return True
        except IOError as e:
            logger.error(f"Error creating directory {full_path}: {e}")
            return False
    
    def get_file_size(self, path: str) -> int:
        """Get the size of a file in bytes."""
        full_path = self._get_full_path(path)
        try:
            return full_path.stat().st_size
        except FileNotFoundError:
            logger.error(f"File not found: {full_path}")
            raise
        except IOError as e:
            logger.error(f"Error getting file size for {full_path}: {e}")
            raise
    
    def get_file_modified_time(self, path: str) -> float:
        """Get the last modified time of a file."""
        full_path = self._get_full_path(path)
        try:
            return full_path.stat().st_mtime
        except FileNotFoundError:
            logger.error(f"File not found: {full_path}")
            raise
        except IOError as e:
            logger.error(f"Error getting file modified time for {full_path}: {e}")
            raise 