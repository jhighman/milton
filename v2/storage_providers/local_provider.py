"""
Local storage provider implementation.

This module implements the StorageProvider interface for local filesystem operations.
"""

import os
import glob
import shutil
from typing import List, Optional, Union, BinaryIO
from pathlib import Path
import logging
import fnmatch
from .base_provider import BaseStorageProvider

logger = logging.getLogger(__name__)

class LocalStorageProvider(BaseStorageProvider):
    """Provider for local filesystem storage operations."""

    def __init__(self, base_path: str = ".", input_folder: str = "input",
                 output_folder: Optional[str] = None, archive_folder: Optional[str] = None,
                 cache_folder: Optional[str] = None):
        """Initialize the local storage provider.

        Args:
            base_path: Base path for all storage operations
            input_folder: Input folder name relative to base_path
            output_folder: Output folder name relative to base_path (defaults to input_folder)
            archive_folder: Archive folder name relative to base_path (defaults to input_folder)
            cache_folder: Cache folder name relative to base_path (defaults to input_folder)
        """
        # Convert base_path to Path object and resolve it to absolute path
        self.base_path = Path(os.path.realpath(base_path))
        
        # Store folder names
        self.input_folder = input_folder
        self.output_folder = output_folder or input_folder
        self.archive_folder = archive_folder or input_folder
        self.cache_folder = cache_folder or input_folder

        # Create folders if they don't exist
        for folder in [self.input_folder, self.output_folder, self.archive_folder, self.cache_folder]:
            folder_path = self.base_path / folder
            if not folder_path.exists():
                try:
                    folder_path.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    logger.error(f"Failed to create directory {folder_path}: {str(e)}")
                    raise OSError(f"Failed to create directory {folder_path}: {str(e)}")

        logger.info(f"Initialized LocalStorageProvider with base path: {self.base_path}")
    
    def _normalize_path(self, path: str) -> str:
        """Normalize a path by converting backslashes to forward slashes and making it relative.

        Args:
            path: Path to normalize

        Returns:
            Normalized path
        """
        # Convert to Path object
        path_obj = Path(path)
        
        # Make absolute path relative to base_path
        if path_obj.is_absolute():
            try:
                path_obj = path_obj.relative_to(self.base_path)
            except ValueError:
                # If paths are on different drives or not relative, just use the filename
                path_obj = Path(path_obj.name)
        
        # Convert to string with forward slashes
        normalized = str(path_obj).replace('\\', '/')
        
        # Remove leading slashes
        normalized = normalized.lstrip('/')
        
        return normalized

    def _get_full_path(self, path: str) -> Path:
        """Get the full filesystem path for a given path.

        Args:
            path: Path relative to base directory

        Returns:
            Full filesystem path as Path object
        """
        # Simply normalize the path and join it with the base path
        normalized_path = self._normalize_path(path)
        return self.base_path / normalized_path
    
    def read_file(self, path: str) -> bytes:
        """Read a file from storage.

        Args:
            path: Path to file

        Returns:
            File contents as bytes

        Raises:
            FileNotFoundError: If file does not exist
            PermissionError: If access is denied
            OSError: If there is an error reading the file
        """
        full_path = self._get_full_path(path)
        try:
            if not full_path.exists():
                logger.error(f"File not found: {path}")
                raise FileNotFoundError(f"File not found: {path}")
            return full_path.read_bytes()
        except (FileNotFoundError, PermissionError) as e:
            logger.error(f"Error reading file {path}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error reading file {path}: {str(e)}")
            raise OSError(f"Error reading file {path}: {str(e)}")
    
    def write_file(self, path: str, content: Union[str, bytes]) -> bool:
        """Write content to a file.

        Args:
            path: Path to file
            content: Content to write (string or bytes)

        Returns:
            True if successful, False otherwise
        """
        full_path = self._get_full_path(path)
        try:
            full_path.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(content, str):
                full_path.write_text(content)
            else:
                full_path.write_bytes(content)
            return True
        except PermissionError as e:
            logger.error(f"Permission denied writing file {path}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error writing file {path}: {str(e)}")
            raise OSError(f"Error writing file {path}: {str(e)}")
    
    def list_files(self, path: str = "", pattern: str = None) -> List[str]:
        """List files in a directory.

        Args:
            path: Directory path to list
            pattern: Optional pattern to filter files

        Returns:
            List of file paths
        """
        full_path = self._get_full_path(path)
        try:
            files = []
            for file_path in full_path.rglob('*'):
                if file_path.is_file():
                    relative_path = str(file_path.relative_to(full_path))
                    if pattern is None or fnmatch.fnmatch(relative_path, pattern):
                        files.append(relative_path)
            return files
        except Exception as e:
            logger.error(f"Error listing files in {path}: {str(e)}")
            raise
    
    def delete_file(self, path: str) -> bool:
        """Delete a file.

        Args:
            path: Path to file

        Returns:
            True if successful, False otherwise
        """
        full_path = self._get_full_path(path)
        try:
            if full_path.exists():
                full_path.unlink()
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting file {path}: {str(e)}")
            raise
    
    def move_file(self, source: str, dest: str) -> bool:
        """Move a file from source to destination.

        Args:
            source: Source path
            dest: Destination path

        Returns:
            True if successful, False otherwise
        """
        source_path = self._get_full_path(source)
        dest_path = self._get_full_path(dest)
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.rename(dest_path)
            return True
        except FileNotFoundError as e:
            logger.error(f"File not found when moving from {source} to {dest}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error moving file from {source} to {dest}: {str(e)}")
            return False
    
    def file_exists(self, path: str) -> bool:
        """Check if a file exists.

        Args:
            path: Path to file

        Returns:
            True if file exists, False otherwise
        """
        full_path = self._get_full_path(path)
        return full_path.exists()
    
    def create_directory(self, path: str) -> bool:
        """Create a directory.

        Args:
            path: Directory path to create

        Returns:
            True if successful, False otherwise
        """
        try:
            # Get the full path for the directory
            full_path = self._get_full_path(path)
            full_path.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"Error creating directory {path}: {str(e)}")
            return False
    
    def get_file_size(self, path: str) -> int:
        """Get the size of a file in bytes.

        Args:
            path: Path to file

        Returns:
            File size in bytes

        Raises:
            FileNotFoundError: If file does not exist
        """
        full_path = self._get_full_path(path)
        try:
            return full_path.stat().st_size
        except FileNotFoundError:
            logger.error(f"File not found: {path}")
            raise
    
    def get_file_modified_time(self, path: str) -> float:
        """Get the last modified time of a file.

        Args:
            path: Path to file

        Returns:
            Last modified time as Unix timestamp

        Raises:
            FileNotFoundError: If file does not exist
        """
        full_path = self._get_full_path(path)
        try:
            return full_path.stat().st_mtime
        except FileNotFoundError:
            logger.error(f"File not found: {path}")
            raise 