"""
Local storage provider implementation using standard Python file operations.
"""

import os
import glob
import shutil
from typing import List, Optional, Union, BinaryIO
from pathlib import Path

from .base import StorageProvider

class LocalStorageProvider(StorageProvider):
    """Implementation of StorageProvider for local file system operations."""
    
    def __init__(self, input_folder: str = "drop", output_folder: str = "output",
                 archive_folder: str = "archive", cache_folder: str = "cache"):
        """
        Initialize the local storage provider.
        
        Args:
            input_folder (str): Path to input folder
            output_folder (str): Path to output folder
            archive_folder (str): Path to archive folder
            cache_folder (str): Path to cache folder
        """
        super().__init__()
        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.archive_folder = Path(archive_folder)
        self.cache_folder = Path(cache_folder)
        
        # Create base directories if they don't exist
        for folder in [self.input_folder, self.output_folder, self.archive_folder, self.cache_folder]:
            folder.mkdir(parents=True, exist_ok=True)
    
    def read_file(self, path: str) -> bytes:
        """Read a file and return its contents as bytes."""
        try:
            with open(path, 'rb') as f:
                return f.read()
        except FileNotFoundError:
            self.logger.error(f"File not found: {path}")
            raise
        except IOError as e:
            self.logger.error(f"Error reading file {path}: {str(e)}")
            raise
    
    def write_file(self, path: str, content: Union[str, bytes, BinaryIO]) -> bool:
        """Write content to a file."""
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            # Handle different content types
            if isinstance(content, str):
                content = content.encode('utf-8')
            elif isinstance(content, BinaryIO):
                content = content.read()
            
            with open(path, 'wb') as f:
                f.write(content)
            return True
        except IOError as e:
            self.logger.error(f"Error writing file {path}: {str(e)}")
            return False
    
    def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
        """List files in a directory, optionally filtered by pattern."""
        try:
            if pattern:
                return glob.glob(os.path.join(directory, pattern))
            return os.listdir(directory)
        except FileNotFoundError:
            self.logger.error(f"Directory not found: {directory}")
            raise
        except IOError as e:
            self.logger.error(f"Error listing directory {directory}: {str(e)}")
            raise
    
    def delete_file(self, path: str) -> bool:
        """Delete a file."""
        try:
            if os.path.exists(path):
                os.remove(path)
                return True
            return False
        except IOError as e:
            self.logger.error(f"Error deleting file {path}: {str(e)}")
            return False
    
    def move_file(self, source: str, destination: str) -> bool:
        """Move a file from source to destination."""
        try:
            # Ensure the destination directory exists
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            
            shutil.move(source, destination)
            return True
        except FileNotFoundError:
            self.logger.error(f"Source file not found: {source}")
            return False
        except IOError as e:
            self.logger.error(f"Error moving file from {source} to {destination}: {str(e)}")
            return False
    
    def file_exists(self, path: str) -> bool:
        """Check if a file exists."""
        return os.path.exists(path)
    
    def create_directory(self, path: str) -> bool:
        """Create a directory."""
        try:
            os.makedirs(path, exist_ok=True)
            return True
        except IOError as e:
            self.logger.error(f"Error creating directory {path}: {str(e)}")
            return False
    
    def get_file_size(self, path: str) -> int:
        """Get the size of a file in bytes."""
        try:
            return os.path.getsize(path)
        except FileNotFoundError:
            self.logger.error(f"File not found: {path}")
            raise
        except IOError as e:
            self.logger.error(f"Error getting file size for {path}: {str(e)}")
            raise
    
    def get_file_modified_time(self, path: str) -> float:
        """Get the last modified time of a file."""
        try:
            return os.path.getmtime(path)
        except FileNotFoundError:
            self.logger.error(f"File not found: {path}")
            raise
        except IOError as e:
            self.logger.error(f"Error getting modified time for {path}: {str(e)}")
            raise 