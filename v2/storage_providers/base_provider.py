"""
Base storage provider interface.

This module defines the abstract base class for storage providers,
which provides a common interface for both local and S3 storage operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, BinaryIO
import logging

logger = logging.getLogger(__name__)

class BaseStorageProvider(ABC):
    """Interface for storage operations."""
    
    def __init__(self):
        """Initialize the storage provider."""
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def initialize(self, config: Dict[str, Any]):
        """Initialize with configuration dictionary."""
        pass
    
    @abstractmethod
    def save_file(self, file_path: str, content: Any) -> bool:
        """Save content to a file."""
        pass
    
    @abstractmethod
    def read_file(self, file_path: str, storage_type: str = None) -> Optional[Any]:
        """Read content from a file.
        
        Args:
            file_path: Path to the file to read
            storage_type: Type of storage (input, output, archive, cache)
            
        Returns:
            File contents
        """
        pass
    
    @abstractmethod
    def delete_file(self, file_path: str) -> bool:
        """Delete a file."""
        pass
    
    @abstractmethod
    def list_files(self, directory: str = "", pattern: Optional[str] = None, storage_type: str = None) -> List[str]:
        """List files in directory.
        
        Args:
            directory: Directory to list files from
            pattern: Optional glob pattern to filter files
            storage_type: Type of storage (input, output, archive, cache)
            
        Returns:
            List of file paths relative to the storage type directory
        """
        pass
    
    def move_file(self, source: str, dest: str, source_type: str = None, dest_type: str = None) -> bool:
        """Move a file from source to destination.
        
        Args:
            source: Source path
            dest: Destination path
            source_type: Type of source storage (input, output, archive, cache)
            dest_type: Type of destination storage (input, output, archive, cache)
            
        Returns:
            True if successful, False otherwise
        """
        if self.save_file(dest, self.read_file(source, source_type)):
            return self.delete_file(source)
        return False
    
    @abstractmethod
    def write_file(self, path: str, content: Union[str, bytes, BinaryIO], storage_type: str = None) -> bool:
        """
        Write content to a file.
        
        Args:
            path: The path where the file should be written.
            content: The content to write (string, bytes, or file-like object).
            storage_type: Type of storage (input, output, archive, cache)
            
        Returns:
            True if the write was successful, False otherwise.
            
        Raises:
            IOError: If there is an error writing the file.
        """
        pass
    
    @abstractmethod
    def file_exists(self, path: str, storage_type: str = None) -> bool:
        """
        Check if a file exists.
        
        Args:
            path: The path to check.
            storage_type: Type of storage (input, output, archive, cache)
            
        Returns:
            True if the file exists, False otherwise.
            
        Raises:
            IOError: If there is an error checking the file.
        """
        pass
    
    @abstractmethod
    def create_directory(self, path: str, storage_type: str = None) -> bool:
        """
        Create a directory.
        
        Args:
            path: The path to the directory to create.
            storage_type: Type of storage (input, output, archive, cache)
            
        Returns:
            True if the directory was created successfully, False otherwise.
            
        Raises:
            IOError: If there is an error creating the directory.
        """
        pass
    
    @abstractmethod
    def get_file_size(self, path: str, storage_type: str = None) -> int:
        """
        Get the size of a file in bytes.
        
        Args:
            path: The path to the file.
            storage_type: Type of storage (input, output, archive, cache)
            
        Returns:
            The size of the file in bytes.
            
        Raises:
            FileNotFoundError: If the file does not exist.
            IOError: If there is an error getting the file size.
        """
        pass
    
    @abstractmethod
    def get_file_modified_time(self, path: str, storage_type: str = None) -> float:
        """
        Get the last modified time of a file.
        
        Args:
            path: The path to the file.
            storage_type: Type of storage (input, output, archive, cache)
            
        Returns:
            The last modified time as a Unix timestamp.
            
        Raises:
            FileNotFoundError: If the file does not exist.
            IOError: If there is an error getting the modified time.
        """
        pass