"""
Base storage provider interface.

This module defines the abstract base class for storage providers,
which provides a common interface for both local and S3 storage operations.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Union, BinaryIO
import logging

logger = logging.getLogger(__name__)

class StorageProvider(ABC):
    """Interface for storage operations."""
    
    def __init__(self):
        """Initialize the storage provider."""
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def read_file(self, path: str) -> bytes:
        """
        Read a file and return its contents as bytes.
        
        Args:
            path: The path to the file to read.
            
        Returns:
            The contents of the file as bytes.
            
        Raises:
            FileNotFoundError: If the file does not exist.
            IOError: If there is an error reading the file.
        """
        pass
    
    @abstractmethod
    def write_file(self, path: str, content: Union[str, bytes, BinaryIO]) -> bool:
        """
        Write content to a file.
        
        Args:
            path: The path where the file should be written.
            content: The content to write (string, bytes, or file-like object).
            
        Returns:
            True if the write was successful, False otherwise.
            
        Raises:
            IOError: If there is an error writing the file.
        """
        pass
    
    @abstractmethod
    def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
        """
        List files in a directory, optionally filtered by pattern.
        
        Args:
            directory: The directory to list files from.
            pattern: Optional glob pattern to filter files.
            
        Returns:
            List of file paths relative to the base directory.
            
        Raises:
            FileNotFoundError: If the directory does not exist.
            IOError: If there is an error listing files.
        """
        pass
    
    @abstractmethod
    def delete_file(self, path: str) -> bool:
        """
        Delete a file.
        
        Args:
            path: The path to the file to delete.
            
        Returns:
            True if the deletion was successful, False otherwise.
            
        Raises:
            FileNotFoundError: If the file does not exist.
            IOError: If there is an error deleting the file.
        """
        pass
    
    @abstractmethod
    def move_file(self, source: str, destination: str) -> bool:
        """
        Move a file from source to destination.
        
        Args:
            source: The path to the source file.
            destination: The path to the destination.
            
        Returns:
            True if the move was successful, False otherwise.
            
        Raises:
            FileNotFoundError: If the source file does not exist.
            IOError: If there is an error moving the file.
        """
        pass
    
    @abstractmethod
    def file_exists(self, path: str) -> bool:
        """
        Check if a file exists.
        
        Args:
            path: The path to check.
            
        Returns:
            True if the file exists, False otherwise.
            
        Raises:
            IOError: If there is an error checking the file.
        """
        pass
    
    @abstractmethod
    def create_directory(self, path: str) -> bool:
        """
        Create a directory.
        
        Args:
            path: The path to the directory to create.
            
        Returns:
            True if the directory was created successfully, False otherwise.
            
        Raises:
            IOError: If there is an error creating the directory.
        """
        pass
    
    @abstractmethod
    def get_file_size(self, path: str) -> int:
        """
        Get the size of a file in bytes.
        
        Args:
            path: The path to the file.
            
        Returns:
            The size of the file in bytes.
            
        Raises:
            FileNotFoundError: If the file does not exist.
            IOError: If there is an error getting the file size.
        """
        pass
    
    @abstractmethod
    def get_file_modified_time(self, path: str) -> float:
        """
        Get the last modified time of a file.
        
        Args:
            path: The path to the file.
            
        Returns:
            The last modified time as a Unix timestamp.
            
        Raises:
            FileNotFoundError: If the file does not exist.
            IOError: If there is an error getting the modified time.
        """
        pass 