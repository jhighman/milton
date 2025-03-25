"""
Base storage provider interface for file operations.
This module defines the abstract base class that all storage providers must implement.
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
            path (str): Path to the file to read
            
        Returns:
            bytes: The contents of the file
            
        Raises:
            FileNotFoundError: If the file does not exist
            IOError: If there is an error reading the file
        """
        pass
    
    @abstractmethod
    def write_file(self, path: str, content: Union[str, bytes, BinaryIO]) -> bool:
        """
        Write content to a file.
        
        Args:
            path (str): Path where the file should be written
            content (Union[str, bytes, BinaryIO]): Content to write to the file
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            IOError: If there is an error writing the file
        """
        pass
    
    @abstractmethod
    def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
        """
        List files in a directory, optionally filtered by pattern.
        
        Args:
            directory (str): Directory to list files from
            pattern (Optional[str]): Optional glob pattern to filter files
            
        Returns:
            List[str]: List of file paths
            
        Raises:
            FileNotFoundError: If the directory does not exist
            IOError: If there is an error listing files
        """
        pass
    
    @abstractmethod
    def delete_file(self, path: str) -> bool:
        """
        Delete a file.
        
        Args:
            path (str): Path to the file to delete
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            FileNotFoundError: If the file does not exist
            IOError: If there is an error deleting the file
        """
        pass
    
    @abstractmethod
    def move_file(self, source: str, destination: str) -> bool:
        """
        Move a file from source to destination.
        
        Args:
            source (str): Source path of the file
            destination (str): Destination path for the file
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            FileNotFoundError: If the source file does not exist
            IOError: If there is an error moving the file
        """
        pass
    
    @abstractmethod
    def file_exists(self, path: str) -> bool:
        """
        Check if a file exists.
        
        Args:
            path (str): Path to check
            
        Returns:
            bool: True if the file exists, False otherwise
        """
        pass
    
    @abstractmethod
    def create_directory(self, path: str) -> bool:
        """
        Create a directory.
        
        Args:
            path (str): Path of the directory to create
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            IOError: If there is an error creating the directory
        """
        pass
    
    @abstractmethod
    def get_file_size(self, path: str) -> int:
        """
        Get the size of a file in bytes.
        
        Args:
            path (str): Path to the file
            
        Returns:
            int: Size of the file in bytes
            
        Raises:
            FileNotFoundError: If the file does not exist
            IOError: If there is an error getting the file size
        """
        pass
    
    @abstractmethod
    def get_file_modified_time(self, path: str) -> float:
        """
        Get the last modified time of a file.
        
        Args:
            path (str): Path to the file
            
        Returns:
            float: Last modified time as a Unix timestamp
            
        Raises:
            FileNotFoundError: If the file does not exist
            IOError: If there is an error getting the modified time
        """
        pass 