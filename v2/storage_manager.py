"""
Storage manager for handling file operations across different storage providers.
"""

import json
import os
from typing import Dict, Any, Optional
from pathlib import Path
import logging

from storage_providers.base import StorageProvider
from storage_providers.local_provider import LocalStorageProvider
from storage_providers.s3_provider import S3StorageProvider

logger = logging.getLogger(__name__)

class StorageManager:
    """Factory and unified interface for storage operations."""
    
    def __init__(self, config_path: str = "config.json"):
        """
        Initialize the storage manager.
        
        Args:
            config_path (str): Path to the configuration file
        """
        self.config = self._load_config(config_path)
        self.provider = self._create_provider()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            return config
        except FileNotFoundError:
            self.logger.warning(f"Config file not found at {config_path}, using defaults")
            return self._get_default_config()
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing config file: {str(e)}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            "storage": {
                "mode": "local",
                "local": {
                    "input_folder": "drop",
                    "output_folder": "output",
                    "archive_folder": "archive",
                    "cache_folder": "cache"
                },
                "s3": {
                    "aws_region": "us-east-1",
                    "input_bucket": "",
                    "input_prefix": "input/",
                    "output_bucket": "",
                    "output_prefix": "output/",
                    "archive_bucket": "",
                    "archive_prefix": "archive/",
                    "cache_bucket": "",
                    "cache_prefix": "cache/"
                }
            }
        }
    
    def _create_provider(self) -> StorageProvider:
        """Create a storage provider based on configuration."""
        storage_mode = self.config.get("storage", {}).get("mode", "local")
        
        if storage_mode == "local":
            local_config = self.config.get("storage", {}).get("local", {})
            return LocalStorageProvider(
                input_folder=local_config.get("input_folder", "drop"),
                output_folder=local_config.get("output_folder", "output"),
                archive_folder=local_config.get("archive_folder", "archive"),
                cache_folder=local_config.get("cache_folder", "cache")
            )
        elif storage_mode == "s3":
            s3_config = self.config.get("storage", {}).get("s3", {})
            return S3StorageProvider(
                aws_region=s3_config.get("aws_region", "us-east-1"),
                input_bucket=s3_config.get("input_bucket", ""),
                input_prefix=s3_config.get("input_prefix", "input/"),
                output_bucket=s3_config.get("output_bucket", ""),
                output_prefix=s3_config.get("output_prefix", "output/"),
                archive_bucket=s3_config.get("archive_bucket", ""),
                archive_prefix=s3_config.get("archive_prefix", "archive/"),
                cache_bucket=s3_config.get("cache_bucket", ""),
                cache_prefix=s3_config.get("cache_prefix", "cache/")
            )
        else:
            raise ValueError(f"Unsupported storage mode: {storage_mode}")
    
    def read_file(self, path: str) -> bytes:
        """Read a file using the configured storage provider."""
        return self.provider.read_file(path)
    
    def write_file(self, path: str, content: Any) -> bool:
        """Write content to a file using the configured storage provider."""
        return self.provider.write_file(path, content)
    
    def list_files(self, directory: str, pattern: Optional[str] = None) -> list:
        """List files in a directory using the configured storage provider."""
        return self.provider.list_files(directory, pattern)
    
    def delete_file(self, path: str) -> bool:
        """Delete a file using the configured storage provider."""
        return self.provider.delete_file(path)
    
    def move_file(self, source: str, destination: str) -> bool:
        """Move a file using the configured storage provider."""
        return self.provider.move_file(source, destination)
    
    def file_exists(self, path: str) -> bool:
        """Check if a file exists using the configured storage provider."""
        return self.provider.file_exists(path)
    
    def create_directory(self, path: str) -> bool:
        """Create a directory using the configured storage provider."""
        return self.provider.create_directory(path)
    
    def get_file_size(self, path: str) -> int:
        """Get the size of a file using the configured storage provider."""
        return self.provider.get_file_size(path)
    
    def get_file_modified_time(self, path: str) -> float:
        """Get the last modified time of a file using the configured storage provider."""
        return self.provider.get_file_modified_time(path)
    
    def update_config(self, new_config: Dict[str, Any]) -> None:
        """
        Update the storage configuration and recreate the provider if needed.
        
        Args:
            new_config (Dict[str, Any]): New configuration to apply
        """
        old_mode = self.config.get("storage", {}).get("mode", "local")
        self.config = new_config
        new_mode = self.config.get("storage", {}).get("mode", "local")
        
        if old_mode != new_mode:
            self.logger.info(f"Storage mode changed from {old_mode} to {new_mode}, recreating provider")
            self.provider = self._create_provider()
    
    def save_config(self, config_path: str = "config.json") -> bool:
        """
        Save the current configuration to a file.
        
        Args:
            config_path (str): Path to save the configuration to
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with open(config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            return True
        except IOError as e:
            self.logger.error(f"Error saving config file: {str(e)}")
            return False 