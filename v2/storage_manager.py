"""
Storage manager module.

This module provides a unified interface for file storage operations using the storage providers.
"""

import logging
from typing import Dict, Any, Optional, Union, BinaryIO, List
from pathlib import Path

from storage_providers import StorageProvider, StorageProviderFactory

logger = logging.getLogger(__name__)

class StorageManager:
    """Factory and unified interface for storage operations."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the storage manager.
        
        Args:
            config: Configuration dictionary containing storage settings.
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._providers: Dict[str, StorageProvider] = {}
        self._initialize_providers()
    
    def _initialize_providers(self):
        """Initialize storage providers based on configuration."""
        storage_config = self.config.get('storage', {})
        mode = storage_config.get('mode', 'local')
        
        if mode == 'local':
            local_config = storage_config.get('local', {})
            self._providers['input'] = StorageProviderFactory.create_provider({
                'type': 'local',
                'base_path': local_config.get('input_folder', 'drop')
            })
            self._providers['output'] = StorageProviderFactory.create_provider({
                'type': 'local',
                'base_path': local_config.get('output_folder', 'output')
            })
            self._providers['archive'] = StorageProviderFactory.create_provider({
                'type': 'local',
                'base_path': local_config.get('archive_folder', 'archive')
            })
            self._providers['cache'] = StorageProviderFactory.create_provider({
                'type': 'local',
                'base_path': local_config.get('cache_folder', 'cache')
            })
        elif mode == 's3':
            s3_config = storage_config.get('s3', {})
            region = s3_config.get('aws_region', 'us-east-1')
            
            self._providers['input'] = StorageProviderFactory.create_provider({
                'type': 's3',
                'aws_region': region,
                'bucket_name': s3_config.get('input_bucket', ''),
                'base_prefix': s3_config.get('input_prefix', 'input/')
            })
            self._providers['output'] = StorageProviderFactory.create_provider({
                'type': 's3',
                'aws_region': region,
                'bucket_name': s3_config.get('output_bucket', ''),
                'base_prefix': s3_config.get('output_prefix', 'output/')
            })
            self._providers['archive'] = StorageProviderFactory.create_provider({
                'type': 's3',
                'aws_region': region,
                'bucket_name': s3_config.get('archive_bucket', ''),
                'base_prefix': s3_config.get('archive_prefix', 'archive/')
            })
            self._providers['cache'] = StorageProviderFactory.create_provider({
                'type': 's3',
                'aws_region': region,
                'bucket_name': s3_config.get('cache_bucket', ''),
                'base_prefix': s3_config.get('cache_prefix', 'cache/')
            })
        else:
            raise ValueError(f"Unsupported storage mode: {mode}")
    
    def read_file(self, path: str, storage_type: str = 'input') -> bytes:
        """Read a file from the specified storage."""
        provider = self._get_provider(storage_type)
        return provider.read_file(path)
    
    def write_file(self, path: str, content: Union[str, bytes, BinaryIO], storage_type: str = 'output') -> bool:
        """Write content to a file in the specified storage."""
        provider = self._get_provider(storage_type)
        return provider.write_file(path, content)
    
    def list_files(self, directory: str, pattern: Optional[str] = None, storage_type: str = 'input') -> List[str]:
        """List files in a directory in the specified storage."""
        provider = self._get_provider(storage_type)
        return provider.list_files(directory, pattern)
    
    def delete_file(self, path: str, storage_type: str = 'input') -> bool:
        """Delete a file from the specified storage."""
        provider = self._get_provider(storage_type)
        return provider.delete_file(path)
    
    def move_file(self, source: str, destination: str, source_type: str = 'input', dest_type: str = 'archive') -> bool:
        """Move a file between storages."""
        source_provider = self._get_provider(source_type)
        dest_provider = self._get_provider(dest_type)
        
        # Read from source
        content = source_provider.read_file(source)
        
        # Write to destination
        if dest_provider.write_file(destination, content):
            # Delete from source
            return source_provider.delete_file(source)
        return False
    
    def file_exists(self, path: str, storage_type: str = 'input') -> bool:
        """Check if a file exists in the specified storage."""
        provider = self._get_provider(storage_type)
        return provider.file_exists(path)
    
    def create_directory(self, path: str, storage_type: str = 'input') -> bool:
        """Create a directory in the specified storage."""
        provider = self._get_provider(storage_type)
        return provider.create_directory(path)
    
    def get_file_size(self, path: str, storage_type: str = 'input') -> int:
        """Get the size of a file in the specified storage."""
        provider = self._get_provider(storage_type)
        return provider.get_file_size(path)
    
    def get_file_modified_time(self, path: str, storage_type: str = 'input') -> float:
        """Get the last modified time of a file in the specified storage."""
        provider = self._get_provider(storage_type)
        return provider.get_file_modified_time(path)
    
    def _get_provider(self, storage_type: str) -> StorageProvider:
        """Get the storage provider for the specified type."""
        if storage_type not in self._providers:
            raise ValueError(f"Invalid storage type: {storage_type}")
        return self._providers[storage_type]
    
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