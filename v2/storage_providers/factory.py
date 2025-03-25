"""
Storage provider factory module.

This module provides a factory class to create storage provider instances
based on configuration settings.
"""

import logging
from typing import Dict, Any, Optional
from .base import StorageProvider
from .local_provider import LocalStorageProvider
from .s3_provider import S3StorageProvider

logger = logging.getLogger(__name__)

class StorageProviderFactory:
    """Factory class for creating storage provider instances."""
    
    @staticmethod
    def create_provider(config: Dict[str, Any]) -> Optional[StorageProvider]:
        """
        Create a storage provider instance based on configuration.
        
        Args:
            config: Dictionary containing storage configuration.
                   Required keys:
                   - type: 'local' or 's3'
                   For local storage:
                   - base_path: Base directory path
                   For S3 storage:
                   - aws_region: AWS region
                   - bucket_name: S3 bucket name
                   - base_prefix: Base prefix for all operations (optional)
        
        Returns:
            An instance of StorageProvider or None if configuration is invalid.
        """
        provider_type = config.get('type', '').lower()
        
        if provider_type == 'local':
            base_path = config.get('base_path')
            if not base_path:
                logger.error("Missing required 'base_path' for local storage provider")
                return None
            return LocalStorageProvider(base_path=base_path)
            
        elif provider_type == 's3':
            aws_region = config.get('aws_region')
            bucket_name = config.get('bucket_name')
            if not aws_region or not bucket_name:
                logger.error("Missing required 'aws_region' or 'bucket_name' for S3 storage provider")
                return None
            return S3StorageProvider(
                aws_region=aws_region,
                bucket_name=bucket_name,
                base_prefix=config.get('base_prefix', '')
            )
            
        else:
            logger.error(f"Unsupported storage provider type: {provider_type}")
            return None 