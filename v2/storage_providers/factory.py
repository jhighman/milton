"""
Storage provider factory module.

This module provides a factory class to create storage provider instances
based on configuration settings.
"""

import logging
import json
from typing import Dict, Any, Optional
from .base import StorageProvider
from .local_provider import LocalStorageProvider
from .s3_provider import S3StorageProvider

logger = logging.getLogger(__name__)

class StorageProviderFactory:
    """Factory class for creating storage provider instances."""
    
    @staticmethod
    def create_provider(config: Dict[str, Any]) -> StorageProvider:
        """
        Create a storage provider based on configuration.
        
        Args:
            config: Dictionary containing provider configuration
            
        Returns:
            Configured storage provider instance
        """
        logger.debug(f"Creating storage provider with config: {json.dumps(config, indent=2)}")
        
        # Get provider type from mode
        provider_type = config.get('mode', '').lower()
        logger.debug(f"Provider type from mode: {provider_type}")
        
        if not provider_type:
            raise ValueError("No provider type specified in configuration")
            
        if provider_type == 'local':
            logger.debug("Creating local storage provider")
            if 'local' not in config:
                raise ValueError("Local storage configuration missing")
            
            local_config = config['local']
            if 'base_path' not in local_config:
                raise ValueError("Local storage base path not specified")
            
            return LocalStorageProvider(local_config['base_path'])
            
        elif provider_type == 's3':
            logger.debug("Creating S3 storage provider")
            if 's3' not in config:
                raise ValueError("S3 storage configuration missing")
            
            s3_config = config['s3']
            required_params = ['aws_region', 'bucket_name', 'base_prefix']
            missing_params = [p for p in required_params if p not in s3_config]
            if missing_params:
                raise ValueError(f"Missing required S3 parameters: {', '.join(missing_params)}")
            
            return S3StorageProvider(
                region=s3_config['aws_region'],
                bucket_name=s3_config['bucket_name'],
                base_prefix=s3_config['base_prefix']
            )
            
        else:
            raise ValueError(f"Unsupported storage provider type: {provider_type}") 