"""
Storage provider factory module.

This module provides a factory class to create storage provider instances
based on configuration settings.
"""

import logging
import json
from typing import Dict, Any, Optional
from .base_provider import BaseStorageProvider
from .local_provider import LocalStorageProvider
from .s3_provider import S3StorageProvider

logger = logging.getLogger(__name__)

class StorageProviderFactory:
    """Factory class for creating storage provider instances."""
    
    @staticmethod
    def create_provider(config: Dict[str, Any]) -> BaseStorageProvider:
        """
        Create a storage provider based on configuration.
        
        Args:
            config: Dictionary containing provider configuration
            
        Returns:
            Configured storage provider instance
        """
        logger.debug(f"Creating storage provider with config: {json.dumps(config, indent=2)}")
        
        # Get provider type from mode or type field for backward compatibility
        provider_type = config.get('mode', config.get('type', '')).lower()
        logger.debug(f"Provider type from config: {provider_type}")
        
        if not provider_type:
            raise ValueError("No provider type specified in configuration")
            
        if provider_type == 'local':
            logger.debug("Creating local storage provider")
            base_path = config.get('base_path')
            if not base_path:
                if 'local' in config:
                    base_path = config['local'].get('base_path')
            
            if not base_path:
                raise ValueError("Local storage base path not specified")
            
            # Get folder names from config, with fallbacks to local config section
            input_folder = config.get('input_folder')
            if not input_folder and 'local' in config:
                input_folder = config['local'].get('input_folder', 'input')
            
            output_folder = config.get('output_folder')
            if not output_folder and 'local' in config:
                output_folder = config['local'].get('output_folder')
            
            archive_folder = config.get('archive_folder')
            if not archive_folder and 'local' in config:
                archive_folder = config['local'].get('archive_folder')
            
            cache_folder = config.get('cache_folder')
            if not cache_folder and 'local' in config:
                cache_folder = config['local'].get('cache_folder')
            
            return LocalStorageProvider(
                base_path=base_path,
                input_folder=input_folder,
                output_folder=output_folder,
                archive_folder=archive_folder,
                cache_folder=cache_folder
            )
            
        elif provider_type == 's3':
            logger.debug("Creating S3 storage provider")
            s3_config = config.get('s3', {})
            
            # Support both old and new config formats
            aws_region = config.get('aws_region', s3_config.get('aws_region'))
            if not aws_region:
                raise ValueError("Missing required S3 parameter: aws_region")
            
            return S3StorageProvider(
                aws_region=aws_region,
                input_bucket=s3_config.get('input_bucket'),
                input_prefix=s3_config.get('input_prefix'),
                output_bucket=s3_config.get('output_bucket'),
                output_prefix=s3_config.get('output_prefix'),
                archive_bucket=s3_config.get('archive_bucket'),
                archive_prefix=s3_config.get('archive_prefix'),
                cache_bucket=s3_config.get('cache_bucket'),
                cache_prefix=s3_config.get('cache_prefix')
            )
            
        else:
            raise ValueError(f"Unsupported storage provider type: {provider_type}") 