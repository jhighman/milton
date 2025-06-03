"""
Storage provider factory module.

This module provides a factory class to create storage provider instances
based on configuration settings.
"""

import logging
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
from storage_providers.base_provider import BaseStorageProvider
from storage_providers.local_provider import LocalStorageProvider
from storage_providers.s3_provider import S3StorageProvider

logger = logging.getLogger(__name__)

class StorageProviderFactory:
    """Factory class for creating storage provider instances."""
    
    @staticmethod
    def create_provider(config: Dict[str, Any]) -> BaseStorageProvider:
        """
        Create and return appropriate storage provider based on configuration.

        Args:
            config (Dict[str, Any]): Storage configuration dictionary containing:
                - mode: Provider type ('local' or 's3')
                - local: Local storage settings with input_folder, output_folder, etc.
                - s3: S3 storage settings with bucket and prefix information

        Returns:
            BaseStorageProvider: Configured storage provider instance

        Raises:
            ValueError: If provider type is invalid or required config missing
        """
        if not isinstance(config, dict):
            raise ValueError(f"Config must be a dictionary, got {type(config)}")
            
        provider_type = config.get('mode', 'local').lower()
        logger.info(f"Creating {provider_type} storage provider")
        logger.debug(f"Full config: {json.dumps(config, indent=2)}")

        if provider_type == 'local':
            return StorageProviderFactory._create_local_provider(config)
        elif provider_type == 's3':
            return StorageProviderFactory._create_s3_provider(config)
        else:
            raise ValueError(f"Invalid storage provider type: {provider_type}")
    
    @staticmethod
    def _create_local_provider(config: Dict[str, Any]) -> LocalStorageProvider:
        """Create and configure a local storage provider."""
        provider = LocalStorageProvider()
        
        # Ensure local config exists
        local_config = config.get('local', {})
        if not local_config:
            logger.warning("Local configuration missing, using defaults")
            local_config = {
                'input_folder': 'drop',
                'output_folder': 'output',
                'archive_folder': 'archive',
                'cache_folder': 'cache'
            }
        
        # Get current working directory as root
        root_dir = Path(os.getcwd())
        
        # Create provider configuration
        provider_config = {
            'base_path': str(root_dir),
            'input_path': str(root_dir / local_config.get('input_folder', 'drop')),
            'output_path': str(root_dir / local_config.get('output_folder', 'output')),
            'archive_path': str(root_dir / local_config.get('archive_folder', 'archive')),
            'cache_path': str(root_dir / local_config.get('cache_folder', 'cache'))
        }
        
        # Initialize provider with configuration
        provider.initialize(provider_config)
        
        logger.info(f"Local provider initialized with paths: {json.dumps(provider_config, indent=2)}")
        return provider
    
    @staticmethod
    def _create_s3_provider(config: Dict[str, Any]) -> S3StorageProvider:
        """Create and configure an S3 storage provider."""
        s3_config = config.get('s3')
        if not s3_config:
            raise ValueError("S3 configuration missing")
            
        required_fields = [
            'aws_region',
            'input_bucket',
            'output_bucket',
            'archive_bucket',
            'cache_bucket'
        ]
        
        missing_fields = [field for field in required_fields if not s3_config.get(field)]
        if missing_fields:
            raise ValueError(f"Missing required S3 configuration fields: {', '.join(missing_fields)}")
        
        provider = S3StorageProvider()
        provider.initialize(s3_config)
        
        logger.info(f"S3 provider initialized with region: {s3_config['aws_region']}")
        logger.debug(f"S3 buckets configured: input={s3_config['input_bucket']}, output={s3_config['output_bucket']}")
        
        return provider 