"""
Main file utilities module.

This module provides utility functions for file operations.
"""

import os
import shutil
import json
import logging
from datetime import datetime
from typing import Dict, Optional, Any, List
from storage_manager import StorageManager

logger = logging.getLogger('main_file_utils')

def setup_folders(storage_manager: StorageManager):
    """Set up required folders using storage manager."""
    try:
        # Use the configured folder names from storage manager
        storage_manager.create_directory('')  # Create base directory if needed
        storage_manager.create_directory('', storage_type='input')
        storage_manager.create_directory('', storage_type='output')
        storage_manager.create_directory('', storage_type='archive')
        storage_manager.create_directory('', storage_type='cache')
    except Exception as e:
        logger.error(f"Failed to create folders: {str(e)}")
        raise

def save_checkpoint(csv_file: str, line_number: int, storage_manager: StorageManager):
    """Save processing checkpoint using storage manager."""
    checkpoint = {
        'csv_file': csv_file,
        'line_number': line_number
    }
    storage_manager.write_file('checkpoint.json', json.dumps(checkpoint), storage_type='output')

def load_checkpoint(storage_manager: StorageManager) -> tuple:
    """Load processing checkpoint using storage manager."""
    try:
        if storage_manager.file_exists('checkpoint.json', storage_type='output'):
            content = storage_manager.read_file('checkpoint.json', storage_type='output')
            if not content:
                logger.warning("Checkpoint file is empty")
                return '', 0
                
            if isinstance(content, str):
                try:
                    checkpoint = json.loads(content)
                except json.JSONDecodeError:
                    logger.error("Invalid JSON in checkpoint file")
                    return '', 0
            else:
                checkpoint = content
                
            return checkpoint.get('csv_file', ''), checkpoint.get('line_number', 0)
    except Exception as e:
        logger.error(f"Error loading checkpoint: {str(e)}")
    return '', 0

def get_csv_files(storage_manager: StorageManager) -> list:
    """Get list of CSV files from input folder using storage manager.
    
    Args:
        storage_manager: The storage manager instance to use for file operations.
        
    Returns:
        list: A list of CSV filenames found in the input directory.
    """
    logger.debug("Attempting to list CSV files from input directory")
    try:
        # List all CSV files in the root of the input directory
        files = storage_manager.list_files('', pattern='*.csv', storage_type='input')
        logger.debug(f"Found {len(files)} CSV files in input directory")
        return files
    except Exception as e:
        logger.error(f"Failed to list CSV files from input directory: {str(e)}", exc_info=True)
        return []

def archive_file(file_path: str, storage_manager: StorageManager):
    """Archive a file using storage manager."""
    try:
        # file_path might be just the filename or a full path
        filename = os.path.basename(file_path)
        
        # Move the file from input to archive
        storage_manager.move_file(filename, filename, source_type='input', dest_type='archive')
        logger.info(f"Archived file: {filename}")
    except Exception as e:
        logger.error(f"Error archiving file {file_path}: {str(e)}")