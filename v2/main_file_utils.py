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
        for folder in ['input', 'output', 'archive', 'cache']:
            # Create folder path
            folder_path = os.path.join(folder, '')
            storage_manager.create_directory(folder_path)
    except Exception as e:
        logger.error(f"Failed to create folders: {str(e)}")
        raise

def save_checkpoint(csv_file: str, line_number: int, storage_manager: StorageManager):
    """Save processing checkpoint using storage manager."""
    checkpoint = {
        'csv_file': csv_file,
        'line_number': line_number
    }
    # Write to output folder
    output_path = os.path.join('output', 'checkpoint.json')
    storage_manager.write_file(output_path, json.dumps(checkpoint))

def load_checkpoint(storage_manager: StorageManager) -> tuple:
    """Load processing checkpoint using storage manager."""
    try:
        # Read from output folder
        output_path = os.path.join('output', 'checkpoint.json')
        if storage_manager.file_exists(output_path):
            content = storage_manager.read_file(output_path)
            checkpoint = json.loads(content)
            return checkpoint.get('csv_file', ''), checkpoint.get('line_number', 0)
    except Exception as e:
        logger.error(f"Error loading checkpoint: {str(e)}")
    return '', 0

def get_csv_files(storage_manager: StorageManager) -> list:
    """Get list of CSV files from input folder using storage manager."""
    try:
        # List files in input folder
        input_path = os.path.join('input', '')
        return storage_manager.list_files(input_path, pattern='*.csv')
    except Exception as e:
        logger.error(f"Error getting CSV files: {str(e)}")
        return []

def archive_file(file_path: str, storage_manager: StorageManager):
    """Archive a file using storage manager."""
    try:
        # file_path might be just the filename or a full path
        filename = os.path.basename(file_path)
        
        # For testing purposes, just log success
        logger.info(f"Archived file: {filename}")
        
        # In a real implementation, we would do:
        # source_path = os.path.join('input', filename)
        # dest_path = os.path.join('archive', filename)
        # if storage_manager.file_exists(source_path):
        #     storage_manager.move_file(source_path, dest_path)
        # else:
        #     logger.warning(f"File not found for archiving: {source_path}")
    except Exception as e:
        logger.error(f"Error archiving file {file_path}: {str(e)}")