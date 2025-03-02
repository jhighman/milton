import os
import shutil
import json
import logging
from datetime import datetime
from typing import Dict, Optional, Any, List
from main_config import INPUT_FOLDER, OUTPUT_FOLDER, ARCHIVE_FOLDER, CHECKPOINT_FILE

logger = logging.getLogger('main_file_utils')

def setup_folders():
    for folder in [INPUT_FOLDER, OUTPUT_FOLDER, ARCHIVE_FOLDER]:
        try:
            os.makedirs(folder, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create folder {folder}: {str(e)}")
            raise

def load_checkpoint() -> Optional[Dict[str, Any]]:
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.error(f"Error loading checkpoint: {str(e)}")
        return None

def save_checkpoint(csv_file: str, line_number: int):
    if not csv_file or line_number is None:
        logger.error(f"Cannot save checkpoint: csv_file={csv_file}, line_number={line_number}")
        return
    try:
        checkpoint_path = str(CHECKPOINT_FILE)
        with open(checkpoint_path, 'w') as f:
            json.dump({"csv_file": csv_file, "line": line_number}, f)
        logger.debug(f"Checkpoint saved: {csv_file}, line {line_number}")
    except Exception as e:
        logger.error(f"Error saving checkpoint: {str(e)}")

def get_csv_files() -> List[str]:
    try:
        files = sorted([f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith('.csv')])
        logger.debug(f"Found CSV files: {files}")
        return files
    except Exception as e:
        logger.error(f"Error listing CSV files in {INPUT_FOLDER}: {str(e)}")
        return []

def archive_file(csv_file_path: str):
    date_str = datetime.now().strftime("%m-%d-%Y")
    archive_subfolder = os.path.join(ARCHIVE_FOLDER, date_str)
    try:
        os.makedirs(archive_subfolder, exist_ok=True)
        dest_path = os.path.join(archive_subfolder, os.path.basename(csv_file_path))
        shutil.move(csv_file_path, dest_path)
        logger.info(f"Archived {csv_file_path} to {dest_path}")
    except Exception as e:
        logger.error(f"Error archiving {csv_file_path}: {str(e)}")