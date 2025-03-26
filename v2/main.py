"""
Main application module.

This module contains the main application logic and entry point.
"""

import argparse
import csv
import json
import os
import signal
import sys
import logging
from typing import Dict, Set, Any
from main_config import DEFAULT_WAIT_TIME, OUTPUT_FOLDER, load_config, save_config, INPUT_FOLDER, CHECKPOINT_FILE, get_storage_config
from main_file_utils import setup_folders, load_checkpoint, save_checkpoint, get_csv_files, archive_file
from main_csv_processing import CSVProcessor
from main_menu_helper import display_menu, handle_menu_choice
from services import FinancialServicesFacade
from logger_config import setup_logging, reconfigure_logging, flush_logs
from storage_manager import StorageManager

logger = logging.getLogger('main')
csv_processor = CSVProcessor()  # Create a global instance
storage_manager = None  # Global storage manager instance

def signal_handler(sig, frame):
    """Handle system signals."""
    global storage_manager
    if csv_processor.current_csv and csv_processor.current_line > 0:
        logger.info(f"Signal received ({signal.Signals(sig).name}), saving checkpoint: {csv_processor.current_csv}, line {csv_processor.current_line}")
        if storage_manager:
            save_checkpoint(csv_processor.current_csv, csv_processor.current_line, storage_manager)
        else:
            logger.warning("Cannot save checkpoint: storage_manager is None")
    logger.info("Exiting due to signal")
    sys.exit(0)

# This function is now imported from main_file_utils.py
# def setup_folders(storage_manager):
#     """Set up required folders using storage manager."""
#     try:
#         for folder in ['input', 'output', 'archive', 'cache']:
#             storage_manager.create_directory('', storage_type=folder)
#     except Exception as e:
#         logger.error(f"Failed to create folders: {str(e)}")
#         raise

# This function is now imported from main_file_utils.py
# def save_checkpoint(csv_file: str, line_number: int, storage_manager: StorageManager):
#     """Save processing checkpoint using storage manager."""
#     checkpoint = {
#         'csv_file': csv_file,
#         'line_number': line_number
#     }
#     storage_manager.write_file('checkpoint.json', json.dumps(checkpoint), storage_type='output')

# These functions are now imported from main_file_utils.py
# def load_checkpoint(storage_manager) -> tuple:
#     """Load processing checkpoint using storage manager."""
#     try:
#         if storage_manager.file_exists('checkpoint.json', storage_type='output'):
#             content = storage_manager.read_file('checkpoint.json', storage_type='output')
#             checkpoint = json.loads(content)
#             return checkpoint.get('csv_file', ''), checkpoint.get('line_number', 0)
#     except Exception as e:
#         logger.error(f"Error loading checkpoint: {str(e)}")
#     return '', 0
#
# def get_csv_files(storage_manager) -> list:
#     """Get list of CSV files from input folder using storage manager."""
#     try:
#         return storage_manager.list_files('', pattern='*.csv', storage_type='input')
#     except Exception as e:
#         logger.error(f"Error getting CSV files: {str(e)}")
#         return []
#
# def archive_file(file_path: str, storage_manager):
#     """Archive a file using storage manager."""
#     try:
#         filename = os.path.basename(file_path)
#         storage_manager.move_file(file_path, filename, source_type='input', dest_type='archive')
#         logger.info(f"Archived file: {filename}")
#     except Exception as e:
#         logger.error(f"Error archiving file {file_path}: {str(e)}")

def run_batch_processing(facade: FinancialServicesFacade, config: Dict[str, Any], wait_time: float, loggers: Dict[str, logging.Logger]):
    """Run batch processing with the given configuration."""
    global storage_manager, csv_processor
    
    # Initialize storage manager if not already done
    if storage_manager is None:
        storage_manager = StorageManager(config)
    
    # Set up folders
    setup_folders(storage_manager)
    
    # Get CSV files
    csv_files = get_csv_files(storage_manager)
    if not csv_files:
        logger.info("No CSV files found in input folder")
        return
    
    # Load checkpoint
    last_csv, last_line = load_checkpoint(storage_manager)
    if last_csv and last_csv in csv_files:
        csv_files = csv_files[csv_files.index(last_csv):]
        logger.info(f"Resuming from checkpoint: {last_csv}, line {last_line}")
    
    # Set storage manager in CSV processor
    csv_processor.set_storage_manager(storage_manager)
    
    # Process each CSV file
    for csv_file in csv_files:
        try:
            csv_processor.process_csv(
                csv_file,
                start_line=last_line if csv_file == last_csv else 0,
                facade=facade,
                config=config,
                wait_time=wait_time
            )
            archive_file(csv_file, storage_manager)
        except Exception as e:
            logger.error(f"Error processing file {csv_file}: {str(e)}")
            continue

def main(test_mode=False):
    """Main application entry point.
    
    Args:
        test_mode: If True, run in test mode (no infinite loop)
    """
    parser = argparse.ArgumentParser(description="Compliance CSV Processor")
    parser.add_argument('--diagnostic', action='store_true', help="Enable verbose debug logging")
    parser.add_argument('--wait-time', type=float, default=DEFAULT_WAIT_TIME, help=f"Seconds to wait between records (default: {DEFAULT_WAIT_TIME})")
    parser.add_argument('--skip-disciplinary', action='store_true', help="Skip disciplinary review for all claims")
    parser.add_argument('--skip-arbitration', action='store_true', help="Skip arbitration review for all claims")
    parser.add_argument('--skip-regulatory', action='store_true', help="Skip regulatory review for all claims")
    parser.add_argument('--headless', action='store_true', help="Run in headless mode with specified settings")
    args = parser.parse_args()

    loggers = setup_logging(args.diagnostic)
    global logger
    logger = loggers['main']

    logger.info("=== Starting application ===")
    logger.debug("Debug logging is enabled" if args.diagnostic else "Debug logging is disabled")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Initialize storage manager
    config = load_config()
    global storage_manager
    storage_manager = StorageManager(config)
    
    # Set up folders
    setup_folders(storage_manager)

    try:
        facade = FinancialServicesFacade()
    except Exception as e:
        logger.error(f"Failed to initialize FinancialServicesFacade: {str(e)}", exc_info=True)
        return

    LOG_LEVELS = {
        "1": ("DEBUG", logging.DEBUG),
        "2": ("INFO", logging.INFO),
        "3": ("WARNING", logging.WARNING),
        "4": ("ERROR", logging.ERROR),
        "5": ("CRITICAL", logging.CRITICAL)
    }

    if args.headless:
        config = {
            "evaluate_name": True,
            "evaluate_license": True,
            "evaluate_exams": True,
            "evaluate_disclosures": True,
            "skip_disciplinary": args.skip_disciplinary,
            "skip_arbitration": args.skip_arbitration,
            "skip_regulatory": args.skip_regulatory,
            "enabled_logging_groups": ["core"],
            "logging_levels": {"core": "INFO"},
            "config_file": "config.json",
            "default_wait_time": DEFAULT_WAIT_TIME
        }
        if not (args.skip_disciplinary or args.skip_arbitration or args.skip_regulatory):
            loaded_config = load_config()
            config.update({
                "skip_disciplinary": loaded_config.get("skip_disciplinary", True),
                "skip_arbitration": loaded_config.get("skip_arbitration", True),
                "skip_regulatory": loaded_config.get("skip_regulatory", True),
                "enabled_logging_groups": loaded_config.get("enabled_logging_groups", ["core"]),
                "logging_levels": loaded_config.get("logging_levels", {"core": "INFO"})
            })
        reconfigure_logging(loggers, set(config["enabled_logging_groups"]), config["logging_levels"])
        run_batch_processing(facade, config, args.wait_time, loggers)
        return

    skip_disciplinary = True
    skip_arbitration = True
    skip_regulatory = True
    enabled_groups = {"core"}
    group_levels = {"core": "INFO"}
    wait_time = DEFAULT_WAIT_TIME

    config = {
        "evaluate_name": True,
        "evaluate_license": True,
        "evaluate_exams": True,
        "evaluate_disclosures": True,
        "skip_disciplinary": skip_disciplinary,
        "skip_arbitration": skip_arbitration,
        "skip_regulatory": skip_regulatory,
        "enabled_logging_groups": list(enabled_groups),
        "logging_levels": dict(group_levels),
        "config_file": "config.json",
        "default_wait_time": DEFAULT_WAIT_TIME
    }

    # In test mode, we'll only run one iteration of the loop
    run_loop = True
    while run_loop:
        choice = display_menu(skip_disciplinary, skip_arbitration, skip_regulatory, wait_time)
        if choice == "1":
            logger.info(f"Running batch with config: {config}, wait_time: {wait_time}")
            reconfigure_logging(loggers, enabled_groups, {k: LOG_LEVELS[v][1] if v in LOG_LEVELS else logging.INFO for k, v in group_levels.items()})
            run_batch_processing(facade, config, wait_time, loggers)
        else:
            skip_disciplinary, skip_arbitration, skip_regulatory, enabled_groups, group_levels, wait_time = handle_menu_choice(
                choice, skip_disciplinary, skip_arbitration, skip_regulatory, enabled_groups, group_levels, wait_time,
                config, loggers, LOG_LEVELS, save_config, flush_logs
            )
            if choice == "11":
                break
        if choice in ["8", "9"]:
            reconfigure_logging(loggers, enabled_groups, {k: LOG_LEVELS[v][1] if v in LOG_LEVELS else logging.INFO for k, v in group_levels.items()})
        
        # Exit after one iteration if in test mode
        if test_mode:
            run_loop = False

if __name__ == "__main__":
    main()