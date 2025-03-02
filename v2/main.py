import argparse
import csv
import json
import os
import signal
import sys
import logging
from typing import Dict, Set
from main_config import DEFAULT_WAIT_TIME, OUTPUT_FOLDER, load_config, save_config, INPUT_FOLDER, CHECKPOINT_FILE
from main_file_utils import setup_folders, load_checkpoint, save_checkpoint, get_csv_files, archive_file
from main_csv_processing import process_csv, write_skipped_records, generate_reference_id, current_csv, current_line, skipped_records
from main_menu_helper import display_menu, handle_menu_choice
from services import FinancialServicesFacade
from logger_config import setup_logging, reconfigure_logging, flush_logs

logger = logging.getLogger('main')

def signal_handler(sig, frame):
    if current_csv and current_line > 0:
        logger.info(f"Signal received ({signal.Signals(sig).name}), saving checkpoint: {current_csv}, line {current_line}")
        save_checkpoint(current_csv, current_line)
    logger.info("Exiting due to signal")
    sys.exit(0)

def run_batch_processing(facade: FinancialServicesFacade, config: Dict[str, bool], wait_time: float, loggers: dict):
    global skipped_records
    skipped_records.clear()
    
    print("\nRunning batch processing...")
    checkpoint = load_checkpoint()
    csv_files = get_csv_files()
    if not csv_files:
        logger.warning(f"No CSV files found in {INPUT_FOLDER}")
        print(f"No CSV files found in {INPUT_FOLDER}")
        return

    start_file = checkpoint["csv_file"] if checkpoint else None
    start_line = checkpoint["line"] if checkpoint else 0

    processed_files = 0
    processed_records = 0
    skipped_count = 0

    for csv_file in csv_files:
        csv_path = os.path.join(INPUT_FOLDER, csv_file)
        if start_file and csv_file < start_file:
            logger.debug(f"Skipping {csv_file} - before start_file {start_file}")
            continue
        logger.info(f"Processing {csv_path} from line {start_line}")
        process_csv(csv_path, start_line, facade, config, wait_time)  # No save_checkpoint here
        try:
            with open(csv_path, 'r') as f:
                csv_reader = csv.reader(f)
                next(csv_reader)  # Skip header
                for row in csv_reader:
                    if any(field.strip() for field in row):
                        processed_records += 1
                        ref_id = row[0] if row else generate_reference_id()
                        report_path = os.path.join(OUTPUT_FOLDER, f"{ref_id}.json")
                        if os.path.exists(report_path):
                            with open(report_path, 'r') as rf:
                                report = json.load(rf)
                                if report.get("processing_status") == "skipped":
                                    skipped_count += 1
        except Exception as e:
            logger.error(f"Error counting records in {csv_path}: {str(e)}", exc_info=True)
        archive_file(csv_path)
        processed_files += 1
        start_line = 0

    if skipped_records:
        write_skipped_records()
        skipped_count = sum(len(records) for records in skipped_records.values())
    
    logger.info(f"Processed {processed_files} files, {processed_records} records, {skipped_count} skipped")
    if os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
            logger.debug(f"Removed checkpoint file: {CHECKPOINT_FILE}")
        except Exception as e:
            logger.error(f"Error removing checkpoint file {CHECKPOINT_FILE}: {str(e)}")

def main():
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

    setup_folders()

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

    while True:
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

if __name__ == "__main__":
    main()