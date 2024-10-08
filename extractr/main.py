import sys
from argument_parser import parse_arguments
from logger_setup import setup_logger
from config_loader import load_config
from api_client import ApiClient
from checkpoint_manager import CheckpointManager
from csv_processor import CsvProcessor
from signal_handler import register_signal_handlers
from constants import (
    INPUT_FOLDER,
    OUTPUT_FOLDER,
    ARCHIVE_FOLDER,
    CACHE_FOLDER,
    CHECKPOINT_FILE
)
from exceptions import RateLimitExceeded

def main():
    # Parse command-line arguments
    args = parse_arguments()

    # Setup logging
    logger = setup_logger(args.diagnostic)

    # Load configuration
    try:
        config = load_config()
        logger.info("Configuration loaded successfully.")
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        sys.exit(1)

    # Initialize components
    api_client = ApiClient(cache_folder=CACHE_FOLDER, wait_time=args.wait_time, logger=logger)
    checkpoint_manager = CheckpointManager(checkpoint_file=CHECKPOINT_FILE, logger=logger)
    csv_processor = CsvProcessor(
        api_client=api_client,
        config=config,
        logger=logger,
        checkpoint_manager=checkpoint_manager,
        input_folder=INPUT_FOLDER,
        output_folder=OUTPUT_FOLDER,
        archive_folder=ARCHIVE_FOLDER
    )

    # Register signal handlers to ensure checkpoint saving on interruption
    register_signal_handlers(lambda: checkpoint_manager.save_checkpoint({
        'current_csv_file': csv_processor.current_csv_file,
        'last_processed_line': csv_processor.last_processed_line
    }))

    # Load the last checkpoint if it exists
    checkpoint_data = checkpoint_manager.load_checkpoint()

    # Start processing the CSV files from the checkpoint if available
    try:
        csv_processor.process_files()  # Start processing without start_file and start_line arguments
    except RateLimitExceeded:
        logger.info("Process terminated due to rate limiting.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
