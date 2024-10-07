# logger_setup.py

import logging
import os

def setup_logger(log_file: str = None):
    """
    Sets up the logger with INFO level, formatting, and optional file logging.
    """
    logger = logging.getLogger('evaluation_framework')
    logger.setLevel(logging.DEBUG)  # Set the lowest level to capture all types of log messages

    # Define log format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Set level to INFO for console output
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Set up file handler if log file is specified
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # Capture DEBUG and above in the file
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
