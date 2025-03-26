"""Logging configuration."""
import logging

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(levelname)s    %(name)s:%(filename)s:%(lineno)d %(message)s')
console_handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(console_handler) 