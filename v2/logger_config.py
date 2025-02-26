import logging
import os
from pathlib import Path

def setup_logging(diagnostic: bool = False):
    """Configure logging for all modules"""
    # Create logs directory
    os.makedirs('logs', exist_ok=True)

    # Set level based on --diagnostic flag
    base_level = logging.DEBUG if diagnostic else logging.INFO

    # Clear any existing handlers from the root logger
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    # Create handlers
    file_handler = logging.FileHandler('logs/app.log')
    file_handler.setLevel(base_level)
    file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s'))
    
    handlers = [
        file_handler  # Always log to file
    ]

    # Add console handler only in diagnostic mode
    if diagnostic:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(base_level)
        console_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s'))
        handlers.append(console_handler)

    # Configure the root logger - this affects ALL loggers
    logging.basicConfig(
        level=base_level,
        handlers=handlers
    )

    # Get named loggers for each module
    loggers = {
        'main': logging.getLogger('main'),
        'services': logging.getLogger('FinancialServicesFacade'),
        'normalizer': logging.getLogger('normalizer'),
        'marshaller': logging.getLogger('Marshaller'),
        'business': logging.getLogger('business'),
        'finra_disciplinary': logging.getLogger('finra_disciplinary_agent'),
        'sec_disciplinary': logging.getLogger('sec_disciplinary_agent'),
        'evaluation': logging.getLogger('evaluation_processor'),
        'evaluation_builder': logging.getLogger('evaluation_report_builder'),
        'evaluation_director': logging.getLogger('evaluation_report_director')
    }

    # Set their levels
    for logger in loggers.values():
        logger.setLevel(base_level)

    return loggers 