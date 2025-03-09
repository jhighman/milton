import logging
import logging.handlers
import os
from pathlib import Path
from typing import Dict, Set

def setup_logging(debug: bool = False) -> Dict[str, logging.Logger]:
    """Configure logging for all modules"""
    # Define logger groups
    LOGGER_GROUPS = {
        'services': {
            'services': 'services',
            'normalizer': 'normalizer',
            'marshaller': 'marshaller',
            'business': 'business',
            'name_matcher': 'name_matcher'
        },
        'agents': {
            'finra_disciplinary': 'finra_disciplinary_agent',
            'sec_disciplinary': 'sec_disciplinary_agent',
            'finra_arbitration': 'finra_arbitration_agent',
            'finra_brokercheck': 'finra_brokercheck_agent',
            'nfa_basic': 'nfa_basic_agent',
            'sec_arbitration': 'sec_arbitration_agent',
            'sec_iapd': 'sec_iapd_agent',
            'agent_manager': 'agent_manager'
        },
        'evaluation': {
            'evaluation': 'evaluation_processor',
            'evaluation_builder': 'evaluation_report_builder',
            'evaluation_director': 'evaluation_report_director'
        },
        'core': {
            'main': 'main',
            'business': 'business',
            'services': 'services',
            'finra_disciplinary': 'finra_disciplinary_agent',
            'sec_disciplinary': 'sec_disciplinary_agent',
            'finra_arbitration': 'finra_arbitration_agent',
            'finra_brokercheck': 'finra_brokercheck_agent',
            'nfa_basic': 'nfa_basic_agent',
            'sec_arbitration': 'sec_arbitration_agent',
            'sec_iapd': 'sec_iapd_agent',
            'agent_manager': 'agent_manager'
        }
    }

    # Create logs directory
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # Set level based on --diagnostic flag
    base_level = logging.DEBUG if debug else logging.INFO

    # Clear any existing handlers from the root logger
    root_logger = logging.getLogger()
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)

    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "app.log"),
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )

    # Set levels
    console_handler.setLevel(base_level)
    file_handler.setLevel(base_level)

    # Create formatters and add it to handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to root logger
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Initialize all loggers from groups
    loggers = {}
    for group_name, group_loggers in LOGGER_GROUPS.items():
        for logger_key, logger_name in group_loggers.items():
            logger = logging.getLogger(logger_name)
            logger.setLevel(base_level)
            loggers[logger_key] = logger

    # Add group information to the loggers dict
    loggers['_groups'] = LOGGER_GROUPS

    return loggers

def reconfigure_logging(loggers: Dict[str, logging.Logger], enabled_groups: Set[str], group_levels: Dict[str, str]) -> None:
    """
    Reconfigure logging settings for specified logger groups.
    
    Args:
        loggers (Dict[str, logging.Logger]): Dictionary of logger instances
        enabled_groups (Set[str]): Set of logger group names to enable
        group_levels (Dict[str, str]): Dictionary mapping group names to their desired log levels
    """
    groups = loggers.get('_groups', {})
    
    for group_name, group_loggers in groups.items():
        # If group is enabled, set all its loggers to specified level
        if group_name in enabled_groups:
            level = group_levels.get(group_name, logging.INFO)
            # If level is already numeric, use it directly
            numeric_level = level if isinstance(level, int) else getattr(logging, str(level).upper(), logging.INFO)
            for logger_name in group_loggers.values():
                logger = logging.getLogger(logger_name)
                logger.setLevel(numeric_level)
                logger.disabled = False
        # If group is disabled, disable all its loggers
        else:
            for logger_name in group_loggers.values():
                logger = logging.getLogger(logger_name)
                logger.disabled = True

def flush_logs():
    """Flush all log handlers to ensure logs are written to disk."""
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        handler.flush() 