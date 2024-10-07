# config_loader.py

import json
import os

def load_config(config_file='config.json'):
    """
    Load configuration settings from a JSON file.

    Args:
        config_file (str): Path to the configuration file.

    Returns:
        dict: Configuration settings.
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file '{config_file}' not found.")

    with open(config_file, 'r') as f:
        config = json.load(f)

    return config
