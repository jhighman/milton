# argument_parser.py

import argparse

def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description='Evaluation Framework')
    parser.add_argument('--diagnostic', action='store_true', help='Enable diagnostic mode')
    parser.add_argument('--wait-time', type=int, default=7, help='Wait time between requests in seconds (default: 7)')
    return parser.parse_args()
