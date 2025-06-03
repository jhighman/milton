"""
name_matcher.py

This module re-exports name matching functions from evaluation_processor.py
to provide a consistent interface for name matching throughout the application.

The implementation in evaluation_processor.py is used because it provides
a more comprehensive nickname handling with the full nicknames.json file.
"""

import logging
from typing import Dict, Any, Optional, Set, Tuple, List
from evaluation_processor import (
    parse_name,
    get_name_variants,
    are_nicknames,
    match_name_part,
    evaluate_name
)

logger = logging.getLogger('name_matcher')

# Re-export the functions from evaluation_processor.py
__all__ = [
    'parse_name',
    'get_name_variants',
    'are_nicknames',
    'match_name_part',
    'evaluate_name'
]