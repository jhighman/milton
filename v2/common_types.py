"""
common_types.py

This module contains common types, enums, and constants used across the codebase.
"""

from enum import Enum

class MatchThreshold(Enum):
    """Threshold levels for name matching.  Must be float between 0 and 100."""
    STRICT = 90.0
    MODERATE = 85.0
    LENIENT = 80.0 