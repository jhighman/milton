"""
common_types.py

This module contains common types, enums, and constants used across the codebase.
"""

from enum import Enum
from typing import Dict, Any, Optional, Set, List

class MatchThreshold(Enum):
    """Threshold levels for name matching.  Must be float between 0 and 100."""
    STRICT = 90.0
    MODERATE = 85.0
    LENIENT = 80.0

class DataSource(Enum):
    """
    Standardized data sources used throughout the application.
    This consolidates all source values to ensure consistency.
    """
    # Primary sources
    FINRA_BROKERCHECK = "FINRA_BrokerCheck"
    IAPD = "IAPD"
    SEC_IAPD = "SEC_IAPD"  # Alternative name for IAPD used in some parts of the code
    
    # Disciplinary sources
    FINRA_DISCIPLINARY = "FINRA_Disciplinary"
    SEC_DISCIPLINARY = "SEC_Disciplinary"
    
    # Arbitration sources
    FINRA_ARBITRATION = "FINRA_Arbitration"
    SEC_ARBITRATION = "SEC_Arbitration"
    
    # Regulatory sources
    NFA_REGULATORY = "NFA_Regulatory"
    
    # Other sources
    ENTITY_SEARCH = "Entity_Search"
    DEFAULT = "Default"
    UNKNOWN = "Unknown"
    
    @classmethod
    def get_display_name(cls, source_value: str) -> str:
        """
        Get a standardized display name for a source value.
        This helps handle legacy source values that may not match the enum exactly.
        
        Args:
            source_value: The source value to standardize
            
        Returns:
            A standardized source value from the enum
        """
        # Handle None case
        if source_value is None:
            return cls.UNKNOWN.value
            
        # Try direct match first
        try:
            return cls(source_value).value
        except ValueError:
            pass
            
        # Handle legacy/alternative names
        source_map = {
            "SEC_IAPD": cls.IAPD.value,
            "IAPD": cls.IAPD.value,
            "FINRA_BrokerCheck": cls.FINRA_BROKERCHECK.value,
            "Entity_Search": cls.ENTITY_SEARCH.value,
            "Default": cls.DEFAULT.value,
        }
        
        return source_map.get(source_value, cls.UNKNOWN.value)