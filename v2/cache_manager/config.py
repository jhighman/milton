"""
==============================================
📌 CONFIGURATION MODULE OVERVIEW
==============================================

🗂 PURPOSE
This module defines constants and configuration settings for the CacheManager package.
It centralizes values like cache folder location and TTL to make them easily adjustable.

🗂 USAGE
Import constants as needed:
    from cache_manager.config import DEFAULT_CACHE_FOLDER, CACHE_TTL_DAYS

🗂 NOTES
- Adjust `DEFAULT_CACHE_FOLDER` for custom environments.
- `CACHE_TTL_DAYS` controls stale cache cleanup (default: 90 days).
==============================================
"""

from pathlib import Path

# Cache Configuration
DEFAULT_CACHE_FOLDER = Path(__file__).parent.parent / "cache"  # Default cache directory relative to package
CACHE_TTL_DAYS = 90  # Cache expiration in days; files older than this are considered stale
DATE_FORMAT = "%Y%m%d"  # Standardized date format for filenames (e.g., 20250308)
MANIFEST_FILE = "manifest.txt"  # File to track last cache update per agent (not yet implemented)

# Logging Configuration
LOG_LEVEL = "WARNING"  # Logging level; set to WARNING to suppress info logs
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"  # Format for log messages