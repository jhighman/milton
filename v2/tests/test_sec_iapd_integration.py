import pytest
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Any

# Add parent directory to path if not already there
test_dir = Path(__file__).parent
project_root = test_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from agents.sec_iapd_agent import (
    search_individual,
    search_individual_detailed_info
)

# Set up test logger
logger = logging.getLogger("test_sec_iapd_integration")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

DELAY = 3  # seconds between tests

@pytest.mark.integration
class TestSECIAPDIntegration:
    """Integration tests for SEC IAPD API"""

    def setup_method(self):
        """Setup method to add delay between tests"""
        time.sleep(DELAY)

    def test_real_adviser_search(self):
        """Test searching for a known investment adviser"""
        result = search_individual("1438859", "TEST_EMP", logger)
        
        assert result is not None, "Expected result to not be None"
        assert result["hits"]["total"] >= 1, f"Expected at least 1 hit, got {result['hits']['total']}"
        
        hit = result["hits"]["hits"][0]["_source"]
        assert hit["ind_firstname"] == "RICHARD"
        assert hit["ind_lastname"] == "GOTTERER"
        assert hit["ind_ia_scope"] == "Active"
        assert hit["ind_bc_scope"] == "NotInScope"

    def test_real_adviser_details(self):
        """Test fetching detailed info for a known investment adviser"""
        result = search_individual_detailed_info("1438859", "TEST_EMP", logger)
        
        assert result is not None, "Expected result to not be None"
        assert result["basicInformation"]["firstName"] == "RICHARD"
        assert result["basicInformation"]["lastName"] == "GOTTERER"
        assert result["basicInformation"]["iaScope"] == "Active"
        assert "registeredStates" in result
        assert result["disclosureFlag"] == "N"

    def test_nonexistent_adviser(self):
        """Test searching for a nonexistent CRD"""
        result = search_individual("999999999999", "TEST_EMP", logger)
        assert result["hits"]["total"] == 0

    def test_broker_not_adviser(self):
        """Test searching for someone who is a broker but not an investment adviser"""
        result = search_individual("2216783", "TEST_EMP", logger)
        
        assert result is not None
        if result["hits"]["total"] > 0:
            hit = result["hits"]["hits"][0]["_source"]
            assert hit["ind_ia_scope"] != "Active"  # Should not be an active IA 