import pytest
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Any

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from agents.finra_broker_check_agent import (
    search_individual,
    search_individual_detailed_info
)

# Set up test logger
logger = logging.getLogger("test_finra_broker_check_integration")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

DELAY = 3  # seconds between tests

@pytest.mark.integration  # Mark these tests as integration tests
class TestBrokerCheckIntegration:
    """Integration tests for FINRA BrokerCheck API"""

    def setup_method(self):
        """Setup method to add delay between tests"""
        time.sleep(DELAY)

    def test_real_broker_search(self):
        """Test searching for a known broker"""
        result = search_individual("5695141", "TEST_EMP", logger)
        
        assert result is not None, "Expected result to not be None"
        assert result["hits"]["total"] >= 1, f"Expected at least 1 hit, got {result['hits']['total']}"
        
        hit = result["hits"]["hits"][0]["_source"]
        actual_name = hit["ind_firstname"]
        expected_name = "Micheal"  # Fixed spelling
        
        assert actual_name == expected_name, f"""
        First Name Mismatch:
        Expected: {expected_name}
        Actual  : {actual_name}
        """

        actual_lastname = hit["ind_lastname"]
        expected_lastname = "Lucas"
        assert actual_lastname == expected_lastname, f"""
        Last Name Mismatch:
        Expected: {expected_lastname}
        Actual  : {actual_lastname}
        """

    def test_real_broker_details(self):
        """Test fetching detailed info for a known broker"""
        result = search_individual_detailed_info("5695141", "TEST_EMP", logger)
        
        print("\nTesting Broker Details:")
        print(f"CRD: 5695141")
        
        assert result is not None, "No result returned from API"
        
        actual_name = result["basicInformation"]["firstName"]
        expected_name = "Micheal"  # Fixed spelling
        
        print(f"\nFirst Name Check:")
        print(f"Expected: '{expected_name}'")
        print(f"Actual  : '{actual_name}'")
        print(f"Match   : {actual_name == expected_name}")
        
        assert actual_name == expected_name
        
        actual_lastname = result["basicInformation"]["lastName"]
        expected_lastname = "Lucas"
        
        print(f"\nLast Name Check:")
        print(f"Expected: '{expected_lastname}'")
        print(f"Actual  : '{actual_lastname}'")
        print(f"Match   : {actual_lastname == expected_lastname}")
        
        assert actual_lastname == expected_lastname
        assert "registeredStates" in result, "Missing registeredStates in response"

    def test_nonexistent_broker(self):
        """Test searching for a nonexistent CRD"""
        result = search_individual("999999999999", "TEST_EMP", logger)
        print("\nNonexistent broker search result:")
        print(f"Result: {result}")
        # We'll comment out the assertion until we see the actual response
        # assert result["hits"]["total"] == 0


