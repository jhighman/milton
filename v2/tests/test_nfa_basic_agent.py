import pytest
import logging
import sys
from pathlib import Path
from selenium import webdriver
from typing import Dict, Any

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from agents.nfa_basic_agent import search_individual, create_driver

# Set up test logger
logger = logging.getLogger("test_nfa_basic")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

@pytest.fixture
def driver():
    """Create a WebDriver instance for tests"""
    with create_driver(headless=True, logger=logger) as driver:
        yield driver

def test_no_results(driver):
    """Test case for a name with no NFA profile"""
    results = search_individual("Izq", "Qzv", driver=driver, logger=logger)
    
    print("\nTesting No Results Case:")
    print(f"Received: {results}")
    
    assert isinstance(results, dict)
    assert results["result"] == "No Results Found"

def test_found_results(driver):
    """Test case for a name with NFA profile(s)"""
    results = search_individual("Sam", "Smith", driver=driver, logger=logger)
    
    print("\nTesting Found Results Case:")
    print(f"Received: {results}")
    
    assert isinstance(results, dict)
    assert isinstance(results["result"], list)
    assert len(results["result"]) > 0

    # Verify result structure
    for profile in results["result"]:
        assert "Name" in profile
        assert "NFA ID" in profile
        assert "Firm" in profile
        assert "Current NFA Membership Status" in profile
        assert "Current Registration Types" in profile
        assert "Regulatory Actions" in profile
        assert "Details Available" in profile