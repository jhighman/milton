import pytest
import logging
import sys
from pathlib import Path
from selenium import webdriver

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from agents.finra_arbitration_agent import search_individual, create_driver

# Set up logging for tests
logger = logging.getLogger("test_finra_arbitration")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

def test_no_results():
    """Test case for a name with no arbitration awards"""
    with create_driver(headless=True) as driver:
        results = search_individual("Izq", "Qzv", driver, logger)
    
    print("\nTesting No Results Case:")
    print(f"Expected: No Results Found")
    print(f"Received: {results}")
    
    assert isinstance(results, dict), f"Expected dict, got {type(results)}"
    assert results["result"] == "No Results Found"

def test_multiple_awards():
    """Test case for a name with multiple arbitration awards"""
    with create_driver(headless=True) as driver:
        results = search_individual("Izq", "Que", driver, logger)
    
    print("\nTesting Multiple Awards Case:")
    print(f"Expected: Multiple arbitration awards")
    print(f"Received: {results}")
    
    assert isinstance(results, dict), f"Expected dict, got {type(results)}"
    assert results.get("result") != "No Results Found"
    assert isinstance(results["result"], list)
    assert len(results["result"]) > 1
    
    print(f"\nFound {len(results['result'])} awards") 