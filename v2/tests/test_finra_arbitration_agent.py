import pytest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from finra_arbitration_agent import search_individual, create_driver

def test_no_results():
    """Test case for a name with no arbitration awards"""
    with create_driver(headless=True) as driver:
        results = search_individual(driver, "Izq", "Qzv")
    
    print("\nTesting No Results Case:")
    print(f"Expected: No Results Found")
    print(f"Received: {results}")
    
    # Check results structure
    assert isinstance(results, dict), f"Expected dict, got {type(results)}"
    assert results["result"] == "No Results Found", f"Expected 'No Results Found', got {results.get('result')}"

def test_multiple_awards():
    """Test case for a name with multiple arbitration awards"""
    with create_driver(headless=True) as driver:
        results = search_individual(driver, "Izq", "Que")
    
    print("\nTesting Multiple Awards Case:")
    print(f"Expected: Multiple arbitration awards")
    print(f"Received: {results}")
    
    # Check results structure
    assert isinstance(results, dict), f"Expected dict, got {type(results)}"
    assert results.get("result") != "No Results Found", "Expected to find some results"
    assert isinstance(results["result"], list), f"Expected list, got {type(results.get('result'))}"
    assert len(results["result"]) > 1, f"Expected multiple results, got {len(results['result'])}"
    
    print(f"\nFound {len(results['result'])} awards") 