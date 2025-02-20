import pytest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from agents.nfa_basic_agent import search_nfa_profile, create_driver

def test_no_results():
    """Test case for a name with no NFA profile"""
    with create_driver(headless=True) as driver:
        results = search_nfa_profile(driver, "Izq", "Qzv")
    
    print("\nTesting No Results Case:")
    print(f"Received: {results}")
    
    assert isinstance(results, dict), f"Expected dict, got {type(results)}"
    assert results["result"] == "No Results Found", f"Expected 'No Results Found', got {results.get('result')}"

def test_found_results():
    """Test case for a name with NFA profile(s)"""
    with create_driver(headless=True) as driver:
        results = search_nfa_profile(driver, "Sam", "Smith")
    
    print("\nTesting Found Results Case:")
    print(f"Received: {results}")
    
    assert isinstance(results, dict), f"Expected dict, got {type(results)}"
    assert isinstance(results["result"], list), f"Expected list of results, got {type(results.get('result'))}"
    assert len(results["result"]) > 0, f"Expected at least one result, got {len(results['result'])}"