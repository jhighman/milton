import pytest
import os
import time
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Tuple

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from agents.sec_arbitration_agent import process_name, process_claim

# Set up test logger
logger = logging.getLogger("test_sec_arbitration")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# Only keep LONG_WAIT for the override tests
LONG_WAIT = 10

def test_no_results():
    """Test case for a name with no enforcement actions"""
    results, stats = process_name("John", "Doe", headless=True, logger=logger)
    
    print("\nTesting No Results Case:")
    print(f"Received: {results}")
    print(f"Stats: {stats}")
    
    assert isinstance(results, dict)
    assert results["result"] == "No Results Found"
    
    # Verify stats
    assert stats["individuals_searched"] == 1
    assert stats["total_searches"] == 1
    assert stats["no_enforcement_actions"] == 1
    assert stats["enforcement_actions"] == 0
    assert stats["errors"] == 0

def test_single_enforcement_action():
    """Test case for Mark Miller who has one enforcement action"""
    results, stats = process_name("Mark", "Miller", headless=True, logger=logger)
    
    print("\nTesting Single Enforcement Action Case:")
    print(f"Expected: One enforcement action for W. MARK MILLER from July 2, 2014")
    print(f"Received: {results}")
    
    # Check results structure
    assert isinstance(results, dict), f"Expected dict, got {type(results)}"
    assert isinstance(results["result"], list), f"Expected list, got {type(results.get('result'))}"
    assert len(results["result"]) >= 1, f"Expected at least 1 result, got {len(results['result'])}"
    assert results["total_actions"] >= 1, f"Expected total_actions at least 1, got {results.get('total_actions')}"
    
    # Check the enforcement action details for each action
    print("\nChecking Enforcement Action Details:")
    print(f"Expected fields: Enforcement Action, Date Filed, Documents")
    
    # Find the specific action we're looking for (W. MARK MILLER from 2014)
    w_mark_miller_action = None
    for action in results["result"]:
        print(f"Received action: {action}")
        assert "Enforcement Action" in action, f"Missing 'Enforcement Action' in {action.keys()}"
        assert "Date Filed" in action, f"Missing 'Date Filed' in {action.keys()}"
        assert "Documents" in action, f"Missing 'Documents' in {action.keys()}"
        assert isinstance(action["Documents"], list), f"Expected Documents to be list, got {type(action['Documents'])}"
        
        if "W. MARK MILLER" in action["Enforcement Action"] and "July 2, 2014" in action["Date Filed"]:
            w_mark_miller_action = action
    
    # Verify we found the specific action we're looking for
    assert w_mark_miller_action is not None, "Could not find W. MARK MILLER action from July 2, 2014"

def test_multiple_enforcement_actions():
    """Test case for Andrew Miller who has multiple enforcement actions"""
    results, stats = process_name("Andrew", "Miller", headless=True, logger=logger)
    
    print("\nTesting Multiple Enforcement Actions Case:")
    print(f"Expected: Multiple enforcement actions")
    print(f"Received: {results}")
    
    # Check results structure
    assert isinstance(results, dict), f"Expected dict, got {type(results)}"
    assert isinstance(results["result"], list), f"Expected list, got {type(results.get('result'))}"
    
    print(f"\nNumber of actions found: {len(results['result'])}")
    assert len(results["result"]) > 1, f"Expected multiple results, got {len(results['result'])}"
    assert results["total_actions"] > 1, f"Expected multiple actions, got {results.get('total_actions')}"
    
    # Check each enforcement action has required fields
    for i, action in enumerate(results["result"], 1):
        print(f"\nChecking Action {i}:")
        print(f"Action details: {action}")
        assert "Enforcement Action" in action, f"Missing 'Enforcement Action' in action {i}"
        assert "Date Filed" in action, f"Missing 'Date Filed' in action {i}"
        assert "Documents" in action, f"Missing 'Documents' in action {i}"
        assert isinstance(action["Documents"], list), f"Expected Documents to be list in action {i}"

def test_alternate_names():
    """Test searching with alternate names"""
    claim_data = {
        "first_name": "Mark",
        "last_name": "Miller",
        "search_evaluation": {
            "individual": {
                "ind_other_names": ["William Miller", "M. Miller"]
            }
        }
    }
    
    results, stats = process_claim(claim_data, headless=True, logger=logger)
    
    # Should have results for each name variant
    assert isinstance(results, list)
    assert len(results) == 3  # Primary name + 2 alternates
    
    # Check stats reflect multiple searches
    assert stats["individuals_searched"] == 1
    assert stats["total_searches"] == 3
    
    # At least one result should have enforcement actions
    assert stats["enforcement_actions"] > 0

def test_visible_browser():
    """Test that browser can run in visible mode"""
    results, stats = process_name("Mark", "Miller", headless=False, logger=logger)
    
    # Results should be the same as headless mode
    assert isinstance(results, dict)
    assert isinstance(results["result"], list)
    assert len(results["result"]) >= 1

def test_found_results():
    """Test case for a name with enforcement actions"""
    results, stats = process_name("Mark", "Miller", headless=True, logger=logger)
    
    assert isinstance(results, dict)
    assert isinstance(results["result"], list)
    assert len(results["result"]) > 0
    
    # Verify result structure
    for action in results["result"]:
        assert "Enforcement Action" in action
        assert "Date Filed" in action
        assert "Documents" in action
        assert isinstance(action["Documents"], list)

@pytest.mark.slow
def test_wait_time_override():
    """Test that wait time can be overridden"""
    start_time = time.time()
    results, _ = process_name("John", "Doe", headless=True, wait_time=LONG_WAIT, logger=logger)
    elapsed_time = time.time() - start_time
    
    # Should take at least LONG_WAIT seconds
    assert elapsed_time >= LONG_WAIT 