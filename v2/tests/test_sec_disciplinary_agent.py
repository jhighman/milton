import pytest
import logging
import sys
from pathlib import Path
from selenium import webdriver

# Add parent directory to path (assuming this test is in a subdirectory like 'tests/')
sys.path.append(str(Path(__file__).parent.parent))

# Import from your SEC script (adjust the module name if different)
from agents.sec_disciplinary_agent import create_driver, search_individual  # Replace 'sec_search_tool' with your script's filename

# Set up logging for tests
logger = logging.getLogger("test_sec_search")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


def test_create_driver_headless():
    """Test that create_driver initializes a headless Chrome WebDriver."""
    driver = create_driver(headless=True, logger=logger)
    try:
        assert isinstance(driver, webdriver.Chrome), "Driver should be a Chrome WebDriver instance"
        
        # Check if headless mode is enabled (via capabilities or command line options)
        capabilities = driver.capabilities
        chrome_options = capabilities.get("goog:chromeOptions", {})
        args = chrome_options.get("args", [])
        
        # Check for either old or new headless syntax
        headless_enabled = any(
            arg in args for arg in ["--headless", "--headless=new"]
        ) or chrome_options.get("headless", False)
        
        assert headless_enabled, "Headless mode not enabled"
        
    finally:
        driver.quit()


def test_search_individual_invalid_last_name():
    """Test that search_individual raises ValueError with an invalid last_name."""
    with pytest.raises(ValueError):
        search_individual("John", "", logger=logger)
    with pytest.raises(ValueError):
        search_individual("John", None, logger=logger)
    with pytest.raises(ValueError):
        search_individual("John", "   ", logger=logger)


def test_search_no_results():
    """Test case for a name with no disciplinary actions."""
    # Using a deliberately obscure name to avoid real results
    with create_driver(headless=True) as driver:
        results = search_individual("Xzq", "Yzv", driver, logger)
    
    print("\nTesting No Results Case:")
    print(f"Expected: No Results Found")
    print(f"Received: {results}")
    
    assert isinstance(results, dict), f"Expected dict, got {type(results)}"
    assert results["result"] == "No Results Found", "Expected no results found"


def test_search_with_results():
    """Test case for a name likely to have disciplinary actions."""
    # Using "Mark Miller" as a common name that might return results (based on your script's local test)
    with create_driver(headless=True) as driver:
        results = search_individual("Mark", "Miller", driver, logger)
    
    print("\nTesting Results Case:")
    print(f"Expected: At least one result")
    print(f"Received: {results}")
    
    assert isinstance(results, dict), f"Expected dict, got {type(results)}"
    assert "result" in results, "Result key should exist"
    assert results["result"] != "No Results Found", "Expected results, got none"
    assert isinstance(results["result"], list), "Results should be a list"
    assert len(results["result"]) > 0, "Expected at least one result"
    
    # Verify structure of first result
    first_result = results["result"][0]
    assert "Name" in first_result, "Result should have a Name field"
    assert "Date Filed" in first_result, "Result should have a Date Filed field"
    print(f"\nFound {len(results['result'])} disciplinary actions")


if __name__ == "__main__":
    pytest.main([__file__])