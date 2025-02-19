import pytest
import json
import os
from unittest.mock import Mock, patch
from finra_disciplinary_agent import (
    search_individual,
    search_with_alternates,
    validate_json_data,
    process_finra_search,
    get_driver
)
from selenium.common.exceptions import TimeoutException

# Test data
VALID_JSON = {
    "claim": {
        "first_name": "John",
        "last_name": "Doe"
    },
    "alternate_names": [
        ["Johnny", "Doe"],
        ["Jon", "Doe"]
    ]
}

INVALID_JSON_MISSING_CLAIM = {
    "first_name": "John",
    "last_name": "Doe"
}

INVALID_JSON_MISSING_NAME = {
    "claim": {
        "last_name": "Doe"
    }
}

@pytest.fixture
def mock_driver():
    """Create a mock Selenium driver"""
    with patch('selenium.webdriver.Chrome') as mock:
        yield mock

class TestJsonValidation:
    def test_valid_json(self):
        """Test validation of properly formatted JSON"""
        is_valid, error = validate_json_data(VALID_JSON, "test.json")
        assert is_valid
        assert error == ""

    def test_missing_claim(self):
        """Test validation fails when claim object is missing"""
        is_valid, error = validate_json_data(INVALID_JSON_MISSING_CLAIM, "test.json")
        assert not is_valid
        assert "Missing 'claim' object" in error

    def test_missing_name(self):
        """Test validation fails when name fields are missing"""
        is_valid, error = validate_json_data(INVALID_JSON_MISSING_NAME, "test.json")
        assert not is_valid
        assert "Missing or empty 'first_name'" in error

class TestSearch:
    def test_search_validation(self, mock_driver):
        """Test input validation for search"""
        with pytest.raises(ValueError):
            search_individual(mock_driver, "", "Doe")
        with pytest.raises(ValueError):
            search_individual(mock_driver, "John", "")

    @patch('finra_disciplinary_agent.process_finra_search')
    def test_search_individual_results(self, mock_search, mock_driver):
        """Test search results are properly returned"""
        mock_search.return_value = {
            "result": [
                {
                    "Case ID": "2022076589301",
                    "Case Summary": "Test summary",
                    "Document Type": "Complaints",
                    "Firms/Individuals": "John Doe",
                    "Action Date": "01/30/2025"
                }
            ]
        }
        
        result = search_individual(mock_driver, "John", "Doe")
        assert "result" in result
        assert isinstance(result["result"], list)
        assert len(result["result"]) > 0

    @patch('finra_disciplinary_agent.process_finra_search')
    def test_search_with_alternates_results(self, mock_search, mock_driver):
        """Test searching with alternate names"""
        mock_search.return_value = {"result": "No Results Found"}
        results = search_with_alternates(
            mock_driver,
            "John", 
            "Doe", 
            alternate_names=[["Johnny", "Doe"]]
        )
        assert len(results) == 2  # Primary + 1 alternate

    @patch('finra_disciplinary_agent.process_finra_search')
    def test_search_individual_no_results(self, mock_search, mock_driver):
        """Test search results when no results are found"""
        mock_search.return_value = {"result": "No Results Found"}
        
        result = search_individual(mock_driver, "zballs", "maginszi")
        assert "result" in result
        assert result["result"] == "No Results Found"

def test_process_finra_search_mock(mock_driver):
    """Test FINRA search process with mocked responses"""
    # Mock the necessary Selenium elements
    mock_input = Mock()
    mock_checkbox = Mock()
    mock_submit = Mock()
    mock_table = Mock()
    
    # Set up WebDriverWait mock
    mock_wait = Mock()
    # First wait.until should raise an exception for "No Results Found" check
    mock_wait.until.side_effect = [
        mock_input,  # for edit-individuals
        mock_checkbox,  # for terms checkbox
        mock_submit,  # for submit button
        TimeoutException(),  # for "No Results Found" check
        mock_table  # for table check
    ]
    
    # Mock the driver.get method
    mock_driver.get = Mock()
    
    with patch('finra_disciplinary_agent.WebDriverWait', return_value=mock_wait), \
         patch('selenium.webdriver.support.expected_conditions.presence_of_element_located') as mock_presence:
        # Set up the page source after form submission
        mock_driver.page_source = """
            <table class="table views-table views-view-table cols-5">
                <tr><th>Case ID</th><th>Case Summary</th><th>Document Type</th><th>Firms/Individuals</th><th>Action Date</th></tr>
                <tr><td>12345</td><td>Test Case</td><td>AWC</td><td>John Doe</td><td>2023-01-01</td></tr>
            </table>
        """
        
        # Mock the JavaScript execution
        mock_driver.execute_script = Mock()
        
        result = process_finra_search(mock_driver, "John", "Doe")
        
        # Verify the search steps were called
        assert mock_driver.get.called
        assert mock_wait.until.call_count == 5  # Updated count to include both checks
        assert mock_driver.execute_script.call_count == 2
        
        # Verify the result
        assert "result" in result
        assert isinstance(result["result"], list)
        assert len(result["result"]) == 1
        assert result["result"][0]["Case ID"] == "12345"

def test_process_finra_search_no_results_mock(mock_driver):
    """Test FINRA search process with no results response"""
    # Mock the necessary Selenium elements
    mock_input = Mock()
    mock_checkbox = Mock()
    mock_submit = Mock()
    mock_table = Mock()
    
    # Set up WebDriverWait mock
    mock_wait = Mock()
    mock_wait.until.side_effect = [mock_input, mock_checkbox, mock_submit, mock_table]
    
    with patch('finra_disciplinary_agent.WebDriverWait', return_value=mock_wait):
        # Set up the page source after form submission
        mock_driver.page_source = """
            <table class="table views-table views-view-table cols-5">
                <tr><th>Case ID</th><th>Case Summary</th><th>Document Type</th><th>Firms/Individuals</th><th>Action Date</th></tr>
            </table>
        """
        
        # Mock the JavaScript execution
        mock_driver.execute_script = Mock()
        
        result = process_finra_search(mock_driver, "zballs", "maginszi")
        
        # Verify the search steps were called
        assert mock_driver.get.called
        assert mock_wait.until.call_count == 4
        assert mock_driver.execute_script.call_count == 2
        
        # Verify the result
        assert "result" in result
        assert result["result"] == "No Results Found"

@pytest.mark.integration
class TestIntegration:
    def test_real_search(self):
        """Test actual FINRA website search"""
        with get_driver(headless=True) as driver:
            result = search_individual(driver, "John", "Doe")
            assert "result" in result
            assert isinstance(result["result"], list)
            assert len(result["result"]) > 0

    def test_real_search_no_results(self):
        """Test actual FINRA website search with no results"""
        with get_driver(headless=True) as driver:
            result = search_individual(driver, "zballs", "maginszi")
            assert "result" in result
            assert result["result"] == "No Results Found"

    