import pytest
import json
import os
import logging
from typing import Dict, Any
from unittest.mock import Mock, patch
from agents.finra_disciplinary_agent import (
    search_individual,
    validate_json_data,
    process_finra_search,
    get_driver
)
from selenium.common.exceptions import TimeoutException

# Set up test logger
logger = logging.getLogger("test_finra_disciplinary")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# Test data
VALID_JSON = {
    "claim": {
        "first_name": "John",
        "last_name": "Doe"
    }
}

class TestJsonValidation:
    def test_valid_json(self):
        """Test validation of properly formatted JSON"""
        is_valid, error = validate_json_data(VALID_JSON, "test.json", logger)
        assert is_valid
        assert error == ""

    def test_missing_claim(self):
        """Test validation fails when claim object is missing"""
        invalid_json = {"first_name": "John", "last_name": "Doe"}
        is_valid, error = validate_json_data(invalid_json, "test.json", logger)
        assert not is_valid
        assert "Missing 'claim' object" in error

class TestSearch:
    def test_search_validation(self):
        """Test input validation for search"""
        with pytest.raises(ValueError):
            search_individual("", "Doe", logger=logger)
        with pytest.raises(ValueError):
            search_individual("John", "", logger=logger)
        with pytest.raises(ValueError):
            search_individual(None, "Doe", logger=logger)
        with pytest.raises(ValueError):
            search_individual("John", None, logger=logger)

    @patch('agents.finra_disciplinary_agent.create_driver')
    @patch('agents.finra_disciplinary_agent.process_finra_search')
    def test_search_individual_results(self, mock_search, mock_driver):
        """Test search results are properly returned"""
        mock_driver.return_value.__enter__.return_value = Mock()
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
        
        result = search_individual("John", "Doe", logger=logger)
        assert "result" in result
        assert isinstance(result["result"], list)
        assert len(result["result"]) > 0

@pytest.mark.integration
class TestIntegration:
    def test_real_search(self):
        """Test actual FINRA website search"""
        result = search_individual("John", "Doe", logger=logger)
        assert "result" in result
        assert isinstance(result["result"], list)
        assert len(result["result"]) > 0

    def test_real_search_no_results(self):
        """Test actual FINRA website search with no results"""
        result = search_individual("zballs", "maginszi", logger=logger)
        assert "result" in result
        assert result["result"] == "No Results Found"

    