import pytest
import logging
import sys
import requests
from pathlib import Path
from typing import Dict, Any
from unittest.mock import patch, Mock

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from agents.finra_broker_check_agent import (
    search_individual,
    search_individual_detailed_info
)
from agents.exceptions import RateLimitExceeded

# Set up test logger
logger = logging.getLogger("test_finra_broker_check")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# Test data
MOCK_BASIC_RESPONSE = {
    "hits": {
        "total": 1,
        "hits": [{
            "_type": "_doc",
            "_source": {
                "ind_source_id": "1234567",
                "ind_firstname": "John",
                "ind_lastname": "Doe",
                "ind_bc_scope": "Active",
                "ind_ia_scope": "Active",
                "ind_bc_disclosure_fl": "N",
                "ind_approved_finra_registration_count": 2,
                "ind_employments_count": 1,
                "ind_industry_days": "3650"
            }
        }]
    }
}

MOCK_DETAILED_RESPONSE = {
    "hits": {
        "hits": [{
            "_source": {
                "content": """
                {
                    "basicInformation": {
                        "individualId": 1234567,
                        "firstName": "John",
                        "lastName": "Doe",
                        "bcScope": "Active",
                        "iaScope": "Active",
                        "daysInIndustry": 3650
                    },
                    "currentEmployments": [],
                    "disclosureFlag": "N",
                    "examsCount": {"total": 2},
                    "registeredStates": ["NY", "CA"]
                }
                """
            }
        }]
    }
}

class TestBrokerCheckAgent:
    """Tests for the FINRA BrokerCheck API agent"""

    def test_invalid_crd(self):
        """Test handling of invalid CRD numbers"""
        assert search_individual("", logger=logger) is None
        assert search_individual(None, logger=logger) is None
        assert search_individual_detailed_info("", logger=logger) is None

    @patch('requests.get')
    def test_basic_search_success(self, mock_get):
        """Test successful basic search"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = MOCK_BASIC_RESPONSE
        mock_get.return_value = mock_response

        result = search_individual("1234567", "EMP123", logger)
        
        assert result is not None
        assert result["hits"]["total"] == 1
        assert result["hits"]["hits"][0]["_source"]["ind_firstname"] == "John"
        
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        assert "brokercheck.finra.org" in args[0]
        assert kwargs["params"]["query"] == "1234567"

    @patch('requests.get')
    def test_detailed_search_success(self, mock_get):
        """Test successful detailed search"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = MOCK_DETAILED_RESPONSE
        mock_get.return_value = mock_response

        result = search_individual_detailed_info("5695141", "FAKEEMPLOYERID", logger)
        assert result["basicInformation"]["firstName"] == "John"

    @patch('requests.get')
    def test_rate_limit_handling(self, mock_get):
        """Test handling of rate limit responses"""
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_get.return_value = mock_response

        with pytest.raises(RateLimitExceeded):
            search_individual("1234567", logger=logger)

    @patch('requests.get')
    def test_error_handling(self, mock_get):
        """Test handling of various error conditions"""
        # Test network error
        mock_get.side_effect = requests.exceptions.RequestException()
        assert search_individual("1234567", logger=logger) is None

        # Test bad response
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_get.side_effect = None
        mock_get.return_value = mock_response
        assert search_individual("1234567", logger=logger) is None

    @patch('requests.get')
    def test_json_parsing_error(self, mock_get):
        """Test handling of invalid JSON responses"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {
                "hits": [{
                    "_source": {
                        "content": "invalid json"
                    }
                }]
            }
        }
        mock_get.return_value = mock_response

        result = search_individual_detailed_info("1234567", logger=logger)
        assert result is None 