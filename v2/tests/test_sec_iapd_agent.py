import pytest
import logging
import sys
import json
import requests
from pathlib import Path
from typing import Dict, Any
from unittest.mock import patch, Mock

# Add parent directory to path if not already there
test_dir = Path(__file__).parent
project_root = test_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import directly from the agent file, not through __init__
from agents.sec_iapd_agent import (
    search_individual,
    search_individual_detailed_info
)
from agents.exceptions import RateLimitExceeded

# Set up test logger
logger = logging.getLogger("test_sec_iapd")
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
                "ind_source_id": "1438859",
                "ind_firstname": "RICHARD",
                "ind_middlename": "ALLAN",
                "ind_lastname": "GOTTERER",
                "ind_other_names": [],
                "ind_bc_scope": "NotInScope",
                "ind_ia_scope": "Active",
                "ind_ia_disclosure_fl": "N",
                "ind_approved_finra_registration_count": 0,
                "ind_employments_count": 1,
                "ind_industry_cal_date_iapd": "2010-11-04",
                "ind_ia_current_employments": [{
                    "firm_id": "143490",
                    "firm_name": "CALAMOS WEALTH MANAGEMENT LLC",
                    "branch_city": "Coral Gables",
                    "branch_state": "FL",
                    "branch_zip": "33134",
                    "ia_only": "Y"
                }]
            }
        }]
    }
}

MOCK_DETAILED_RESPONSE = {
    "hits": {
        "hits": [{
            "_source": {
                "iacontent": json.dumps({
                    "basicInformation": {
                        "individualId": 1438859,
                        "firstName": "RICHARD",
                        "middleName": "ALLAN",
                        "lastName": "GOTTERER",
                        "otherNames": [],
                        "bcScope": "NotInScope",
                        "iaScope": "Active",
                        "daysInIndustryCalculatedDateIAPD": "11/4/2010"
                    },
                    "currentIAEmployments": [{
                        "firmId": 143490,
                        "firmName": "CALAMOS WEALTH MANAGEMENT LLC",
                        "iaOnly": "Y",
                        "registrationBeginDate": "7/28/2015"
                    }],
                    "disclosureFlag": "N",
                    "iaDisclosureFlag": "N",
                    "registeredStates": [
                        {"state": "Florida", "regScope": "IA", "status": "APPROVED"},
                        {"state": "Illinois", "regScope": "IA", "status": "APPROVED"}
                    ]
                })
            }
        }]
    }
}

class TestSECIAPDAgent:
    """Unit tests for the SEC IAPD API agent"""

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

        result = search_individual("1438859", "TEST_EMP", logger)
        
        assert result is not None
        assert result["hits"]["total"] == 1
        hit = result["hits"]["hits"][0]["_source"]
        assert hit["ind_firstname"] == "RICHARD"
        assert hit["ind_lastname"] == "GOTTERER"
        assert hit["ind_ia_scope"] == "Active"
        
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        assert "adviserinfo.sec.gov" in args[0]
        assert kwargs["params"]["query"] == "1438859"

    @patch('requests.get')
    def test_detailed_search_success(self, mock_get):
        """Test successful detailed search"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = MOCK_DETAILED_RESPONSE
        mock_get.return_value = mock_response

        result = search_individual_detailed_info("1438859", "TEST_EMP", logger)
        
        assert result is not None
        assert result["basicInformation"]["firstName"] == "RICHARD"
        assert result["basicInformation"]["lastName"] == "GOTTERER"
        assert len(result["registeredStates"]) == 2
        assert result["disclosureFlag"] == "N"

    @patch('requests.get')
    def test_rate_limit_handling(self, mock_get):
        """Test handling of rate limit responses"""
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_get.return_value = mock_response

        with pytest.raises(RateLimitExceeded):
            search_individual("1438859", logger=logger)

    @patch('requests.get')
    def test_error_handling(self, mock_get):
        """Test handling of various error conditions"""
        # Test network error
        mock_get.side_effect = requests.exceptions.RequestException()
        assert search_individual("1438859", logger=logger) is None

        # Test bad response
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_get.side_effect = None
        mock_get.return_value = mock_response
        assert search_individual("1438859", logger=logger) is None

@pytest.mark.integration
class TestSECIAPDIntegration:
    """Integration tests for SEC IAPD API"""

    def setup_method(self):
        """Setup method to add delay between tests"""
        import time
        time.sleep(3)  # 3 second delay between tests

    def test_real_adviser_search(self):
        """Test searching for a known investment adviser"""
        result = search_individual("1438859", "TEST_EMP", logger)
        
        print("\nTesting Investment Adviser Search:")
        print(f"CRD: 1438859")
        
        assert result is not None, "Expected result to not be None"
        assert result["hits"]["total"] >= 1, f"Expected at least 1 hit, got {result['hits']['total']}"
        
        hit = result["hits"]["hits"][0]["_source"]
        print("\nBasic Info:")
        print(json.dumps(hit, indent=2))
        
        assert hit["ind_firstname"] == "RICHARD"
        assert hit["ind_lastname"] == "GOTTERER"

    def test_real_adviser_details(self):
        """Test fetching detailed info for a known investment adviser"""
        result = search_individual_detailed_info("1438859", "TEST_EMP", logger)
        
        print("\nTesting Investment Adviser Details:")
        print(f"CRD: 1438859")
        print("\nDetailed Response:")
        print(json.dumps(result, indent=2))
        
        assert result is not None, "Expected result to not be None"
        assert result["basicInformation"]["firstName"] == "RICHARD"
        assert result["basicInformation"]["lastName"] == "GOTTERER"

    def test_nonexistent_adviser(self):
        """Test searching for a nonexistent CRD"""
        result = search_individual("999999999999", "TEST_EMP", logger)
        print("\nNonexistent adviser search result:")
        print(json.dumps(result, indent=2))
        assert result["hits"]["total"] == 0 