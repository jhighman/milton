import pytest
import logging
from unittest.mock import patch
import json

# Import the module (now saved as normalizer.py)
from normalizer import create_individual_record

# Set up test logging
logger = logging.getLogger("test_normalizer")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# Test fixtures
@pytest.fixture
def default_extracted_info():
    """Default empty extracted_info structure."""
    return {
        "fetched_name": "",
        "other_names": [],
        "bc_scope": "",
        "ia_scope": "",
        "disclosures": [],
        "arbitrations": [],
        "exams": [],
        "current_ia_employments": []
    }

@pytest.fixture
def brokercheck_basic_info():
    """Sample BrokerCheck basic_info structure."""
    return {
        "hits": {
            "hits": [{
                "_source": {
                    "ind_firstname": "John",
                    "ind_middlename": "A",
                    "ind_lastname": "Doe",
                    "ind_other_names": ["Johnny Doe", "J. Doe"],
                    "ind_bc_scope": "Active",
                    "ind_ia_scope": ""
                }
            }]
        }
    }

@pytest.fixture
def brokercheck_detailed_info():
    """Sample BrokerCheck detailed_info structure."""
    return {
        "hits": {
            "hits": [{
                "_source": {
                    "content": json.dumps({
                        "disclosures": [
                            {"type": "Regulatory", "date": "2020-01-01"},
                            {"type": "Civil", "date": "2021-06-15"}
                        ]
                    })
                }
            }]
        }
    }

@pytest.fixture
def iapd_basic_info():
    """Sample IAPD basic_info structure."""
    return {
        "hits": {
            "hits": [{
                "_source": {
                    "ind_firstname": "Jane",
                    "ind_middlename": "",
                    "ind_lastname": "Smith",
                    "ind_other_names": ["Jane S."],
                    "ind_bc_scope": "",
                    "ind_ia_scope": "Active",
                    "iacontent": json.dumps({
                        "currentIAEmployments": [
                            {
                                "firmId": "12345",
                                "firmName": "Smith Advisory",
                                "registrationBeginDate": "2019-03-01",
                                "branchOfficeLocations": [
                                    {"street1": "123 Main St", "city": "New York", "state": "NY", "zipCode": "10001"}
                                ]
                            }
                        ],
                        "disclosures": [{"type": "Minor Violation", "date": "2022-01-01"}]
                    })
                }
            }]
        }
    }

@pytest.fixture
def iapd_detailed_info():
    """Sample IAPD detailed_info structure."""
    return {
        "hits": {
            "hits": [{
                "_source": {
                    "iacontent": json.dumps({
                        "disclosures": [
                            {"type": "Regulatory", "date": "2023-01-01"}
                        ],
                        "arbitrations": [
                            {"case": "ARB123", "date": "2022-06-01"}
                        ],
                        "stateExamCategory": ["Series 63"],
                        "principalExamCategory": ["Series 24"],
                        "productExamCategory": ["Series 7"]
                    })
                }
            }]
        }
    }

# Tests

def test_no_basic_info(default_extracted_info):
    """Test with no basic_info provided."""
    with patch("normalizer.logger") as mock_logger:
        result = create_individual_record("BrokerCheck", None, None)
        assert result == default_extracted_info
        mock_logger.warning.assert_called_once_with("No basic_info provided. Returning empty extracted_info.")

def test_brokercheck_empty_hits(default_extracted_info):
    """Test BrokerCheck with empty hits in basic_info."""
    basic_info = {"hits": {"hits": []}}
    with patch("normalizer.logger") as mock_logger:
        result = create_individual_record("BrokerCheck", basic_info, None)
        assert result == default_extracted_info
        mock_logger.warning.assert_called_once_with("BrokerCheck: basic_info had no hits. Returning mostly empty extracted_info.")

def test_brokercheck_full_record(brokercheck_basic_info, brokercheck_detailed_info):
    """Test BrokerCheck with complete basic and detailed info."""
    result = create_individual_record("BrokerCheck", brokercheck_basic_info, brokercheck_detailed_info)
    assert result["fetched_name"] == "John A Doe"
    assert result["other_names"] == ["Johnny Doe", "J. Doe"]
    assert result["bc_scope"] == "Active"
    assert result["ia_scope"] == ""
    assert len(result["disclosures"]) == 2
    assert result["disclosures"][0]["type"] == "Regulatory"
    assert result["arbitrations"] == []
    assert result["exams"] == []
    assert result["current_ia_employments"] == []

def test_brokercheck_no_detailed_info(brokercheck_basic_info):
    """Test BrokerCheck with only basic_info."""
    with patch("normalizer.logger") as mock_logger:
        result = create_individual_record("BrokerCheck", brokercheck_basic_info, None)
        assert result["fetched_name"] == "John A Doe"
        assert result["other_names"] == ["Johnny Doe", "J. Doe"]
        assert result["bc_scope"] == "Active"
        assert result["disclosures"] == []
        mock_logger.info.assert_called_once_with("No BrokerCheck detailed_info provided or empty, skipping disclosures parsing.")

def test_brokercheck_malformed_content(brokercheck_basic_info):
    """Test BrokerCheck with malformed content JSON in detailed_info."""
    detailed_info = {
        "hits": {
            "hits": [{
                "_source": {"content": "invalid json"}
            }]
        }
    }
    with patch("normalizer.logger") as mock_logger:
        result = create_individual_record("BrokerCheck", brokercheck_basic_info, detailed_info)
        assert result["fetched_name"] == "John A Doe"
        assert result["disclosures"] == []
        mock_logger.warning.assert_called_once_with("Failed to parse BrokerCheck 'content' JSON: Expecting value: line 1 column 1 (char 0)")

def test_iapd_full_record(iapd_basic_info, iapd_detailed_info):
    """Test IAPD with complete basic and detailed info."""
    result = create_individual_record("IAPD", iapd_basic_info, iapd_detailed_info)
    assert result["fetched_name"] == "Jane Smith"
    assert result["other_names"] == ["Jane S."]
    assert result["bc_scope"] == ""
    assert result["ia_scope"] == "Active"
    assert len(result["disclosures"]) == 1  # Detailed info overrides basic_info disclosures
    assert result["disclosures"][0]["type"] == "Regulatory"
    assert len(result["arbitrations"]) == 1
    assert result["arbitrations"][0]["case"] == "ARB123"
    assert result["exams"] == ["Series 63", "Series 24", "Series 7"]
    assert len(result["current_ia_employments"]) == 1
    assert result["current_ia_employments"][0]["firm_name"] == "Smith Advisory"

def test_iapd_no_detailed_info(iapd_basic_info):
    """Test IAPD with only basic_info."""
    result = create_individual_record("IAPD", iapd_basic_info, None)
    assert result["fetched_name"] == "Jane Smith"
    assert result["ia_scope"] == "Active"
    assert len(result["current_ia_employments"]) == 1
    assert result["disclosures"] == [{"type": "Minor Violation", "date": "2022-01-01"}]  # From basic_info iacontent
    assert result["arbitrations"] == []
    assert result["exams"] == []

def test_iapd_malformed_iacontent(iapd_basic_info):
    """Test IAPD with malformed iacontent in basic_info."""
    iapd_basic_info["hits"]["hits"][0]["_source"]["iacontent"] = "invalid json"
    with patch("normalizer.logger") as mock_logger:
        result = create_individual_record("IAPD", iapd_basic_info, None)
        assert result["fetched_name"] == "Jane Smith"
        assert result["disclosures"] == []
        assert result["current_ia_employments"] == []
        mock_logger.warning.assert_called_once_with("IAPD basic_info iacontent parse error: Expecting value: line 1 column 1 (char 0)")

def test_unknown_data_source(default_extracted_info):
    """Test with an unknown data source."""
    basic_info = {"hits": {"hits": [{"_source": {"ind_firstname": "Test"}}]}}
    with patch("normalizer.logger") as mock_logger:
        result = create_individual_record("Unknown", basic_info, None)
        assert result["fetched_name"] == "Test"
        assert result["disclosures"] == []
        mock_logger.error.assert_called_once_with("Unknown data source 'Unknown'. Returning minimal extracted_info.")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])