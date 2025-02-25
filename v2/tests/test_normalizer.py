import pytest
import logging
from unittest.mock import patch
import json

# Import the module
from normalizer import create_individual_record, create_disciplinary_record, VALID_DISCIPLINARY_SOURCES

# Set up test logging
logger = logging.getLogger("test_normalizer")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# Test fixtures for individual records
@pytest.fixture
def default_individual_info():
    """Default empty extracted_info structure for individual records."""
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

# Test fixtures for disciplinary records
@pytest.fixture
def default_disciplinary_record():
    """Default empty disciplinary record structure."""
    return {
        "source": "",
        "primary_name": "",
        "disciplinary_actions": []
    }

@pytest.fixture
def sec_disciplinary_data():
    """Sample SEC Disciplinary data structure."""
    return {
        "result": [
            {
                "Name": "Mark Miller",
                "Also Known As": "M. Miller; Mark J. Miller",
                "Enforcement Action": "Case123",
                "Date Filed": "2023-01-15",
                "Documents": [
                    {"title": "Complaint", "link": "https://sec.gov/doc1", "date": "2023-01-15"}
                ],
                "State": "NY",
                "Current Age": "45"
            }
        ]
    }

@pytest.fixture
def finra_disciplinary_data():
    """Sample FINRA Disciplinary data structure."""
    return {
        "result": [
            {
                "Firms/Individuals": "Jane Doe, Acme Corp",
                "Case ID": "FINRA456",
                "Case Summary": "Fraudulent trading",
                "Action Date": "2022-06-01",
                "Document Type": "Decision"
            }
        ]
    }

# Tests for create_individual_record

def test_individual_no_basic_info(default_individual_info):
    """Test with no basic_info provided."""
    with patch("normalizer.logger") as mock_logger:
        result = create_individual_record("BrokerCheck", None, None)
        assert result == default_individual_info
        mock_logger.warning.assert_called_once_with("No basic_info provided. Returning empty extracted_info.")

def test_brokercheck_full_record(brokercheck_basic_info, brokercheck_detailed_info):
    """Test BrokerCheck with complete basic and detailed info."""
    result = create_individual_record("BrokerCheck", brokercheck_basic_info, brokercheck_detailed_info)
    assert result["fetched_name"] == "John A Doe"
    assert result["other_names"] == ["Johnny Doe", "J. Doe"]
    assert result["bc_scope"] == "Active"
    assert len(result["disclosures"]) == 2
    assert result["disclosures"][0]["type"] == "Regulatory"

def test_iapd_full_record(iapd_basic_info, iapd_detailed_info):
    """Test IAPD with complete basic and detailed info."""
    result = create_individual_record("IAPD", iapd_basic_info, iapd_detailed_info)
    assert result["fetched_name"] == "Jane Smith"
    assert result["ia_scope"] == "Active"
    assert len(result["disclosures"]) == 1
    assert result["disclosures"][0]["type"] == "Regulatory"
    assert len(result["arbitrations"]) == 1
    assert result["exams"] == ["Series 63", "Series 24", "Series 7"]
    assert len(result["current_ia_employments"]) == 1

def test_brokercheck_malformed_content(brokercheck_basic_info):
    """Test BrokerCheck with malformed content JSON."""
    detailed_info = {"hits": {"hits": [{"_source": {"content": "invalid json"}}]}}
    with patch("normalizer.logger") as mock_logger:
        result = create_individual_record("BrokerCheck", brokercheck_basic_info, detailed_info)
        assert result["fetched_name"] == "John A Doe"
        assert result["disclosures"] == []
        mock_logger.warning.assert_called_once()

def test_iapd_no_detailed_info(iapd_basic_info):
    """Test IAPD with only basic_info."""
    result = create_individual_record("IAPD", iapd_basic_info, None)
    assert result["fetched_name"] == "Jane Smith"
    assert result["disclosures"] == [{"type": "Minor Violation", "date": "2022-01-01"}]

# Tests for create_disciplinary_record

def test_disciplinary_invalid_source(default_disciplinary_record):
    """Test with an invalid disciplinary source."""
    with patch("normalizer.logger") as mock_logger:
        result = create_disciplinary_record("Invalid", None)
        assert result["source"] == "Invalid"
        assert result["primary_name"] == ""
        assert result["disciplinary_actions"] == []
        mock_logger.error.assert_called_once_with(f"Invalid source 'Invalid'. Must be one of {VALID_DISCIPLINARY_SOURCES}.")

def test_disciplinary_no_data(default_disciplinary_record):
    """Test with no data provided."""
    with patch("normalizer.logger") as mock_logger:
        result = create_disciplinary_record("SEC_Disciplinary", None)
        expected = default_disciplinary_record.copy()
        expected["source"] = "SEC_Disciplinary"
        assert result == expected
        mock_logger.warning.assert_called_once_with("No results found in SEC_Disciplinary data.")

def test_disciplinary_no_results(default_disciplinary_record):
    """Test with 'No Results Found' data."""
    data = {"result": "No Results Found"}
    with patch("normalizer.logger") as mock_logger:
        result = create_disciplinary_record("FINRA_Disciplinary", data)
        expected = default_disciplinary_record.copy()
        expected["source"] = "FINRA_Disciplinary"
        assert result == expected
        mock_logger.warning.assert_called_once_with("No results found in FINRA_Disciplinary data.")

def test_sec_disciplinary_full_record(sec_disciplinary_data):
    """Test SEC Disciplinary with complete data."""
    with patch("normalizer.logger") as mock_logger:
        result = create_disciplinary_record("SEC_Disciplinary", sec_disciplinary_data)
        assert result["source"] == "SEC_Disciplinary"
        assert result["primary_name"] == "Mark Miller"
        assert len(result["disciplinary_actions"]) == 1
        action = result["disciplinary_actions"][0]
        assert action["case_id"] == "Case123"
        assert action["associated_names"] == ["Mark Miller", "M. Miller", "Mark J. Miller"]
        assert action["additional_info"]["state"] == "NY"
        mock_logger.info.assert_called_once_with("Normalized SEC_Disciplinary data for Mark Miller")

def test_finra_disciplinary_full_record(finra_disciplinary_data):
    """Test FINRA Disciplinary with complete data."""
    with patch("normalizer.logger") as mock_logger:
        result = create_disciplinary_record("FINRA_Disciplinary", finra_disciplinary_data)
        assert result["source"] == "FINRA_Disciplinary"
        assert result["primary_name"] == "Jane Doe, Acme Corp"
        assert len(result["disciplinary_actions"]) == 1
        action = result["disciplinary_actions"][0]
        assert action["case_id"] == "FINRA456"
        assert action["description"] == "Fraudulent trading"
        assert action["associated_names"] == ["Jane Doe", "Acme Corp"]
        assert action["documents"][0]["title"] == "Decision"
        mock_logger.info.assert_called_once_with("Normalized FINRA_Disciplinary data for Jane Doe, Acme Corp")

def test_sec_disciplinary_multiple_results():
    """Test SEC Disciplinary with multiple results and inconsistent names."""
    data = {
        "result": [
            {"Name": "Mark Miller", "Enforcement Action": "Case123", "Date Filed": "2023-01-15"},
            {"Name": "Mark J. Miller", "Enforcement Action": "Case456", "Date Filed": "2023-02-01"}
        ]
    }
    with patch("normalizer.logger") as mock_logger:
        result = create_disciplinary_record("SEC_Disciplinary", data)
        assert result["primary_name"] == "Mark Miller"
        assert len(result["disciplinary_actions"]) == 2
        assert result["disciplinary_actions"][1]["associated_names"] == ["Mark J. Miller"]
        mock_logger.warning.assert_called_once_with("Inconsistent names in SEC Disciplinary results: Mark J. Miller vs Mark Miller")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])