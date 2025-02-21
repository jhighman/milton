"""
Business Logic Overview
----------------------

This test suite verifies the logic for searching financial regulatory databases based on different 
claim scenarios. Here's how the system decides which databases to search:

1. When both individual CRD and organization CRD are provided:
   - Uses the organization CRD to search SEC IAPD database
   - This is useful when we know both the individual and their firm's identifiers

2. When only organization CRD is provided:
   - Returns an error as entity-only searches aren't supported yet
   - The system requires an individual CRD for proper searching

3. When only individual CRD is provided:
   - First searches FINRA BrokerCheck
   - If no results found, then searches SEC IAPD as fallback
   - This ensures we check both broker and investment adviser databases

4. When individual CRD and organization name are provided:
   - First tries FINRA BrokerCheck using individual CRD
   - If no results, looks up organization's CRD using the name
   - Then searches SEC IAPD with organization CRD if found
   - This helps when we have partial information about both individual and firm

5. When no CRD numbers or organization information is provided:
   - Returns empty results as there's not enough information to search

Key Terms:
- CRD: Central Registration Depository number (unique identifier for individuals and firms)
- SEC IAPD: Investment Adviser Public Disclosure database
- FINRA BrokerCheck: Financial Industry Regulatory Authority's broker database

The system prioritizes BrokerCheck for individual searches and uses SEC IAPD as a fallback
or when organization information is available.
"""

import unittest
from typing import Dict, Any, Optional
from unittest.mock import Mock, patch
from business import process_claim, determine_search_strategy, FinancialServicesFacade

class TestBusinessLogic(unittest.TestCase):
    def setUp(self) -> None:
        # Mock FinancialServicesFacade with type hints
        self.facade = Mock(spec=FinancialServicesFacade)
        self.employee_number = "EMP123"

    def test_both_crd_and_org_crd(self) -> None:
        """Test when claim has both crd and org_crd, should search SEC IAPD with org_crd."""
        claim: Dict[str, Any] = {
            "crd": "12345",
            "org_crd": "67890"
        }
        mock_result: Dict[str, Any] = {"hits": {"total": 1, "hits": [{"_source": {"ind_source_id": "67890"}}]}}
        self.facade.search_sec_iapd_individual.return_value = mock_result

        result = process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["source"], "SEC_IAPD_Organization")
        self.assertEqual(result["result"], mock_result)
        self.facade.search_sec_iapd_individual.assert_called_once_with("67890", "EMP123")
        self.facade.search_finra_brokercheck_individual.assert_not_called()

    def test_only_org_crd(self) -> None:
        """Test when claim has only org_crd, should return entity search error."""
        claim: Dict[str, Any] = {"org_crd": "67890"}
        result = process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["source"], "Entity_Search")
        self.assertEqual(
            result["result"],
            {"error": "Entity search using org_crd is not supported at this time. Please provide an individual crd."}
        )
        self.facade.search_finra_brokercheck_individual.assert_not_called()
        self.facade.search_sec_iapd_individual.assert_not_called()

    def test_only_crd_brokercheck_hit(self) -> None:
        """Test when claim has only crd and BrokerCheck finds a hit."""
        claim: Dict[str, Any] = {"crd": "12345"}
        mock_result: Dict[str, Any] = {"hits": {"total": 1, "hits": [{"_source": {"ind_source_id": "12345"}}]}}
        self.facade.search_finra_brokercheck_individual.return_value = mock_result

        result = process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["source"], "BrokerCheck")
        self.assertEqual(result["result"], mock_result)
        self.facade.search_finra_brokercheck_individual.assert_called_once_with("12345", "EMP123")
        self.facade.search_sec_iapd_individual.assert_not_called()

    def test_only_crd_no_brokercheck_hit(self) -> None:
        """Test when claim has only crd and BrokerCheck returns no hits."""
        claim: Dict[str, Any] = {"crd": "12345"}
        broker_result: Dict[str, Any] = {"hits": {"total": 0, "hits": []}}
        sec_result: Dict[str, Any] = {"hits": {"total": 1, "hits": [{"_source": {"ind_source_id": "12345"}}]}}
        self.facade.search_finra_brokercheck_individual.return_value = broker_result
        self.facade.search_sec_iapd_individual.return_value = sec_result

        result = process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["source"], "SEC_IAPD")
        self.assertEqual(result["result"], sec_result)
        self.facade.search_finra_brokercheck_individual.assert_called_once_with("12345", "EMP123")
        self.facade.search_sec_iapd_individual.assert_called_once_with("12345", "EMP123")

    def test_crd_and_org_name_brokercheck_no_hit_org_crd_found(self) -> None:
        """Test when crd fails in BrokerCheck but org_name yields a valid CRD."""
        claim: Dict[str, Any] = {"crd": "12345", "organization_name": "ABC Securities"}
        broker_result: Dict[str, Any] = {"hits": {"total": 0, "hits": []}}
        sec_result: Dict[str, Any] = {"hits": {"total": 1, "hits": [{"_source": {"ind_source_id": "67890"}}]}}
        self.facade.search_finra_brokercheck_individual.return_value = broker_result
        self.facade.get_organization_crd.return_value = "67890"
        self.facade.search_sec_iapd_individual.return_value = sec_result

        result = process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["source"], "SEC_IAPD_Organization")
        self.assertEqual(result["result"], sec_result)
        self.facade.search_finra_brokercheck_individual.assert_called_once_with("12345", "EMP123")
        self.facade.get_organization_crd.assert_called_once_with("ABC Securities", "EMP123")
        self.facade.search_sec_iapd_individual.assert_called_once_with("67890", "EMP123")

    def test_crd_and_org_name_brokercheck_no_hit_org_crd_not_found(self) -> None:
        """Test when crd fails in BrokerCheck and org_name doesn't yield a CRD."""
        claim: Dict[str, Any] = {"crd": "12345", "organization_name": "Unknown Corp"}
        broker_result: Dict[str, Any] = {"hits": {"total": 0, "hits": []}}
        self.facade.search_finra_brokercheck_individual.return_value = broker_result
        self.facade.get_organization_crd.return_value = "NOT_FOUND"

        result = process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["source"], "SEC_IAPD_Organization")
        self.assertEqual(
            result["result"],
            {"error": "Organization supplied was not found in our index, and no org_crd was included in the search please supply a CRD"}
        )
        self.facade.search_finra_brokercheck_individual.assert_called_once_with("12345", "EMP123")
        self.facade.get_organization_crd.assert_called_once_with("Unknown Corp", "EMP123")
        self.facade.search_sec_iapd_individual.assert_not_called()

    def test_no_usable_fields(self) -> None:
        """Test when claim has no usable fields."""
        claim: Dict[str, Any] = {}
        result = process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["source"], "Default")
        self.assertEqual(result["result"], {"hits": {"total": 0, "hits": []}})
        self.facade.search_finra_brokercheck_individual.assert_not_called()
        self.facade.search_sec_iapd_individual.assert_not_called()

if __name__ == "__main__":
    unittest.main()