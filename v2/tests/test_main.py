import unittest
from unittest.mock import Mock
from typing import Dict, Any
import logging
import business

# Note on Business Logic:
# This test suite validates the business logic in business.py, which processes financial claims
# to retrieve data from SEC IAPD and FINRA BrokerCheck. The logic:
# 1. Uses determine_search_strategy() to select a search function based on claim fields:
#    - individual_name + organization_crd_number -> search_with_correlated
#    - crd_number + organization_crd_number -> search_with_both_crds
#    - crd_number + organization_name -> search_with_crd_and_org_name
#    - crd_number only -> search_with_crd_only
#    - organization_crd_number only -> search_with_entity
#    - organization_name only -> search_with_org_name_only
#    - insufficient fields -> search_default
# 2. Each search function queries the FinancialServicesFacade, prioritizing BrokerCheck where
#    applicable, falling back to SEC IAPD, and handling unsupported cases (e.g., entity-only).
# 3. process_claim() orchestrates the strategy selection and execution, handling None results.

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')

class TestBusinessLogic(unittest.TestCase):
    def setUp(self):
        self.facade = Mock(spec=business.FinancialServicesFacade)
        self.employee_number = "EMP001"

    def test_determine_search_strategy_correlated(self):
        claim = {"individual_name": "John Doe", "organization_crd_number": "12345"}
        strategy = business.determine_search_strategy(claim)
        self.assertEqual(strategy, business.search_with_correlated)

    def test_determine_search_strategy_both_crds(self):
        claim = {"crd_number": "67890", "organization_crd_number": "12345"}
        strategy = business.determine_search_strategy(claim)
        self.assertEqual(strategy, business.search_with_both_crds)

    def test_determine_search_strategy_crd_and_org_name(self):
        claim = {"crd_number": "67890", "organization_name": "Acme Corp"}
        strategy = business.determine_search_strategy(claim)
        self.assertEqual(strategy, business.search_with_crd_and_org_name)

    def test_determine_search_strategy_crd_only(self):
        claim = {"crd_number": "67890"}
        strategy = business.determine_search_strategy(claim)
        self.assertEqual(strategy, business.search_with_crd_only)

    def test_determine_search_strategy_entity(self):
        claim = {"organization_crd_number": "12345"}
        strategy = business.determine_search_strategy(claim)
        self.assertEqual(strategy, business.search_with_entity)

    def test_determine_search_strategy_org_name_only(self):
        claim = {"organization_name": "Acme Corp"}
        strategy = business.determine_search_strategy(claim)
        self.assertEqual(strategy, business.search_with_org_name_only)

    def test_determine_search_strategy_default(self):
        claim = {}
        strategy = business.determine_search_strategy(claim)
        self.assertEqual(strategy, business.search_default)

    def test_process_claim_correlated(self):
        claim = {"individual_name": "John Doe", "organization_crd_number": "12345"}
        self.facade.search_sec_iapd_correlated.return_value = {"crd_number": "67890"}
        self.facade.search_sec_iapd_detailed.return_value = {"details": "some data"}
        result = business.process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["source"], "SEC_IAPD")
        self.assertEqual(result["crd_number"], "67890")
        self.assertEqual(result["search_strategy"], "search_with_correlated")

    def test_process_claim_crd_only_brokercheck(self):
        claim = {"crd_number": "67890"}
        self.facade.search_finra_brokercheck_individual.return_value = {"fetched_name": "John Doe"}
        self.facade.search_finra_brokercheck_detailed.return_value = {"details": "broker data"}
        result = business.process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["source"], "BrokerCheck")
        self.assertEqual(result["search_strategy"], "search_with_crd_only")

    def test_process_claim_crd_only_sec_iapd(self):
        claim = {"crd_number": "67890"}
        self.facade.search_finra_brokercheck_individual.return_value = {"fetched_name": ""}
        self.facade.search_sec_iapd_individual.return_value = {"data": "iapd data"}
        self.facade.search_sec_iapd_detailed.return_value = {"details": "iapd details"}
        result = business.process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["source"], "SEC_IAPD")
        self.assertEqual(result["search_strategy"], "search_with_crd_only")

    def test_process_claim_default(self):
        claim = {}
        result = business.process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["source"], "Default")
        self.assertIsNone(result["crd_number"])
        self.assertEqual(result["search_strategy"], "search_default")

    def test_process_claim_none_result(self):
        claim = {"crd_number": "67890"}
        self.facade.search_finra_brokercheck_individual.return_value = None
        self.facade.search_sec_iapd_individual.return_value = None
        result = business.process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["source"], "SEC_IAPD")
        self.assertIsNone(result["basic_result"])
        self.assertIsNone(result["detailed_result"])
        self.assertEqual(result["search_strategy"], "search_with_crd_only")

if __name__ == "__main__":
    print("Welcome to the Business Logic Test Runner!")
    print("Options:")
    print("  1: Run all tests")
    print("  2: Test determine_search_strategy functions")
    print("  3: Test process_claim functions")
    print("  4: Exit")
    
    while True:
        choice = input("Enter your choice (1-4): ").strip()
        
        if choice == "1":
            unittest.main(argv=[''], exit=False)
            print("\nAll tests completed.")
        elif choice == "2":
            suite = unittest.TestSuite()
            suite.addTests([
                TestBusinessLogic('test_determine_search_strategy_correlated'),
                TestBusinessLogic('test_determine_search_strategy_both_crds'),
                TestBusinessLogic('test_determine_search_strategy_crd_and_org_name'),
                TestBusinessLogic('test_determine_search_strategy_crd_only'),
                TestBusinessLogic('test_determine_search_strategy_entity'),
                TestBusinessLogic('test_determine_search_strategy_org_name_only'),
                TestBusinessLogic('test_determine_search_strategy_default')
            ])
            unittest.TextTestRunner().run(suite)
            print("\nStrategy tests completed.")
        elif choice == "3":
            suite = unittest.TestSuite()
            suite.addTests([
                TestBusinessLogic('test_process_claim_correlated'),
                TestBusinessLogic('test_process_claim_crd_only_brokercheck'),
                TestBusinessLogic('test_process_claim_crd_only_sec_iapd'),
                TestBusinessLogic('test_process_claim_default'),
                TestBusinessLogic('test_process_claim_none_result')
            ])
            unittest.TextTestRunner().run(suite)
            print("\nProcess claim tests completed.")
        elif choice == "4":
            print("Exiting test runner.")
            break
        else:
            print("Invalid choice, please select 1-4.")