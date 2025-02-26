import unittest
from unittest.mock import Mock
from typing import Dict, Any
import logging
import business  # Assumes business.py is in the same directory

# Note on Business Logic:
# This test suite validates the business logic in business.py, which processes financial claims
# to retrieve data from regulatory sources (SEC IAPD and FINRA BrokerCheck). The logic:
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
# The tests mock the facade to isolate and verify this decision-making and result structure.

# Setup basic logging for test visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')

class TestBusinessLogic(unittest.TestCase):
    def setUp(self):
        # Mock the FinancialServicesFacade
        self.facade = Mock(spec=business.FinancialServicesFacade)
        self.employee_number = "EMP001"

    def test_determine_search_strategy_correlated(self):
        # Test both organization_crd_number and organization_crd
        claim1 = {"individual_name": "John Doe", "organization_crd_number": "12345"}
        claim2 = {"individual_name": "John Doe", "organization_crd": "12345"}
        
        strategy1 = business.determine_search_strategy(claim1)
        strategy2 = business.determine_search_strategy(claim2)
        
        self.assertEqual(strategy1, business.search_with_correlated)
        self.assertEqual(strategy2, business.search_with_correlated)

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

    def test_process_claim_full_workflow(self):
        """Test the complete claim processing workflow with all new features"""
        claim = {
            "individual_name": "John Doe",
            "organization_crd": "12345",
            "reference_id": "TEST-123"
        }
        
        # Mock facade responses
        self.facade.search_sec_iapd_correlated.return_value = {
            "crd_number": "67890",
            "fetched_name": "John Doe",
            "other_names": ["Johnny Doe"],
            "bc_scope": "Broker",
            "ia_scope": "Investment Advisor"
        }
        
        self.facade.search_sec_iapd_detailed.return_value = {
            "exams": ["Series 7"],
            "disclosures": ["None"]
        }
        
        self.facade.perform_disciplinary_review.return_value = {
            "primary_name": "John Doe",
            "disciplinary_actions": [],
            "due_diligence": {"status": "Complete"}
        }
        
        self.facade.perform_arbitration_review.return_value = {
            "primary_name": "John Doe",
            "arbitration_actions": [],
            "due_diligence": {"status": "Complete"}
        }
        
        result = business.process_claim(claim, self.facade, self.employee_number)
        
        # Verify the complete structure of the result
        self.assertEqual(result["search_evaluation"]["source"], "SEC_IAPD")
        self.assertEqual(result["search_evaluation"]["search_strategy"], "search_with_correlated")
        self.assertEqual(result["search_evaluation"]["crd_number"], "67890")
        self.assertEqual(result["individual"]["fetched_name"], "John Doe")
        self.assertEqual(result["exams"], ["Series 7"])
        self.assertEqual(result["disclosures"], ["None"])
        
        # Verify name parsing
        self.facade.perform_disciplinary_review.assert_called_once_with("John", "Doe", self.employee_number)
        self.facade.perform_arbitration_review.assert_called_once_with("John", "Doe", self.employee_number)

    def test_process_claim_name_parsing(self):
        """Test the name parsing logic in process_claim"""
        claim = {
            "individual_name": "John Middle Doe",
            "organization_crd": "12345"
        }
        
        self.facade.search_sec_iapd_correlated.return_value = {"crd_number": "67890"}
        result = business.process_claim(claim, self.facade, self.employee_number)
        
        # Verify that the name was correctly parsed
        self.facade.perform_disciplinary_review.assert_called_once_with("John", "Middle Doe", self.employee_number)

    def test_process_claim_separate_names(self):
        """Test processing with separate first/last names"""
        claim = {
            "first_name": "John",
            "last_name": "Doe",
            "organization_crd": "12345"
        }
        
        self.facade.search_sec_iapd_correlated.return_value = {"crd_number": "67890"}
        result = business.process_claim(claim, self.facade, self.employee_number)
        
        # Verify that the separate names were used directly
        self.facade.perform_disciplinary_review.assert_called_once_with("John", "Doe", self.employee_number)

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
        self.facade.search_finra_brokercheck_individual.return_value = None  # Simulate BrokerCheck failure
        self.facade.search_sec_iapd_individual.return_value = None  # Simulate SEC IAPD failure
        result = business.process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["source"], "SEC_IAPD")  # Still returns SEC_IAPD as source
        self.assertIsNone(result["basic_result"])  # No basic result
        self.assertIsNone(result["detailed_result"])  # No detailed result
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
            unittest.main(argv=[''], exit=False)  # Runs all tests
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
                TestBusinessLogic('test_process_claim_full_workflow'),
                TestBusinessLogic('test_process_claim_name_parsing'),
                TestBusinessLogic('test_process_claim_separate_names'),
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