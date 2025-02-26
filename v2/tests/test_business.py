import unittest
from unittest.mock import Mock, patch
from typing import Dict, Any
import logging
import sys
from pathlib import Path

# Add the parent directory to the Python path to find the business module
sys.path.append(str(Path(__file__).parent.parent))
import business

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
        
        # Mock facade responses with proper structure
        basic_result = {
            "crd_number": "67890",
            "fetched_name": "John Doe",
            "other_names": ["Johnny Doe"],
            "bc_scope": "Broker",
            "ia_scope": "Investment Advisor"
        }
        
        detailed_result = {
            "exams": [{"examCategory": "Series 7", "examName": "Series 7", "examStatus": "PASSED"}],
            "disclosures": [{"type": "None"}]
        }
        
        search_result = {
            "source": "SEC_IAPD",
            "search_strategy": "search_with_correlated",
            "crd_number": "67890",
            "basic_result": basic_result,
            "detailed_result": detailed_result
        }
        
        self.facade.search_sec_iapd_correlated.return_value = basic_result
        self.facade.search_sec_iapd_detailed.return_value = detailed_result
        
        with patch('business.search_with_correlated', return_value=search_result) as mock_search:
            # Set the __name__ attribute on the mock
            mock_search.__name__ = 'search_with_correlated'
            
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
            self.assertEqual(result["search_evaluation"]["basic_result"]["fetched_name"], "John Doe")

    def test_process_claim_name_parsing(self):
        """Test the name parsing logic in process_claim"""
        claim = {
            "individual_name": "John Middle Doe",
            "organization_crd": "12345"
        }
        
        # Mock with proper structure
        basic_result = {
            "crd_number": "67890",
            "fetched_name": "John Middle Doe",
            "other_names": [],
            "bc_scope": "",
            "ia_scope": ""
        }
        detailed_result = {
            "exams": [],
            "disclosures": []
        }
        
        self.facade.search_sec_iapd_correlated.return_value = basic_result
        self.facade.search_sec_iapd_detailed.return_value = detailed_result
        
        # Mock disciplinary and arbitration reviews
        self.facade.perform_disciplinary_review.return_value = {
            "primary_name": "John Middle Doe",
            "disciplinary_actions": [],
            "due_diligence": {"status": "Complete"}
        }
        
        self.facade.perform_arbitration_review.return_value = {
            "primary_name": "John Middle Doe",
            "arbitration_actions": [],
            "due_diligence": {"status": "Complete"}
        }
        
        result = business.process_claim(claim, self.facade, self.employee_number)
        
        # Verify that the name was correctly parsed
        self.facade.perform_disciplinary_review.assert_called_once()
        call_args = self.facade.perform_disciplinary_review.call_args[0]
        self.assertEqual(call_args[0], "John")  # first_name
        self.assertEqual(call_args[1], "Middle Doe")  # last_name

    def test_process_claim_separate_names(self):
        """Test processing with separate first/last names"""
        claim = {
            "first_name": "John",
            "last_name": "Doe",
            "organization_crd": "12345",
            "individual_name": "John Doe"  # Add this to trigger correlated search
        }
        
        # Mock with proper structure
        basic_result = {
            "crd_number": "67890",
            "fetched_name": "John Doe",
            "other_names": [],
            "bc_scope": "",
            "ia_scope": ""
        }
        detailed_result = {
            "exams": [],
            "disclosures": []
        }
        
        search_result = {
            "source": "SEC_IAPD",
            "search_strategy": "search_with_correlated",
            "crd_number": "67890",
            "basic_result": basic_result,
            "detailed_result": detailed_result
        }
        
        self.facade.search_sec_iapd_correlated.return_value = basic_result
        self.facade.search_sec_iapd_detailed.return_value = detailed_result
        
        with patch('business.search_with_correlated', return_value=search_result) as mock_search:
            # Set the __name__ attribute on the mock
            mock_search.__name__ = 'search_with_correlated'
            
            # Mock disciplinary and arbitration reviews
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
            
            # Verify that the separate names were used directly
            self.facade.perform_disciplinary_review.assert_called_once_with("John", "Doe", self.employee_number)

    def test_process_claim_correlated(self):
        claim = {"individual_name": "John Doe", "organization_crd_number": "12345"}
        
        # Mock with proper structure
        basic_result = {
            "crd_number": "67890",
            "fetched_name": "John Doe",
            "other_names": [],
            "bc_scope": "",
            "ia_scope": ""
        }
        detailed_result = {
            "exams": [],
            "disclosures": []
        }
        
        self.facade.search_sec_iapd_correlated.return_value = basic_result
        self.facade.search_sec_iapd_detailed.return_value = detailed_result
        
        # Mock disciplinary and arbitration reviews
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
        
        # Updated assertions to match new structure
        self.assertEqual(result["search_evaluation"]["source"], "SEC_IAPD")
        self.assertEqual(result["search_evaluation"]["crd_number"], "67890")
        self.assertEqual(result["search_evaluation"]["search_strategy"], "search_with_correlated")

    def test_process_claim_crd_only_brokercheck(self):
        """Test BrokerCheck search path"""
        claim = {"crd_number": "67890"}
        
        # Mock BrokerCheck responses
        basic_result = {"fetched_name": "John Doe", "crd_number": "67890"}
        detailed_result = {"exams": [], "disclosures": []}
        
        self.facade.search_finra_brokercheck_individual.return_value = basic_result
        self.facade.search_finra_brokercheck_detailed.return_value = detailed_result
        
        # Mock disciplinary and arbitration reviews
        self.facade.perform_disciplinary_review.return_value = {
            "primary_name": "Unknown",
            "disciplinary_actions": [],
            "due_diligence": {"status": "No name provided for search"}
        }
        
        self.facade.perform_arbitration_review.return_value = {
            "primary_name": "Unknown",
            "arbitration_actions": [],
            "due_diligence": {"status": "No name provided for search"}
        }
        
        result = business.process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["search_evaluation"]["source"], "BrokerCheck")
        self.assertEqual(result["search_evaluation"]["search_strategy"], "search_with_crd_only")

    def test_process_claim_crd_only_sec_iapd(self):
        claim = {"crd_number": "67890"}
        self.facade.search_finra_brokercheck_individual.return_value = {"fetched_name": ""}
        
        # Mock with proper structure
        basic_result = {
            "crd_number": "67890",
            "fetched_name": "John Doe",
            "other_names": [],
            "bc_scope": "",
            "ia_scope": ""
        }
        detailed_result = {
            "exams": [],
            "disclosures": []
        }
        
        self.facade.search_sec_iapd_individual.return_value = basic_result
        self.facade.search_sec_iapd_detailed.return_value = detailed_result
        
        # Mock disciplinary and arbitration reviews
        self.facade.perform_disciplinary_review.return_value = {
            "primary_name": "Unknown",
            "disciplinary_actions": [],
            "due_diligence": {"status": "No name provided for search"}
        }
        
        self.facade.perform_arbitration_review.return_value = {
            "primary_name": "Unknown",
            "arbitration_actions": [],
            "due_diligence": {"status": "No name provided for search"}
        }
        
        result = business.process_claim(claim, self.facade, self.employee_number)
        self.assertEqual(result["search_evaluation"]["source"], "SEC_IAPD")
        self.assertEqual(result["search_evaluation"]["search_strategy"], "search_with_crd_only")

    def test_process_claim_default(self):
        """Test default search path with empty claim"""
        claim = {}
        
        # Mock the module-level search_default function
        empty_result = {
            "source": "Default",
            "search_strategy": "search_default",
            "crd_number": None,
            "basic_result": {},
            "detailed_result": None
        }
        
        with patch('business.search_default', return_value=empty_result) as mock_search:
            # Set the __name__ attribute on the mock
            mock_search.__name__ = 'search_default'
            
            # Mock disciplinary and arbitration reviews for empty case
            self.facade.perform_disciplinary_review.return_value = {
                "primary_name": "Unknown",
                "disciplinary_actions": [],
                "due_diligence": {"status": "No name provided for search"}
            }
            
            self.facade.perform_arbitration_review.return_value = {
                "primary_name": "Unknown",
                "arbitration_actions": [],
                "due_diligence": {"status": "No name provided for search"}
            }
            
            result = business.process_claim(claim, self.facade, self.employee_number)
            self.assertEqual(result["search_evaluation"]["source"], "Default")
            self.assertEqual(result["search_evaluation"]["search_strategy"], "search_default")
            self.assertIsNone(result["search_evaluation"]["crd_number"])

    def test_process_claim_none_result(self):
        """Test handling of None results from searches"""
        claim = {"crd_number": "67890"}
        
        # Mock all search methods to return properly structured empty results
        empty_result = {
            "source": "SEC_IAPD",
            "search_strategy": "search_with_crd_only",
            "crd_number": None,
            "basic_result": {},
            "detailed_result": None
        }
        
        self.facade.search_finra_brokercheck_individual.return_value = None
        self.facade.search_sec_iapd_individual.return_value = None
        self.facade.search_finra_brokercheck_detailed.return_value = None
        self.facade.search_sec_iapd_detailed.return_value = None
        
        with patch('business.search_with_crd_only', return_value=empty_result) as mock_search:
            # Set the __name__ attribute on the mock
            mock_search.__name__ = 'search_with_crd_only'
            
            # Mock disciplinary and arbitration reviews with empty results
            self.facade.perform_disciplinary_review.return_value = {
                "primary_name": "Unknown",
                "disciplinary_actions": [],
                "due_diligence": {"status": "No name provided for search"}
            }
            
            self.facade.perform_arbitration_review.return_value = {
                "primary_name": "Unknown",
                "arbitration_actions": [],
                "due_diligence": {"status": "No name provided for search"}
            }
            
            result = business.process_claim(claim, self.facade, self.employee_number)
            
            # Verify structure with None/empty values
            self.assertEqual(result["search_evaluation"]["source"], "SEC_IAPD")
            self.assertEqual(result["search_evaluation"]["search_strategy"], "search_with_crd_only")
            self.assertIsNone(result["search_evaluation"]["crd_number"])
            self.assertEqual(result["search_evaluation"]["basic_result"], {})

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