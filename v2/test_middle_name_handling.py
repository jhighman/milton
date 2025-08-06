import unittest
import logging
import sys
from business import determine_search_strategy

# Configure logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("test_middle_name")

class TestMiddleNameHandling(unittest.TestCase):
    """Test cases for middle name handling in determine_search_strategy function."""

    def test_middle_name_included_in_individual_name(self):
        """Test that middle name is properly included when combining names."""
        # Test case from the issue: George Anderson Brown
        claim = {
            "first_name": "George",
            "middle_name": "Anderson",
            "last_name": "Brown",
            "crd_number": "2719984",
            "employee_number": "EMP-202503062026",
            "packageName": "BROKERCHECK"
        }
        
        # Call the function that should combine the names
        determine_search_strategy(claim)
        
        # Verify that individual_name includes the middle name
        self.assertEqual(claim["individual_name"], "George Anderson Brown")
        logger.info(f"Successfully combined names into: {claim['individual_name']}")
        
    def test_middle_name_handling_with_empty_values(self):
        """Test middle name handling with empty or None values."""
        # Test with empty middle name
        claim1 = {
            "first_name": "John",
            "middle_name": "",
            "last_name": "Doe",
        }
        determine_search_strategy(claim1)
        self.assertEqual(claim1["individual_name"], "John Doe")
        
        # Test with None middle name
        claim2 = {
            "first_name": "Jane",
            "middle_name": None,
            "last_name": "Smith",
        }
        determine_search_strategy(claim2)
        self.assertEqual(claim2["individual_name"], "Jane Smith")
        
    def test_middle_name_with_existing_individual_name(self):
        """Test that existing individual_name is not overwritten."""
        claim = {
            "first_name": "George",
            "middle_name": "Anderson",
            "last_name": "Brown",
            "individual_name": "George A. Brown"  # Pre-existing individual_name
        }
        determine_search_strategy(claim)
        # Should not change the existing individual_name
        self.assertEqual(claim["individual_name"], "George A. Brown")
        
    def test_middle_name_with_missing_first_or_last(self):
        """Test middle name handling when first or last name is missing."""
        # Missing last name
        claim1 = {
            "first_name": "George",
            "middle_name": "Anderson",
            "last_name": ""
        }
        determine_search_strategy(claim1)
        self.assertEqual(claim1["individual_name"], "George Anderson")
        
        # Missing first name
        claim2 = {
            "first_name": "",
            "middle_name": "Anderson",
            "last_name": "Brown"
        }
        determine_search_strategy(claim2)
        self.assertEqual(claim2["individual_name"], "Anderson Brown")

if __name__ == "__main__":
    unittest.main()