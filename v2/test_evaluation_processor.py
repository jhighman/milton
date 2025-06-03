import unittest
import logging
import json
import os
from evaluation_processor import are_nicknames, get_name_variants, match_name_part, nickname_dict, reverse_nickname_dict

# Configure logging to capture debug output
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("evaluation_processor")

class TestNicknameMatching(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Verify that nicknames.json exists and is loaded correctly."""
        # Check if nicknames.json exists
        if not os.path.exists("nicknames.json"):
            cls.fail("nicknames.json not found in the current directory. Ensure it exists and is accessible.")
        
        # Verify the file is valid JSON and contains 'douglas'
        try:
            with open("nicknames.json", "r", encoding="utf-8") as f:
                data = json.load(f)
            if "douglas" not in data or "doug" not in data.get("douglas", []):
                logger.warning(
                    "nicknames.json does not contain mapping for 'douglas' to 'doug'. "
                    "Tests may fail if this is not the expected configuration."
                )
        except json.JSONDecodeError as e:
            cls.fail(f"nicknames.json is malformed: {e}")
        except Exception as e:
            cls.fail(f"Failed to read nicknames.json: {e}")

        # Log the loaded nickname_dict for debugging
        logger.debug("Loaded nickname dictionaries successfully")

    def setUp(self):
        """Clear any test-specific state before each test."""
        self.maxDiff = None  # Show full diff in case of assertion failure
        logger.debug("Starting test: %s", self._testMethodName)

    def test_nickname_dict_content(self):
        """Test that nickname_dict contains expected mappings."""
        self.assertIn(
            "douglas",
            nickname_dict,
            "Expected 'douglas' in nickname_dict. Check nicknames.json content."
        )
        self.assertIn(
            "doug",
            nickname_dict["douglas"],
            "Expected 'doug' in nickname_dict['douglas']. Update nicknames.json to include this mapping."
        )

    def test_reverse_nickname_dict_content(self):
        """Test that reverse_nickname_dict contains expected mappings."""
        self.assertIn(
            "doug",
            reverse_nickname_dict,
            "Expected 'doug' in reverse_nickname_dict. Check nicknames.json content."
        )
        self.assertIn(
            "douglas",
            reverse_nickname_dict["doug"],
            "Expected 'douglas' in reverse_nickname_dict['doug']. Update nicknames.json to include this mapping."
        )

    def test_get_name_variants_douglas(self):
        """Test that get_name_variants returns correct variants for 'douglas'."""
        expected_variants = {"douglas", "doug"}
        variants = get_name_variants("douglas")
        self.assertEqual(
            variants,
            expected_variants,
            f"Expected variants {expected_variants}, but got {variants}. Check nicknames.json for 'douglas' mapping."
        )

    def test_get_name_variants_doug(self):
        """Test that get_name_variants returns correct variants for 'doug'."""
        expected_variants = {"doug", "douglas"}
        variants = get_name_variants("doug")
        self.assertEqual(
            variants,
            expected_variants,
            f"Expected variants {expected_variants}, but got {variants}. Check nicknames.json for 'doug' mapping."
        )

    def test_are_nicknames_douglas_doug(self):
        """Test that are_nicknames recognizes 'douglas' and 'doug' as nicknames."""
        result = are_nicknames("douglas", "doug")
        self.assertTrue(
            result,
            "Expected are_nicknames('douglas', 'doug') to return True. "
            "Check nicknames.json for correct 'douglas' to 'doug' mapping."
        )

    def test_are_nicknames_case_insensitive(self):
        """Test that are_nicknames is case-insensitive."""
        result = are_nicknames("Douglas", "Doug")
        self.assertTrue(
            result,
            "Expected are_nicknames('Douglas', 'Doug') to return True (case-insensitive). "
            "Check nicknames.json for correct mappings."
        )

    def test_match_name_part_douglas_doug(self):
        """Test that match_name_part returns 1.0 for 'Douglas' vs 'Doug' as first names."""
        score = match_name_part("Douglas", "Doug", "first")
        self.assertEqual(
            score,
            1.0,
            f"Expected match_name_part('Douglas', 'Doug', 'first') to return 1.0, but got {score}. "
            "Check nicknames.json for 'douglas' to 'doug' mapping."
        )

    def test_match_name_part_non_nickname(self):
        """Test that match_name_part returns low score for non-nickname mismatch."""
        score = match_name_part("Douglas", "Robert", "first")
        self.assertLess(
            score,
            0.85,
            f"Expected match_name_part('Douglas', 'Robert', 'first') to return < 0.85, but got {score}. "
            "Non-nickname names should not match highly."
        )

    def test_match_name_part_douglas_doug_scott(self):
        """Test that match_name_part handles middle name variations correctly."""
        # Test first name matching with middle name present
        score = match_name_part("Douglas", "DOUG SCOTT", "first")
        self.assertEqual(
            score,
            1.0,
            f"Expected match_name_part('Douglas', 'DOUG SCOTT', 'first') to return 1.0, but got {score}. "
            "First name 'Douglas' should match 'DOUG' even with middle name present."
        )

    def test_match_name_part_case_insensitive_douglas(self):
        """Test that match_name_part is case-insensitive with middle names."""
        # Test case insensitivity with middle name
        score = match_name_part("Douglas", "doug scott", "first")
        self.assertEqual(
            score,
            1.0,
            f"Expected match_name_part('Douglas', 'doug scott', 'first') to return 1.0, but got {score}. "
            "Case should not affect nickname matching."
        )

    def test_match_name_part_full_name_douglas(self):
        """Test that match_name_part works with full name containing middle name."""
        # Test full name matching
        score = match_name_part("Douglas Couden", "DOUG SCOTT COUDEN", "full")
        self.assertGreater(
            score,
            0.85,
            f"Expected match_name_part('Douglas Couden', 'DOUG SCOTT COUDEN', 'full') to return > 0.85, but got {score}. "
            "Full name should match despite middle name difference."
        )

if __name__ == "__main__":
    unittest.main()