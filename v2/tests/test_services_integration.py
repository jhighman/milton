import pytest
import os
from services import FinancialServicesFacade

@pytest.fixture
def facade():
    """Create a FinancialServicesFacade instance for testing"""
    return FinancialServicesFacade()

class TestFinancialServicesFacadeIntegration:
    """Integration tests for FinancialServicesFacade"""

    def test_get_organization_crd_found(self, facade):
        """Test getting CRD for an organization that exists in the index"""
        # Test with known organization
        org_name = "Able Wealth Management, LLC"
        result = facade.get_organization_crd(org_name)
        
        assert result is not None
        assert result != "NOT_FOUND"
        assert isinstance(result, str)
        assert result.isdigit()  # CRD should be numeric

    def test_get_organization_crd_case_insensitive(self, facade):
        """Test that organization lookup is case insensitive"""
        variations = [
            "ABLE WEALTH MANAGEMENT, LLC",
            "able wealth management, llc",
            "Able Wealth Management, LLC",
            "aBLe WeAlTh MaNaGeMeNt, LLC"
        ]
        
        expected_crd = facade.get_organization_crd("Able Wealth Management, LLC")
        assert expected_crd is not None  # Ensure we have a baseline
        
        for variant in variations:
            result = facade.get_organization_crd(variant)
            assert result == expected_crd, f"Failed for variant: {variant}"

    def test_get_organization_crd_not_found(self, facade):
        """Test getting CRD for a non-existent organization"""
        result = facade.get_organization_crd("NonExistent Wealth LLC")
        assert result == "NOT_FOUND"

    def test_get_organization_crd_with_spaces(self, facade):
        """Test that extra spaces don't affect lookup"""
        variations = [
            "Able  Wealth  Management, LLC",
            " Able Wealth Management, LLC ",
            "Able Wealth  Management,  LLC"
        ]
        
        expected_crd = facade.get_organization_crd("Able Wealth Management, LLC")
        assert expected_crd is not None
        
        for variant in variations:
            result = facade.get_organization_crd(variant)
            assert result == expected_crd, f"Failed for variant: {variant}"

    @pytest.mark.skipif(not os.path.exists("input/organizationsCrd.jsonl"),
                       reason="Organizations cache file not found")
    def test_cache_file_exists(self):
        """Verify that the organizations cache file exists"""
        assert os.path.exists("input/organizationsCrd.jsonl")
        assert os.path.getsize("input/organizationsCrd.jsonl") > 0