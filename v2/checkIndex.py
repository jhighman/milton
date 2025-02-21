from services import FinancialServicesFacade
import json

def check_organization(org_name: str, facade: FinancialServicesFacade):
    """Check a specific organization and print detailed lookup results"""
    print(f"\nChecking organization: {org_name}")
    print("=" * 50)
    
    # Test the actual lookup method
    crd = facade.get_organization_crd(org_name)
    print(f"\nDirect lookup result: {crd}")
    
    # Look at what's in the cache
    orgs_data = facade._load_organizations_cache()
    if not orgs_data:
        print("\nERROR: Organizations cache is empty or couldn't be loaded!")
        return
        
    print(f"\nTotal organizations in cache: {len(orgs_data)}")
    
    # Show the normalized version
    normalized_test = facade._normalize_organization_name(org_name)
    print(f"\nOriginal name: {org_name}")
    print(f"Normalized to: {normalized_test}")
    
    # Print all entries that might be similar
    print("\nPotential matches in cache:")
    found = False
    for org in orgs_data:
        name = org.get('name', '')
        normalized = org.get('normalizedName', '')
        
        # Split the search name into words and look for matches
        search_words = normalized_test.lower().split()
        org_words = normalized.lower().split()
        
        if any(word in org_words for word in search_words):
            found = True
            print(f"\nOriginal name: {name}")
            print(f"Normalized name: {normalized}")
            print(f"CRD: {org.get('organizationCRD', 'N/A')}")
            
    if not found:
        print("\nNo similar organizations found in cache!")

def main():
    """
    Check organization lookups in the index with various test cases.
    """
    facade = FinancialServicesFacade()
    
    # Test the specific organization from the failing test
    check_organization("Able Wealth Management, LLC", facade)
    
    # Show a few sample entries from the cache to verify format
    print("\nFirst 3 entries in cache (for format verification):")
    print("=" * 50)
    orgs_data = facade._load_organizations_cache()
    for org in orgs_data[:3]:
        print(f"\nOriginal name: {org.get('name', '')}")
        print(f"Normalized name: {org.get('normalizedName', '')}")
        print(f"CRD: {org.get('organizationCRD', 'N/A')}")

if __name__ == "__main__":
    main() 