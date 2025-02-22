from agents.sec_iapd_agent import search_individual_by_firm
import json

def main():
    """
    Test SEC IAPD firm search functionality

    Matt's CRD is 2112848 it's registered at SEC, not FINRA

    """
    print("\nTesting SEC IAPD firm search:")
    print("=" * 50)
    
    result = search_individual_by_firm(
        individual_name="Matthew Vetto",
        employee_number="SEC_CORR_1",
        firm_crd="282563"
    )
    
    if result:
        print("\nDirect firm search result:")
        print(json.dumps(result, indent=2))
    else:
        print("\nNo results found for direct firm search")

if __name__ == "__main__":
    main() 