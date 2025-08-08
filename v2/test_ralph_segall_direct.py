import json
import logging
import sys
from business import process_claim, AlertEncoder
from services import FinancialServicesFacade

# Configure logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("test_ralph_segall")

def json_dumps_with_alerts(obj, **kwargs):
    """Helper function to serialize objects that may contain Alert instances."""
    return json.dumps(obj, cls=AlertEncoder, **kwargs)

def main():
    """Test processing a claim for Ralph Segall with the specified parameters."""
    # Initialize the facade
    facade = FinancialServicesFacade()
    
    # Create a claim with the specified parameters
    claim = {
        "first_name": "Ralph",
        "last_name": "Segall",
        "crd_number": "111",  # provided but invalid
        "employee_number": "EMP-TEST-RALPH",
        "reference_id": "TEST-RALPH-SEGALL-001"
        # No organization_crd or organization_name as specified
    }
    
    logger.info(f"Processing claim for Ralph Segall with parameters: {json_dumps_with_alerts(claim)}")
    
    # Process the claim
    result = process_claim(
        claim,
        facade,
        employee_number=claim["employee_number"],
        skip_disciplinary=False,
        skip_arbitration=False,
        skip_regulatory=False
    )
    
    # Print the result
    logger.info("Claim processing completed")
    print("\nResult for Ralph Segall:")
    print(json_dumps_with_alerts(result, indent=2))
    
    # Save the result to a file for easier examination
    with open("ralph_segall_report.json", "w") as f:
        f.write(json_dumps_with_alerts(result, indent=2))
    
    logger.info("Report saved to ralph_segall_report.json")
    
    # Analyze the report to verify our fix
    if "individual_name" in result["claim"]:
        print(f"\nIndividual name in report: {result['claim']['individual_name']}")
    else:
        print("\nNo individual_name found in the report")
    
    # Check if the search evaluation was successful
    if "search_evaluation" in result:
        print(f"\nSearch strategy used: {result['search_evaluation'].get('search_strategy', 'Unknown')}")
        print(f"Search compliance: {result['search_evaluation'].get('compliance', False)}")
        print(f"Search explanation: {result['search_evaluation'].get('compliance_explanation', 'Not provided')}")
    
    # Check name evaluation
    if "name_evaluation" in result:
        print(f"\nName evaluation status: {result['name_evaluation'].get('status', 'Unknown')}")
        print(f"Name evaluation explanation: {result['name_evaluation'].get('explanation', 'Not provided')}")

if __name__ == "__main__":
    main()