import json
import logging
import sys
from business import process_claim, AlertEncoder
from services import FinancialServicesFacade

# Configure logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("test_invalid_crd_detection")

def json_dumps_with_alerts(obj, **kwargs):
    """Helper function to serialize objects that may contain Alert instances."""
    return json.dumps(obj, cls=AlertEncoder, **kwargs)

def main():
    """Test the invalid CRD detection functionality."""
    # Initialize the facade
    facade = FinancialServicesFacade()
    
    # Create a claim with an invalid CRD number
    claim = {
        "first_name": "Ralph",
        "last_name": "Segall",
        "crd_number": "111",  # Invalid CRD
        "employee_number": "EMP-TEST-INVALID-CRD",
        "reference_id": "TEST-INVALID-CRD-001"
    }
    
    logger.info(f"Processing claim with invalid CRD: {json_dumps_with_alerts(claim)}")
    
    # Process the claim
    result = process_claim(
        claim,
        facade,
        employee_number=claim["employee_number"]
    )
    
    # Print the result
    logger.info("Claim processing completed")
    print("\nResult for Invalid CRD Test:")
    print(json_dumps_with_alerts(result, indent=2))
    
    # Save the result to a file for easier examination
    with open("invalid_crd_report.json", "w") as f:
        f.write(json_dumps_with_alerts(result, indent=2))
    
    logger.info("Report saved to invalid_crd_report.json")
    
    # Verify the invalid CRD detection
    search_evaluation = result.get("search_evaluation", {})
    
    print("\n=== VERIFICATION RESULTS ===")
    
    # Check if the search evaluation has the is_invalid_crd flag
    if search_evaluation.get("is_invalid_crd", False):
        print("✅ SUCCESS: Invalid CRD was correctly detected")
        print(f"Explanation: {search_evaluation.get('compliance_explanation', 'No explanation provided')}")
    else:
        print("❌ FAILURE: Invalid CRD was not detected")
        print(f"Compliance: {search_evaluation.get('compliance', False)}")
        print(f"Explanation: {search_evaluation.get('compliance_explanation', 'No explanation provided')}")
    
    # Check if the source is set to CRD_Validation
    if search_evaluation.get("source") == "CRD_Validation":
        print("✅ SUCCESS: Source correctly set to CRD_Validation")
    else:
        print(f"❌ FAILURE: Source incorrectly set to {search_evaluation.get('source', 'None')}")
    
    # Check if compliance is set to False
    if not search_evaluation.get("compliance", True):
        print("✅ SUCCESS: Compliance correctly set to False")
    else:
        print("❌ FAILURE: Compliance incorrectly set to True")
    
    # Check if the CRD mismatch is detected
    explanation = search_evaluation.get("compliance_explanation", "")
    if "CRD number mismatch" in explanation or "invalid" in explanation.lower():
        print("✅ SUCCESS: Explanation correctly indicates CRD issue")
    else:
        print(f"❌ FAILURE: Explanation does not indicate CRD issue: {explanation}")
    
    print("\nTest completed.")

if __name__ == "__main__":
    main()