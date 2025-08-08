import json
import logging
import sys
from business import process_claim
from services import FinancialServicesFacade

# Configure logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("test_ralph_segall")

class AlertEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Alert objects."""
    def default(self, obj):
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        return super().default(obj)

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

if __name__ == "__main__":
    main()