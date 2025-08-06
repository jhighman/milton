import json
import logging
from business import process_claim
from services import FinancialServicesFacade
from evaluation_processor import Alert, AlertSeverity

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("test_robert_sykes")

class AlertEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Alert objects."""
    def default(self, obj):
        if isinstance(obj, Alert):
            return obj.to_dict()
        return super().default(obj)

def json_dumps_with_alerts(obj, **kwargs):
    """Helper function to serialize objects that may contain Alert instances."""
    return json.dumps(obj, cls=AlertEncoder, **kwargs)

def test_robert_sykes():
    """
    Test case for Robert Sykes to ensure consistent processing.
    """
    # Create a claim with Robert Sykes's data
    claim = {
        "first_name": "Robert",
        "middle_name": "",
        "last_name": "Sykes",
        "suffix": "",
        "workProductNumber": "072225-2-JH",
        "crd_number": "2775993",  # Using individualCRDNumber as crd_number
        "organization_crd": "072225-2a-JH",  # Using organizationCrdNumber as organization_crd
        "employee_number": "TEST_ROBERT_SYKES",
        "reference_id": "TEST_ROBERT_SYKES",
        "packageName": "BROKERCHECK"  # Add this to prioritize BrokerCheck
    }
    
    # Initialize the facade
    facade = FinancialServicesFacade()
    
    # Process the claim
    logger.info(f"Processing claim for Robert Sykes: {json_dumps_with_alerts(claim)}")
    result = process_claim(claim, facade, "TEST_ROBERT_SYKES")
    
    # Analyze the result
    logger.info("Analyzing result...")
    
    # Check for employment evaluation
    employment_evaluation = result.get("employment_evaluation", {})
    employment_alerts = employment_evaluation.get("alerts", [])
    
    if employment_alerts:
        logger.warning("Employment alerts found:")
        for alert in employment_alerts:
            logger.warning(f"Alert: {json.dumps(alert, indent=2)}")
            logger.warning(f"Alert type: {alert.get('alert_type')}")
            logger.warning(f"Alert description: {alert.get('description')}")
            logger.warning(f"Alert metadata: {json.dumps(alert.get('metadata', {}), indent=2)}")
    else:
        logger.info("No employment alerts found.")
    
    # Check for employments data
    employments = result.get("employments", [])
    logger.info(f"Employments data: {json.dumps(employments, indent=2)}")
    
    # Print the full result for detailed analysis
    logger.info("Full result:")
    print(json_dumps_with_alerts(result, indent=2))
    
    return result

if __name__ == "__main__":
    print("Running test for Robert Sykes...")
    try:
        result = test_robert_sykes()
        print("\nTest completed successfully.")
    except Exception as e:
        print(f"\nTest failed with error: {str(e)}")
        import traceback
        traceback.print_exc()