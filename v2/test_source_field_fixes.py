"""
Test script to verify the source field fixes.
"""

import json
from common_types import DataSource
from evaluation_processor import (
    Alert,
    AlertSeverity,
    generate_disclosure_alert,
    json_dumps_with_alerts
)
from evaluation_report_builder import EvaluationReportBuilder

def test_alert_source_field():
    """Test that the source field is properly included in alerts."""
    print("Testing source field in alerts\n")
    
    # Create a sample disclosure
    disclosure = {
        'disclosureType': 'Customer Dispute',
        'eventDate': '2000-04-26',
        'disclosureResolution': 'Settled',
        'disclosureDetail': {
            'Allegations': 'ON JANUARY 11, 1999, CLIENT ALLEGES THAT MR. BROWN BOUGHT INSTEAD OF SOLD 25 CONTRACTS OF DATA BROADCASTING CALLS.',
            'Damage Amount Requested': '$75,000.00',
            'Settlement Amount': '$9,999.00',
            'DisplayAAOLinkIfExists': 'Y',
            'arbitrationClaimFiledDetail': '2000-008427',
            'arbitrationDocketNumber': '',
            'Broker Comment': [
                'CLAIMANT VOLUNTARILY DISMISSED BROKER FROM ARBITRATION AND SETTLED WITH DAIN RAUSCHER INCORPORATED.'
            ]
        }
    }
    
    # Generate an alert with a source
    alert = generate_disclosure_alert(disclosure, DataSource.FINRA_BROKERCHECK.value)
    
    # Check if the alert has the correct source
    if alert and alert.source == DataSource.FINRA_BROKERCHECK.value:
        print(f"✅ Alert has the correct source: {alert.source}")
    else:
        print(f"❌ Alert has incorrect source: {alert.source if alert else 'None'}")
    
    # Convert the alert to a dictionary and check if it has a source field
    alert_dict = alert.to_dict() if alert else {}
    if 'source' in alert_dict:
        print(f"✅ Alert dictionary has a source field: {alert_dict['source']}")
    else:
        print("❌ Alert dictionary does not have a source field")
    
    print("\n" + "-"*80 + "\n")

def test_final_evaluation_no_source():
    """Test that the final_evaluation section does not have a source field."""
    print("Testing final_evaluation without source field\n")
    
    # Create a sample final evaluation
    final_evaluation = {
        "overall_compliance": False,
        "compliance_explanation": "One or more compliance checks failed.",
        "overall_risk_level": "High",
        "recommendations": "Immediate action required due to critical compliance issues.",
        "alerts": [
            {
                "alert_type": "Customer Dispute Disclosure",
                "alert_category": "DISCLOSURE",
                "source": "FINRA_BrokerCheck",
                "severity": "HIGH",
                "metadata": {},
                "description": "Customer dispute on 4/26/2000. Resolution: Settled."
            }
        ]
    }
    
    # Create a report builder and set the final evaluation
    builder = EvaluationReportBuilder("test_reference_id")
    builder.set_final_evaluation(final_evaluation)
    
    # Build the report and check if final_evaluation has a source field
    report = builder.build()
    if 'source' not in report['final_evaluation']:
        print("✅ final_evaluation does not have a source field")
    else:
        print(f"❌ final_evaluation has a source field: {report['final_evaluation']['source']}")
    
    # Check if the alert in final_evaluation still has its source field
    if 'alerts' in report['final_evaluation'] and report['final_evaluation']['alerts']:
        alert = report['final_evaluation']['alerts'][0]
        if 'source' in alert:
            print(f"✅ Alert in final_evaluation has a source field: {alert['source']}")
        else:
            print("❌ Alert in final_evaluation does not have a source field")
    
    # Print the final evaluation for inspection
    print("\nFinal Evaluation:")
    print(json.dumps(report['final_evaluation'], indent=2))
    
    print("\n" + "-"*80 + "\n")

if __name__ == "__main__":
    test_alert_source_field()
    test_final_evaluation_no_source()
    print("All tests completed.")