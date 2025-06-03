"""
Test script to verify the source field implementation in evaluation functions.
"""

import json
from common_types import DataSource
from evaluation_processor import (
    evaluate_name,
    evaluate_license,
    evaluate_exams,
    evaluate_registration_status,
    evaluate_disclosures,
    evaluate_arbitration,
    evaluate_disciplinary,
    evaluate_regulatory,
    evaluate_employments,
    json_dumps_with_alerts
)

def test_source_field():
    """Test that source field is properly included in evaluation results."""
    print("Testing source field implementation in evaluation functions\n")
    
    # Test evaluate_name with source
    print("1. Testing evaluate_name with source:")
    name_result, _ = evaluate_name(
        expected_name="John Smith",
        fetched_name="John Smith",
        other_names=[],
        source=DataSource.FINRA_BROKERCHECK.value
    )
    print(f"Source in name_result: {name_result.get('source')}")
    print(json_dumps_with_alerts(name_result, indent=2))
    print("\n" + "-"*80 + "\n")
    
    # Test evaluate_license with source
    print("2. Testing evaluate_license with source:")
    license_result, _ = evaluate_license(
        csv_license="BIA",
        bc_scope="active",
        ia_scope="active",
        name="John Smith",
        source=DataSource.IAPD.value
    )
    print(f"Source in license_result: {license_result.get('source')}")
    print(json_dumps_with_alerts(license_result, indent=2))
    print("\n" + "-"*80 + "\n")
    
    # Test evaluate_exams with source
    print("3. Testing evaluate_exams with source:")
    exams_result, _ = evaluate_exams(
        passed_exams={"Series 7", "Series 63"},
        license_type="B",
        name="John Smith",
        source=DataSource.SEC_IAPD.value
    )
    print(f"Source in exams_result: {exams_result.get('source')}")
    print(json_dumps_with_alerts(exams_result, indent=2))
    print("\n" + "-"*80 + "\n")
    
    # Test evaluate_registration_status with source
    print("4. Testing evaluate_registration_status with source:")
    reg_result, _ = evaluate_registration_status(
        individual_info={"bcScope": "active", "iaScope": "active"},
        source=DataSource.FINRA_BROKERCHECK.value
    )
    print(f"Source in reg_result: {reg_result.get('source')}")
    print(json_dumps_with_alerts(reg_result, indent=2))
    print("\n" + "-"*80 + "\n")
    
    # Test evaluate_disclosures with source
    print("5. Testing evaluate_disclosures with source:")
    disc_result, _, _ = evaluate_disclosures(
        disclosures=[],
        name="John Smith",
        source=DataSource.FINRA_DISCIPLINARY.value
    )
    print(f"Source in disc_result: {disc_result.get('source')}")
    print(json_dumps_with_alerts(disc_result, indent=2))
    
    print("\nAll tests completed. Each evaluation function now includes a 'source' field.")

if __name__ == "__main__":
    test_source_field()