"""
Test script to verify that:
1. The "compliance" attribute is always a boolean in all evaluation types
2. "compliance_explanation" is used consistently instead of "explanation"
"""

import json
import logging
from evaluation_report_builder import EvaluationReportBuilder
from evaluation_report_director import EvaluationReportDirector
from evaluation_processor import (
    evaluate_employments,
    evaluate_name,
    evaluate_registration_status,
    evaluate_license,
    evaluate_exams,
    evaluate_disclosures,
    evaluate_arbitration,
    evaluate_disciplinary,
    evaluate_regulatory
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_compliance_attribute")

def test_compliance_attribute_consistency():
    """Test that compliance is always a boolean and compliance_explanation is used consistently."""
    print("\n=== Testing compliance attribute consistency ===\n")
    
    # Create a sample claim and extracted info
    claim = {
        "reference_id": "TEST-123",
        "first_name": "John",
        "last_name": "Doe",
        "employee_number": "EN-123456",
        "license_type": "BIA"
    }
    
    extracted_info = {
        "search_evaluation": {
            "source": "FINRA_BROKERCHECK",
            "search_strategy": "search_with_crd_only",
            "compliance": True,
            "compliance_explanation": "Search completed successfully."
        },
        "fetched_name": "John Doe",
        "other_names": [],
        "bc_scope": "active",
        "ia_scope": "active",
        "exams": [
            {
                "examCategory": "Series 7",
                "examName": "General Securities Representative Examination",
                "examTakenDate": "1/31/2020",
                "examScope": "BC"
            },
            {
                "examCategory": "Series 66",
                "examName": "Uniform Combined State Law Examination",
                "examTakenDate": "2/15/2020",
                "examScope": "BC"
            }
        ],
        "employments": [
            {
                "firm_id": "123456",
                "firm_name": "Test Firm",
                "registration_begin_date": "3/1/2020",
                "branch_offices": [],
                "status": "current",
                "type": "registered_firm"
            }
        ],
        "disclosures": [],
        "disciplinary_evaluation": {
            "actions": [],
            "due_diligence": {}
        },
        "arbitration_evaluation": {
            "actions": [],
            "due_diligence": {}
        },
        "regulatory_evaluation": {
            "actions": [],
            "due_diligence": {}
        }
    }
    
    # Build a report
    builder = EvaluationReportBuilder("TEST-123")
    director = EvaluationReportDirector(builder)
    report = director.construct_evaluation_report(claim, extracted_info)
    
    # Check each section for compliance attribute type and explanation field
    sections_to_check = [
        "search_evaluation",
        "status_evaluation",
        "name_evaluation",
        "license_evaluation",
        "exam_evaluation",
        "employment_evaluation",
        "disclosure_review",
        "disciplinary_evaluation",
        "arbitration_review",
        "regulatory_evaluation",
        "final_evaluation"
    ]
    
    all_passed = True
    
    for section in sections_to_check:
        if section not in report:
            print(f"❌ Section {section} not found in report")
            all_passed = False
            continue
            
        section_data = report[section]
        
        # Check if compliance is a boolean
        if "compliance" not in section_data:
            print(f"❌ Section {section} is missing 'compliance' attribute")
            all_passed = False
        elif not isinstance(section_data["compliance"], bool):
            print(f"❌ Section {section} has 'compliance' as {type(section_data['compliance']).__name__}, expected bool")
            all_passed = False
        else:
            print(f"✅ Section {section} has 'compliance' as boolean: {section_data['compliance']}")
        
        # Check if compliance_explanation is used (not explanation)
        if "explanation" in section_data:
            print(f"❌ Section {section} uses 'explanation' instead of 'compliance_explanation'")
            all_passed = False
        elif "compliance_explanation" not in section_data and section != "claim":
            print(f"❌ Section {section} is missing 'compliance_explanation' attribute")
            all_passed = False
        else:
            print(f"✅ Section {section} correctly uses 'compliance_explanation'")
    
    # Print the full report for inspection
    print("\nFull report for inspection:")
    print(json.dumps(report, indent=2))
    
    return all_passed

def test_direct_evaluation_functions():
    """Test that the evaluation functions directly return compliance as boolean and use compliance_explanation."""
    print("\n=== Testing direct evaluation functions ===\n")
    
    all_passed = True
    
    # Test evaluate_name
    print("1. Testing evaluate_name:")
    name_result, _ = evaluate_name(
        expected_name="John Smith",
        fetched_name="John Smith",
        other_names=[]
    )
    if not isinstance(name_result.get("compliance"), bool):
        print(f"❌ evaluate_name returns 'compliance' as {type(name_result.get('compliance')).__name__}, expected bool")
        all_passed = False
    else:
        print(f"✅ evaluate_name returns 'compliance' as boolean: {name_result.get('compliance')}")
    
    if "explanation" in name_result:
        print("❌ evaluate_name uses 'explanation' instead of 'compliance_explanation'")
        all_passed = False
    elif "compliance_explanation" not in name_result:
        print("❌ evaluate_name is missing 'compliance_explanation'")
        all_passed = False
    else:
        print("✅ evaluate_name correctly uses 'compliance_explanation'")
    
    # Test evaluate_employments
    print("\n2. Testing evaluate_employments:")
    employment_result, _, _ = evaluate_employments(
        employments=[{
            "firm_name": "Test Firm",
            "registration_begin_date": "2020-01-01"
        }],
        name="John Smith"
    )
    if not isinstance(employment_result.get("compliance"), bool):
        print(f"❌ evaluate_employments returns 'compliance' as {type(employment_result.get('compliance')).__name__}, expected bool")
        all_passed = False
    else:
        print(f"✅ evaluate_employments returns 'compliance' as boolean: {employment_result.get('compliance')}")
    
    if "explanation" in employment_result:
        print("❌ evaluate_employments uses 'explanation' instead of 'compliance_explanation'")
        all_passed = False
    elif "compliance_explanation" not in employment_result:
        print("❌ evaluate_employments is missing 'compliance_explanation'")
        all_passed = False
    else:
        print("✅ evaluate_employments correctly uses 'compliance_explanation'")
    
    # Test evaluate_registration_status
    print("\n3. Testing evaluate_registration_status:")
    status_result, _ = evaluate_registration_status(
        individual_info={"bcScope": "active", "iaScope": "active"}
    )
    if not isinstance(status_result.get("compliance"), bool):
        print(f"❌ evaluate_registration_status returns 'compliance' as {type(status_result.get('compliance')).__name__}, expected bool")
        all_passed = False
    else:
        print(f"✅ evaluate_registration_status returns 'compliance' as boolean: {status_result.get('compliance')}")
    
    if "explanation" in status_result:
        print("❌ evaluate_registration_status uses 'explanation' instead of 'compliance_explanation'")
        all_passed = False
    elif "compliance_explanation" not in status_result:
        print("❌ evaluate_registration_status is missing 'compliance_explanation'")
        all_passed = False
    else:
        print("✅ evaluate_registration_status correctly uses 'compliance_explanation'")
    
    return all_passed

if __name__ == "__main__":
    print("Testing compliance attribute consistency and naming...")
    report_test_passed = test_compliance_attribute_consistency()
    functions_test_passed = test_direct_evaluation_functions()
    
    if report_test_passed and functions_test_passed:
        print("\n✅ All tests passed! The fixes have been successfully implemented.")
    else:
        print("\n❌ Some tests failed. Please check the output above for details.")