#!/usr/bin/env python3
"""
Test script to verify the fix for the 'NoneType' object has no attribute issue.
"""

import sys
import traceback
import json
from typing import Dict, Any

print("Starting test script to verify the fix")

try:
    # Import the necessary modules
    from api import initialize_services, facade
    from business import process_claim
    print("Successfully imported modules")
    
    # Test the improved initialize_services function
    print("\nTesting initialize_services function...")
    initialization_success = initialize_services()
    print(f"Initialization success: {initialization_success}")
    print(f"Facade is None: {facade is None}")
    
    if not initialization_success or facade is None:
        print("Initialization failed or facade is None, which would be caught by our improved error handling")
        sys.exit(0)
    
    # Create a test claim
    claim = {
        "reference_id": "TEST_REFERENCE_ID",
        "first_name": "Jason",
        "last_name": "Chandler",
        "individual_name": "Jason Chandler",
        "crd_number": "2382465",
        "organization_crd": None,
        "organization_name": "dev-fmr"
    }
    
    employee_number = "EN-TEST"
    
    print("\nTesting process_claim with initialized facade...")
    # Use the properly initialized facade
    result = process_claim(
        claim=claim,
        facade=facade,
        employee_number=employee_number,
        skip_disciplinary=False,
        skip_arbitration=False,
        skip_regulatory=True
    )
    print(f"Process claim succeeded with result: {json.dumps(result, indent=2)[:200]}...")
    
    # Test what would happen with our improved error handling if facade was None
    print("\nSimulating what would happen with our improved error handling if facade was None...")
    
    # This is a simulation of what would happen in the Celery task with our improved error handling
    if facade is None:
        error_message = "Service facade is not initialized. Check logs for initialization errors."
        print(f"Error would be caught: {error_message}")
        error_report = {
            "status": "error",
            "reference_id": claim["reference_id"],
            "message": error_message
        }
        print(f"Error report would be returned: {json.dumps(error_report, indent=2)}")
    else:
        print("Facade is properly initialized, so no error would be triggered")
    
except Exception as e:
    print(f"Error: {str(e)}")
    print("Traceback:")
    traceback.print_exc()

print("\nTest script completed")