#!/usr/bin/env python3
"""
Simple test script to reproduce the 'NoneType' object has no attribute issue
seen in production with the facade object.
"""

import sys
import traceback

print("Starting simple test script")

try:
    # Import the necessary modules
    from business import process_claim
    print("Successfully imported process_claim")
    
    # Create a test claim similar to the one in the error log
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
    
    print("Calling process_claim with None facade")
    # Intentionally use None as facade to reproduce the error
    result = process_claim(
        claim=claim,
        facade=None,  # This should cause the same error as in production
        employee_number=employee_number,
        skip_disciplinary=False,
        skip_arbitration=False,
        skip_regulatory=True
    )
    print(f"Result: {result}")
    
except Exception as e:
    print(f"Error: {str(e)}")
    print("Traceback:")
    traceback.print_exc()

print("Test script completed")