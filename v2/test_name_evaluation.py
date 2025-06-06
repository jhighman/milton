#!/usr/bin/env python3
"""
Test script to demonstrate the new name evaluation structure.
This script uses the example data from the original task to show
how the name evaluation works with the new structure.
"""

import json
from evaluation_processor import evaluate_name

def main():
    # Example data from the original task
    expected_name = "Jonathan Bacon"
    fetched_name = "JONATHAN CARSON BACON"
    other_names = ["Jon Carson Bacon"]
    
    print(f"Testing name evaluation with:")
    print(f"  Expected name: {expected_name}")
    print(f"  Fetched name: {fetched_name}")
    print(f"  Other names: {other_names}")
    print("\n" + "="*80 + "\n")
    
    # Evaluate the name
    result, alert = evaluate_name(expected_name, fetched_name, other_names)
    
    # Print the result in a formatted way
    print("NAME EVALUATION RESULT:")
    print(json.dumps({"name_evaluation": result}, indent=2))
    
    # Print alert if present
    if alert:
        print("\nALERT GENERATED:")
        print(json.dumps(alert.to_dict(), indent=2))
    else:
        print("\nNo alerts generated - names matched successfully!")
    
    print("\n" + "="*80 + "\n")
    print("Key features of the new structure:")
    print("1. Preserves original case in all name fields")
    print("2. Provides detailed component scores (first, middle, last)")
    print("3. Includes all matches with their individual scores")
    print("4. Identifies the best match with its score")
    print("5. Clearly indicates compliance status and explanation")

if __name__ == "__main__":
    main()