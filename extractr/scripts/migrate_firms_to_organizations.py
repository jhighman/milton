"""
Converts old firms.json format to new organizationsCrd.json format.
"""
import json
import os

def migrate_firms_data():
    # Read old format
    with open('cache/firms.json', 'r') as f:
        old_data = json.load(f)
    
    # Convert to new format
    new_records = []
    for firm in old_data:
        new_record = {
            "entityName": firm["OrganizationName"],
            "organizationCRD": firm["CRD"],
            "normalizedName": firm["OrganizationName"].lower().replace(" ", ""),
            # Add any additional fields needed
        }
        new_records.append(new_record)
    
    # Write in JSONL format
    os.makedirs('input', exist_ok=True)
    with open('input/organizationsCrd.json', 'w') as f:
        for record in new_records:
            f.write(json.dumps(record) + '\n') 