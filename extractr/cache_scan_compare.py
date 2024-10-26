import os
import json
from deepdiff import DeepDiff

# Normalize the JSON string from the content field
def normalize_content_field(content_field):
    try:
        return json.loads(content_field)
    except json.JSONDecodeError:
        return {}

# Fields to ignore from comparison
EXCLUDED_FIELDS = [
    "daysInIndustryCalculatedDateIAPD", 
    "daysInIndustryCalculatedDate", 
    "daysInIndustryIAPD", 
    "daysInIndustry"
]

# Field equivalencies
EQUIVALENT_FIELDS = {
    "disclosures": "iaDisclosures"
}

# Function to handle field equivalencies
def apply_field_equivalencies(content):
    if isinstance(content, dict):
        normalized_content = {}
        for key, value in content.items():
            # Apply field equivalency
            equivalent_key = EQUIVALENT_FIELDS.get(key, key)
            # Recursively apply equivalency to nested structures
            normalized_value = apply_field_equivalencies(value)
            normalized_content[equivalent_key] = normalized_value
        return normalized_content
    elif isinstance(content, list):
        # Recursively apply equivalency to each item in the list
        return [apply_field_equivalencies(item) for item in content]
    else:
        return content

# Function to check if the new value is a truncated version of the old value
def is_truncated(old_value, new_value):
    if isinstance(old_value, dict) and isinstance(new_value, dict):
        for key, new_val in new_value.items():
            old_val = old_value.get(key, None)
            if isinstance(new_val, (dict, list)):
                if not is_truncated(old_val, new_val):
                    return False
            elif old_val != new_val:
                # New value differs from old value, meaning it's not a simple truncation
                return False
        return True
    elif isinstance(old_value, list) and isinstance(new_value, list):
        # Check if each new item is a subset of any item in the old list
        return all(any(is_truncated(old_item, new_item) for old_item in old_value) for new_item in new_value)
    else:
        # For basic data types, simply check equality
        return old_value == new_value

# Function to compare disclosures and break them down at field level
def compare_disclosures(disclosures1, disclosures2):
    differences = []
    for index, (d1, d2) in enumerate(zip(disclosures1, disclosures2)):
        if is_truncated(d1, d2):
            differences.append(f"Disclosure {index} was truncated to a subset (allowed).")
        else:
            disclosure_comparison = DeepDiff(d1, d2, ignore_order=True)
            if disclosure_comparison:
                differences.append(f"Disclosure {index} has differences:")
                if 'values_changed' in disclosure_comparison:
                    for path, change in disclosure_comparison['values_changed'].items():
                        old_value = change['old_value']
                        new_value = change['new_value']
                        differences.append(f"  Field '{path}' changed from '{old_value}' to '{new_value}'.")
                if 'dictionary_item_added' in disclosure_comparison:
                    for path in disclosure_comparison['dictionary_item_added']:
                        differences.append(f"  Field '{path}' was added (new information).")
                if 'dictionary_item_removed' in disclosure_comparison:
                    for path in disclosure_comparison['dictionary_item_removed']:
                        differences.append(f"  Field '{path}' was removed.")
    return differences

# Function to explain the differences in the 'content' field, including detailed disclosure breakdowns
def explain_differences_in_content(old_content, new_content):
    # Use DeepDiff to compare the parsed JSON content field, ignoring specified fields
    content_comparison = DeepDiff(old_content, new_content, ignore_order=True, exclude_paths=EXCLUDED_FIELDS)
    
    explanations = []
    
    # Compare disclosures (or iaDisclosures) separately at field level
    old_disclosures = old_content.get('disclosures', old_content.get('iaDisclosures', []))
    new_disclosures = new_content.get('disclosures', new_content.get('iaDisclosures', []))

    if old_disclosures and new_disclosures:
        disclosure_diffs = compare_disclosures(old_disclosures, new_disclosures)
        explanations.extend(disclosure_diffs)

    # Detecting other value changes outside of disclosures
    if 'values_changed' in content_comparison:
        for path, change in content_comparison['values_changed'].items():
            old_value = change['old_value']
            new_value = change['new_value']
            explanations.append(f"Field '{path}' has changed from '{old_value}' to '{new_value}'.")

    # Detecting added fields
    if 'dictionary_item_added' in content_comparison:
        for path in content_comparison['dictionary_item_added']:
            explanations.append(f"Field '{path}' was added in the new content (new information).")

    # Detecting removed fields
    if 'dictionary_item_removed' in content_comparison:
        for path in content_comparison['dictionary_item_removed']:
            explanations.append(f"Field '{path}' was removed from the old content.")

    return explanations

# Function to compare two JSON files and explain differences, with a focus on disclosures
def compare_files(brokercheck_file, sec_file, result_file):
    with open(brokercheck_file, 'r') as f1, open(sec_file, 'r') as f2:
        json1 = json.load(f1)
        json2 = json.load(f2)

    # Extract and normalize the 'content' field from both JSON objects
    content1 = normalize_content_field(json1['hits']['hits'][0]['_source'].get('content', ''))
    content2 = normalize_content_field(json2['hits']['hits'][0]['_source'].get('iacontent', ''))

    # Apply field equivalencies to both contents
    content1 = apply_field_equivalencies(content1)
    content2 = apply_field_equivalencies(content2)

    # Compare the 'content' fields and provide explanations
    content_explanations = explain_differences_in_content(content1, content2)

    if content_explanations:
        result_file.write(f"Differences between {brokercheck_file} and {sec_file} in the 'content' field:\n")
        for explanation in content_explanations:
            result_file.write(f"{explanation}\n")
        result_file.write("\n")
    else:
        result_file.write(f"{brokercheck_file} and {sec_file} are identical in the 'content' field.\n\n")

# Function to handle the comparison for all matching files
def handle_comparisons(folder_path, output_file):
    files = os.listdir(folder_path)
    
    with open(output_file, 'w') as result_file:
        for file in files:
            if "brokercheck" in file and "detailed_info" in file:
                sec_file = file.replace("brokercheck", "sec")
                brokercheck_file_path = os.path.join(folder_path, file)
                sec_file_path = os.path.join(folder_path, sec_file)

                if os.path.exists(sec_file_path):
                    compare_files(brokercheck_file_path, sec_file_path, result_file)
                else:
                    crd_number = file.split('_')[1]
                    result_file.write(f"No SEC filing for this CRD: {crd_number}\n\n")

# Example usage:
# Set your folder path where JSON files are stored and output file
folder_path = "./cache"  # Replace with your actual folder path
output_file = "compare_results.txt"

# Run the comparison for all matching files in the folder
handle_comparisons(folder_path, output_file)
