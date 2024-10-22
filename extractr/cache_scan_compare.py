import os
import json
from deepdiff import DeepDiff

# Path to the folder containing the JSON files
folder_path = "./cache"
output_file = "compare_results.txt"

# Alias mapping for substituting 'iacontent' with 'content'
alias_mapping = {
    "iacontent": "content"
}

# Function to normalize keys based on alias mapping
def normalize_keys(obj, alias_mapping):
    if isinstance(obj, dict):
        normalized_obj = {}
        for key, value in obj.items():
            # Apply alias if found, otherwise keep the original key
            new_key = alias_mapping.get(key, key)
            # Recursively normalize values that are dicts or lists
            normalized_obj[new_key] = normalize_keys(value, alias_mapping)
        return normalized_obj
    elif isinstance(obj, list):
        # Recursively normalize each item in the list
        return [normalize_keys(item, alias_mapping) for item in obj]
    else:
        # Base case: if it's neither a dict nor a list, return the value as is
        return obj

# Function to compare two JSON files and write the results to a file
def compare_files(brokercheck_file, sec_file, result_file):
    with open(brokercheck_file, 'r') as f1, open(sec_file, 'r') as f2:
        json1 = json.load(f1)
        json2 = json.load(f2)

    # Normalize the JSON objects
    normalized_json1 = normalize_keys(json1, alias_mapping)
    normalized_json2 = normalize_keys(json2, alias_mapping)

    # Compare the normalized objects
    comparison = DeepDiff(normalized_json1, normalized_json2, ignore_order=True)

    if comparison:
        result_file.write(f"Differences between {brokercheck_file} and {sec_file}:\n")
        result_file.write(f"{comparison}\n\n")
    else:
        result_file.write(f"{brokercheck_file} and {sec_file} are identical.\n\n")

# Open the results file
with open(output_file, 'w') as result_file:

    # Get all files in the folder
    files = os.listdir(folder_path)

    # Find matching pairs of brokercheck and sec files
    for file in files:
        if "brokercheck" in file and "detailed_info" in file:
            # Get the corresponding SEC file by replacing 'brokercheck' with 'sec'
            sec_file = file.replace("brokercheck", "sec")
            
            # Paths to the brokercheck and sec files
            brokercheck_file_path = os.path.join(folder_path, file)
            sec_file_path = os.path.join(folder_path, sec_file)

            # Check if the SEC file exists
            if os.path.exists(sec_file_path):
                # Compare the files
                compare_files(brokercheck_file_path, sec_file_path, result_file)
            else:
                # Extract the CRD number from the filename
                crd_number = file.split('_')[1]
                result_file.write(f"No SEC filing for this CRD: {crd_number}\n\n")
