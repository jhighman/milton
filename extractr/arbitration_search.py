import os
import json

# Define folder paths
folder_path = './'
input_folder = os.path.join(folder_path, 'drop2')
output_folder = os.path.join(folder_path, 'output2')
archive_folder = os.path.join(folder_path, 'archive2')
cache_folder = os.path.join(folder_path, 'cache2')

def scan_folder_for_json(folder_path):
    """Scans the folder for .json files."""
    return [f for f in os.listdir(folder_path) if f.endswith('.json')]

def process_json_file(file_path):
    """Processes a JSON file, extracts data, and writes to cache if valid."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # Check for 'claim' object
        claim = data.get("claim")
        if not claim or not isinstance(claim, dict):
            print(f"'claim' object not found in file: {file_path}")
            return

        # Extract 'employee_number' and 'name'
        employee_number = claim.get("employee_number")
        name = claim.get("name")
        
        # Skip records without valid 'employee_number' or 'name'
        if not employee_number or not name:
            print(f"Missing 'employee_number' or 'name' in file: {file_path}")
            return

        # Create directory in the cache folder named by 'employee_number'
        employee_dir = os.path.join(cache_folder, employee_number)
        os.makedirs(employee_dir, exist_ok=True)

        # Write extracted 'name' to as_1.json in the created directory
        output_file = os.path.join(employee_dir, "as_1.json")
        with open(output_file, 'w', encoding='utf-8') as outfile:
            json.dump({"name": name}, outfile, indent=4)
        
        print(f"Processed and cached: {employee_number}")
    except (json.JSONDecodeError, FileNotFoundError, KeyError) as e:
        print(f"Error processing file {file_path}: {e}")

def main():
    # Ensure required directories exist
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(archive_folder, exist_ok=True)
    os.makedirs(input_folder, exist_ok=True)
    os.makedirs(cache_folder, exist_ok=True)

    # Scan the input folder for JSON files
    json_files = scan_folder_for_json(input_folder)
    if not json_files:
        print("No .json files found in the input folder.")
        return
    
    # Process each JSON file
    for json_file in json_files:
        file_path = os.path.join(input_folder, json_file)
        process_json_file(file_path)

if __name__ == "__main__":
    main()
