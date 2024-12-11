import os
import json

# Define the cache folder path
cache_folder = './cache2'

# Global counters and storage for results
disciplinary_action_files = []
no_disciplinary_action_files = []
error_files = []
disciplinary_action_count = 0

def scan_cache_folder(folder_path):
    """Scans the cache folder for JSON files and identifies files with disciplinary actions."""
    global disciplinary_action_files, no_disciplinary_action_files, error_files, disciplinary_action_count

    # Ensure the folder exists
    if not os.path.exists(folder_path):
        print(f"Cache folder '{folder_path}' does not exist.")
        return

    # Get all JSON files in the cache folder
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)

                try:
                    # Read the JSON file
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    # Check for disciplinary actions
                    result = data.get("result", [])
                    if isinstance(result, list) and len(result) > 0:
                        disciplinary_action_files.append(file_path)
                        disciplinary_action_count += len(result)
                    elif result == "No Results Found":
                        no_disciplinary_action_files.append(file_path)
                    else:
                        error_files.append(file_path)

                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Error reading file {file_path}: {e}")
                    error_files.append(file_path)

def summarize_results():
    """Prints a summary of the disciplinary action scan."""
    print("\n--- Disciplinary Action Scan Summary ---")
    print(f"Total Files with Disciplinary Actions: {len(disciplinary_action_files)}")
    print(f"Total Disciplinary Actions Found: {disciplinary_action_count}")
    print(f"Total Files with No Disciplinary Actions: {len(no_disciplinary_action_files)}")
    print(f"Total Files with Errors: {len(error_files)}")

    if disciplinary_action_files:
        print("\nFiles with Disciplinary Actions:")
        for file in disciplinary_action_files:
            print(f"- {file}")



def main():
    # Scan the cache folder
    print(f"Scanning cache folder: {cache_folder}")
    scan_cache_folder(cache_folder)

    # Summarize results
    summarize_results()

if __name__ == "__main__":
    main()
