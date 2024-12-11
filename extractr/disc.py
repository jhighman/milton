import os
import json
import requests
from bs4 import BeautifulSoup

# Define folder paths
folder_path = './'
input_folder = os.path.join(folder_path, 'drop2')
output_folder = os.path.join(folder_path, 'output2')
archive_folder = os.path.join(folder_path, 'archive2')
cache_folder = os.path.join(folder_path, 'cache2')

# Global counters for summary
no_disciplinary_count = 0
disciplinary_alerts_count = 0
skipped_files_count = 0
individuals_searched_count = 0
total_searches_count = 0
skipped_files = []  # List of skipped files

def scan_folder_for_json(folder_path):
    """Scans the folder for .json files."""
    return [f for f in os.listdir(folder_path) if f.endswith('.json')]

def generate_search_url(name):
    """Generates the search URL using the provided name."""
    base_url = ("https://www.finra.org/rules-guidance/oversight-enforcement/finra-disciplinary-actions"
                "?search={}&firms=&individuals=&field_fda_case_id_txt=&"
                "field_core_official_dt%5Bmin%5D=&field_core_official_dt%5Bmax%5D=&field_fda_document_type_tax=All")
    return base_url.format(name)

def fetch_and_parse(input_data):
    """
    Fetches HTML from the given URL and parses it for disciplinary action data or a 'No Results' message.

    Parameters:
        input_data (dict): Input object containing "name" and "search" URL.

    Returns:
        dict: Extracted data or a "No Results Found" message.
    """
    global no_disciplinary_count, disciplinary_alerts_count, total_searches_count

    search_url = input_data.get("search")
    name = input_data.get("name", "Unknown")

    try:
        # Increment total searches
        total_searches_count += 1

        # Perform the web request
        response = requests.get(search_url)
        response.raise_for_status()
        html_content = response.text

        # Parse the HTML content
        soup = BeautifulSoup(html_content, 'html.parser')

        # Locate table rows
        table = soup.find("table", class_="views-table")
        if not table:
            no_disciplinary_count += 1
            return {"name": name, "result": "No Results Found"}

        rows = table.find_all("tr")[1:]  # Skip header row
        if not rows:
            no_disciplinary_count += 1
            return {"name": name, "result": "No Results Found"}

        disciplinary_alerts_count += 1
        data = []
        for row in rows:
            cells = row.find_all("td")
            case_id = cells[0].text.strip() if len(cells) > 0 else "N/A"
            case_summary = cells[1].text.strip() if len(cells) > 1 else "N/A"
            document_type = cells[2].text.strip() if len(cells) > 2 else "N/A"
            firms_individuals = cells[3].text.strip() if len(cells) > 3 else "N/A"
            action_date = cells[4].text.strip() if len(cells) > 4 else "N/A"

            data.append({
                "Case ID": case_id,
                "Case Summary": case_summary,
                "Document Type": document_type,
                "Firms/Individuals": firms_individuals,
                "Action Date": action_date
            })

        return {"name": name, "result": data}

    except requests.exceptions.RequestException as e:
        return {"name": name, "error": str(e)}

def process_json_file(file_path):
    """Processes a JSON file, extracts data, and writes to cache if valid."""
    global skipped_files_count, skipped_files, individuals_searched_count

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # Check for 'claim' object
        claim = data.get("claim")
        if not claim or not isinstance(claim, dict):
            print(f"'claim' object not found in file: {file_path}")
            skipped_files_count += 1
            skipped_files.append(file_path)
            return

        # Extract 'employee_number' and 'name'
        employee_number = claim.get("employee_number")
        name = claim.get("name")
        ind_other_names = data.get("search_evaluation", {}).get("individual", {}).get("ind_other_names", [])

        # Skip records without valid 'employee_number' or 'name'
        if not employee_number or not name:
            print(f"Missing 'employee_number' or 'name' in file: {file_path}")
            skipped_files_count += 1
            skipped_files.append(file_path)
            return

        # Increment individuals searched
        individuals_searched_count += 1

        # Create directory in the cache folder named by 'employee_number'
        employee_dir = os.path.join(cache_folder, employee_number)
        os.makedirs(employee_dir, exist_ok=True)

        # Generate search URL for primary name
        all_names = [name] + ind_other_names

        # Perform searches for all name variations
        for idx, search_name in enumerate(all_names, start=1):
            search_url = generate_search_url(search_name)
            search_data = {"name": search_name, "search": search_url}
            result = fetch_and_parse(search_data)

            # Cache result with incremented filename and 'ds_' prefix
            output_file = os.path.join(employee_dir, f"ds_{idx}.json")
            with open(output_file, 'w', encoding='utf-8') as outfile:
                json.dump(result, outfile, indent=4)
        
        print(f"Processed and cached: {employee_number} ({len(all_names)} searches)")

    except (json.JSONDecodeError, FileNotFoundError, KeyError) as e:
        print(f"Error processing file {file_path}: {e}")
        skipped_files_count += 1
        skipped_files.append(file_path)

def summarize_results():
    """Prints the summary of disciplinary action results."""
    if skipped_files:
        print("\nList of Skipped Files:")
        for file in skipped_files:
            print(f"- {file}")  
  
    print("\n--- Summary of Results ---")
    print(f"Individuals Searched: {individuals_searched_count}")
    print(f"Total Searches: {total_searches_count}")
    print(f"No Disciplinary Actions: {no_disciplinary_count}")
    print(f"Disciplinary Alerts: {disciplinary_alerts_count}")
    print(f"Skipped Files: {skipped_files_count}")


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

    # Summarize results
    summarize_results()

if __name__ == "__main__":
    main()
