import os
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
import argparse
from contextlib import contextmanager

"""
FINRA Disciplinary Actions Online Search Tool

This script processes JSON files to search for disciplinary actions on FINRA's website.
Each JSON file in the 'drop' directory should have the following structure:

{
    "claim": {
        "first_name": "John",
        "last_name": "Doe"
    },
    "alternate_names": [
        ["Jane", "Doe"],
        ["Johnny", "Doe"]
    ]
}

Required JSON fields:
- claim.first_name: Primary first name to search
- claim.last_name: Primary last name to search
- alternate_names: (Optional) List of alternate name pairs to search

The script will:
1. Search for the primary name
2. Search for all alternate names if provided
3. Save results to the output directory
4. Cache intermediate results in the cache directory
"""

# Constants
RUN_HEADLESS = True  # Set to False to run with the browser visible

# Folder paths
folder_path = './'
input_folder = os.path.join(folder_path, 'drop')
output_folder = os.path.join(folder_path, 'output')
cache_folder = os.path.join(folder_path, 'cache')

def create_driver(headless=True):
    """Create and configure a Chrome WebDriver"""
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
    
    service = ChromeService()
    return webdriver.Chrome(service=service, options=options)

@contextmanager
def get_driver(headless=True):
    """Context manager for WebDriver"""
    driver = create_driver(headless)
    try:
        yield driver
    finally:
        driver.quit()

def search_individual(driver, first_name, last_name):
    """
    Search for an individual's disciplinary actions
    
    Args:
        driver: Selenium WebDriver instance
        first_name (str): First name to search
        last_name (str): Last name to search
    
    Returns:
        dict: Search results
    """
    if not first_name or not last_name:
        raise ValueError("Both first_name and last_name are required")
    
    return process_finra_search(driver, first_name, last_name)

def search_with_alternates(driver, first_name, last_name, alternate_names=None):
    """
    Search for an individual including alternate names
    
    Args:
        driver: Selenium WebDriver instance
        first_name (str): Primary first name
        last_name (str): Primary last name
        alternate_names (list): Optional list of [first_name, last_name] pairs
    
    Returns:
        list: Results for primary and alternate names
    """
    all_names = [(first_name, last_name)] + (alternate_names or [])
    return [search_individual(driver, fname, lname) for fname, lname in all_names]

def process_finra_search(driver, first_name, last_name):
    """
    Perform a FINRA disciplinary actions search and extract results.
    """
    print(f"Step 1: Performing search for {first_name} {last_name}...")
    try:
        # Navigate to FINRA search page
        print("Step 2: Navigating to the FINRA disciplinary actions page...")
        driver.get("https://www.finra.org/rules-guidance/oversight-enforcement/finra-disciplinary-actions-online")

        # Input search details
        print("Step 3: Filling in search fields...")
        search_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "edit-individuals"))
        )
        search_input.send_keys(f"{first_name} {last_name}")

        # Agree to terms and submit
        print("Step 5: Agreeing to 'Terms of Service' and submitting the form...")
        try:
            terms_checkbox = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "edit-terms-of-service"))
            )
            driver.execute_script("arguments[0].click();", terms_checkbox)
            print("Step 5.1: 'Terms of Service' checkbox clicked.")

            submit_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "edit-actions-submit"))
            )
            driver.execute_script("arguments[0].click();", submit_button)
            print("Step 5.2: Submit button clicked.")
        except Exception as e:
            print(f"Error during form submission: {e}")
            return {"error": str(e)}

        # Wait for either "No Results Found" or the results table
        print("Step 6: Waiting for results...")
        try:
            no_results = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//em[text()='No Results Found']"))
            )
            print("Step 7: No results found for this search.")
            return {"result": "No Results Found"}
        except:
            # If "No Results Found" isn't found, look for the table
            print("Step 6.1: Looking for results table...")
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.table-responsive.col > table.views-table.views-view-table.cols-5"))
            )
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup.find("table", class_="table views-table views-view-table cols-5")

            if not table:
                print("Step 7: No results table found.")
                return {"result": "No Results Found"}

            # Parse table data
            print("Step 7: Extracting disciplinary action data from the table...")
            headers = ["Case ID", "Case Summary", "Document Type", "Firms/Individuals", "Action Date"]
            result_rows = []
            for tr in table.find_all("tr")[1:]:  # Skip header row
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                result_rows.append(dict(zip(headers, cells)))

            print(f"Step 8: Extracted {len(result_rows)} disciplinary actions.")
            return {"result": result_rows}

    except Exception as e:
        print(f"Step 8: Error during the search: {e}")
        return {"error": str(e)}


def validate_json_data(data, file_path):
    """
    Validate that the JSON data has the required fields.
    Returns (is_valid, error_message)
    """
    if not isinstance(data, dict):
        return False, f"Invalid JSON structure in {file_path}: expected object, got {type(data)}"
    
    # Check for claim object
    if "claim" not in data:
        return False, f"Missing 'claim' object in {file_path}"
    
    claim = data.get("claim", {})
    if not isinstance(claim, dict):
        return False, f"Invalid 'claim' structure in {file_path}: expected object, got {type(claim)}"
    
    # Check for required name fields
    if not claim.get("first_name"):
        return False, f"Missing or empty 'first_name' in claim: {file_path}"
    if not claim.get("last_name"):
        return False, f"Missing or empty 'last_name' in claim: {file_path}"
    
    # Validate alternate_names format if present
    if "alternate_names" in data:
        alt_names = data["alternate_names"]
        if not isinstance(alt_names, list):
            return False, f"Invalid 'alternate_names' structure: expected list in {file_path}"
        
        for i, name_pair in enumerate(alt_names):
            if not isinstance(name_pair, list) or len(name_pair) != 2:
                return False, f"Invalid alternate name pair at index {i} in {file_path}"
            if not all(isinstance(n, str) and n.strip() for n in name_pair):
                return False, f"Invalid or empty name in alternate name pair at index {i} in {file_path}"
    
    return True, ""

def batch_process_folder(input_dir='drop', output_dir='output', cache_dir='cache', headless=True):
    """
    Process all JSON files in the input directory
    """
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(cache_dir, exist_ok=True)
    
    stats = {
        'total_individuals': 0,
        'disciplinary_actions': 0,
        'no_results': 0,
        'errors': 0
    }
    
    with get_driver(headless) as driver:
        json_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]
        if not json_files:
            print("No JSON files found in the input folder.")
            return stats
            
        for json_file in json_files:
            file_path = os.path.join(input_dir, json_file)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                is_valid, error = validate_json_data(data, file_path)
                if not is_valid:
                    stats['errors'] += 1
                    print(f"Error: {error}")
                    continue

                claim = data["claim"]
                results = search_with_alternates(
                    driver,
                    claim["first_name"],
                    claim["last_name"],
                    data.get("alternate_names", [])
                )
                
                stats['total_individuals'] += 1
                
                # Update stats and handle results
                handle_search_results(
                    results,
                    claim["first_name"],
                    claim["last_name"],
                    output_dir,
                    cache_dir,
                    stats
                )

            except Exception as e:
                stats['errors'] += 1
                print(f"Error processing file {file_path}: {e}")
    
    print_summary(stats)
    return stats

def handle_search_results(results, first_name, last_name, output_dir, cache_dir, stats):
    """Handle search results including caching and stats"""
    # Cache results
    cache_path = os.path.join(cache_dir, f"{first_name}_{last_name}", "results.json")
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)

    # Update stats
    for result in results:
        if result.get("result") == "No Results Found":
            stats['no_results'] += 1
        elif "error" in result:
            stats['errors'] += 1
        else:
            stats['disciplinary_actions'] += len(result["result"])

    # Save to output if actions found
    if any(r.get("result") != "No Results Found" for r in results):
        output_path = os.path.join(output_dir, f"{first_name}_{last_name}", "results.json")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=4)

def print_summary(stats):
    """Print processing summary"""
    print("\n--- Summary ---")
    print(f"Total Individuals Processed: {stats['total_individuals']}")
    print(f"Total Disciplinary Actions Found: {stats['disciplinary_actions']}")
    print(f"Total No Results: {stats['no_results']}")
    print(f"Total Errors: {stats['errors']}")

def main():
    parser = argparse.ArgumentParser(description='Search FINRA disciplinary actions')
    parser.add_argument('--first-name', help='First name to search')
    parser.add_argument('--last-name', help='Last name to search')
    parser.add_argument('--batch', action='store_true', help='Process all JSON files in drop folder')
    parser.add_argument('--headless', action='store_true', default=True, help='Run in headless mode')
    
    args = parser.parse_args()
    
    if args.first_name and args.last_name:
        with get_driver(args.headless) as driver:
            result = search_individual(driver, args.first_name, args.last_name)
            print(json.dumps(result, indent=2))
    else:
        batch_process_folder(headless=args.headless)

if __name__ == "__main__":
    main()
