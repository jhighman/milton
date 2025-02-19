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

# Chrome options for WebDriver
chrome_options = Options()
if RUN_HEADLESS:
    chrome_options.add_argument("--headless")  # Run headless
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")

# Initialize Chrome service
service = ChromeService()

# Global counters
total_individuals_count = 0
disciplinary_action_count = 0
no_results_count = 0
errors_count = 0

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
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "edit-individuals"))
        ).send_keys(f"{first_name} {last_name}")

        # Select 'All Document Types'
        print("Step 4: Selecting 'All Document Types' from the dropdown...")
        dropdown = Select(driver.find_element(By.ID, "edit-document-type"))
        dropdown.select_by_visible_text("All Document Types")

        # Agree to terms and submit
        print("Step 5: Agreeing to 'Terms of Service' and submitting the form...")
        try:
            terms_checkbox = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "edit-terms-of-service"))
            )
            terms_checkbox.click()
            print("Step 5.1: 'Terms of Service' checkbox clicked.")
        except Exception:
            print("Step 5.1: Direct click failed. Using JavaScript to select the checkbox.")
            driver.execute_script("arguments[0].click();", terms_checkbox)

        try:
            submit_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "edit-actions-submit"))
            )
            submit_button.click()
            print("Step 5.2: Submit button clicked.")
        except Exception:
            print("Step 5.2: Submit button click failed. Using JavaScript to submit the form.")
            driver.execute_script("arguments[0].click();", submit_button)

        # Wait for results table to load
        print("Step 6: Waiting for the results table to load...")
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.table-responsive.col > table.views-table.views-view-table.cols-5"))
        )
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find("table", class_="table views-table views-view-table cols-5")

        if not table:
            print("Step 7: No results found for this search.")
            return {"result": "No Results Found"}

        # Parse table data
        print("Step 7: Extracting disciplinary action data from the table...")
        headers = ["Case ID", "Case Summary", "Document Type", "Firms/Individuals", "Action Date"]
        rows = []
        for tr in table.find_all("tr")[1:]:  # Skip the header row
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            rows.append(dict(zip(headers, cells)))

        print(f"Step 8: Extracted {len(rows)} disciplinary actions.")
        return {"result": rows}

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

def process_json_file(driver, file_path):
    """
    Process a single JSON file for disciplinary actions.
    """
    global total_individuals_count, disciplinary_action_count, no_results_count, errors_count

    try:
        print(f"\nProcessing file: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError as e:
                errors_count += 1
                print(f"Error: Invalid JSON format in {file_path}: {str(e)}")
                return
        
        # Validate JSON structure
        is_valid, error_message = validate_json_data(data, file_path)
        if not is_valid:
            errors_count += 1
            print(f"Error: {error_message}")
            return

        claim = data["claim"]
        first_name = claim["first_name"]
        last_name = claim["last_name"]
        alternate_names = data.get("alternate_names", [])

        total_individuals_count += 1
        all_names = [(first_name, last_name)] + alternate_names

        results = []
        for idx, (fname, lname) in enumerate(all_names, start=1):
            print(f"\n--- Performing search {idx} for: {fname} {lname} ---")
            result = process_finra_search(driver, fname, lname)
            results.append(result)

            # Cache each result
            employee_dir = os.path.join(cache_folder, f"{first_name}_{last_name}")
            os.makedirs(employee_dir, exist_ok=True)
            with open(os.path.join(employee_dir, f"result_{idx}.json"), 'w', encoding='utf-8') as cache_file:
                json.dump(result, cache_file, indent=4)

            # Count actions or no results
            if result.get("result") == "No Results Found":
                no_results_count += 1
            elif "error" in result:
                errors_count += 1
            else:
                disciplinary_action_count += len(result["result"])

        # Output results if disciplinary actions found
        if any(r.get("result") != "No Results Found" for r in results):
            print("Step 9: Writing results to the output folder...")
            output_dir = os.path.join(output_folder, f"{first_name}_{last_name}")
            os.makedirs(output_dir, exist_ok=True)
            with open(os.path.join(output_dir, "results.json"), 'w', encoding='utf-8') as output_file:
                json.dump(results, output_file, indent=4)

    except Exception as e:
        errors_count += 1
        print(f"Error processing file {file_path}: {e}")


def summarize_results():
    """
    Print a summary of the processing results.
    """
    print("\n--- Summary ---")
    print(f"Total Individuals Processed: {total_individuals_count}")
    print(f"Total Disciplinary Actions Found: {disciplinary_action_count}")
    print(f"Total No Results: {no_results_count}")
    print(f"Total Errors: {errors_count}")


class FinraSearcher:
    """
    Interface for searching FINRA disciplinary actions.
    """
    def __init__(self, headless=True):
        """Initialize the searcher with browser options"""
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--window-size=1920,1080")
        self.chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
        
        self.service = ChromeService()
        self.driver = None
        
    def __enter__(self):
        """Context manager entry"""
        self.driver = webdriver.Chrome(service=self.service, options=self.chrome_options)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.driver:
            self.driver.quit()
            
    def search_individual(self, first_name, last_name, alternate_names=None, cache_dir=None):
        """
        Search for disciplinary actions for an individual.
        
        Args:
            first_name (str): Primary first name
            last_name (str): Primary last name
            alternate_names (list): Optional list of [first_name, last_name] pairs
            cache_dir (str): Optional directory to cache results
            
        Returns:
            dict: Search results including primary and alternate name searches
        """
        if not self.driver:
            raise RuntimeError("Searcher not initialized. Use with context manager.")
            
        if not first_name or not last_name:
            raise ValueError("Both first_name and last_name are required")
            
        alternate_names = alternate_names or []
        all_names = [(first_name, last_name)] + alternate_names
        results = []
        
        for idx, (fname, lname) in enumerate(all_names, start=1):
            print(f"\n--- Performing search {idx} for: {fname} {lname} ---")
            result = process_finra_search(self.driver, fname, lname)
            results.append(result)
            
            # Cache result if directory provided
            if cache_dir:
                employee_dir = os.path.join(cache_dir, f"{first_name}_{last_name}")
                os.makedirs(employee_dir, exist_ok=True)
                with open(os.path.join(employee_dir, f"result_{idx}.json"), 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=4)
                    
        return results

def batch_process_folder(input_dir='drop', output_dir='output', cache_dir='cache', headless=True):
    """
    Process all JSON files in the input directory.
    
    This maintains the original batch processing functionality.
    """
    global total_individuals_count, disciplinary_action_count, no_results_count, errors_count
    
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(cache_dir, exist_ok=True)
    
    with FinraSearcher(headless=headless) as searcher:
        json_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]
        if not json_files:
            print("No JSON files found in the input folder.")
            return
            
        for json_file in json_files:
            file_path = os.path.join(input_dir, json_file)
            process_json_file(searcher.driver, file_path)
            
        summarize_results()

# Example usage:
if __name__ == "__main__":
    # Batch processing (original functionality)
    batch_process_folder()
