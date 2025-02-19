import os
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import argparse
from contextlib import contextmanager
import time

"""
SEC Action Lookup Search Tool

This script processes JSON files to search for SEC actions.
Each JSON file in the 'drop2' directory should have the following structure:

{
    "claim": {
        "first_name": "John",
        "last_name": "Doe",
        "employee_number": "12345"
    },
    "search_evaluation": {
        "individual": {
            "ind_other_names": ["Jane Doe", "Johnny Doe"]
        }
    }
}
"""

# Constants and folder paths
folder_path = './'
input_folder = os.path.join(folder_path, 'drop2')
output_folder = os.path.join(folder_path, 'output2')

def create_driver(headless=True):
    """
    Create and configure a Chrome WebDriver
    
    Args:
        headless (bool): Whether to run browser in headless mode (default: True)
                        Can be overridden by setting environment variable SEC_HEADLESS=false
    """
    options = Options()
    # Allow environment variable to override headless setting
    env_headless = os.getenv('SEC_HEADLESS', 'true').lower()
    headless = headless and env_headless != 'false'
    
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

def generate_sec_search_url(first_name, last_name):
    """Generates the search URL using first and last names."""
    base_url = "https://www.sec.gov/litigations/sec-action-look-up?last_name={}&first_name={}"
    return base_url.format(last_name, first_name)

def search_individual(driver, first_name, last_name):
    """
    Search for an individual's SEC actions
    
    Returns one of three possible result structures:
    1. No Results:
        {
            "first_name": str,
            "last_name": str,
            "result": "No Results Found"
        }
    2. Single Result:
        {
            "first_name": str,
            "last_name": str,
            "result": [
                {
                    "Enforcement Action": str,
                    "Date Filed": str,
                    "Documents": list[dict]
                }
            ],
            "total_actions": 1
        }
    3. Multiple Results:
        {
            "first_name": str,
            "last_name": str,
            "result": [
                {
                    "Enforcement Action": str,
                    "Date Filed": str,
                    "Documents": list[dict]
                },
                ...additional actions...
            ],
            "total_actions": n
        }
    """
    print(f"\n🔍 Searching SEC actions for: {first_name} {last_name}")
    search_url = generate_sec_search_url(first_name, last_name)
    print(f"📡 Using URL: {search_url}")
    
    try:
        # Load the search page
        print("⏳ Loading search page...")
        driver.get(search_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "main-content"))
        )
        print("✅ Page loaded successfully")

        # Parse the page content
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')

        # First check: No results case
        labels = soup.find_all("span", class_="views-label")
        if not labels:
            print("ℹ️ No enforcement actions found")
            return {
                "first_name": first_name,
                "last_name": last_name,
                "result": "No Results Found"
            }

        # Process all enforcement actions found
        data = []
        for label in labels:
            if "Enforcement Action:" in label.text:
                action = label.find_next_sibling().get_text(strip=True)
                print(f"⚖️ Action: {action}")
                
                # Get the parent card
                card = label.find_parent("div", class_="card")
                if card:
                    # Find date filed
                    date_label = card.find("span", class_="views-label", string=lambda x: "Date Filed:" in str(x))
                    date_filed = date_label.find_next_sibling().get_text(strip=True) if date_label else "Unknown"
                    print(f"📅 Date: {date_filed}")
                    
                    # Find documents
                    documents = []
                    doc_label = card.find("span", class_="views-label", string=lambda x: "Releases & Documents:" in str(x))
                    if doc_label:
                        doc_list = doc_label.find_next_sibling()
                        if doc_list:
                            for doc in doc_list.find_all("a"):
                                title = doc.get_text(strip=True)
                                link = doc.get("href")
                                if link and title:
                                    documents.append({"title": title, "link": link})
                                    print(f"📎 Document: {title}")
                    
                    data.append({
                        "Enforcement Action": action,
                        "Date Filed": date_filed,
                        "Documents": documents
                    })

        # Second check: No valid actions found
        if not data:
            print("ℹ️ No enforcement actions found")
            return {
                "first_name": first_name,
                "last_name": last_name,
                "result": "No Results Found"
            }

        # Return results based on number of actions found
        result = {
            "first_name": first_name,
            "last_name": last_name,
            "result": data,
            "total_actions": len(data)
        }
        
        print(f"✅ Found {len(data)} enforcement action(s)")
        return result

    except Exception as e:
        print(f"❌ Error during search: {str(e)}")
        return {
            "first_name": first_name,
            "last_name": last_name,
            "error": str(e)
        }

def validate_json_data(data, file_path):
    """
    Validate that the JSON data has the required fields.
    Returns (is_valid, error_message)
    """
    if not isinstance(data, dict):
        return False, f"Invalid JSON structure in {file_path}"
    
    if "claim" not in data:
        return False, f"Missing 'claim' object in {file_path}"
    
    claim = data.get("claim", {})
    if not isinstance(claim, dict):
        return False, f"Invalid 'claim' structure in {file_path}"
    
    if not claim.get("first_name"):
        return False, f"Missing or empty 'first_name' in claim: {file_path}"
    if not claim.get("last_name"):
        return False, f"Missing or empty 'last_name' in claim: {file_path}"
    
    return True, ""

def process_name(first_name, last_name, output_dir=output_folder, headless=None, wait_time=None):
    """
    Search for SEC enforcement actions for a single name.
    """
    claim_data = {
        "first_name": first_name,
        "last_name": last_name
    }
    results, stats = process_claim(claim_data, output_dir, headless, wait_time)
    
    # For single name search, return just the first result
    if isinstance(results, list) and len(results) > 0:
        return results[0], stats
    
    # If something went wrong, return a properly formatted empty result
    return {
        "first_name": first_name,
        "last_name": last_name,
        "result": "No Results Found"
    }, stats

def process_claim(claim_data, output_dir=output_folder, headless=None, wait_time=None):
    """
    Process a claim with possible alternate names to search.

    Args:
        claim_data (dict): {
            'first_name': str,
            'last_name': str,
            'search_evaluation': {          # Optional
                'individual': {
                    'ind_other_names': [    # List of alternate names to search
                        "Full Name 1",      # Each name should be "First Last"
                        "Full Name 2"
                    ]
                }
            }
        }
        output_dir (str): Directory for output files (default: output2/)
        headless (bool, optional): Whether to run browser in headless mode
                                 If None, uses default (True) or SEC_HEADLESS env var
        wait_time (int, optional): Time to wait between requests in seconds
                                 If None, uses SEC_WAIT_TIME env var or SHORT_WAIT

    Returns:
        tuple: (results, stats) where:
            results (list): List of search results, one per name searched.
            Each result is a dict containing:
            {
                'first_name': str,
                'last_name': str,
                'result': list of actions or "No Results Found",
                'total_actions': int (only present if actions found)
            }
            Each action in the result list is a dict containing:
            {
                'Enforcement Action': str,  # Description of the action
                'Date Filed': str,          # Date in format "Month Day, Year"
                'Documents': list[dict]     # List of related documents
            }
            Each document is a dict containing:
            {
                'title': str,  # Document identifier (e.g., "34-72516")
                'link': str    # Full URL to the document
            }

            stats (dict): {
                'individuals_searched': int,
                'total_searches': int,
                'no_enforcement_actions': int,
                'enforcement_actions': int,
                'errors': int
            }

    Example:
        >>> # Run with visible browser
        >>> results, stats = process_claim(claim_data, headless=False)
        >>> # Use environment variable or default
        >>> results, stats = process_claim(claim_data)
    """
    stats = {
        'individuals_searched': 1,
        'total_searches': 0,
        'no_enforcement_actions': 0,
        'enforcement_actions': 0,
        'errors': 0
    }

    try:
        # Validate claim data
        is_valid, error = validate_json_data({"claim": claim_data}, "claim_data")
        if not is_valid:
            raise ValueError(error)

        alternate_names = claim_data.get("search_evaluation", {}).get("individual", {}).get("ind_other_names", [])

        # Process primary name and alternates
        all_names = [(claim_data["first_name"], claim_data["last_name"])] + [
            (name.split()[0], name.split()[-1]) for name in alternate_names
        ]

        with get_driver(headless) as driver:
            results = []
            for first_name, last_name in all_names:
                stats['total_searches'] += 1
                result = search_individual(driver, first_name, last_name)
                results.append(result)

                if result.get("error"):
                    stats['errors'] += 1
                elif result.get("result") == "No Results Found":
                    stats['no_enforcement_actions'] += 1
                else:
                    stats['enforcement_actions'] += 1
                
                # Use wait_time directly if provided
                if wait_time:
                    time.sleep(wait_time)

            return results, stats

    except Exception as e:
        stats['errors'] += 1
        print(f"Error processing claim: {e}")
        return [], stats

def batch_process_folder(input_dir=input_folder, output_dir=output_folder, headless=True):
    """Process all JSON files in the input directory"""
    stats = {
        'individuals_searched': 0,
        'total_searches': 0,
        'no_enforcement_actions': 0,
        'enforcement_actions': 0,
        'errors': 0,
        'skipped_files': []
    }

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(input_dir, exist_ok=True)

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
                print(f"Error: {error}")
                stats['skipped_files'].append(file_path)
                continue

            results, claim_stats = process_claim(
                data["claim"],
                output_dir=output_dir,
                headless=headless
            )

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            stats['skipped_files'].append(file_path)

    return stats

def main():
    parser = argparse.ArgumentParser(description='Search SEC enforcement actions')
    parser.add_argument('--first-name', help='First name to search')
    parser.add_argument('--last-name', help='Last name to search')
    parser.add_argument('--batch', action='store_true', help='Process all JSON files in drop folder')
    parser.add_argument('--headless', action='store_true', default=True, help='Run in headless mode')
    
    args = parser.parse_args()
    
    if args.first_name and args.last_name:
        claim_data = {
            "first_name": args.first_name,
            "last_name": args.last_name,
        }
        results, _ = process_claim(claim_data, headless=args.headless)
        if isinstance(results, list):
            total_actions = sum(1 for r in results if isinstance(r.get('result'), list))
            print(f"\nFound {total_actions} SEC enforcement action(s)")
        elif isinstance(results, dict):
            if isinstance(results.get('result'), list):
                print(f"\nFound {len(results['result'])} SEC enforcement action(s)")
            else:
                print("\nNo SEC enforcement actions found")
    else:
        batch_process_folder(headless=args.headless)

if __name__ == "__main__":
    main()
