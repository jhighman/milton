import os
import json
import argparse
import time
from typing import Dict, List, Tuple, Optional, Any
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import logging
from logging import Logger

logger = logging.getLogger('nfa_basic_agent')

"""
NFA BASIC Search Tool Agent for Individuals and NFA IDs

This script searches for profiles on the NFA BASIC website using either individual names or NFA IDs.
It can process single searches via command-line arguments or batch process JSON files from the 'drop' folder.
"""

# Constants
RUN_HEADLESS: bool = False
input_folder: str = './drop'
output_folder: str = './output'
cache_folder: str = './cache'

def create_driver(headless: bool = True, logger: Logger = logger) -> webdriver.Chrome:
    """
    Create and configure a Chrome WebDriver.
    """
    logger.debug("Initializing Chrome WebDriver", extra={"headless": headless})
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/115.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=ChromeService(), options=options)

def search_individual(first_name: str, last_name: str, driver: webdriver.Chrome,
                     employee_number: Optional[str] = None,
                     logger: Logger = logger) -> Dict[str, Any]:
    """
    Search for an individual's profile in NFA BASIC.
    """
    search_term = f"{first_name} {last_name}"
    print(f"Step 1: Starting search for {search_term}")
    logger.info("Starting NFA profile search", extra={"search_term": search_term})
    
    try:
        print("Step 2: Navigating to NFA BASIC search page")
        logger.debug("Navigating to NFA BASIC search page")
        driver.get("https://www.nfa.futures.org/BasicNet/#profile")
        
        print("Step 3: Waiting for page to load")
        logger.debug("Waiting for page to load")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "landing_search_tabs"))
        )
        print("Step 4: Page loaded, locating Individual tab")

        logger.debug("Locating Individual tab")
        individual_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//ul[@id='landing_search_tabs']//a[contains(text(), 'Individual')]"))
        )
        parent_classes = individual_tab.find_element(By.XPATH, "..").get_attribute("class")
        if "active" not in parent_classes:
            print("Step 5: Clicking Individual tab")
            driver.execute_script("arguments[0].click();", individual_tab)
            logger.debug("Clicked Individual tab")
            time.sleep(0.5)
        else:
            print("Step 5: Individual tab already active")
            logger.debug("Individual tab already active")

        print("Step 6: Entering first name")
        logger.debug("Entering first name")
        fname_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "fname_in_lsearch_tabs"))
        )
        fname_input.clear()
        fname_input.send_keys(first_name)
        logger.debug("First name entered", extra={"first_name": first_name})

        print("Step 7: Entering last name")
        logger.debug("Entering last name")
        lname_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@id='individual']//input[@placeholder='Last Name (required)']"))
        )
        lname_input.clear()
        lname_input.send_keys(last_name)
        logger.debug("Last name entered", extra={"last_name": last_name})

        print("Step 8: Submitting search form")
        logger.debug("Submitting search form")
        submit_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@id='individual']//button[contains(text(), 'Search')]"))
        )
        driver.execute_script("arguments[0].click();", submit_button)
        print("Step 9: Search submitted, starting result processing")

        # Retry loop for table rows
        logger.debug("Processing search results with retry loop")
        max_wait_time = 100  # Total time to wait in seconds
        retry_interval = 10  # Wait time between retries in seconds
        start_time = time.time()

        while time.time() - start_time <= max_wait_time:
            elapsed_time = time.time() - start_time
            print(f"Step 10: Checking for results table with rows at {elapsed_time:.1f} seconds")

            try:
                # Check for the table
                results_table = driver.find_element(By.ID, "table_individual_name_results")
                print(f"Step 11: Results table found after {elapsed_time:.1f} seconds")
                logger.debug("Results table found")
                time.sleep(1)  # Brief pause to ensure table is fully loaded
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                table = soup.find("table", id="table_individual_name_results")
                
                if not table or not table.find("tbody"):
                    print(f"Step 12: Table found but no tbody after {elapsed_time:.1f} seconds, retrying")
                    logger.debug("Table found but no tbody, retrying", extra={"elapsed_time": elapsed_time})
                else:
                    # Check for rows
                    rows = table.find("tbody").find_all("tr")
                    if not rows:
                        print(f"Step 12: Table found with tbody but no rows after {elapsed_time:.1f} seconds, retrying")
                        logger.debug("Table found with tbody but no rows, retrying", extra={"elapsed_time": elapsed_time})
                    else:
                        print(f"Step 12: Parsing results table with {len(rows)} rows after {elapsed_time:.1f} seconds")
                        logger.debug("Parsing results table")
                        headers = ["Individual Name", "Current NFA Membership Status", "Current Registration Types", "Regulatory Actions"]
                        result_rows = []
                        for tr in rows:
                            row: Dict[str, str] = {}
                            cells = tr.find_all("td")
                            name_cell = cells[0]
                            name = name_cell.find("h4").get_text(strip=True) if name_cell.find("h4") else ""
                            small_text = name_cell.find("small").get_text(strip=True) if name_cell.find("small") else ""
                            nfa_id, firm = small_text.split(" | ", 1) if " | " in small_text else (small_text, "")
                            
                            row["Name"] = name
                            row["NFA ID"] = nfa_id
                            row["Firm"] = firm
                            
                            for header, td in zip(headers[1:], cells[1:4]):
                                row[header] = td.find("span").get_text(strip=True) if td.find("span") else td.get_text(strip=True)
                            
                            row["Details Available"] = "Yes" if cells[-1].find("div", class_="btn") else "No"
                            result_rows.append(row)
                        
                        print(f"Step 13: Search completed with {len(result_rows)} profiles after {elapsed_time:.1f} seconds")
                        logger.info("Search completed", extra={"profile_count": len(result_rows)})
                        return {"result": result_rows}

            except:
                elapsed_time = time.time() - start_time
                print(f"Step 11: Table not found yet after {elapsed_time:.1f} seconds, retrying in {retry_interval} seconds")
                logger.debug(f"Table not found yet, retrying in {retry_interval} seconds", 
                            extra={"elapsed_time": elapsed_time})

            if elapsed_time + retry_interval > max_wait_time:
                print(f"Step 14: Max wait time ({max_wait_time} seconds) exceeded, returning 'No Results Found'")
                logger.error("Max wait time exceeded, no rows found")
                driver.save_screenshot("debug_table_timeout.png")
                return {"result": "No Results Found"}
            time.sleep(retry_interval)

    except Exception as e:
        print(f"Step 15: Unexpected error occurred: {str(e)}")
        logger.error("Search failed", extra={"error": str(e)})
        driver.save_screenshot("debug_error.png")
        return {"error": str(e)}

def search_nfa(nfa_id: str, driver: webdriver.Chrome, logger: Logger = logger) -> Dict[str, Any]:
    """
    Search for a profile in NFA BASIC using an NFA ID and extract regulatory actions.
    """
    search_term = nfa_id
    print(f"Step 1: Starting NFA ID search for {search_term}")
    logger.info("Starting NFA ID profile search", extra={"search_term": search_term})
    
    try:
        print("Step 2: Navigating to NFA BASIC search page")
        logger.debug("Navigating to NFA BASIC search page")
        driver.get("https://www.nfa.futures.org/BasicNet/#profile")
        
        print("Step 3: Waiting for page to load")
        logger.debug("Waiting for page to load")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "landing_search_tabs"))
        )
        print("Step 4: Page loaded, locating NFA ID tab")

        logger.debug("Locating NFA ID tab")
        nfa_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//ul[@id='landing_search_tabs']//a[contains(text(), 'NFA ID')]"))
        )
        parent_classes = nfa_tab.find_element(By.XPATH, "..").get_attribute("class")
        if "active" not in parent_classes:
            print("Step 5: Clicking NFA ID tab")
            driver.execute_script("arguments[0].click();", nfa_tab)
            logger.debug("Clicked NFA ID tab")
            print("Step 5a: Waiting for input field after tab click")
            # Updated to use XPath for input with placeholder="NFA ID"
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//input[@type='search' and @placeholder='NFA ID']"))
            )
            print("Step 5b: Input field found")
            time.sleep(2)  # Delay for dynamic content
        else:
            print("Step 5: NFA ID tab already active")
            logger.debug("NFA ID tab already active")

        print("Step 6: Entering NFA ID")
        logger.debug("Entering NFA ID")
        nfa_input = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@type='search' and @placeholder='NFA ID']"))
        )
        print("Step 6a: Input field located")
        nfa_input.clear()
        nfa_input.send_keys(nfa_id)
        print(f"Step 6b: NFA ID '{nfa_id}' entered")
        logger.debug("NFA ID entered", extra={"nfa_id": nfa_id})

        print("Step 7: Submitting search form")
        logger.debug("Submitting search form")
        search_button = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "#nfaid button.btn.btn-primary"
            ))
        )
        driver.execute_script("arguments[0].click();", search_button)
        print("Step 7: Search submitted, waiting for profile page results")

        # Retry loop for "View All Actions" button
        logger.debug("Waiting for 'View All Actions' button with retry loop")
        max_wait_time = 100
        retry_interval = 10
        start_time = time.time()

        while time.time() - start_time <= max_wait_time:
            elapsed_time = time.time() - start_time
            print(f"Step 9: Checking for 'View All Actions' button at {elapsed_time:.1f} seconds")

            try:
                view_all_button = driver.find_element(By.ID, "view-all-actions")
                print(f"Step 10: 'View All Actions' button found after {elapsed_time:.1f} seconds")
                logger.debug("View All Actions button found")
                break
            except:
                elapsed_time = time.time() - start_time
                print(f"Step 10: 'View All Actions' button not found yet after {elapsed_time:.1f} seconds, retrying in {retry_interval} seconds")
                logger.debug(f"Button not found yet, retrying in {retry_interval} seconds", 
                            extra={"elapsed_time": elapsed_time})

            if elapsed_time + retry_interval > max_wait_time:
                print(f"Step 11: Max wait time ({max_wait_time} seconds) exceeded for button, returning 'No Results Found'")
                logger.error("Max wait time exceeded, no button found")
                driver.save_screenshot("debug_button_timeout.png")
                return {"result": "No Results Found"}
            time.sleep(retry_interval)

        # Click the "View All Actions" button
        print("Step 11: Clicking 'View All Actions' button")
        driver.execute_script("arguments[0].click();", view_all_button)
        logger.debug("Clicked 'View All Actions' button")

        # Retry loop for Regulatory Actions table
        logger.debug("Waiting for Regulatory Actions table with retry loop")
        start_time = time.time()
        while time.time() - start_time <= max_wait_time:
            elapsed_time = time.time() - start_time
            print(f"Step 12: Checking for actions table at {elapsed_time:.1f} seconds")

            try:
                # The table ID includes the NFA ID, so we construct it dynamically
                table_id = f"actionsIndexTable{nfa_id}"
                actions_table = driver.find_element(By.ID, table_id)
                print(f"Step 13: Actions table found after {elapsed_time:.1f} seconds")
                logger.debug("Actions table found")
                time.sleep(1)  # Brief pause to ensure table is fully loaded

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                table = soup.find("table", id=table_id)
                
                if not table or not table.find("tbody"):
                    print(f"Step 14: Table found but no tbody after {elapsed_time:.1f} seconds, retrying")
                    logger.debug("Table found but no tbody, retrying", extra={"elapsed_time": elapsed_time})
                else:
                    rows = table.find("tbody").find_all("tr")
                    if not rows:
                        print(f"Step 14: Table found with tbody but no rows after {elapsed_time:.1f} seconds, retrying")
                        logger.debug("Table found with tbody but no rows, retrying", extra={"elapsed_time": elapsed_time})
                    else:
                        print(f"Step 14: Parsing actions table with {len(rows)} rows after {elapsed_time:.1f} seconds")
                        logger.debug("Parsing actions table")
                        result_rows = []

                        for tr in rows:
                            cells = tr.find_all("td")
                            # Skip spacer or incomplete rows
                            if len(cells) < 5:
                                continue
                            
                            row: Dict[str, Any] = {}
                            # Extract data from each column safely
                            row["Effective Date"] = cells[0].get_text(strip=True)
                            row["Contributor"] = cells[1].get_text(strip=True)
                            row["Action Type"] = [li.get_text(strip=True) for li in cells[2].find_all("li")]
                            row["Case Outcome"] = [li.get_text(strip=True) for li in cells[3].find_all("li")]
                            case_link = cells[4].find("a")
                            row["Case #"] = case_link.get_text(strip=True) if case_link else cells[4].get_text(strip=True)
                            row["Case Link"] = case_link["href"] if case_link else ""
                            
                            result_rows.append(row)
                        
                        print(f"Step 15: Search completed with {len(result_rows)} regulatory actions after {elapsed_time:.1f} seconds")
                        logger.info("Search completed", extra={"nfa_id": nfa_id, "action_count": len(result_rows)})
                        return {"result": {"nfa_id": nfa_id, "regulatory_actions": result_rows}}

            except:
                elapsed_time = time.time() - start_time
                print(f"Step 13: Actions table not found yet after {elapsed_time:.1f} seconds, retrying in {retry_interval} seconds")
                logger.debug(f"Table not found yet, retrying in {retry_interval} seconds", 
                            extra={"elapsed_time": elapsed_time})

            if elapsed_time + retry_interval > max_wait_time:
                print(f"Step 14: Max wait time ({max_wait_time} seconds) exceeded for table, returning 'No Results Found'")
                logger.error("Max wait time exceeded, no table found")
                driver.save_screenshot("debug_table_timeout.png")
                return {"result": "No Results Found"}

            time.sleep(retry_interval)

    except Exception as e:
        print(f"Step 15: Unexpected error occurred: {str(e)}")
        logger.error("Search failed", extra={"error": str(e)})
        driver.save_screenshot("debug_error.png")
        return {"error": str(e)}


def validate_json_data(data: Any, file_path: str, logger: Logger = logger) -> Tuple[bool, str]:
    """
    Validate JSON data structure for required fields.
    """
    logger.debug("Validating JSON data", extra={"file_path": file_path})
    if "claim" not in data or not all(k in data["claim"] for k in ["first_name", "last_name"]):
        error = f"Missing or invalid 'claim' in {file_path}"
        logger.error(error)
        return False, error
    logger.debug("JSON data validated successfully")
    return True, ""

def search_with_alternates(driver: webdriver.Chrome, first_name: str, last_name: str, 
                         alternate_names: Optional[List[List[str]]] = None, 
                         logger: Logger = logger) -> List[Dict[str, Any]]:
    """
    Search for an individual including alternate names.
    """
    all_names = [(first_name, last_name)] + (alternate_names or [])
    logger.debug("Searching with alternates", 
                extra={"primary_name": f"{first_name} {last_name}", 
                      "alternate_count": len(alternate_names or [])})
    return [search_individual(fname, lname, driver=driver, logger=logger) 
            for fname, lname in all_names]

def batch_process_folder(logger: Logger = logger) -> Dict[str, int]:
    """
    Process all JSON files in the input folder.
    """
    os.makedirs(input_folder, exist_ok=True)
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(cache_folder, exist_ok=True)
    
    stats = {'total_searches': 0, 'profiles_found': 0, 'no_results': 0, 'errors': 0}
    logger.info("Starting batch processing", extra={"input_folder": input_folder})
    
    with create_driver(RUN_HEADLESS, logger) as driver:
        json_files = [f for f in os.listdir(input_folder) if f.endswith('.json')]
        if not json_files:
            logger.warning("No JSON files found in input folder")
            return stats
        
        for json_file in json_files:
            file_path = os.path.join(input_folder, json_file)
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                is_valid, error = validate_json_data(data, file_path, logger)
                if not is_valid:
                    stats['errors'] += 1
                    logger.error("Validation failed", extra={"file_path": file_path, "error": error})
                    continue

                claim = data["claim"]
                results = search_with_alternates(driver, claim["first_name"], claim["last_name"], 
                                               data.get("alternate_names", []), logger)
                
                stats['total_searches'] += len(results)
                handle_search_results(results, f"{claim['first_name']}_{claim['last_name']}", stats, logger)

            except Exception as e:
                stats['errors'] += 1
                logger.error("Error processing file", extra={"file_path": file_path, "error": str(e)})
    
    logger.info("Batch processing completed", extra=stats)
    print(f"Summary: Searches={stats['total_searches']}, Profiles={stats['profiles_found']}, No Results={stats['no_results']}, Errors={stats['errors']}")
    return stats

def handle_search_results(results: List[Dict[str, Any]], output_name: str, 
                        stats: Dict[str, int], logger: Logger = logger) -> None:
    """
    Handle search results, update stats, and save to output.
    """
    logger.debug("Handling search results", extra={"output_name": output_name, "result_count": len(results)})
    
    for result in results:
        if result.get("result") == "No Results Found":
            stats['no_results'] += 1
        elif "error" in result:
            stats['errors'] += 1
        else:
            stats['profiles_found'] += len(result["result"])
    
    output_path = os.path.join(output_folder, f"{output_name}.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=4)
    logger.debug("Results saved", extra={"output_path": output_path})

def main() -> None:
    parser = argparse.ArgumentParser(description='Search NFA BASIC Individual Profiles or NFA IDs')
    parser.add_argument('--first-name', help='First name to search')
    parser.add_argument('--last-name', help='Last name to search')
    parser.add_argument('--nfa-id', help='NFA ID to search')
    parser.add_argument('--batch', action='store_true', help='Process JSON files in drop folder')
    parser.add_argument('--headless', action='store_true', default=RUN_HEADLESS, help='Run in headless mode')
    
    args = parser.parse_args()

    sample_searches = [
        {"first_name": "John", "last_name": "Doe", "description": "Common name (may have results)"},
        {"first_name": "Xzq", "last_name": "Yzv", "description": "Obscure name (likely no results)"},
        {"nfa_id": "1234567", "description": "Sample NFA ID search"},
    ]

    def run_individual_search(first_name: str, last_name: str, headless: bool = args.headless) -> None:
        """Helper function to execute and display an individual search."""
        with create_driver(headless, logger) as driver:
            result = search_individual(first_name, last_name, driver=driver, logger=logger)
            print(f"\nSearch Results for {first_name} {last_name}:")
            print(json.dumps(result, indent=2))

    def run_nfa_search(nfa_id: str, headless: bool = args.headless) -> None:
        """Helper function to execute and display an NFA ID search."""
        with create_driver(headless, logger) as driver:
            result = search_nfa(nfa_id, driver=driver, logger=logger)
            print(f"\nSearch Results for NFA ID {nfa_id}:")
            print(json.dumps(result, indent=2))

    if args.first_name and args.last_name:
        run_individual_search(args.first_name, args.last_name, args.headless)
    elif args.nfa_id:
        run_nfa_search(args.nfa_id, args.headless)
    elif args.batch:
        batch_process_folder(logger)
    else:
        while True:
            print("\nNFA BASIC Search Tool")
            print("==================================")
            print("1. Run a sample search")
            print("2. Perform a custom individual search")
            print("3. Perform a custom NFA ID search")
            print("4. Process JSON files in drop folder (batch mode)")
            print("5. Exit")
            choice = input("Enter your choice (1-5): ").strip()

            if choice == "1":
                print("\nAvailable Sample Searches:")
                for i, sample in enumerate(sample_searches, 1):
                    if "nfa_id" in sample:
                        print(f"{i}. NFA ID: {sample['nfa_id']} - {sample['description']}")
                    else:
                        print(f"{i}. {sample['first_name']} {sample['last_name']} - {sample['description']}")
                sample_choice = input("Select a sample (1-{}): ".format(len(sample_searches))).strip()
                try:
                    idx = int(sample_choice) - 1
                    if 0 <= idx < len(sample_searches):
                        sample = sample_searches[idx]
                        if "nfa_id" in sample:
                            run_nfa_search(sample["nfa_id"])
                        else:
                            run_individual_search(sample["first_name"], sample["last_name"])
                    else:
                        print("Invalid sample number.")
                except ValueError:
                    print("Please enter a valid number.")

            elif choice == "2":
                first_name = input("Enter first name: ").strip()
                last_name = input("Enter last name (required): ").strip()
                if not last_name:
                    print("Error: Last name is required.")
                    continue
                run_individual_search(first_name, last_name)

            elif choice == "3":
                nfa_id = input("Enter NFA ID: ").strip()
                if not nfa_id:
                    print("Error: NFA ID is required.")
                    continue
                run_nfa_search(nfa_id)

            elif choice == "4":
                print("\nRunning batch process...")
                batch_process_folder(logger)

            elif choice == "5":
                print("Exiting...")
                break

            else:
                print("Invalid choice. Please enter 1, 2, 3, 4, or 5.")

if __name__ == "__main__":
    main()