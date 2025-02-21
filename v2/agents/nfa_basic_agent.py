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

"""
NFA BASIC Search Tool Agent for Individuals

This script searches for individual profiles on the NFA BASIC website.
It can process single searches via command-line arguments or batch process
JSON files from the 'drop' folder with the following structure:

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
"""

# Module-level logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Constants
RUN_HEADLESS: bool = True
input_folder: str = './drop'
output_folder: str = './output'
cache_folder: str = './cache'

def create_driver(headless: bool = True, logger: Logger = logger) -> webdriver.Chrome:
    """
    Create and configure a Chrome WebDriver.

    Args:
        headless (bool): Whether to run the browser in headless mode. Defaults to True.
        logger (Logger): Logger instance for structured logging. Defaults to module logger.

    Returns:
        webdriver.Chrome: Configured WebDriver instance.
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

    Args:
        first_name (str): First name to search.
        last_name (str): Last name to search.
        driver (webdriver.Chrome): Selenium WebDriver instance.
        employee_number (Optional[str]): Optional identifier for logging context.
        logger (Logger): Logger instance for structured logging.

    Returns:
        Dict[str, Any]: Dictionary containing either:
            - {"result": List[Dict]} for results found
            - {"result": "No Results Found"} for no results 
            - {"error": str} for errors
    """
    search_term = f"{first_name} {last_name}"
    logger.info("Starting NFA profile search", extra={"search_term": search_term})
    
    try:
        logger.debug("Navigating to NFA BASIC search page")
        driver.get("https://www.nfa.futures.org/BasicNet/#profile")
        
        logger.debug("Waiting 2 seconds for page to load")
        time.sleep(2)

        logger.debug("Locating Individual tab")
        individual_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//ul[@id='landing_search_tabs']//a[contains(text(), 'Individual')]"))
        )
        parent_classes = individual_tab.find_element(By.XPATH, "..").get_attribute("class")
        if "active" not in parent_classes:
            driver.execute_script("arguments[0].click();", individual_tab)
            logger.debug("Clicked Individual tab")
            time.sleep(0.5)
        else:
            logger.debug("Individual tab already active")

        logger.debug("Entering first name")
        fname_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "fname_in_lsearch_tabs"))
        )
        fname_input.clear()
        fname_input.send_keys(first_name)
        logger.debug("First name entered", extra={"first_name": first_name})

        logger.debug("Entering last name")
        lname_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@id='individual']//input[@placeholder='Last Name (required)']"))
        )
        lname_input.clear()
        lname_input.send_keys(last_name)
        logger.debug("Last name entered", extra={"last_name": last_name})

        logger.debug("Submitting search form")
        submit_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@id='individual']//button[contains(text(), 'Search')]"))
        )
        driver.execute_script("arguments[0].click();", submit_button)
        logger.debug("Waiting 5 seconds after submitting search")
        time.sleep(5)

        logger.debug("Processing search results")
        try:
            results_table = driver.find_element(By.ID, "table_individual_name_results")
            logger.debug("Results table found")
            time.sleep(1)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.find("table", id="table_individual_name_results")
            
            if not table or not table.find("tbody"):
                logger.warning("No valid results table found", 
                              extra={"table_found": bool(table), "tbody_found": bool(table and table.find("tbody"))})
                driver.save_screenshot("debug_table_not_found.png")
                return {"result": "No Results Found"}

            logger.debug("Parsing results table")
            headers = ["Individual Name", "Current NFA Membership Status", "Current Registration Types", "Regulatory Actions"]
            result_rows = []
            for tr in table.find("tbody").find_all("tr"):
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
            
            logger.info("Search completed", extra={"profile_count": len(result_rows)})
            return {"result": result_rows} if result_rows else {"result": "No Results Found"}

        except:
            try:
                no_results = driver.find_element(By.ID, "basic_search_no_results")
                logger.info("No results found", extra={"message": no_results.text})
                return {"result": "No Results Found"}
            except:
                logger.error("Failed to detect results or no-results message")
                driver.save_screenshot("debug_no_elements_found.png")
                return {"error": "Could not detect results or no-results message"}

    except Exception as e:
        logger.error("Search failed", extra={"error": str(e)})
        driver.save_screenshot("debug_error.png")
        return {"error": str(e)}

def validate_json_data(data: Any, file_path: str, logger: Logger = logger) -> Tuple[bool, str]:
    """
    Validate JSON data structure for required fields.

    Args:
        data (Any): JSON data to validate.
        file_path (str): Path to the JSON file for error reporting.
        logger (Logger): Logger instance for structured logging. Defaults to module logger.

    Returns:
        Tuple[bool, str]: (is_valid, error_message)
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

    Args:
        driver (webdriver.Chrome): Selenium WebDriver instance.
        first_name (str): Primary first name.
        last_name (str): Primary last name.
        alternate_names (Optional[List[List[str]]]): List of [first_name, last_name] pairs.
        logger (Logger): Logger instance for structured logging.

    Returns:
        List[Dict[str, Any]]: List of search results for all names.
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

    Args:
        logger (Logger): Logger instance for structured logging. Defaults to module logger.

    Returns:
        Dict[str, int]: Processing statistics.
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

    Args:
        results (List[Dict[str, Any]]): List of search results.
        output_name (str): Name for the output file (typically first_last).
        stats (Dict[str, int]): Dictionary to update with processing stats.
        logger (Logger): Logger instance for structured logging. Defaults to module logger.
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
    parser = argparse.ArgumentParser(description='Search NFA BASIC Individual Profiles')
    parser.add_argument('--first-name', help='First name to search')
    parser.add_argument('--last-name', help='Last name to search')
    parser.add_argument('--batch', action='store_true', help='Process JSON files in drop folder')
    parser.add_argument('--headless', action='store_true', default=RUN_HEADLESS, help='Run in headless mode')
    
    args = parser.parse_args()
    
    if args.first_name and args.last_name:
        with create_driver(args.headless, logger) as driver:
            result = search_individual(args.first_name, args.last_name, driver=driver, logger=logger)
            print(json.dumps(result, indent=2))
    elif args.batch:
        batch_process_folder(logger)
    else:
        logger.warning("No valid arguments provided")
        print("Please provide --first-name and --last-name, or use --batch.")

if __name__ == "__main__":
    main()