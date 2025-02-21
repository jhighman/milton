import os
import json
from typing import Dict, List, Tuple, Optional, Any, Generator
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from bs4 import BeautifulSoup
import argparse
from contextlib import contextmanager
import logging
from logging import Logger

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
"""

# Module-level logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Constants
RUN_HEADLESS = True  # Set to False to run with the browser visible

# Folder paths
folder_path = './'
input_folder = os.path.join(folder_path, 'drop')
output_folder = os.path.join(folder_path, 'output')
cache_folder = os.path.join(folder_path, 'cache')

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
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
    
    service = ChromeService()
    return webdriver.Chrome(service=service, options=options)

@contextmanager
def get_driver(headless: bool = True) -> Generator[webdriver.Chrome, None, None]:
    """
    Context manager for creating and cleaning up a Chrome WebDriver.
    
    Args:
        headless: Whether to run browser in headless mode
        
    Yields:
        A configured Chrome WebDriver instance
    """
    driver = create_driver(headless)
    try:
        yield driver
    finally:
        driver.quit()

def search_individual(first_name: str, last_name: str, employee_number: Optional[str] = None, 
                     logger: Logger = logger) -> Dict[str, Any]:
    """
    Search for an individual's disciplinary actions on FINRA.

    Args:
        first_name (str): First name to search.
        last_name (str): Last name to search.
        employee_number (Optional[str]): Optional identifier for logging context.
        logger (Logger): Logger instance for structured logging.

    Returns:
        Dict[str, Any]: Dictionary containing either:
            - {"result": List[Dict]} for results found
            - {"result": "No Results Found"} for no results 
            - {"error": str} for errors

    Raises:
        ValueError: If first_name or last_name is empty or None.
    """
    # Validate inputs first
    if not first_name or not isinstance(first_name, str) or first_name.strip() == "":
        raise ValueError("first_name cannot be empty or None")
    if not last_name or not isinstance(last_name, str) or last_name.strip() == "":
        raise ValueError("last_name cannot be empty or None")

    logger.info("Starting FINRA disciplinary search", 
               extra={"first_name": first_name, "last_name": last_name, 
                     "employee_number": employee_number})

    try:
        with create_driver(RUN_HEADLESS) as driver:
            return process_finra_search(driver, first_name, last_name, logger)
    except Exception as e:
        logger.error("Search failed", extra={"error": str(e), 
                    "first_name": first_name, "last_name": last_name})
        return {"error": str(e)}

def process_finra_search(driver: webdriver.Chrome, first_name: str, last_name: str, 
                        logger: Logger) -> Dict[str, Any]:
    """Internal function to process the FINRA search with a WebDriver instance."""
    logger.info("Starting FINRA search process", 
               extra={"first_name": first_name, "last_name": last_name})
    
    try:
        # Navigate to FINRA search page
        logger.debug("Navigating to FINRA disciplinary actions page")
        driver.get("https://www.finra.org/rules-guidance/oversight-enforcement/finra-disciplinary-actions-online")

        # Input search details
        logger.debug("Filling in search fields")
        search_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "edit-individuals"))
        )
        search_input.send_keys(f"{first_name} {last_name}")

        # Agree to terms and submit
        logger.debug("Agreeing to terms and submitting form")
        try:
            terms_checkbox = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "edit-terms-of-service"))
            )
            driver.execute_script("arguments[0].click();", terms_checkbox)
            logger.debug("Terms of Service checkbox clicked")

            submit_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "edit-actions-submit"))
            )
            driver.execute_script("arguments[0].click();", submit_button)
            logger.debug("Submit button clicked")
        except Exception as e:
            logger.error("Error during form submission", extra={"error": str(e)})
            return {"error": f"Form submission failed: {str(e)}"}

        # Wait for results or no results message
        logger.debug("Waiting for search results")
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//em[text()='No Results Found']"))
            )
            logger.info("No results found for search")
            return {"result": "No Results Found"}
        except:
            # Look for results table
            logger.debug("Checking for results table")
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.table-responsive.col > table.views-table.views-view-table.cols-5"))
            )
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup.find("table", class_="table views-table views-view-table cols-5")

            if not table:
                logger.warning("No results table found after search")
                return {"result": "No Results Found"}

            # Parse table data
            logger.debug("Extracting disciplinary action data")
            headers = ["Case ID", "Case Summary", "Document Type", "Firms/Individuals", "Action Date"]
            result_rows = [
                dict(zip(headers, [td.get_text(strip=True) for td in tr.find_all("td")]))
                for tr in table.find_all("tr")[1:]  # Skip header row
            ]
            logger.info("Search completed successfully", 
                       extra={"result_count": len(result_rows)})
            return {"result": result_rows}

    except Exception as e:
        logger.error("Search process failed", 
                    extra={"first_name": first_name, "last_name": last_name, "error": str(e)})
        return {"error": str(e)}

def validate_json_data(data: Any, file_path: str, logger: Logger = logger) -> Tuple[bool, str]:
    """
    Validate that the JSON data has the required fields.

    Args:
        data (Any): JSON data to validate.
        file_path (str): Path to the JSON file for error reporting.
        logger (Logger): Logger instance for structured logging. Defaults to module logger.

    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    logger.debug("Validating JSON data", extra={"file_path": file_path})
    
    if not isinstance(data, dict):
        error = f"Invalid JSON structure in {file_path}: expected object, got {type(data)}"
        logger.error(error)
        return False, error
    
    if "claim" not in data:
        error = f"Missing 'claim' object in {file_path}"
        logger.error(error)
        return False, error
    
    claim = data.get("claim", {})
    if not isinstance(claim, dict):
        error = f"Invalid 'claim' structure in {file_path}: expected object, got {type(claim)}"
        logger.error(error)
        return False, error
    
    if not claim.get("first_name"):
        error = f"Missing or empty 'first_name' in claim: {file_path}"
        logger.error(error)
        return False, error
    if not claim.get("last_name"):
        error = f"Missing or empty 'last_name' in claim: {file_path}"
        logger.error(error)
        return False, error
    
    if "alternate_names" in data:
        alt_names = data["alternate_names"]
        if not isinstance(alt_names, list):
            error = f"Invalid 'alternate_names' structure: expected list in {file_path}"
            logger.error(error)
            return False, error
        
        for i, name_pair in enumerate(alt_names):
            if not isinstance(name_pair, list) or len(name_pair) != 2:
                error = f"Invalid alternate name pair at index {i} in {file_path}"
                logger.error(error)
                return False, error
            if not all(isinstance(n, str) and n.strip() for n in name_pair):
                error = f"Invalid or empty name in alternate name pair at index {i} in {file_path}"
                logger.error(error)
                return False, error
    
    logger.debug("JSON data validated successfully", extra={"file_path": file_path})
    return True, ""

def batch_process_folder(input_dir: str = 'drop', output_dir: str = 'output', 
                        cache_dir: str = 'cache', headless: bool = True, 
                        logger: Logger = logger) -> Dict[str, int]:
    """
    Process all JSON files in the input directory.

    Args:
        input_dir (str): Directory containing input JSON files. Defaults to 'drop'.
        output_dir (str): Directory for output files. Defaults to 'output'.
        cache_dir (str): Directory for caching results. Defaults to 'cache'.
        headless (bool): Whether to run browser in headless mode. Defaults to True.
        logger (Logger): Logger instance for structured logging.

    Returns:
        Dict[str, int]: Processing statistics.
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
    
    logger.info("Starting batch processing", extra={"input_dir": input_dir})
    
    json_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]
    if not json_files:
        logger.warning("No JSON files found in input folder", extra={"input_dir": input_dir})
        return stats
    
    for json_file in json_files:
        file_path = os.path.join(input_dir, json_file)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            is_valid, error = validate_json_data(data, file_path, logger)
            if not is_valid:
                stats['errors'] += 1
                logger.error("Skipping file due to validation error", extra={"file_path": file_path})
                continue

            claim = data["claim"]
            result = search_individual(
                claim["first_name"], 
                claim["last_name"],
                employee_number=claim.get("employee_number"),
                logger=logger
            )
            
            stats['total_individuals'] += 1
            handle_search_results(
                [result], claim["first_name"], claim["last_name"], 
                output_dir, cache_dir, stats, logger
            )

        except Exception as e:
            stats['errors'] += 1
            logger.error("Error processing file", 
                        extra={"file_path": file_path, "error": str(e)})

    print_summary(stats, logger)
    return stats

def handle_search_results(results: List[Dict[str, Any]], first_name: str, last_name: str, 
                        output_dir: str, cache_dir: str, stats: Dict[str, int], 
                        logger: Logger = logger) -> None:
    """
    Handle search results including caching and stats updating.

    Args:
        results (List[Dict[str, Any]]): List of search results.
        first_name (str): First name of the individual.
        last_name (str): Last name of the individual.
        output_dir (str): Directory for output results.
        cache_dir (str): Directory for caching results.
        stats (Dict[str, int]): Dictionary to update with processing stats.
        logger (Logger): Logger instance for structured logging. Defaults to module logger.
    """
    logger.debug("Handling search results", 
                extra={"first_name": first_name, "last_name": last_name, "result_count": len(results)})
    
    cache_path = os.path.join(cache_dir, f"{first_name}_{last_name}", "results.json")
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)
    logger.debug("Results cached", extra={"cache_path": cache_path})

    for result in results:
        if result.get("result") == "No Results Found":
            stats['no_results'] += 1
        elif "error" in result:
            stats['errors'] += 1
        else:
            stats['disciplinary_actions'] += len(result["result"])

    if any(r.get("result") != "No Results Found" for r in results):
        output_path = os.path.join(output_dir, f"{first_name}_{last_name}", "results.json")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=4)
        logger.debug("Results saved to output", extra={"output_path": output_path})

def print_summary(stats: Dict[str, int], logger: Logger = logger) -> None:
    """
    Print processing summary.

    Args:
        stats (Dict[str, int]): Processing statistics.
        logger (Logger): Logger instance for structured logging. Defaults to module logger.
    """
    logger.info("Processing summary", extra=stats)
    print("\n--- Summary ---")
    print(f"Total Individuals Processed: {stats['total_individuals']}")
    print(f"Total Disciplinary Actions Found: {stats['disciplinary_actions']}")
    print(f"Total No Results: {stats['no_results']}")
    print(f"Total Errors: {stats['errors']}")

def main() -> None:
    parser = argparse.ArgumentParser(description='Search FINRA disciplinary actions')
    parser.add_argument('--first-name', help='First name to search')
    parser.add_argument('--last-name', help='Last name to search')
    parser.add_argument('--batch', action='store_true', help='Process all JSON files in drop folder')
    parser.add_argument('--headless', action='store_true', default=True, help='Run in headless mode')
    
    args = parser.parse_args()
    
    if args.first_name and args.last_name:
        with get_driver(args.headless) as driver:
            result = search_individual(args.first_name, args.last_name)
            print(json.dumps(result, indent=2))
    else:
        batch_process_folder(headless=args.headless, logger=logger)

if __name__ == "__main__":
    main()