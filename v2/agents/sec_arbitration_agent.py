import os
import json
from typing import Dict, List, Tuple, Optional, Any, Generator
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
import logging
from logging import Logger

"""
SEC Arbitration Search Tool

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

# Module-level logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Constants and folder paths
folder_path: str = './'
input_folder: str = os.path.join(folder_path, 'drop2')
output_folder: str = os.path.join(folder_path, 'output2')

def create_driver(headless: bool = True, logger: Logger = logger) -> webdriver.Chrome:
    """
    Create and configure a Chrome WebDriver.

    Args:
        headless (bool): Whether to run browser in headless mode. Can be overridden by SEC_HEADLESS env var. Defaults to True.
        logger (Logger): Logger instance for structured logging. Defaults to module logger.

    Returns:
        webdriver.Chrome: Configured WebDriver instance.
    """
    env_headless = os.getenv('SEC_HEADLESS', 'true').lower()
    effective_headless = headless and env_headless != 'false'
    logger.debug("Initializing Chrome WebDriver", extra={"headless": effective_headless})
    
    options = Options()
    if effective_headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
    
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

def generate_sec_search_url(first_name: str, last_name: str) -> str:
    """
    Generates the search URL using first and last names.

    Args:
        first_name (str): First name for the search.
        last_name (str): Last name for the search.

    Returns:
        str: Formatted SEC search URL.
    """
    base_url = "https://www.sec.gov/litigations/sec-action-look-up?last_name={}&first_name={}"
    return base_url.format(last_name, first_name)

def search_individual(driver: webdriver.Chrome, first_name: str, last_name: str, 
                     logger: Logger = logger) -> Dict[str, Any]:
    """
    Search for an individual's SEC actions.

    Args:
        driver (webdriver.Chrome): Selenium WebDriver instance.
        first_name (str): First name to search.
        last_name (str): Last name to search.
        logger (Logger): Logger instance for structured logging.

    Returns:
        Dict[str, Any]: Dictionary containing search results and individual info
    """
    logger.info("Searching SEC actions", extra={"first_name": first_name, "last_name": last_name})
    search_url = generate_sec_search_url(first_name, last_name)
    logger.debug("Generated search URL", extra={"url": search_url})
    
    base_result = {
        "first_name": first_name,
        "last_name": last_name
    }
    
    try:
        logger.debug("Loading search page")
        driver.get(search_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "main-content"))
        )
        logger.debug("Page loaded successfully")

        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')

        labels = soup.find_all("span", class_="views-label")
        if not labels:
            logger.info("No enforcement actions found")
            return {**base_result, "result": "No Results Found"}

        data: List[Dict[str, Any]] = []
        for label in labels:
            if "Enforcement Action:" in label.text:
                action = label.find_next_sibling().get_text(strip=True)
                logger.debug("Found enforcement action", extra={"action": action})
                
                card = label.find_parent("div", class_="card")
                if card:
                    date_label = card.find("span", class_="views-label", string=lambda x: "Date Filed:" in str(x))
                    date_filed = date_label.find_next_sibling().get_text(strip=True) if date_label else "Unknown"
                    logger.debug("Extracted date filed", extra={"date": date_filed})
                    
                    documents: List[Dict[str, str]] = []
                    doc_label = card.find("span", class_="views-label", string=lambda x: "Releases & Documents:" in str(x))
                    if doc_label:
                        doc_list = doc_label.find_next_sibling()
                        if doc_list:
                            for doc in doc_list.find_all("a"):
                                title = doc.get_text(strip=True)
                                link = doc.get("href")
                                if link and title:
                                    documents.append({"title": title, "link": link})
                                    logger.debug("Found document", extra={"title": title, "link": link})
                    
                    data.append({
                        "Enforcement Action": action,
                        "Date Filed": date_filed,
                        "Documents": documents
                    })

        if not data:
            logger.info("No valid enforcement actions found")
            return {**base_result, "result": "No Results Found"}

        result = {
            **base_result,
            "result": data,
            "total_actions": len(data)
        }
        logger.info("Search completed", extra={"action_count": len(data)})
        return result

    except Exception as e:
        logger.error("Error during search", extra={"error": str(e)})
        return {**base_result, "result": [], "error": str(e)}

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
        error = f"Invalid JSON structure in {file_path}"
        logger.error(error)
        return False, error
    
    if "claim" not in data:
        error = f"Missing 'claim' object in {file_path}"
        logger.error(error)
        return False, error
    
    claim = data.get("claim", {})
    if not isinstance(claim, dict):
        error = f"Invalid 'claim' structure in {file_path}"
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
    
    logger.debug("JSON data validated successfully")
    return True, ""

def process_name(first_name: str, last_name: str, output_dir: str = output_folder, 
                headless: Optional[bool] = None, wait_time: Optional[int] = None, 
                logger: Logger = logger) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """
    Search for SEC enforcement actions for a single name.

    Args:
        first_name (str): First name to search.
        last_name (str): Last name to search.
        output_dir (str): Directory for output files. Defaults to output_folder.
        headless (Optional[bool]): Whether to run browser in headless mode. If None, uses default or env var.
        wait_time (Optional[int]): Time to wait between requests in seconds. If None, no wait.
        logger (Logger): Logger instance for structured logging. Defaults to module logger.

    Returns:
        Tuple[Dict[str, Any], Dict[str, int]]: (result, stats)
    """
    claim_data = {
        "first_name": first_name,
        "last_name": last_name
    }
    logger.debug("Processing single name", extra={"first_name": first_name, "last_name": last_name})
    results, stats = process_claim(claim_data, output_dir, headless, wait_time, logger)
    
    if isinstance(results, list) and len(results) > 0:
        return results[0], stats
    
    return {
        "first_name": first_name,
        "last_name": last_name,
        "result": "No Results Found"
    }, stats

def process_claim(claim_data: Dict[str, Any], output_dir: str = output_folder, 
                 headless: Optional[bool] = None, wait_time: Optional[int] = None, 
                 logger: Logger = logger) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Process a claim with possible alternate names to search.

    Args:
        claim_data (Dict[str, Any]): Claim data with 'first_name', 'last_name', and optional 'search_evaluation'.
        output_dir (str): Directory for output files. Defaults to output_folder.
        headless (Optional[bool]): Whether to run browser in headless mode. If None, uses default or env var.
        wait_time (Optional[int]): Time to wait between requests in seconds. If None, no wait.
        logger (Logger): Logger instance for structured logging. Defaults to module logger.

    Returns:
        Tuple[List[Dict[str, Any]], Dict[str, int]]: (results, stats)
    """
    stats = {
        'individuals_searched': 1,
        'total_searches': 0,
        'no_enforcement_actions': 0,
        'enforcement_actions': 0,
        'errors': 0
    }
    logger.debug("Processing claim", extra={"claim_data": claim_data})

    try:
        is_valid, error = validate_json_data({"claim": claim_data}, "claim_data", logger)
        if not is_valid:
            raise ValueError(error)

        alternate_names = claim_data.get("search_evaluation", {}).get("individual", {}).get("ind_other_names", [])
        all_names = [(claim_data["first_name"], claim_data["last_name"])] + [
            (name.split()[0], name.split()[-1]) for name in alternate_names
        ]

        with get_driver(headless) as driver:
            results: List[Dict[str, Any]] = []
            for first_name, last_name in all_names:
                stats['total_searches'] += 1
                result = search_individual(driver, first_name, last_name, logger)
                results.append(result)

                if result.get("error"):
                    stats['errors'] += 1
                elif result.get("result") == "No Results Found":
                    stats['no_enforcement_actions'] += 1
                else:
                    stats['enforcement_actions'] += 1
                
                if wait_time:
                    logger.debug("Waiting between searches", extra={"wait_time": wait_time})
                    time.sleep(wait_time)

            return results, stats

    except Exception as e:
        stats['errors'] += 1
        logger.error("Error processing claim", extra={"error": str(e)})
        return [], stats

def batch_process_folder(input_dir: str = input_folder, output_dir: str = output_folder, 
                       headless: bool = True, logger: Logger = logger) -> Dict[str, Any]:
    """
    Process all JSON files in the input directory.

    Args:
        input_dir (str): Directory containing input JSON files. Defaults to input_folder.
        output_dir (str): Directory for output files. Defaults to output_folder.
        headless (bool): Whether to run browser in headless mode. Defaults to True.
        logger (Logger): Logger instance for structured logging. Defaults to module logger.

    Returns:
        Dict[str, Any]: Processing statistics including skipped files.
    """
    stats = {
        'individuals_searched': 0,
        'total_searches': 0,
        'no_enforcement_actions': 0,
        'enforcement_actions': 0,
        'errors': 0,
        'skipped_files': []
    }
    logger.info("Starting batch processing", extra={"input_dir": input_dir})

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(input_dir, exist_ok=True)

    json_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]
    if not json_files:
        logger.warning("No JSON files found in input folder")
        return stats

    for json_file in json_files:
        file_path = os.path.join(input_dir, json_file)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            is_valid, error = validate_json_data(data, file_path, logger)
            if not is_valid:
                logger.error("Skipping file due to validation error", extra={"file_path": file_path, "error": error})
                stats['skipped_files'].append(file_path)
                continue

            results, claim_stats = process_claim(data["claim"], output_dir, headless, logger=logger)
            stats['individuals_searched'] += claim_stats['individuals_searched']
            stats['total_searches'] += claim_stats['total_searches']
            stats['no_enforcement_actions'] += claim_stats['no_enforcement_actions']
            stats['enforcement_actions'] += claim_stats['enforcement_actions']
            stats['errors'] += claim_stats['errors']

        except Exception as e:
            logger.error("Error processing file", extra={"file_path": file_path, "error": str(e)})
            stats['skipped_files'].append(file_path)

    logger.info("Batch processing completed", extra=stats)
    return stats

def main() -> None:
    parser = argparse.ArgumentParser(description='Search SEC enforcement actions')
    parser.add_argument('--first-name', help='First name to search')
    parser.add_argument('--last-name', help='Last name to search')
    parser.add_argument('--batch', action='store_true', help='Process all JSON files in drop folder')
    parser.add_argument('--headless', action='store_true', default=True, help='Run in headless mode')
    
    args = parser.parse_args()
    
    if args.first_name and args.last_name:
        claim_data = {"first_name": args.first_name, "last_name": args.last_name}
        results, _ = process_claim(claim_data, headless=args.headless, logger=logger)
        if isinstance(results, list):
            total_actions = sum(1 for r in results if isinstance(r.get('result'), list))
            logger.info("Search results", extra={"total_actions": total_actions})
            print(f"\nFound {total_actions} SEC enforcement action(s)")
        elif isinstance(results, dict):
            if isinstance(results.get('result'), list):
                logger.info("Search results", extra={"action_count": len(results['result'])})
                print(f"\nFound {len(results['result'])} SEC enforcement action(s)")
            else:
                logger.info("No enforcement actions found")
                print("\nNo SEC enforcement actions found")
    else:
        batch_process_folder(headless=args.headless, logger=logger)

if __name__ == "__main__":
    main()