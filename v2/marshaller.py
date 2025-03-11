import argparse
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Callable, List, Union
import json
import logging
import time
from functools import partial
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("Marshaller")

# Configuration
CACHE_FOLDER = Path(os.path.dirname(os.path.abspath(__file__))) / "cache"
CACHE_TTL_DAYS = 90
DATE_FORMAT = "%Y%m%d"
MANIFEST_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
RUN_HEADLESS = True
REQUEST_LOG_FILE = "request_log.txt"


# WebDriver setup (shared across Selenium agents)
def create_driver(headless: bool = RUN_HEADLESS, logger: logging.Logger = logger) -> webdriver.Chrome:
    logger.debug("Initializing Chrome WebDriver", extra={"headless": headless})
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/115.0.0.0 Safari/537.36")
    try:
        driver = webdriver.Chrome(service=ChromeService(), options=options)
        logger.info("Chrome WebDriver initialized successfully")
        return driver
    except Exception as e:
        logger.error(f"Failed to initialize Chrome WebDriver: {str(e)}")
        raise
# Import agent functions
from agents.sec_iapd_agent import (
    search_individual as sec_iapd_search,
    search_individual_detailed_info as sec_iapd_search_detailed,
    search_individual_by_firm as sec_iapd_correlated_search
)
from agents.finra_broker_check_agent import (
    search_individual as finra_bc_search, 
    search_individual_detailed_info as finra_bc_detailed
)
from agents.sec_arbitration_agent import search_individual as sec_arb_search
from agents.finra_disciplinary_agent import search_individual as finra_disc_search
from agents.nfa_basic_agent import (
    search_individual as nfa_search,
    search_nfa as nfa_id_search  # Added new import
)
from agents.finra_arbitration_agent import search_individual as finra_arb_search
from agents.sec_disciplinary_agent import search_individual as sec_disc_search

# Agent service mapping
AGENT_SERVICES: Dict[str, Dict[str, Callable]] = {
    "SEC_IAPD_Agent": {
        "search_individual": sec_iapd_search,
        "search_individual_detailed_info": sec_iapd_search_detailed,
        "search_individual_by_firm": sec_iapd_correlated_search
    },
    "FINRA_BrokerCheck_Agent": {
        "search_individual": finra_bc_search,
        "search_individual_detailed_info": finra_bc_detailed
    },
    "SEC_Arbitration_Agent": {
        "search_individual": sec_arb_search
    },
    "FINRA_Disciplinary_Agent": {
        "search_individual": finra_disc_search
    },
    "NFA_Basic_Agent": {
        "search_individual": nfa_search,
        "search_nfa": nfa_id_search  # Added new service
    },
    "FINRA_Arbitration_Agent": {
        "search_individual": finra_arb_search
    },
    "SEC_Disciplinary_Agent": {
        "search_individual": sec_disc_search
    }
}

# Pure functions

def get_current_date() -> str:
    return datetime.now().strftime(DATE_FORMAT)

def get_manifest_timestamp() -> str:
    return datetime.now().strftime(MANIFEST_DATE_FORMAT)

def is_cache_valid(cached_date: str) -> bool:
    try:
        cached_datetime = datetime.strptime(cached_date, DATE_FORMAT)
        return (datetime.now() - cached_datetime) <= timedelta(days=CACHE_TTL_DAYS)
    except ValueError:
        logger.warning(f"Invalid date format in cache manifest: {cached_date}")
        return False

def build_cache_path(employee_number: str, agent_name: str, service: str) -> Path:
    return CACHE_FOLDER / employee_number / agent_name / service

def build_file_name(agent_name: str, employee_number: str, service: str, date: str, ordinal: Optional[int] = None) -> str:
    base = f"{agent_name}_{employee_number}_{service}_{date}"
    return f"{base}_{ordinal}.json" if ordinal is not None else f"{base}.json"

def read_manifest(cache_path: Path) -> Optional[str]:
    manifest_path = cache_path / "manifest.txt"
    if manifest_path.exists():
        with manifest_path.open("r") as f:
            line = f.readline().strip()
            if line and "Cached on: " in line:
                try:
                    return line.split("Cached on: ")[1].split(" ")[0].replace("-", "")
                except IndexError:
                    logger.warning(f"Malformed manifest file at {manifest_path}: {line}")
                    return None
    return None

def write_manifest(cache_path: Path, timestamp: str) -> None:
    """Write a manifest file with a consistent 'Cached on: ' prefix."""
    cache_path.mkdir(parents=True, exist_ok=True)
    manifest_path = cache_path / "manifest.txt"
    with manifest_path.open("w") as f:
        f.write(f"Cached on: {timestamp}")

def load_cached_data(cache_path: Path, is_multiple: bool = False) -> Union[Optional[Dict], List[Dict]]:
    if not cache_path.exists():
        logger.debug(f"Cache directory not found: {cache_path}")
        return None if not is_multiple else []
    try:
        if is_multiple:
            results = []
            json_files = sorted(cache_path.glob("*.json"))
            if not json_files:
                logger.debug(f"No JSON files in cache directory: {cache_path}")
                return []
            for file_path in json_files:
                with file_path.open("r") as f:
                    content = f.read().strip()
                    if not content:
                        logger.warning(f"Empty cache file: {file_path}")
                        continue
                    results.append(json.loads(content))
            return results if results else []
        else:
            json_files = list(cache_path.glob("*.json"))
            if not json_files:
                logger.debug(f"No JSON files in cache directory: {cache_path}")
                return None
            with json_files[0].open("r") as f:
                content = f.read().strip()
                if not content:
                    logger.warning(f"Empty cache file: {json_files[0]}")
                    return None
                return json.loads(content)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON in cache file at {cache_path}: {e}")
        return None if not is_multiple else []
    except Exception as e:
        logger.error(f"Error reading cache file at {cache_path}: {e}")
        return None if not is_multiple else []

def save_cached_data(cache_path: Path, file_name: str, data: Dict) -> None:
    cache_path.mkdir(parents=True, exist_ok=True)
    file_path = cache_path / file_name
    with file_path.open("w") as f:
        json.dump(data, f, indent=2)

def save_multiple_results(cache_path: Path, agent_name: str, employee_number: str, service: str, date: str, results: List[Dict]) -> None:
    """Save multiple results, ensuring even empty results are cached."""
    if not results:  # Explicitly handle empty results
        file_name = build_file_name(agent_name, employee_number, service, date, 1)
        save_cached_data(cache_path, file_name, {"result": "No Results Found"})
    else:
        for i, result in enumerate(results, 1):
            file_name = build_file_name(agent_name, employee_number, service, date, i)
            save_cached_data(cache_path, file_name, result)

def log_request(employee_number: str, agent_name: str, service: str, status: str, duration: Optional[float] = None) -> None:
    log_path = CACHE_FOLDER / employee_number / REQUEST_LOG_FILE
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {agent_name}/{service} - {status}"
    if duration is not None:
        log_entry += f" (fetch duration: {duration:.2f}s)"
    log_entry += "\n"
    with log_path.open("a") as f:
        f.write(log_entry)

def fetch_agent_data(agent_name: str, service: str, params: Dict[str, Any], driver: Optional[webdriver.Chrome] = None) -> tuple[Union[Optional[Dict], List[Dict]], Optional[float]]:
    try:
        agent_fn = AGENT_SERVICES[agent_name][service]
        start_time = time.time()
        
        if service == "search_individual_by_firm":
            if "organization_crd_number" in params:
                params["organization_crd"] = params.pop("organization_crd_number")
        
        if agent_name in SELENIUM_AGENTS:
            if driver is None:
                raise ValueError(f"Agent {agent_name} requires a WebDriver instance")
            try:
                result = agent_fn(**params, driver=driver, logger=logger)
            except TypeError:
                result = agent_fn(**params, logger=logger)
        else:
            result = agent_fn(**params, logger=logger)
        
        duration = time.time() - start_time
        logger.debug(f"Fetched {agent_name}/{service}: result size = {len(result) if isinstance(result, list) else 1 if result else 0}")
        return result if isinstance(result, list) else [result] if result else [], duration
    except Exception as e:
        logger.error(f"Agent {agent_name} service {service} failed: {str(e)}")
        return [], None

def check_cache_or_fetch(
    agent_name: str, service: str, employee_number: str, params: Dict[str, Any], driver: Optional[webdriver.Chrome] = None
) -> Union[Optional[Dict], List[Dict]]:
    if not employee_number or employee_number.strip() == "":
        logger.error(f"Invalid employee_number: '{employee_number}' for agent {agent_name}/{service}")
        raise ValueError(f"employee_number must be a non-empty string, got '{employee_number}'")
    
    cache_path = build_cache_path(employee_number, agent_name, service)
    date = get_current_date()
    cache_path.mkdir(parents=True, exist_ok=True)

    cached_date = read_manifest(cache_path)
    is_multiple = agent_name not in ["SEC_IAPD_Agent", "FINRA_BrokerCheck_Agent"]
    if cached_date and is_cache_valid(cached_date):
        cached_data = load_cached_data(cache_path, is_multiple)
        if cached_data is not None:
            logger.info(f"Cache hit for {agent_name}/{service}/{employee_number}")
            log_request(employee_number, agent_name, service, "Cached")
            return cached_data

    logger.info(f"Cache miss or stale for {agent_name}/{service}/{employee_number}")
    results, fetch_duration = fetch_agent_data(agent_name, service, params, driver)
    log_request(employee_number, agent_name, service, "Fetched", fetch_duration)
    
    file_name = build_file_name(agent_name, employee_number, service, date)
    if agent_name in ["SEC_IAPD_Agent", "FINRA_BrokerCheck_Agent"]:
        save_cached_data(cache_path, file_name, results[0] if results else {"result": "No Results Found"})
    else:
        save_multiple_results(cache_path, agent_name, employee_number, service, date, results)
    write_manifest(cache_path, get_manifest_timestamp())
    
    return results[0] if len(results) == 1 else results

# Higher-order function to create service-specific fetchers
def create_fetcher(agent_name: str, service: str) -> Callable[[str, Dict[str, Any], Optional[webdriver.Chrome]], Union[Optional[Dict], List[Dict]]]:
    return lambda employee_number, params, driver=None: check_cache_or_fetch(agent_name, service, employee_number, params, driver)

# Define Selenium-dependent agents
SELENIUM_AGENTS = {
    "SEC_Arbitration_Agent",
    "NFA_Basic_Agent",
    "FINRA_Arbitration_Agent",
    "SEC_Disciplinary_Agent"
}

# Fetcher functions for all agent services
fetch_agent_sec_iapd_search = create_fetcher("SEC_IAPD_Agent", "search_individual")
fetch_agent_sec_iapd_detailed = create_fetcher("SEC_IAPD_Agent", "search_individual_detailed_info")
fetch_agent_finra_bc_search = create_fetcher("FINRA_BrokerCheck_Agent", "search_individual")
fetch_agent_finra_bc_detailed = create_fetcher("FINRA_BrokerCheck_Agent", "search_individual_detailed_info")
fetch_agent_sec_arb_search = create_fetcher("SEC_Arbitration_Agent", "search_individual")
fetch_agent_finra_disc_search = create_fetcher("FINRA_Disciplinary_Agent", "search_individual")
fetch_agent_nfa_search = create_fetcher("NFA_Basic_Agent", "search_individual")
fetch_agent_nfa_id_search = create_fetcher("NFA_Basic_Agent", "search_nfa")  # Added new fetcher
fetch_agent_finra_arb_search = create_fetcher("FINRA_Arbitration_Agent", "search_individual")
fetch_agent_sec_iapd_correlated = create_fetcher("SEC_IAPD_Agent", "search_individual_by_firm")
fetch_agent_sec_disc_search = create_fetcher("SEC_Disciplinary_Agent", "search_individual")

def main():
    parser = argparse.ArgumentParser(description='Marshaller for Financial Regulatory Agents')
    parser.add_argument('--employee-number', help='Employee number for the search')
    parser.add_argument('--first-name', help='First name for custom search')
    parser.add_argument('--last-name', help='Last name for custom search')
    parser.add_argument('--crd-number', help='CRD number for custom search')
    parser.add_argument('--nfa-id', help='NFA ID for custom search')  # Added new argument
    parser.add_argument('--headless', action='store_true', default=RUN_HEADLESS, help='Run in headless mode')
    
    args = parser.parse_args()

    def run_search(agent_fetcher: Callable, employee_number: str, params: Dict[str, Any], driver: Optional[webdriver.Chrome] = None):
        result = agent_fetcher(employee_number, params, driver)
        print(f"{agent_fetcher.__name__.replace('fetch_agent_', '')} Result:", json.dumps(result, indent=2))

    if args.employee_number or args.first_name or args.last_name or args.crd_number or args.nfa_id:
        employee_number = args.employee_number or "EMP001"
        driver = create_driver(args.headless)
        try:
            if args.crd_number:
                run_search(fetch_agent_sec_iapd_search, employee_number, {"crd_number": args.crd_number})
                run_search(fetch_agent_finra_bc_search, employee_number, {"crd_number": args.crd_number})
            if args.first_name and args.last_name:
                run_search(fetch_agent_sec_arb_search, employee_number, {"first_name": args.first_name, "last_name": args.last_name}, driver)
                run_search(fetch_agent_finra_disc_search, employee_number, {"first_name": args.first_name, "last_name": args.last_name}, driver)
                run_search(fetch_agent_nfa_search, employee_number, {"first_name": args.first_name, "last_name": args.last_name}, driver)
                run_search(fetch_agent_finra_arb_search, employee_number, {"first_name": args.first_name, "last_name": args.last_name}, driver)
                run_search(fetch_agent_sec_disc_search, employee_number, {"first_name": args.first_name, "last_name": args.last_name}, driver)
            if args.nfa_id:
                run_search(fetch_agent_nfa_id_search, employee_number, {"nfa_id": args.nfa_id}, driver)
        finally:
            driver.quit()
            logger.info("WebDriver closed")
    else:
        driver = create_driver(RUN_HEADLESS)
        try:
            while True:
                print("\nMarshaller Menu:")
                print("1. Run local test with 'Mark Miller' (SEC Disciplinary)")
                print("2. Perform custom search")
                print("3. Run all example searches")
                print("4. Exit")
                choice = input("Enter your choice (1-4): ").strip()

                if choice == "1":
                    print("\nRunning local test with 'Mark Miller'...")
                    run_search(fetch_agent_sec_disc_search, "EMP_TEST", {"first_name": "Mark", "last_name": "Miller"}, driver)
                elif choice == "2":
                    employee_number = input("Enter employee number (e.g., EMP001): ").strip() or "EMP001"
                    search_type = input("Enter search type (1 for CRD, 2 for name, 3 for NFA ID): ").strip()
                    if search_type == "1":
                        crd_number = input("Enter CRD number: ").strip()
                        if crd_number:
                            run_search(fetch_agent_sec_iapd_search, employee_number, {"crd_number": crd_number})
                            run_search(fetch_agent_finra_bc_search, employee_number, {"crd_number": crd_number})
                        else:
                            print("CRD number is required for this search type.")
                    elif search_type == "2":
                        first_name = input("Enter first name (optional, press Enter to skip): ").strip()
                        last_name = input("Enter last name (required): ").strip()
                        if not last_name:
                            print("Last name is required.")
                            continue
                        run_search(fetch_agent_sec_arb_search, employee_number, {"first_name": first_name, "last_name": last_name}, driver)
                        run_search(fetch_agent_finra_disc_search, employee_number, {"first_name": first_name, "last_name": last_name}, driver)
                        run_search(fetch_agent_nfa_search, employee_number, {"first_name": first_name, "last_name": last_name}, driver)
                        run_search(fetch_agent_finra_arb_search, employee_number, {"first_name": first_name, "last_name": last_name}, driver)
                        run_search(fetch_agent_sec_disc_search, employee_number, {"first_name": first_name, "last_name": last_name}, driver)
                    elif search_type == "3":
                        nfa_id = input("Enter NFA ID: ").strip()
                        if not nfa_id:
                            print("NFA ID is required.")
                            continue
                        run_search(fetch_agent_nfa_id_search, employee_number, {"nfa_id": nfa_id}, driver)
                    else:
                        print("Invalid search type. Use 1 for CRD, 2 for name, or 3 for NFA ID.")
                elif choice == "3":
                    employee_number = "EMP001"
                    print("\nRunning all example searches...")
                    run_search(fetch_agent_sec_iapd_search, employee_number, {"crd_number": "12345"})
                    run_search(fetch_agent_sec_iapd_detailed, employee_number, {"crd_number": "12345"})
                    run_search(fetch_agent_finra_bc_search, employee_number, {"crd_number": "67890"})
                    run_search(fetch_agent_finra_bc_detailed, employee_number, {"crd_number": "67890"})
                    run_search(fetch_agent_sec_arb_search, employee_number, {"first_name": "Alice", "last_name": "Johnson"}, driver)
                    run_search(fetch_agent_finra_disc_search, employee_number, {"first_name": "John", "last_name": "Doe"}, driver)
                    run_search(fetch_agent_nfa_search, employee_number, {"first_name": "Jane", "last_name": "Smith"}, driver)
                    run_search(fetch_agent_nfa_id_search, employee_number, {"nfa_id": "1234567"}, driver)  # Added to example searches
                    run_search(fetch_agent_finra_arb_search, employee_number, {"first_name": "Bob", "last_name": "Smith"}, driver)
                    run_search(fetch_agent_sec_iapd_correlated, employee_number, {"individual_name": "Matthew Vetto", "organization_crd": "282563"})
                    run_search(fetch_agent_sec_disc_search, employee_number, {"first_name": "Mark", "last_name": "Miller"}, driver)
                elif choice == "4":
                    print("Exiting...")
                    break
                else:
                    print("Invalid choice. Please enter 1, 2, 3, or 4.")
        finally:
            driver.quit()
            logger.info("WebDriver closed")

if __name__ == "__main__":
    main()