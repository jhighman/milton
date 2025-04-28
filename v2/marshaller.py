"""
Marshaller module for handling agent operations and caching.
"""

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
from storage_providers.factory import StorageProviderFactory
from main_config import get_storage_config, load_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("Marshaller")

# Configuration
CACHE_TTL_DAYS = 90
DATE_FORMAT = "%Y%m%d"
MANIFEST_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
RUN_HEADLESS = True
REQUEST_LOG_FILE = "request_log.txt"

# Initialize storage provider
try:
    config = load_config()
    logger.debug(f"Loaded config: {config}")
    storage_config = get_storage_config(config)
    logger.debug(f"Storage config: {storage_config}")
    
    provider_config = {
        'mode': storage_config.get('mode', 'local'),
        'local': {
            'base_path': storage_config.get('local', {}).get('base_path', str(Path.cwd())),
            'input_folder': storage_config.get('local', {}).get('input_folder', 'drop'),
            'output_folder': storage_config.get('local', {}).get('output_folder', 'output'),
            'archive_folder': storage_config.get('local', {}).get('archive_folder', 'archive'),
            'cache_folder': storage_config.get('local', {}).get('cache_folder', 'cache')
        },
        's3': storage_config.get('s3', {})
    }
    
    storage_provider = StorageProviderFactory.create_provider(provider_config)
    logger.info("Storage provider initialized successfully")
    
    # Get cache folder from config and ensure it exists
    CACHE_FOLDER = Path(provider_config['local']['cache_folder'])
    storage_provider.create_directory(str(CACHE_FOLDER))
    
except Exception as e:
    logger.error(f"Failed to initialize storage provider: {str(e)}", exc_info=True)
    raise

# WebDriver setup
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
    search_individual_detailed_info as finra_bc_detailed,
    search_individual_by_firm as finra_bc_search_by_firm
)
from agents.sec_arbitration_agent import search_individual as sec_arb_search
from agents.finra_disciplinary_agent import search_individual as finra_disc_search
from agents.nfa_basic_agent import (
    search_individual as nfa_search,
    search_nfa as nfa_id_search
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
        "search_individual_detailed_info": finra_bc_detailed,
        "search_individual_by_firm": finra_bc_search_by_firm
    },
    "SEC_Arbitration_Agent": {
        "search_individual": sec_arb_search
    },
    "FINRA_Disciplinary_Agent": {
        "search_individual": finra_disc_search
    },
    "NFA_Basic_Agent": {
        "search_individual": nfa_search,
        "search_nfa": nfa_id_search
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

def is_cache_valid(cache_path: Union[str, Path], max_age_hours: int = 24) -> bool:
    """Check if cached data is still valid.
    
    Args:
        cache_path: Path to the cache directory
        max_age_hours: Maximum age of cache in hours
        
    Returns:
        True if cache is valid, False otherwise
    """
    try:
        manifest = read_manifest(cache_path)
        if not manifest:
            return False
            
        cache_time = datetime.fromisoformat(manifest.get('timestamp', ''))
        age_limit = datetime.now() - timedelta(hours=max_age_hours)
        return cache_time > age_limit
        
    except Exception as e:
        logger.error(f"Error checking cache validity for {cache_path}: {str(e)}", exc_info=True)
        return False

def build_cache_path(employee_number: str, agent_name: str, service: str) -> Path:
    return CACHE_FOLDER / employee_number / agent_name / service

def build_file_name(agent_name: str, employee_number: str, service: str, date: str, ordinal: Optional[int] = None) -> str:
    base = f"{agent_name}_{employee_number}_{service}_{date}"
    return f"{base}_{ordinal}.json" if ordinal is not None else f"{base}.json"

def read_manifest(cache_path: Union[str, Path]) -> Optional[Dict[str, Any]]:
    """Read the manifest file for a cached result.
    
    Args:
        cache_path: Path to the cache directory
        
    Returns:
        Manifest data if found, None otherwise
    """
    try:
        manifest_path_str = str(Path(cache_path) / "manifest.json")
        if storage_provider.file_exists(manifest_path_str):
            content = storage_provider.read_file(manifest_path_str)
            if isinstance(content, bytes):
                return json.loads(content.decode('utf-8'))
            return json.loads(content)
        return None
    except Exception as e:
        logger.error(f"Error reading manifest at {cache_path}: {str(e)}", exc_info=True)
        return None

def write_manifest(cache_path: Union[str, Path], data: Dict[str, Any]) -> bool:
    """Write the manifest file for a cached result.
    
    Args:
        cache_path: Path to the cache directory
        data: Manifest data to write
        
    Returns:
        True if successful, False otherwise
    """
    try:
        manifest_path_str = str(Path(cache_path) / "manifest.json")
        manifest_content = json.dumps(data, indent=2)
        return storage_provider.write_file(manifest_path_str, manifest_content)
    except Exception as e:
        logger.error(f"Error writing manifest to {cache_path}: {str(e)}", exc_info=True)
        return False

def load_cached_data(cache_path: Path, is_multiple: bool = False) -> Union[Optional[Dict], List[Dict]]:
    """Load cached data from the specified path."""
    cache_path_str = str(cache_path)
    if not storage_provider.file_exists(cache_path_str):
        logger.debug(f"Cache directory not found: {cache_path}")
        return None if not is_multiple else []
    try:
        if is_multiple:
            results = []
            json_files = storage_provider.list_files(cache_path_str, "*.json")
            if not json_files:
                logger.debug(f"No JSON files in cache directory: {cache_path}")
                return []
            for file_path in sorted(json_files):
                try:
                    content = storage_provider.read_file(file_path).decode().strip()
                    if not content:
                        logger.warning(f"Empty cache file: {file_path}")
                        continue
                    results.append(json.loads(content))
                except Exception as e:
                    logger.error(f"Error reading cache file {file_path}: {e}")
            return results if results else []
        else:
            json_files = storage_provider.list_files(cache_path_str, "*.json")
            if not json_files:
                logger.debug(f"No JSON files in cache directory: {cache_path}")
                return None
            try:
                content = storage_provider.read_file(json_files[0]).decode().strip()
                if content:
                    return json.loads(content)
            except Exception as e:
                logger.error(f"Error reading cache file {json_files[0]}: {e}")
            return None
    except Exception as e:
        logger.error(f"Error accessing cache directory {cache_path}: {e}")
        return None if not is_multiple else []

def save_cached_data(cache_path: Path, file_name: str, data: Dict) -> None:
    """Save data to cache with the specified file name."""
    storage_provider.create_directory(str(cache_path))
    file_path = str(cache_path / file_name)
    storage_provider.write_file(file_path, json.dumps(data, indent=2))

def save_multiple_results(cache_path: Path, agent_name: str, employee_number: str, service: str, date: str, results: List[Dict]) -> None:
    """Save multiple results, ensuring even empty results are cached."""
    if not results:  # Explicitly handle empty results
        file_name = build_file_name(agent_name, employee_number, service, date, 1)
        save_cached_data(cache_path, file_name, {"hits": {"total": 0, "hits": []}})
    else:
        for i, result in enumerate(results, 1):
            file_name = build_file_name(agent_name, employee_number, service, date, i)
            save_cached_data(cache_path, file_name, result)

def log_request(employee_number: str, agent_name: str, service: str, status: str, duration: float) -> None:
    """Log the request details to a file.
    
    Args:
        employee_number: Employee identifier
        agent_name: Name of the agent making the request
        service: Service being called
        status: Status of the request (success/failure)
        duration: Duration of the request in seconds
    """
    try:
        # Create employee cache directory if it doesn't exist
        employee_cache_dir = CACHE_FOLDER / employee_number
        storage_provider.create_directory(str(employee_cache_dir))
        
        # Construct log path and entry
        log_path = employee_cache_dir / "request_log.txt"
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"{timestamp} - {agent_name}/{service} - {status} in {duration:.2f}s\n"
        
        try:
            # Read existing content if file exists
            existing_content = ""
            if storage_provider.file_exists(str(log_path)):
                content = storage_provider.read_file(str(log_path))
                if isinstance(content, bytes):
                    existing_content = content.decode('utf-8')
                else:
                    existing_content = str(content)
            
            # Combine and write content
            full_content = existing_content + log_entry
            storage_provider.write_file(str(log_path), full_content)
            logger.debug(f"Successfully logged request to {log_path}")
            
        except Exception as e:
            logger.error(f"Error handling log file {log_path}: {str(e)}", exc_info=True)
            # Create new file if reading/appending fails
            storage_provider.write_file(str(log_path), log_entry)
            logger.debug(f"Created new log file at {log_path}")
            
    except Exception as e:
        logger.error(f"Failed to log request for {employee_number}: {str(e)}", exc_info=True)
        # Don't raise the exception to avoid interrupting the main flow

def fetch_agent_data(agent_name: str, service: str, params: Dict[str, Any], driver: Optional[webdriver.Chrome] = None) -> tuple[Union[Optional[Dict], List[Dict]], Optional[float]]:
    """Fetch data from the specified agent service, ensuring a single-item list for single-result agents."""
    try:
        agent_fn = AGENT_SERVICES[agent_name][service]
        start_time = time.time()
        
        # Parameter adjustments for specific services
        if service == "search_individual_by_firm":
            if "organization_crd_number" in params:
                params["organization_crd"] = params.pop("organization_crd_number")
            if "individual_name" not in params:
                raise ValueError(f"Missing required 'individual_name' for {agent_name}/{service}")
            if "organization_crd" not in params:
                raise ValueError(f"Missing required 'organization_crd' for {agent_name}/{service}")

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
        result_size = len(result) if isinstance(result, list) else 1 if result else 0
        logger.debug(f"Fetched {agent_name}/{service}: result size = {result_size}")
        
        # Handle empty or no-hits responses for single-result agents
        empty_result = {"hits": {"total": 0, "hits": []}}
        if agent_name in ["SEC_IAPD_Agent", "FINRA_BrokerCheck_Agent"]:
            if not result:  # Handles None or falsy results
                logger.warning(f"No data returned for {agent_name}/{service} with params={params}")
                return [empty_result], duration
            if isinstance(result, list) and not result:  # Handles empty list
                logger.warning(f"Empty list returned for {agent_name}/{service} with params={params}")
                return [empty_result], duration
            if isinstance(result, dict) and result.get("hits", {}).get("total", 0) == 0:  # Handles no-hits dictionary
                logger.warning(f"No valid data returned for {agent_name}/{service} with params={params}")
                return [result], duration
            return [result] if isinstance(result, dict) else result, duration
        else:
            # For multi-result agents (e.g., arbitration), return as-is
            return result if isinstance(result, list) else [result] if result else [], duration
    except Exception as e:
        logger.error(f"Agent {agent_name} service {service} failed: {str(e)}")
        # Return empty result for single-result agents, empty list for multi-result agents
        return [empty_result] if agent_name in ["SEC_IAPD_Agent", "FINRA_BrokerCheck_Agent"] else [], None

def check_cache_or_fetch(
    agent_name: str, service: str, employee_number: str, params: Dict[str, Any], driver: Optional[webdriver.Chrome] = None
) -> Union[Optional[Dict], List[Dict]]:
    """Check cache or fetch data, handling single-result and multi-result agents appropriately."""
    if not employee_number or employee_number.strip() == "":
        logger.error(f"Invalid employee_number: '{employee_number}' for agent {agent_name}/{service}")
        raise ValueError(f"employee_number must be a non-empty string, got '{employee_number}'")
    
    cache_path = build_cache_path(employee_number, agent_name, service)
    date = get_current_date()
    
    # Ensure cache directory exists
    storage_provider.create_directory(str(cache_path))

    cached_date = read_manifest(cache_path)
    is_multiple = agent_name not in ["SEC_IAPD_Agent", "FINRA_BrokerCheck_Agent"] and service != "search_individual_by_firm"
    if cached_date and is_cache_valid(cache_path):
        cached_data = load_cached_data(cache_path, is_multiple)
        if cached_data is not None:
            logger.info(f"Cache hit for {agent_name}/{service}/{employee_number}")
            log_request(employee_number, agent_name, service, "Cached", 0)
            return cached_data

    logger.info(f"Cache miss or stale for {agent_name}/{service}/{employee_number}")
    results, fetch_duration = fetch_agent_data(agent_name, service, params, driver)
    log_request(employee_number, agent_name, service, "Fetched", fetch_duration)
    
    file_name = build_file_name(agent_name, employee_number, service, date)
    if agent_name in ["SEC_IAPD_Agent", "FINRA_BrokerCheck_Agent"]:
        # Save and return single result or empty result
        result_to_save = results[0] if results else {"hits": {"total": 0, "hits": []}}
        save_cached_data(cache_path, file_name, result_to_save)
        write_manifest(cache_path, {"timestamp": get_manifest_timestamp()})
        return result_to_save
    else:
        # Handle multi-result agents
        save_multiple_results(cache_path, agent_name, employee_number, service, date, results)
        write_manifest(cache_path, {"timestamp": get_manifest_timestamp()})
        return results

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
fetch_agent_finra_bc_search_by_firm = create_fetcher("FINRA_BrokerCheck_Agent", "search_individual_by_firm")
fetch_agent_sec_arb_search = create_fetcher("SEC_Arbitration_Agent", "search_individual")
fetch_agent_finra_disc_search = create_fetcher("FINRA_Disciplinary_Agent", "search_individual")
fetch_agent_nfa_search = create_fetcher("NFA_Basic_Agent", "search_individual")
fetch_agent_nfa_id_search = create_fetcher("NFA_Basic_Agent", "search_nfa")
fetch_agent_finra_arb_search = create_fetcher("FINRA_Arbitration_Agent", "search_individual")
fetch_agent_sec_iapd_correlated = create_fetcher("SEC_IAPD_Agent", "search_individual_by_firm")
fetch_agent_sec_disc_search = create_fetcher("SEC_Disciplinary_Agent", "search_individual")

class Marshaller:
    """Class to manage browser automation and data fetching operations."""
    
    def __init__(self, headless: bool = True):
        """Initialize the Marshaller with configurable headless mode."""
        self.headless = headless
        self.driver = None
        self._is_driver_managed = False
        self.logger = logging.getLogger("Marshaller")
        self.logger.debug(f"Marshaller initialized with headless={headless}")

    def _ensure_driver(self):
        """Ensure WebDriver is initialized with current headless setting."""
        if not self.driver:
            self.driver = create_driver(headless=self.headless)
            self._is_driver_managed = True
            self.logger.debug("Created new WebDriver instance")

    def cleanup(self):
        """Explicitly close the WebDriver."""
        if self._is_driver_managed and self.driver:
            try:
                self.driver.quit()
                self.logger.info("WebDriver closed successfully")
            except Exception as e:
                self.logger.error(f"Failed to close WebDriver: {str(e)}")
            finally:
                self.driver = None
                self._is_driver_managed = False

    def fetch_data(self, agent_name: str, service: str, employee_number: str, params: Dict[str, Any]) -> Union[Optional[Dict], List[Dict]]:
        """Fetch data using the specified agent and service."""
        self._ensure_driver()
        return check_cache_or_fetch(agent_name, service, employee_number, params, self.driver)

    def __del__(self):
        """Ensure WebDriver is cleaned up when the object is destroyed."""
        self.cleanup()

def main():
    parser = argparse.ArgumentParser(description='Marshaller for Financial Regulatory Agents')
    parser.add_argument('--employee-number', help='Employee number for the search')
    parser.add_argument('--first-name', help='First name for custom search')
    parser.add_argument('--last-name', help='Last name for custom search')
    parser.add_argument('--crd-number', help='CRD number for custom search')
    parser.add_argument('--nfa-id', help='NFA ID for custom search')
    parser.add_argument('--organization-crd', help='Organization CRD number for firm-based search')
    parser.add_argument('--headless', action='store_true', default=RUN_HEADLESS, help='Run in headless mode')
    
    args = parser.parse_args()

    def run_search(agent_fetcher: Callable, employee_number: str, params: Dict[str, Any], driver: Optional[webdriver.Chrome] = None):
        result = agent_fetcher(employee_number, params, driver)
        print(f"{agent_fetcher.__name__.replace('fetch_agent_', '')} Result:", json.dumps(result, indent=2))

    if args.employee_number or args.first_name or args.last_name or args.crd_number or args.nfa_id or args.organization_crd:
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
            if args.first_name and args.organization_crd:
                run_search(fetch_agent_finra_bc_search_by_firm, employee_number, {"individual_name": args.first_name, "organization_crd": args.organization_crd})
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
                    search_type = input("Enter search type (1 for CRD, 2 for name, 3 for NFA ID, 4 for firm search): ").strip()
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
                    elif search_type == "4":
                        individual_name = input("Enter individual name (required): ").strip()
                        organization_crd = input("Enter organization CRD number (required): ").strip()
                        if not individual_name or not organization_crd:
                            print("Both individual name and organization CRD are required.")
                            continue
                        run_search(fetch_agent_finra_bc_search_by_firm, employee_number, {"individual_name": individual_name, "organization_crd": organization_crd})
                    else:
                        print("Invalid search type. Use 1 for CRD, 2 for name, 3 for NFA ID, or 4 for firm search.")
                elif choice == "3":
                    employee_number = "EMP001"
                    print("\nRunning all example searches...")
                    run_search(fetch_agent_sec_iapd_search, employee_number, {"crd_number": "12345"})
                    run_search(fetch_agent_sec_iapd_detailed, employee_number, {"crd_number": "12345"})
                    run_search(fetch_agent_finra_bc_search, employee_number, {"crd_number": "67890"})
                    run_search(fetch_agent_finra_bc_detailed, employee_number, {"crd_number": "67890"})
                    run_search(fetch_agent_finra_bc_search_by_firm, employee_number, {"individual_name": "John Doe", "organization_crd": "123456"})
                    run_search(fetch_agent_sec_arb_search, employee_number, {"first_name": "Alice", "last_name": "Johnson"}, driver)
                    run_search(fetch_agent_finra_disc_search, employee_number, {"first_name": "John", "last_name": "Doe"}, driver)
                    run_search(fetch_agent_nfa_search, employee_number, {"first_name": "Jane", "last_name": "Smith"}, driver)
                    run_search(fetch_agent_nfa_id_search, employee_number, {"nfa_id": "1234567"}, driver)
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