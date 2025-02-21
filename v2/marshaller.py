import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Callable, List, Union
import json
import logging
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

# WebDriver setup (shared across Selenium agents)
def create_driver(headless: bool = RUN_HEADLESS, logger: logging.Logger = logger) -> webdriver.Chrome:
    logger.debug("Initializing Chrome WebDriver", extra={"headless": headless})
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/115.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=ChromeService(), options=options)

# Import agent functions (assuming they exist in your modules)
from agents.sec_iapd_agent import search_individual as sec_iapd_search, search_individual_detailed_info as sec_iapd_search_detailed
from agents.finra_broker_check_agent import search_individual as finra_bc_search, search_individual_detailed_info as finra_bc_search_detailed
from agents.sec_arbitration_agent import search_individual as sec_arb_search
from agents.finra_disciplinary_agent import search_individual as finra_disc_search
from agents.nfa_basic_agent import search_individual as nfa_search
from agents.finra_arbitration_agent import search_individual as finra_arb_search  # New agent added

# Agent service mapping
AGENT_SERVICES: Dict[str, Dict[str, Callable]] = {
    "SEC_IAPD_Agent": {
        "search_individual": sec_iapd_search,
        "search_individual_detailed_info": sec_iapd_search_detailed
    },
    "FINRA_BrokerCheck_Agent": {
        "search_individual": finra_bc_search,
        "search_individual_detailed_info": finra_bc_search_detailed
    },
    "SEC_Arbitration_Agent": {
        "search_individual": sec_arb_search
    },
    "FINRA_Disciplinary_Agent": {
        "search_individual": finra_disc_search
    },
    "NFA_Basic_Agent": {
        "search_individual": nfa_search
    },
    "FINRA_Arbitration_Agent": {  # New agent added
        "search_individual": finra_arb_search
    },
}

# Pure functions

def get_current_date() -> str:
    return datetime.now().strftime(DATE_FORMAT)

def get_manifest_timestamp() -> str:
    return datetime.now().strftime(MANIFEST_DATE_FORMAT)

def is_cache_valid(cached_date: str) -> bool:
    cached_datetime = datetime.strptime(cached_date, DATE_FORMAT)
    return (datetime.now() - cached_datetime) <= timedelta(days=CACHE_TTL_DAYS)

def build_cache_path(employee_number: str, agent_name: str) -> Path:
    return CACHE_FOLDER / employee_number / agent_name

def build_file_name(agent_name: str, employee_number: str, service: str, date: str, ordinal: Optional[int] = None) -> str:
    base = f"{agent_name}_{employee_number}_{service}_{date}"
    return f"{base}_{ordinal}.json" if ordinal is not None else f"{base}.json"

def read_manifest(cache_path: Path) -> Optional[str]:
    manifest_path = cache_path / "manifest.txt"
    if manifest_path.exists():
        with manifest_path.open("r") as f:
            line = f.readline().strip()
            return line.split("Cached on: ")[1].split(" ")[0].replace("-", "")
    return None

def write_manifest(cache_path: Path, timestamp: str) -> None:
    manifest_path = cache_path / "manifest.txt"
    with manifest_path.open("w") as f:
        f.write(f"Cached on: {timestamp}")

def load_cached_data(cache_path: Path, file_name: str) -> Optional[Dict]:
    file_path = cache_path / file_name
    if file_path.exists():
        with file_path.open("r") as f:
            return json.load(f)
    return None

def save_cached_data(cache_path: Path, file_name: str, data: Dict) -> None:
    cache_path.mkdir(parents=True, exist_ok=True)
    file_path = cache_path / file_name
    with file_path.open("w") as f:
        json.dump(data, f, indent=2)

def save_multiple_results(cache_path: Path, agent_name: str, employee_number: str, service: str, date: str, results: List[Dict]) -> None:
    for i, result in enumerate(results, 1):
        file_name = build_file_name(agent_name, employee_number, service, date, i)
        save_cached_data(cache_path, file_name, result)

def fetch_agent_data(agent_name: str, service: str, params: Dict[str, Any], driver: Optional[webdriver.Chrome] = None) -> Union[Optional[Dict], List[Dict]]:
    """Call the agent service, passing driver if needed."""
    try:
        agent_fn = AGENT_SERVICES[agent_name][service]
        # Pass driver to Selenium-dependent agents
        if agent_name in SELENIUM_AGENTS:
            if driver is None:
                raise ValueError(f"Agent {agent_name} requires a WebDriver instance")
            result = agent_fn(**params, driver=driver, logger=logger)
        else:
            result = agent_fn(**params, logger=logger)
        return result if isinstance(result, list) else [result] if result else []
    except Exception as e:
        logger.error(f"Agent {agent_name} service {service} failed: {str(e)}")
        return []

def check_cache_or_fetch(
    agent_name: str, service: str, employee_number: str, params: Dict[str, Any], driver: Optional[webdriver.Chrome] = None
) -> Union[Optional[Dict], List[Dict]]:
    cache_path = build_cache_path(employee_number, agent_name)
    date = get_current_date()
    file_name = build_file_name(agent_name, employee_number, service, date)

    cached_date = read_manifest(cache_path)
    if cached_date and is_cache_valid(cached_date):
        cached_data = load_cached_data(cache_path, file_name)
        if cached_data:
            logger.info(f"Cache hit for {agent_name}/{service}/{employee_number}")
            return cached_data if agent_name in ["SEC_IAPD_Agent", "FINRA_BrokerCheck_Agent"] else [cached_data]

    logger.info(f"Cache miss or stale for {agent_name}/{service}/{employee_number}")
    results = fetch_agent_data(agent_name, service, params, driver)
    
    if results:
        if agent_name in ["SEC_IAPD_Agent", "FINRA_BrokerCheck_Agent"]:
            save_cached_data(cache_path, file_name, results[0])
        else:
            save_multiple_results(cache_path, agent_name, employee_number, service, date, results)
        write_manifest(cache_path, get_manifest_timestamp())
    
    return results[0] if len(results) == 1 else results

# Higher-order function to create service-specific fetchers
def create_fetcher(agent_name: str, service: str) -> Callable[[str, Dict[str, Any], Optional[webdriver.Chrome]], Union[Optional[Dict], List[Dict]]]:
    return lambda employee_number, params, driver=None: check_cache_or_fetch(agent_name, service, employee_number, params, driver)

# Define Selenium-dependent agents
SELENIUM_AGENTS = {"SEC_Arbitration_Agent", "FINRA_Disciplinary_Agent", "NFA_Basic_Agent", "FINRA_Arbitration_Agent"}

# Fetcher functions for all agent services
fetch_agent_sec_iapd_search = create_fetcher("SEC_IAPD_Agent", "search_individual")
fetch_agent_sec_iapd_detailed = create_fetcher("SEC_IAPD_Agent", "search_individual_detailed_info")
fetch_agent_finra_bc_search = create_fetcher("FINRA_BrokerCheck_Agent", "search_individual")
fetch_agent_finra_bc_detailed = create_fetcher("FINRA_BrokerCheck_Agent", "search_individual_detailed_info")
fetch_agent_sec_arb_search = create_fetcher("SEC_Arbitration_Agent", "search_individual")
fetch_agent_finra_disc_search = create_fetcher("FINRA_Disciplinary_Agent", "search_individual")
fetch_agent_nfa_search = create_fetcher("NFA_Basic_Agent", "search_individual")
fetch_agent_finra_arb_search = create_fetcher("FINRA_Arbitration_Agent", "search_individual")  # New fetcher added

# Example usage demonstrating all agents
def main():
    employee_number = "EMP001"
    if not employee_number:
        logger.error("Employee number is required")
        return

    try:
        # Initialize WebDriver for Selenium agents
        driver = create_driver()

        # 1. SEC IAPD Agent - search_individual (non-Selenium)
        sec_iapd_result = fetch_agent_sec_iapd_search(employee_number, {"crd_number": "12345"})
        print("SEC IAPD Search Result:", sec_iapd_result)

        # 2. SEC IAPD Agent - search_individual_detailed_info (non-Selenium)
        sec_iapd_detailed_result = fetch_agent_sec_iapd_detailed(employee_number, {"crd_number": "12345"})
        print("SEC IAPD Detailed Search Result:", sec_iapd_detailed_result)

        # 3. FINRA BrokerCheck Agent - search_individual (non-Selenium)
        finra_bc_result = fetch_agent_finra_bc_search(employee_number, {"crd_number": "67890"})
        print("FINRA BrokerCheck Search Result:", finra_bc_result)

        # 4. FINRA BrokerCheck Agent - search_individual_detailed_info (non-Selenium)
        finra_bc_detailed_result = fetch_agent_finra_bc_detailed(employee_number, {"crd_number": "67890"})
        print("FINRA BrokerCheck Detailed Search Result:", finra_bc_detailed_result)

        # 5. SEC Arbitration Agent - search_individual (Selenium)
        sec_arb_result = fetch_agent_sec_arb_search(employee_number, {"first_name": "Alice", "last_name": "Johnson"}, driver)
        print("SEC Arbitration Search Result:", sec_arb_result)

        # 6. FINRA Disciplinary Agent - search_individual (Selenium)
        finra_disc_result = fetch_agent_finra_disc_search(employee_number, {"first_name": "John", "last_name": "Doe"}, driver)
        print("FINRA Disciplinary Search Result:", finra_disc_result)

        # 7. NFA Basic Agent - search_individual (Selenium)
        nfa_result = fetch_agent_nfa_search(employee_number, {"first_name": "Jane", "last_name": "Smith"}, driver)
        print("NFA Basic Search Result:", nfa_result)

        # 8. FINRA Arbitration Agent - search_individual (Selenium)
        finra_arb_result = fetch_agent_finra_arb_search(employee_number, {"first_name": "Bob", "last_name": "Smith"}, driver)
        print("FINRA Arbitration Search Result:", finra_arb_result)

    finally:
        driver.quit()
        logger.info("WebDriver closed")

if __name__ == "__main__":
    main()