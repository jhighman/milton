import os
import json
import time
import requests
from typing import Optional, Dict, Tuple, List
from urllib.parse import urlencode
import logging
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# Constants for API configurations (immutable)
API_CONFIG = {
    "BROKERCHECK": {
        "base_url": "https://api.brokercheck.finra.org/search/individual",
        "default_params": {
            "filter": "active=true,prev=true,bar=true,broker=true,ia=true,brokeria=true",
            "includePrevious": "true",
            "hl": "true",
            "nrows": "12",
            "start": "0",
            "r": "25",
            "wt": "json"
        }
    },
    "SEC": {
        "base_url": "https://api.adviserinfo.sec.gov/search/individual",
        "default_params": {}
    },
    "SEC_ENFORCEMENT": {
        "base_url": "https://www.sec.gov/litigations/sec-action-look-up",
        "default_params": {}
    },
    "FINRA_DISCIPLINARY": {
        "base_url": "https://www.finra.org/rules-guidance/oversight-enforcement/finra-disciplinary-actions",
        "default_params": {}
    },
    "FINRA_ARBITRATION": {
        "base_url": "https://www.finra.org/rules-guidance/oversight-enforcement/finra-disciplinary-actions-online",
        "default_params": {}
    }
}

# Cache-related functions
def get_cache_file_path(cache_folder: str, identifier: str, operation: str, service: str, employee_number: Optional[str] = None) -> str:
    os.makedirs(cache_folder, exist_ok=True)
    if employee_number:
        employee_dir = os.path.join(cache_folder, employee_number)
        os.makedirs(employee_dir, exist_ok=True)
        return os.path.join(employee_dir, f"{service}_{identifier}_{operation}.json")
    return os.path.join(cache_folder, f"{service}_{identifier}_{operation}.json")

def read_cache(cache_folder: str, identifier: str, operation: str, service: str, employee_number: Optional[str], logger: logging.Logger) -> Optional[Dict]:
    cache_file = get_cache_file_path(cache_folder, identifier, operation, service, employee_number)
    if os.path.exists(cache_file):
        logger.debug(f"Loaded {operation} for {identifier} from {service} cache (employee: {employee_number}).")
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def write_cache(cache_folder: str, identifier: str, operation: str, data: Dict, service: str, employee_number: Optional[str], logger: logging.Logger) -> None:
    cache_file = get_cache_file_path(cache_folder, identifier, operation, service, employee_number)
    logger.debug(f"Caching {operation} data for {identifier} from {service} (employee: {employee_number}).")
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

# WebDriver initialization
def create_driver(logger: logging.Logger) -> webdriver.Chrome:
    logger.debug("Initializing WebDriver for SEC/FINRA searches.")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    )
    service = ChromeService(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

# Generic API fetch function
def fetch_api_data(identifier: str, operation: str, service: str, url: str, params: Dict, 
                  cache_folder: str, wait_time: int, logger: logging.Logger, 
                  employee_number: Optional[str] = None, return_cache_filename: bool = False) -> Tuple[Optional[Dict], Optional[str]]:
    cache_file = get_cache_file_path(cache_folder, identifier, operation, service, employee_number)
    cached_data = read_cache(cache_folder, identifier, operation, service, employee_number, logger)
    if cached_data:
        logger.info(f"Retrieved {operation} for {identifier} from {service} cache (employee: {employee_number}).")
        return (cached_data, cache_file) if return_cache_filename else (cached_data, None)

    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            write_cache(cache_folder, identifier, operation, data, service, employee_number, logger)
            logger.info(f"Fetched {operation} for {identifier} from {service} API (employee: {employee_number}).")
            time.sleep(wait_time)
            return (data, cache_file) if return_cache_filename else (data, None)
        elif response.status_code == 403:
            logger.error(f"Rate limit exceeded for {identifier}.")
            return None, None
        else:
            logger.error(f"Error fetching {operation} for {identifier} from {service}: {response.status_code}")
            return None, None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for {identifier} from {service}: {e}")
        return None, None

# Organization CRD lookup
def load_organizations_cache(logger: logging.Logger) -> Optional[List[Dict]]:
    cache_file = os.path.join("input", "organizationsCrd.jsonl")
    if not os.path.exists(cache_file):
        logger.error("Failed to load organizations cache.")
        return None
    try:
        organizations = []
        with open(cache_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    organizations.append(json.loads(line))
        return organizations
    except Exception as e:
        logger.error(f"Error loading organizations cache: {e}")
        return None

def normalize_organization_name(name: str) -> str:
    return name.lower().replace(" ", "")

def get_organization_crd(organization_name: str, logger: logging.Logger) -> Optional[str]:
    if not isinstance(organization_name, str) or not organization_name.strip():
        logger.error("organization_name must be a non-empty string")
        return None

    orgs_data = load_organizations_cache(logger)
    if not orgs_data:
        return None

    normalized_search_name = normalize_organization_name(organization_name)
    for org in orgs_data:
        if org.get("normalizedName") == normalized_search_name:
            crd = org.get("organizationCRD")
            if crd and crd != "N/A":
                logger.info(f"Found CRD {crd} for organization '{organization_name}'.")
                return crd
            logger.warning(f"CRD not found for organization '{organization_name}'.")
            return None
    return "NOT_FOUND"

# Individual basic info
def get_individual_basic_info(crd_number: str, cache_folder: str, wait_time: int, logger: logging.Logger, 
                             employee_number: Optional[str] = None, return_cache_filename: bool = False) -> Tuple[Optional[Dict], Optional[str]]:
    if not isinstance(crd_number, str) or not crd_number.strip():
        logger.error("crd_number must be a non-empty string")
        return None, None

    config = API_CONFIG["BROKERCHECK"]
    params = dict(config["default_params"])  # Immutable copy
    params["query"] = crd_number
    return fetch_api_data(crd_number, "basic_info", "brokercheck", config["base_url"], params, 
                         cache_folder, wait_time, logger, employee_number, return_cache_filename)

# SEC enforcement actions
def generate_sec_search_url(first_name: str, last_name: str) -> str:
    return API_CONFIG["SEC_ENFORCEMENT"]["base_url"] + f"?last_name={last_name}&first_name={first_name}"

def fetch_and_parse_sec(first_name: str, last_name: str, driver: webdriver.Chrome, logger: logging.Logger) -> Dict:
    search_url = generate_sec_search_url(first_name, last_name)
    logger.debug(f"Fetching SEC enforcement data for {first_name} {last_name} at URL: {search_url}")

    try:
        driver.get(search_url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "main-content")))
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')

        no_results_element = soup.find("p", class_="no-results")
        if no_results_element:
            return {"first_name": first_name, "last_name": last_name, "result": "No Results Found"}

        results = soup.find_all("div", class_="card.border-divide.views-row")
        if not results:
            return {"first_name": first_name, "last_name": last_name, "result": "No Results Found"}

        data = [
            {
                "Name": name_elem.get_text(strip=True) if (name_elem := result.find("h2", class_="field-content.card-title")) else f"{first_name} {last_name}",
                "State": state_elem.get_text(strip=True) if (state_elem := result.find("span", class_="field-content")) else "Unknown",
                "Enforcement Action": action_elem.find_next_sibling().get_text(strip=True) if (action_elem := result.find("span", class_="field-content", string="Enforcement Action:")) else "Unknown",
                "Date Filed": date_elem.get_text(strip=True) if (date_elem := result.find("time", class_="datetime")) else "Unknown",
                "Documents": [
                    {"title": doc.find("a").get_text(strip=True), "link": f"https://www.sec.gov{doc.find('a')['href']}"}
                    for doc in result.find_all("div", class_="field__item") if doc.find("a")
                ]
            }
            for result in results
        ]
        return {"first_name": first_name, "last_name": last_name, "result": data}
    except Exception as e:
        logger.exception(f"Error while fetching SEC data for {first_name} {last_name}: {e}")
        return {"first_name": first_name, "last_name": last_name, "error": str(e)}

def get_sec_enforcement_actions(employee_number: str, first_name: str, last_name: str, alternate_names: List[str], 
                               cache_folder: str, wait_time: int, logger: logging.Logger) -> Dict[str, Dict]:
    if not all(isinstance(n, str) and n.strip() for n in [employee_number, first_name, last_name] + (alternate_names or [])):
        logger.error("All names must be non-empty strings")
        return {}

    driver = create_driver(logger)
    try:
        alternate_names = alternate_names or []
        name_variations = [(first_name, last_name)] + [(parts[0], parts[-1]) for alt in alternate_names if len(parts := alt.strip().split()) >= 2]
        results = {}

        for idx, (f_name, l_name) in enumerate(name_variations, start=1):
            cache_key = f"sec_result_{idx}"
            name_key = f"{f_name}_{l_name}"
            url = generate_sec_search_url(f_name, l_name)
            cached_data = read_cache(cache_folder, cache_key, "enforcement", "sec", employee_number, logger)
            if cached_data:
                logger.debug(f"Cache hit for SEC data: {name_key}")
                results[name_key] = cached_data
                continue

            data = fetch_and_parse_sec(f_name, l_name, driver, logger)
            result = {"data": data, "url": url}
            write_cache(cache_folder, cache_key, "enforcement", result, "sec", employee_number, logger)
            results[name_key] = result
            time.sleep(wait_time)

        return results
    finally:
        driver.quit()

# FINRA disciplinary actions
def fetch_and_parse_finra(input_data: Dict[str, str], logger: logging.Logger) -> Dict:
    search_url = input_data.get("search")
    name = input_data.get("name", "Unknown")
    try:
        response = requests.get(search_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find("table", class_="views-table")
        if not table:
            return {"name": name, "results": []}

        rows = table.find_all("tr")[1:]
        if not rows:
            return {"name": name, "results": []}

        results = [
            {
                "Case ID": cells[0].text.strip(),
                "Case Summary": cells[1].text.strip(),
                "Document Type": cells[2].text.strip(),
                "Firms/Individuals": cells[3].text.strip(),
                "Action Date": cells[4].text.strip()
            }
            for row in rows if len(cells := row.find_all("td")) >= 5
        ]
        return {"name": name, "results": results}
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching FINRA disciplinary data for {name}: {e}")
        return {"name": name, "results": [], "error": str(e)}

def get_finra_disciplinary_actions(employee_number: str, first_name: str, last_name: str, alternate_names: List[str], 
                                  cache_folder: str, wait_time: int, logger: logging.Logger) -> Dict[str, Dict]:
    if not all(isinstance(n, str) and n.strip() for n in [employee_number, first_name, last_name] + (alternate_names or [])):
        logger.error("All names must be non-empty strings")
        return {}

    alternate_names = alternate_names or []
    name_variations = [(first_name, last_name)] + [(parts[0], parts[-1]) for alt in alternate_names if len(parts := alt.strip().split()) >= 2]
    results = {}

    base_url = API_CONFIG["FINRA_DISCIPLINARY"]["base_url"] + "?search={}&firms=&individuals=&field_fda_case_id_txt=&field_core_official_dt%5Bmin%5D=&field_core_official_dt%5Bmax%5D=&field_fda_document_type_tax=All"

    for idx, (f_name, l_name) in enumerate(name_variations, start=1):
        cache_key = f"finra_disciplinary_result_{idx}"
        name_key = f"{f_name}_{l_name}"
        search_query = f"{f_name}+{l_name}"
        url = base_url.format(search_query)
        cached_data = read_cache(cache_folder, cache_key, "disciplinary", "finra", employee_number, logger)
        if cached_data:
            results[name_key] = cached_data
            continue

        input_data = {"name": name_key, "search": url}
        result_data = fetch_and_parse_finra(input_data, logger)
        result = {
            "data": result_data,
            "url": url,
            "disciplinary_evaluation": {
                "compliance": not result_data.get("results"),
                "compliance_explanation": f"No disciplinary records found for {f_name} {l_name}." if not result_data.get("results") else f"Disciplinary records found for {f_name} {l_name}.",
                "disciplinary_records": result_data.get("results", [])
            }
        }
        write_cache(cache_folder, cache_key, "disciplinary", result, "finra", employee_number, logger)
        results[name_key] = result
        time.sleep(wait_time)

    return results

# Example usage (commented out)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Test get_organization_crd
    crd = get_organization_crd("Example Firm", logger)
    print(f"CRD: {crd}")

    # Test get_individual_basic_info
    info, _ = get_individual_basic_info("123456", "cache", 1, logger)
    print(f"Basic Info: {info}")

    # Test get_sec_enforcement_actions
    sec_results = get_sec_enforcement_actions("EMP001", "John", "Doe", ["Johnny Doe"], "cache", 1, logger)
    print(f"SEC Results: {sec_results}")

    # Test get_finra_disciplinary_actions
    finra_results = get_finra_disciplinary_actions("EMP001", "John", "Doe", ["Johnny Doe"], "cache", 1, logger)
    print(f"FINRA Results: {finra_results}")
    