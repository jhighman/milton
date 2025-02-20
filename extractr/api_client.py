# api_client.py

import os
import json
import time
import requests
from typing import Optional, Dict, Tuple, List
from exceptions import RateLimitExceeded
import warnings

# Additional imports for WebDriver and parsing
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urlencode
from webdriver_manager.chrome import ChromeDriverManager

class ApiClient:
    def __init__(self, cache_folder: str, wait_time: int, logger, webdriver_enabled: bool = False):
        self.cache_folder = cache_folder
        self.wait_time = wait_time
        self.logger = logger
        self.webdriver_enabled = webdriver_enabled
        os.makedirs(self.cache_folder, exist_ok=True)

        # Initialize WebDriver if requested
        if self.webdriver_enabled:
            self.logger.debug("Initializing WebDriver for SEC searches.")
            self.driver = self._initialize_webdriver()
        else:
            self.driver = None

    def _initialize_webdriver(self):
        """
        Initialize the Chrome WebDriver with predefined options.
        Automatically downloads and uses the correct ChromeDriver version.
        """
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        )

        # Use webdriver_manager to automatically handle ChromeDriver
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver

    def _load_organizations_cache(self) -> Optional[List[Dict]]:
        """Loads the organizationsCrd.jsonl file from input directory."""
        cache_file = os.path.join("input", "organizationsCrd.jsonl")
        if not os.path.exists(cache_file):
            self.logger.error("Failed to load organizations cache.")
            return None
        
        try:
            organizations = []
            with open(cache_file, 'r') as f:
                for line in f:
                    if line.strip():
                        organizations.append(json.loads(line))
            return organizations
        except Exception as e:
            self.logger.error(f"Error loading organizations cache: {e}")
            return None

    def _normalize_organization_name(self, name: str) -> str:
        """
        Normalizes an organization name by:
        1. Converting to lowercase
        2. Removing all spaces
        
        Args:
            name: Raw organization name
            
        Returns:
            Normalized name string
        """
        return name.lower().replace(" ", "")

    def get_organization_crd(self, organization_name: str) -> Optional[str]:
        """
        Looks up the CRD for a given organization name using normalized name matching.
        
        Args:
            organization_name: Name of the organization to look up
            
        Returns:
            str: The organization's CRD number if found
            None: If organization not found or error occurred
            "NOT_FOUND": Special value indicating organization was searched but not found
        """
        orgs_data = self._load_organizations_cache()
        if not orgs_data:
            self.logger.error("Failed to load organizations cache.")
            return None

        # Normalize the input organization name
        normalized_search_name = self._normalize_organization_name(organization_name)
        
        for org in orgs_data:
            # Compare using the pre-normalized name from the data
            if org.get("normalizedName") == normalized_search_name:
                crd = org.get("organizationCRD")
                if crd and crd != "N/A":
                    self.logger.info(f"Found CRD {crd} for organization '{organization_name}'.")
                    return crd
                else:
                    self.logger.warning(f"CRD not found for organization '{organization_name}'.")
                    return None

        return "NOT_FOUND"

    def close(self):
        """
        Closes the WebDriver if initialized.
        """
        if self.driver:
            self.driver.quit()

    def _read_from_cache(self, identifier: str, operation: str, service: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        """Reads data from cache, using employee-based directory if provided."""
        if employee_number:
            employee_dir = os.path.join(self.cache_folder, employee_number)
            cache_file = os.path.join(employee_dir, f"{service}_{identifier}_{operation}.json")
        else:
            cache_file = os.path.join(self.cache_folder, f"{service}_{identifier}_{operation}.json")

        if os.path.exists(cache_file):
            self.logger.debug(f"Loaded {operation} for identifier {identifier} from {service} cache (employee: {employee_number}).")
            with open(cache_file, 'r') as f:
                return json.load(f)
        return None

    def _write_to_cache(self, identifier: str, operation: str, data: Dict, service: str, employee_number: Optional[str] = None):
        """Writes data to cache, using employee-based directory if available."""
        if employee_number:
            employee_dir = os.path.join(self.cache_folder, employee_number)
            os.makedirs(employee_dir, exist_ok=True)
            cache_file = os.path.join(employee_dir, f"{service}_{identifier}_{operation}.json")
        else:
            cache_file = os.path.join(self.cache_folder, f"{service}_{identifier}_{operation}.json")

        self.logger.debug(f"Caching {operation} data for identifier {identifier} from {service} (employee: {employee_number}).")
        with open(cache_file, 'w') as f:
            json.dump(data, f, indent=2)

    def _get_cache_file_path(self, identifier: str, operation: str, service: str, employee_number: Optional[str] = None) -> str:
        """Generates the cache file path, using employee-based directory if available."""
        if employee_number:
            employee_dir = os.path.join(self.cache_folder, employee_number)
            os.makedirs(employee_dir, exist_ok=True)
            cache_file = os.path.join(employee_dir, f"{service}_{identifier}_{operation}.json")
        else:
            cache_file = os.path.join(self.cache_folder, f"{service}_{identifier}_{operation}.json")
        return cache_file

    def get_individual_basic_info(self, crd_number: str, return_cache_filename: bool = False, employee_number: Optional[str] = None) -> Tuple[Optional[Dict], Optional[str]]:
        """Fetches basic info from BrokerCheck."""
        service = "brokercheck"
        cache_file = self._get_cache_file_path(crd_number, "basic_info", service, employee_number)
        cached_data = self._read_from_cache(crd_number, "basic_info", service, employee_number)
        if cached_data:
            self.logger.info(f"Retrieved basic info for CRD {crd_number} from {service} cache (employee: {employee_number}).")
            if return_cache_filename:
                return cached_data, cache_file
            else:
                return cached_data, None

        try:
            url = 'https://api.brokercheck.finra.org/search/individual'
            params = {
                'query': crd_number,
                'filter': 'active=true,prev=true,bar=true,broker=true,ia=true,brokeria=true',
                'includePrevious': 'true',
                'hl': 'true',
                'nrows': '12',
                'start': '0',
                'r': '25',
                'wt': 'json'
            }

            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                self._write_to_cache(crd_number, "basic_info", data, service, employee_number)
                self.logger.info(f"Fetched basic info for CRD {crd_number} from BrokerCheck API (employee: {employee_number}).")
                time.sleep(self.wait_time)
                if return_cache_filename:
                    return data, cache_file
                else:
                    return data, None
            elif response.status_code == 403:
                raise RateLimitExceeded(f"Rate limit exceeded for CRD {crd_number}.")
            else:
                self.logger.error(f"Error fetching basic info for CRD {crd_number} from BrokerCheck: {response.status_code}")
                return None, None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for CRD {crd_number} from BrokerCheck: {e}")
            return None, None

    def get_individual_detailed_info(self, crd_number: str, service: str = 'brokercheck', return_cache_filename: bool = False, employee_number: Optional[str] = None) -> Tuple[Optional[Dict], Optional[str]]:
        """Fetches detailed info from BrokerCheck or SEC."""
        cache_file = self._get_cache_file_path(crd_number, "detailed_info", service, employee_number)
        cached_data = self._read_from_cache(crd_number, "detailed_info", service, employee_number)

        if cached_data:
            self.logger.info(f"Retrieved detailed info for CRD {crd_number} from {service} cache (employee: {employee_number}).")
            if return_cache_filename:
                return cached_data, cache_file
            else:
                return cached_data, None

        try:
            if service == 'brokercheck':
                base_url = f'https://api.brokercheck.finra.org/search/individual/{crd_number}'
                params = {
                    'query': crd_number,
                    'filter': 'active=true,prev=true,bar=true,broker=true,ia=true,brokeria=true',
                    'includePrevious': 'true',
                    'hl': 'true',
                    'nrows': '12',
                    'start': '0',
                    'r': '25',
                    'wt': 'json'
                }
                url = f"{base_url}"
            elif service == 'sec':
                url = f'https://api.adviserinfo.sec.gov/search/individual/{crd_number}'
                params = {}
            else:
                self.logger.error(f"Unsupported service: {service}")
                return None, None

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                self._write_to_cache(crd_number, "detailed_info", data, service, employee_number)
                self.logger.info(f"Fetched detailed info for CRD {crd_number} from {service} API (employee: {employee_number}).")
                time.sleep(self.wait_time)
                if return_cache_filename:
                    return data, cache_file
                else:
                    return data, None
            elif response.status_code == 403:
                raise RateLimitExceeded(f"Rate limit exceeded for CRD {crd_number}.")
            else:
                self.logger.error(f"Error fetching detailed info for CRD {crd_number} from {service}: {response.status_code}")
                return None, None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for CRD {crd_number} from {service}: {e}")
            return None, None

    def get_individual_correlated_firm_info(self, individual_name: str, firm_crd: str, return_cache_filename: bool = False, employee_number: Optional[str] = None) -> Tuple[Optional[Dict], Optional[str]]:
        """Fetches correlated firm info from the SEC."""
        service = "sec"
        cache_key = f"{individual_name}_{firm_crd}"
        cache_file = self._get_cache_file_path(cache_key, "correlated_firm_info", service, employee_number)
        cached_data = self._read_from_cache(cache_key, "correlated_firm_info", service, employee_number=employee_number)
        if cached_data:
            self.logger.info(f"Retrieved correlated firm info for '{individual_name}' at firm {firm_crd} from {service} cache (employee: {employee_number}).")
            if return_cache_filename:
                return cached_data, cache_file
            else:
                return cached_data, None

        try:
            url = 'https://api.adviserinfo.sec.gov/search/individual'
            params = {
                'query': individual_name,
                'firm': firm_crd,
                'start': '0',
                'sortField': 'Relevance',
                'sortOrder': 'Desc',
                'type': 'Individual',
                'investmentAdvisors': 'true',
                'brokerDealers': 'false',
                'isNlSearch': 'false',
                'size': '50'
            }
            full_url = f"{url}?{'&'.join(f'{key}={value}' for key, value in params.items())}"
            self.logger.debug(f"Fetching correlated firm info with URL: {full_url}")
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                self._write_to_cache(cache_key, "correlated_firm_info", data, service, employee_number=employee_number)
                self.logger.info(f"Fetched correlated firm info for '{individual_name}' at firm {firm_crd} from SEC API (employee: {employee_number}).")
                time.sleep(self.wait_time)
                if return_cache_filename:
                    return data, cache_file
                else:
                    return data, None
            elif response.status_code == 403:
                raise RateLimitExceeded(f"Rate limit exceeded for individual '{individual_name}' at firm {firm_crd}.")
            else:
                self.logger.error(f"Error fetching correlated firm info for '{individual_name}' at firm {firm_crd} from SEC API: {response.status_code}")
                return None, None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for individual '{individual_name}' at firm {firm_crd} from SEC API: {e}")
            return None, None

    def get_firm_crd(self, organization_name: str) -> Optional[str]:
        """DEPRECATED: Use get_organization_crd instead. Will be removed in future version."""
        warnings.warn(
            "get_firm_crd is deprecated and will be removed. Use get_organization_crd instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return self.get_organization_crd(organization_name)

    # -------------------------
    # New SEC Enforcement Action Methods
    # -------------------------

    def _generate_sec_search_url(self, first_name: str, last_name: str) -> str:
        base_url = "https://www.sec.gov/litigations/sec-action-look-up?last_name={}&first_name={}"
        return base_url.format(last_name, first_name)

    def _fetch_and_parse_sec(self, first_name: str, last_name: str) -> Dict:
        """Fetches and parses the SEC Enforcement Action Lookup results using Selenium."""
        if not self.driver:
            raise RuntimeError("WebDriver not initialized. Enable webdriver in ApiClient to use SEC search.")

        search_url = self._generate_sec_search_url(first_name, last_name)
        self.logger.debug(f"Fetching SEC enforcement data for {first_name} {last_name} at URL: {search_url}")

        try:
            self.driver.get(search_url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "main-content"))
            )

            html_content = self.driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')

            # Check for "No Results" message
            no_results_element = soup.find("p", class_="no-results")
            if no_results_element:
                return {
                    "first_name": first_name,
                    "last_name": last_name,
                    "result": "No Results Found"
                }

            results = soup.find_all("div", class_="card.border-divide.views-row")
            if not results:
                return {
                    "first_name": first_name,
                    "last_name": last_name,
                    "result": "No Results Found"
                }

            data = []
            for result in results:
                name_elem = result.find("h2", class_="field-content.card-title")
                name = name_elem.get_text(strip=True) if name_elem else f"{first_name} {last_name}"
                state_elem = result.find("span", class_="field-content")
                state = state_elem.get_text(strip=True) if state_elem else "Unknown"
                action_elem = result.find("span", class_="field-content", string="Enforcement Action:")
                action = action_elem.find_next_sibling().get_text(strip=True) if action_elem else "Unknown"
                date_elem = result.find("time", class_="datetime")
                date_filed = date_elem.get_text(strip=True) if date_elem else "Unknown"
                documents = []

                related_documents = result.find_all("div", class_="field__item")
                for doc in related_documents:
                    link_elem = doc.find("a")
                    if link_elem:
                        title = link_elem.get_text(strip=True)
                        link = link_elem["href"]
                        documents.append({"title": title, "link": f"https://www.sec.gov{link}"})

                data.append({
                    "Name": name,
                    "State": state,
                    "Enforcement Action": action,
                    "Date Filed": date_filed,
                    "Documents": documents
                })

            return {
                "first_name": first_name,
                "last_name": last_name,
                "result": data
            }

        except Exception as e:
            self.logger.exception(f"Error while fetching SEC data for {first_name} {last_name}: {e}")
            return {
                "first_name": first_name,
                "last_name": last_name,
                "error": str(e)
            }

    def get_sec_enforcement_actions(self, employee_number: str, first_name: str, last_name: str, alternate_names: List[str] = None) -> Dict[str, Dict]:
        """
        Retrieves SEC enforcement actions for the given individual and optional alternate names.
        This method:
          - Checks the cache folder for existing results before making a new request.
          - If no cached data is found, uses WebDriver to fetch the page and parse results.
        Returns a dict keyed by name variation (f_name_l_name) with the corresponding enforcement action data.
        """
        if not self.webdriver_enabled or not self.driver:
            self.logger.error("WebDriver not enabled. Cannot perform SEC enforcement action lookups.")
            return {}

        if alternate_names is None:
            alternate_names = []

        # Compile all name variations: primary plus alternates
        name_variations = [(first_name, last_name)]
        for alt_name in alternate_names:
            parts = alt_name.strip().split()
            if len(parts) >= 2:
                name_variations.append((parts[0], parts[-1]))

        results = {}
        employee_dir_cache = os.path.join(self.cache_folder, employee_number)
        os.makedirs(employee_dir_cache, exist_ok=True)

        for idx, (f_name, l_name) in enumerate(name_variations, start=1):
            cache_filename = os.path.join(employee_dir_cache, f"sec_result_{idx}.json")
            name_key = f"{f_name}_{l_name}"

            # Construct the URL for the query (example URL placeholder)
            url = f"https://sec-enforcement-actions.example.com/search?firstName={f_name}&lastName={l_name}"

            # Check cache
            if os.path.exists(cache_filename):
                self.logger.debug(f"Cache hit for SEC data: {cache_filename}")
                with open(cache_filename, 'r', encoding='utf-8') as infile:
                    result = json.load(infile)
                # Add the URL to the result if it's not already there
                result["url"] = url
                results[name_key] = result
                continue

            # If not cached, fetch and parse
            self.logger.debug(f"No cache found for SEC data, querying for {f_name} {l_name}.")
            data = self._fetch_and_parse_sec(f_name, l_name)

            # Add the URL to the result
            result = {
                "data": data,
                "url": url
            }

            # Save to cache
            with open(cache_filename, 'w', encoding='utf-8') as outfile:
                json.dump(result, outfile, indent=4)

            results[name_key] = result
            # Delay to respect wait_time
            time.sleep(self.wait_time)

        return results

    def _evaluate_disciplinary_records(self, records: List[Dict]) -> Tuple[bool, str]:
        """
        Evaluates disciplinary records to determine compliance status.
        If any disciplinary records exist, compliance should be False.
        
        Parameters:
            records (List[Dict]): List of disciplinary records found
            
        Returns:
            Tuple[bool, str]: (compliance_status, explanation)
        """
        if not records:
            return True, "No disciplinary history found."
            
        # Any disciplinary records = non-compliant
        record_count = len(records)
        explanation = f"Found {record_count} disciplinary record(s). Further review required."
        return False, explanation

    def get_finra_disciplinary_actions(self, employee_number: str, first_name: str, last_name: str, alternate_names: List[str] = None) -> Dict[str, Dict]:
        """
        Retrieves FINRA disciplinary actions for the given individual and optional alternate names.
        This method:
        - Checks the cache folder for existing results before making a new request.
        - If no cached data is found, queries FINRA Disciplinary Actions Online and parses results.
        Returns a dict keyed by name variation (f_name_l_name) with the corresponding disciplinary action data.
        """
        if alternate_names is None:
            alternate_names = []

        # Compile all name variations: primary plus alternates
        name_variations = [(first_name, last_name)]
        for alt_name in alternate_names:
            parts = alt_name.strip().split()
            if len(parts) >= 2:
                name_variations.append((parts[0], parts[-1]))

        results = {}
        employee_dir_cache = os.path.join(self.cache_folder, employee_number)
        os.makedirs(employee_dir_cache, exist_ok=True)

        base_url = ("https://www.finra.org/rules-guidance/oversight-enforcement/finra-disciplinary-actions"
                    "?search={}&firms=&individuals=&field_fda_case_id_txt=&"
                    "field_core_official_dt%5Bmin%5D=&field_core_official_dt%5Bmax%5D=&field_fda_document_type_tax=All")

        for idx, (f_name, l_name) in enumerate(name_variations, start=1):
            cache_filename = os.path.join(employee_dir_cache, f"finra_disciplinary_result_{idx}.json")
            name_key = f"{f_name}_{l_name}"

            search_query = f"{f_name}+{l_name}"
            url = base_url.format(search_query)

            # Check cache
            if os.path.exists(cache_filename):
                print(f"Cache hit for FINRA disciplinary data: {cache_filename}")
                with open(cache_filename, 'r', encoding='utf-8') as infile:
                    result = json.load(infile)
                result["url"] = url
                results[name_key] = result
                continue

            print(f"No cache found for {name_key}, querying FINRA disciplinary data...")
            input_data = {"name": name_key, "search": url}
            result_data = self._fetch_and_parse_finra(input_data)

            result = {
                "data": result_data,
                "url": url
            }

            # Always use results array, which will be empty if no records found
            result["data"]["disciplinary_evaluation"] = {
                "compliance": True if not result_data.get("results") else False,
                "compliance_explanation": (
                    f"No disciplinary records found for {f_name} {l_name}." 
                    if not result_data.get("results") 
                    else f"Disciplinary records found for {f_name} {l_name}."
                ),
                "disciplinary_records": result_data.get("results", [])
            }

            # Save to cache
            with open(cache_filename, 'w', encoding='utf-8') as outfile:
                json.dump(result, outfile, indent=4)

            results[name_key] = result
            time.sleep(self.wait_time)

        return results

    def _fetch_and_parse_finra(self, input_data: Dict[str, str]) -> Dict:
        """
        Fetches and parses the FINRA Disciplinary Actions Online results.
        Returns a consistent structure whether no results are found or there's an error.
        """
        search_url = input_data.get("search")
        name = input_data.get("name", "Unknown")

        try:
            # Perform the web request
            response = requests.get(search_url)
            response.raise_for_status()
            html_content = response.text

            # Parse the HTML content
            soup = BeautifulSoup(html_content, 'html.parser')

            # Locate table rows
            table = soup.find("table", class_="views-table")
            if not table:
                return {"name": name, "results": []}

            rows = table.find_all("tr")[1:]  # Skip header row
            if not rows:
                return {"name": name, "results": []}

            results = []
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 5:  # Ensure we have all expected columns
                    result = {
                        "Case ID": cells[0].text.strip(),
                        "Case Summary": cells[1].text.strip(),
                        "Document Type": cells[2].text.strip(),
                        "Firms/Individuals": cells[3].text.strip(),
                        "Action Date": cells[4].text.strip()
                    }
                    results.append(result)

            return {
                "name": name,
                "results": results  # Will be empty list if no results found
            }

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching FINRA disciplinary data for {name}: {e}")
            return {"name": name, "results": [], "error": str(e)}
    
    def get_finra_arbitrations(
        self,
        employee_number: str,
        first_name: str,
        last_name: str,
        alternate_names: List[str] = None
    ) -> Dict[str, Dict]:
        """
        Retrieves FINRA arbitration actions for the given individual and optional alternate names.
        Uses a WebDriver-based approach to fill out the FINRA Disciplinary Actions Online form 
        (but specifically for arbitration results). Caches each query so subsequent calls are faster.

        Returns:
            dict: A dictionary keyed by "f_name_l_name" with 'data' and 'url' fields.
                  For example:
                  {
                    "John_Smith": {
                      "data": { ... parsed arbitration data ... },
                      "url": "https://www.finra.org/...some-search-url..."
                    },
                    "Johnny_Smith": { ... }
                  }
        """
        # Ensure WebDriver is available
        if not self.webdriver_enabled or not self.driver:
            self.logger.error("WebDriver not enabled. Cannot perform FINRA arbitration lookups.")
            return {}

        if alternate_names is None:
            alternate_names = []

        # Compile all name variations: primary plus alternates
        name_variations = [(first_name, last_name)]
        for alt_name in alternate_names:
            parts = alt_name.strip().split()
            if len(parts) >= 2:
                name_variations.append((parts[0], parts[-1]))

        results = {}
        employee_dir_cache = os.path.join(self.cache_folder, employee_number)
        os.makedirs(employee_dir_cache, exist_ok=True)

        for idx, (f_name, l_name) in enumerate(name_variations, start=1):
            cache_filename = os.path.join(employee_dir_cache, f"finra_arbitration_result_{idx}.json")
            name_key = f"{f_name}_{l_name}"

            # We'll assume a base URL or direct link to the FINRA form
            # (Below is the same as in your snippet for Disciplinary Actions, but you might
            # have a separate page or dropdown for "Arbitration" specifically.)
            base_url = "https://www.finra.org/rules-guidance/oversight-enforcement/finra-disciplinary-actions-online"
            # If the search URL is more complex, you can build it here:
            url = base_url  # or maybe you pass query params, etc.

            # 1) Check if we've already cached this name variation
            if os.path.exists(cache_filename):
                self.logger.debug(f"Cache hit for FINRA arbitration data: {cache_filename}")
                with open(cache_filename, 'r', encoding='utf-8') as infile:
                    result = json.load(infile)
                # Make sure the result has the URL attached
                result["url"] = url
                results[name_key] = result
                continue

            # 2) Not cached, so fetch & parse via helper function
            self.logger.debug(f"No cache found for {name_key}, querying FINRA arbitration data...")
            data = self._fetch_and_parse_finra_arbitrations(f_name, l_name, url)

            result = {
                "data": data,  # The parsed table or error
                "url": url
            }

            # 3) Write to cache
            with open(cache_filename, 'w', encoding='utf-8') as outfile:
                json.dump(result, outfile, indent=4)

            self.logger.info(f"Fetched and cached result for {name_key}: {json.dumps(result, indent=4)}")
            results[name_key] = result

            # 4) Sleep to respect wait_time
            time.sleep(self.wait_time)

        return results

    def _fetch_and_parse_finra_arbitrations(self, first_name: str, last_name: str, url: str) -> Dict:
        """
        Actually navigates the FINRA site using self.driver, performs the search, 
        and parses the table, mirroring the approach in search_and_extract.

        Returns a dictionary containing "search_parameters" and the "results" table.
        """

        # Define wait intervals
        SHORT_WAIT = 3
        LONG_WAIT = 10

        driver = self.driver

        try:
            # STEP 1: Navigate to the page
            self.logger.debug("Step 1: Navigating to the FINRA page...")
            driver.get(url)

            # STEP 2: Handle cookie consent (if it appears)
            self.logger.debug("Step 2: Checking for cookie consent banner...")
            try:
                cookie_button = WebDriverWait(driver, SHORT_WAIT).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Continue')]"))
                )
                driver.execute_script("arguments[0].click();", cookie_button)
                self.logger.debug("Cookie consent accepted.")

                WebDriverWait(driver, SHORT_WAIT).until(
                    EC.invisibility_of_element((By.XPATH, "//button[contains(text(), 'Continue')]"))
                )
                self.logger.debug("Cookie banner dismissed.")
            except TimeoutException:
                self.logger.debug("No cookie banner detected or already dismissed.")

            # STEP 3: Fill in the 'Individual Name or CRD#' field
            self.logger.debug("Step 3: Filling in the 'Individual Name or CRD#' field...")
            individuals_field = WebDriverWait(driver, SHORT_WAIT).until(
                EC.visibility_of_element_located((By.ID, "edit-individuals"))
            )
            individuals_field.clear()
            individuals_field.send_keys(f"{first_name} {last_name}")

            # STEP 4: Select "All Document Types" in the dropdown
            self.logger.debug("Step 4: Setting 'Document Types' to 'All Document Types'...")

            # STEP 5: Click "Terms of Service" checkbox
            self.logger.debug("Step 5: Clicking 'Terms of Service' checkbox...")
            terms_checkbox = WebDriverWait(driver, SHORT_WAIT).until(
                EC.element_to_be_clickable((By.ID, "edit-terms-of-service"))
            )
            if not terms_checkbox.is_selected():
                try:
                    terms_checkbox.click()
                    self.logger.debug("'Terms of Service' checkbox clicked.")
                except ElementClickInterceptedException:
                    self.logger.debug("Checkbox click intercepted, using JS.")
                    driver.execute_script("arguments[0].click();", terms_checkbox)

            # STEP 6: Click the 'Submit' button
            self.logger.debug("Step 6: Clicking the 'Submit' button...")
            submit_button = WebDriverWait(driver, SHORT_WAIT).until(
                EC.element_to_be_clickable((By.ID, "edit-actions-submit"))
            )
            try:
                submit_button.click()
                self.logger.debug("'Submit' button clicked.")
            except ElementClickInterceptedException:
                self.logger.debug("Submit button click intercepted, using JS.")
                driver.execute_script("arguments[0].click();", submit_button)

            # STEP 7: Wait for the results table to load
            self.logger.debug("Step 7: Waiting for the results table to load...")
            WebDriverWait(driver, LONG_WAIT).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.table-responsive.col > table.views-table.views-view-table.cols-5")
                )
            )
            self.logger.debug("Results table loaded.")

            # STEP 8: Parse the table
            self.logger.debug("Step 8: Extracting table data...")
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup.find("table", class_="table views-table.views-view-table.cols-5")

            if not table:
                # If no table found, either no results or an error
                return {
                    "search_parameters": {"first_name": first_name, "last_name": last_name},
                    "results": [],
                    "message": "No arbitration table found.",
                }

            # Example headers from your snippet:
            headers = ["Case ID", "Case Summary", "Document Type", "Firms/Individuals", "Action Date"]

            rows = []
            for tr in table.find_all("tr")[1:]:  # skip the header
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                row_data = dict(zip(headers, cells))
                rows.append(row_data)
                # If you want to log each row:
                self.logger.debug(json.dumps(row_data, indent=2))

            # STEP 9: Return combined results
            return {
                "search_parameters": {"first_name": first_name, "last_name": last_name},
                "results": rows,
                "arbitration_count": len(rows),
            }

        except Exception as e:
            self.logger.exception(f"Error during FINRA arbitration search or extraction for {first_name} {last_name}: {e}")
            return {
                "search_parameters": {"first_name": first_name, "last_name": last_name},
                "error": str(e)
            }




