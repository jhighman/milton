# api_client.py

import os
import json
import time
import requests
from typing import Optional, Dict, Tuple, List
from exceptions import RateLimitExceeded

# Additional imports for WebDriver and parsing
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urlencode

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

        service = ChromeService()
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver

    def get_firm_crd(self, organization_name: str) -> Optional[str]:
        """Looks up the CRD for a given organization name from the firms.json cache."""
        firms_data = self._load_firms_cache()
        if not firms_data:
            self.logger.error("Failed to load firms cache.")
            return None

        # Normalize the organization name for matching
        normalized_org_name = organization_name.lower()

        # Search for the organization name in the firms data
        for firm in firms_data:
            firm_name = firm.get("OrganizationName", "").lower()
            if firm_name == normalized_org_name:
                crd = firm.get("CRD")
                if crd:
                    self.logger.info(f"Found CRD {crd} for organization '{organization_name}'.")
                    return crd
                else:
                    self.logger.warning(f"CRD not found for organization '{organization_name}'.")
                    return None

        # Return a special value indicating the organization was not found
        self.logger.warning(f"Organization '{organization_name}' not found in firms data.")
        return "NOT_FOUND"
    def close(self):
        """
        Closes the WebDriver if initialized.
        """
        if self.driver:
            self.driver.quit()

    def _load_firms_cache(self) -> Optional[Dict]:
        """Loads the firms.json file from the cache."""
        firms_cache_file = os.path.join(self.cache_folder, "firms.json")
        if os.path.exists(firms_cache_file):
            self.logger.debug(f"Loaded firms cache from {firms_cache_file}.")
            with open(firms_cache_file, 'r') as f:
                return json.load(f)
        else:
            self.logger.error(f"Firms cache file {firms_cache_file} not found.")
            return None



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

    def get_firm_crd_from_brokercheck(self, organization_name: str, employee_number: Optional[str] = None) -> Optional[str]:
        """Fetches firm CRD from BrokerCheck using organization name."""
        service = "brokercheck"
        cache_key = f"{organization_name}"
        cache_file = self._get_cache_file_path(cache_key, "firm_search", service, employee_number)
        cached_data = self._read_from_cache(cache_key, "firm_search", service, employee_number)
        if cached_data:
            self.logger.info(f"Retrieved firm info for '{organization_name}' from {service} cache (employee: {employee_number}).")
            # Extract CRD from cached data
            hits = cached_data.get('hits', {}).get('hits', [])
            if hits:
                firm_crd = hits[0].get('_source', {}).get('firm_crd_nb')
                return firm_crd
            return None

        try:
            url = 'https://api.brokercheck.finra.org/search/firm'
            params = {
                'query': organization_name,
                'filter': 'active=true,previous=true',
                'hl': 'true',
                'nrows': '10',
                'start': '0',
                'wt': 'json'
            }

            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                self._write_to_cache(cache_key, "firm_search", data, service, employee_number=employee_number)
                self.logger.info(f"Fetched firm info for '{organization_name}' from BrokerCheck API (employee: {employee_number}).")
                time.sleep(self.wait_time)
                hits = data.get('hits', {}).get('hits', [])
                if hits:
                    firm_crd = hits[0].get('_source', {}).get('firm_crd_nb')
                    return firm_crd
                return None
            elif response.status_code == 403:
                raise RateLimitExceeded(f"Rate limit exceeded for firm '{organization_name}'.")
            else:
                self.logger.error(f"Error fetching firm info for '{organization_name}' from BrokerCheck: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for firm '{organization_name}' from BrokerCheck: {e}")
            return None

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

            # Save to cache
            with open(cache_filename, 'w', encoding='utf-8') as outfile:
                json.dump(result, outfile, indent=4)

            print(f"Fetched and cached result for {name_key}: {json.dumps(result, indent=4)}")
            results[name_key] = result
            time.sleep(self.wait_time)

        print("Final results for all name variations:")
        for name, data in results.items():
            print(f"{name}: {json.dumps(data, indent=4)}")

        return results

    def _fetch_and_parse_finra(self, input_data: Dict[str, str]) -> Dict:
        """
        Fetches and parses the FINRA Disciplinary Actions Online results.

        Parameters:
            input_data (dict): Input object containing "name" and "search" URL.

        Returns:
            dict: Extracted data or a "No Results Found" message.
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
                return {"name": name, "result": "No Results Found"}

            rows = table.find_all("tr")[1:]  # Skip header row
            if not rows:
                return {"name": name, "result": "No Results Found"}

            data = []
            for row in rows:
                cells = row.find_all("td")
                case_id = cells[0].text.strip() if len(cells) > 0 else "N/A"
                case_summary = cells[1].text.strip() if len(cells) > 1 else "N/A"
                document_type = cells[2].text.strip() if len(cells) > 2 else "N/A"
                firms_individuals = cells[3].text.strip() if len(cells) > 3 else "N/A"
                action_date = cells[4].text.strip() if len(cells) > 4 else "N/A"

                data.append({
                    "Case ID": case_id,
                    "Case Summary": case_summary,
                    "Document Type": document_type,
                    "Firms/Individuals": firms_individuals,
                    "Action Date": action_date
                })

            return {"name": name, "result": data}

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching FINRA disciplinary data for {name}: {e}")
            return {"name": name, "error": str(e)}
