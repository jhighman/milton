import os
import json
import time
import requests
from typing import Optional, Dict
from exceptions import RateLimitExceeded

class ApiClient:
    def __init__(self, cache_folder: str, wait_time: int, logger):
        self.cache_folder = cache_folder
        self.wait_time = wait_time
        self.logger = logger
        os.makedirs(self.cache_folder, exist_ok=True)

    def _read_from_cache(self, crd_number: str, operation: str, service: str) -> Optional[Dict]:
        """Reads data from cache, differentiated by service (BrokerCheck or SEC)."""
        cache_file = os.path.join(self.cache_folder, f"{service}_{crd_number}_{operation}.json")
        if os.path.exists(cache_file):
            self.logger.debug(f"Loaded {operation} for CRD {crd_number} from {service} cache.")
            with open(cache_file, 'r') as f:
                return json.load(f)
        return None

    def _write_to_cache(self, crd_number: str, operation: str, data: Dict, service: str):
        """Writes data to cache, differentiated by service (BrokerCheck or SEC)."""
        cache_file = os.path.join(self.cache_folder, f"{service}_{crd_number}_{operation}.json")
        self.logger.debug(f"Caching {operation} data for CRD {crd_number} from {service}.")
        with open(cache_file, 'w') as f:
            json.dump(data, f, indent=2)

    # BrokerCheck API Methods
    def get_individual_basic_info(self, crd_number: str) -> Optional[Dict]:
        """Fetches basic info from BrokerCheck."""
        service = "brokercheck"
        cached_data = self._read_from_cache(crd_number, "basic_info", service)
        if cached_data:
            self.logger.info(f"Retrieved basic info for CRD {crd_number} from {service} cache.")
            return cached_data

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
                'sort': 'score+desc',
                'wt': 'json'
            }

            response = requests.get(url, params=params)
            if response.status_code == 200:
                time.sleep(self.wait_time)
                data = response.json()
                self._write_to_cache(crd_number, "basic_info", data, service)
                self.logger.info(f"Fetched basic info for CRD {crd_number} from BrokerCheck API.")
                return data
            elif response.status_code == 403:
                raise RateLimitExceeded(f"Rate limit exceeded for CRD {crd_number}.")
            else:
                self.logger.error(f"Error fetching basic info for CRD {crd_number} from BrokerCheck: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for CRD {crd_number} from BrokerCheck: {e}")
            return None

    def get_individual_detailed_info(self, crd_number: str) -> Optional[Dict]:
        """Fetches detailed info from BrokerCheck."""
        service = "brokercheck"
        cached_data = self._read_from_cache(crd_number, "detailed_info", service)
        if cached_data:
            self.logger.info(f"Retrieved detailed info for CRD {crd_number} from {service} cache.")
            return cached_data

        try:
            url = f'https://api.brokercheck.finra.org/search/individual/{crd_number}'
            params = {
                'hl': 'true',
                'includePrevious': 'true',
                'nrows': '12',
                'query': 'john',
                'r': '25',
                'sort': 'bc_lastname_sort asc,bc_firstname_sort asc,bc_middlename_sort asc,score desc',
                'wt': 'json'
            }

            response = requests.get(url, params=params)
            if response.status_code == 200:
                time.sleep(self.wait_time)
                data = response.json()
                self._write_to_cache(crd_number, "detailed_info", data, service)
                self.logger.info(f"Fetched detailed info for CRD {crd_number} from BrokerCheck API.")
                return data
            elif response.status_code == 403:
                raise RateLimitExceeded(f"Rate limit exceeded for CRD {crd_number}.")
            else:
                self.logger.error(f"Error fetching detailed info for CRD {crd_number} from BrokerCheck: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for CRD {crd_number} from BrokerCheck: {e}")
            return None

    # SEC API Methods
    def get_individual_basic_info_from_sec(self, crd_number: str) -> Optional[Dict]:
        """Fetches basic info from SEC's adviser info."""
        service = "sec"
        cached_data = self._read_from_cache(crd_number, "basic_info", service)
        if cached_data:
            self.logger.info(f"Retrieved basic info for CRD {crd_number} from {service} cache.")
            return cached_data

        try:
            url = 'https://api.adviserinfo.sec.gov/search/individual'
            params = {
                'query': crd_number,
                'includePrevious': 'true',
                'hl': 'true',
                'nrows': '12',
                'start': '0',
                'r': '25',
                'sort': 'score+desc',
                'wt': 'json'
            }

            response = requests.get(url, params=params)
            if response.status_code == 200:
                time.sleep(self.wait_time)
                data = response.json()
                self._write_to_cache(crd_number, "basic_info", data, service)
                self.logger.info(f"Fetched basic info for CRD {crd_number} from SEC API.")
                return data
            elif response.status_code == 403:
                raise RateLimitExceeded(f"Rate limit exceeded for CRD {crd_number}.")
            else:
                self.logger.error(f"Error fetching basic info for CRD {crd_number} from SEC: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for CRD {crd_number} from SEC: {e}")
            return None

    def get_individual_detailed_info_from_sec(self, crd_number: str) -> Optional[Dict]:
        """Fetches detailed info from SEC's adviser info."""
        service = "sec"
        cached_data = self._read_from_cache(crd_number, "detailed_info", service)
        if cached_data:
            self.logger.info(f"Retrieved detailed info for CRD {crd_number} from {service} cache.")
            return cached_data

        try:
            url = f'https://api.adviserinfo.sec.gov/search/individual/{crd_number}'
            params = {
                'hl': 'true',
                'includePrevious': 'true',
                'nrows': '12',
                'query': 'john',
                'r': '25',
                'sort': 'bc_lastname_sort asc,bc_firstname_sort asc,bc_middlename_sort asc,score desc',
                'wt': 'json'
            }

            response = requests.get(url, params=params)
            if response.status_code == 200:
                time.sleep(self.wait_time)
                data = response.json()
                self._write_to_cache(crd_number, "detailed_info", data, service)
                self.logger.info(f"Fetched detailed info for CRD {crd_number} from SEC API.")
                return data
            elif response.status_code == 403:
                raise RateLimitExceeded(f"Rate limit exceeded for CRD {crd_number}.")
            else:
                self.logger.error(f"Error fetching detailed info for CRD {crd_number} from SEC: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for CRD {crd_number} from SEC: {e}")
            return None
