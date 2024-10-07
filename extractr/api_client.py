# api_client.py

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

    def _read_from_cache(self, crd_number: str, operation: str) -> Optional[Dict]:
        cache_file = os.path.join(self.cache_folder, f"{crd_number}_{operation}.json")
        if os.path.exists(cache_file):
            self.logger.debug(f"Loaded {operation} for CRD {crd_number} from cache.")
            with open(cache_file, 'r') as f:
                return json.load(f)
        return None

    def _write_to_cache(self, crd_number: str, operation: str, data: Dict):
        cache_file = os.path.join(self.cache_folder, f"{crd_number}_{operation}.json")
        self.logger.debug(f"Caching {operation} data for CRD {crd_number}.")
        with open(cache_file, 'w') as f:
            json.dump(data, f, indent=2)

    def get_individual_basic_info(self, crd_number: str) -> Optional[Dict]:
        cached_data = self._read_from_cache(crd_number, "basic_info")
        if cached_data:
            self.logger.info(f"Retrieved basic info for CRD {crd_number} from cache.")
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
                self._write_to_cache(crd_number, "basic_info", data)
                self.logger.info(f"Fetched basic info for CRD {crd_number} from API.")
                return data
            elif response.status_code == 403:
                raise RateLimitExceeded(f"Rate limit exceeded for CRD {crd_number}.")
            else:
                self.logger.error(f"Error fetching basic info for CRD {crd_number}: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for CRD {crd_number}: {e}")
            return None

    def get_individual_detailed_info(self, crd_number: str) -> Optional[Dict]:
        cached_data = self._read_from_cache(crd_number, "detailed_info")
        if cached_data:
            self.logger.info(f"Retrieved detailed info for CRD {crd_number} from cache.")
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
                self._write_to_cache(crd_number, "detailed_info", data)
                self.logger.info(f"Fetched detailed info for CRD {crd_number} from API.")
                return data
            elif response.status_code == 403:
                raise RateLimitExceeded(f"Rate limit exceeded for CRD {crd_number}.")
            else:
                self.logger.error(f"Error fetching detailed info for CRD {crd_number}: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for CRD {crd_number}: {e}")
            return None
