import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import requests
from typing import Dict, Optional, Any
import json
import logging
from logging import Logger
from agents.exceptions import RateLimitExceeded  
import time
from functools import wraps

"""
FINRA BrokerCheck Agent

This module provides an agent for interacting with the FINRA BrokerCheck API, which offers
public access to professional information about brokers and investment advisors registered
with FINRA (Financial Industry Regulatory Authority). The agent fetches data without handling
caching, leaving that to the calling client.

Key Features:
- Two services: Basic search and detailed profile retrieval.
- Uses CRD (Central Registration Depository) numbers to identify individuals.
- Structured logging with optional employee number for traceability.
- No caching; clients manage persistence.

Terminology (from FINRA BrokerCheck):
- CRD Number: A unique identifier assigned to brokers and firms by FINRA.
- Broker: An individual registered to sell securities (e.g., Series 6, 7).
- Investment Adviser (IA): A registered adviser under the Investment Advisers Act.
- BrokerCheck: FINRA's public tool for researching broker and firm backgrounds.
"""

# Module-level logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Configuration for FINRA BrokerCheck API
BROKERCHECK_CONFIG: Dict[str, Any] = {
    "base_search_url": "https://api.brokercheck.finra.org/search/individual",
    "default_params": {
        "filter": "active=true,prev=true,bar=true,broker=true,ia=true,brokeria=true",  # Filters for active/previously registered brokers/IAs
        "includePrevious": "true",  # Include past registrations
        "hl": "true",  # Highlight search terms in results
        "nrows": "12",  # Number of rows per response
        "start": "0",  # Starting index for pagination
        "r": "25",  # Radius (not typically used for CRD searches, kept for API compatibility)
        "wt": "json"  # Response format (JSON)
    }
}

# Rate limiting configuration
RATE_LIMIT_DELAY = 5  # seconds between API calls

def rate_limit(func):
    """Decorator to enforce rate limiting between API calls"""
    last_call = {}  # Dictionary to track last call time per function
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Get current time
        current_time = time.time()
        
        # Check if we need to wait
        if func.__name__ in last_call:
            elapsed = current_time - last_call[func.__name__]
            if elapsed < RATE_LIMIT_DELAY:
                time.sleep(RATE_LIMIT_DELAY - elapsed)
        
        # Update last call time and execute function
        last_call[func.__name__] = time.time()
        return func(*args, **kwargs)
    
    return wrapper

@rate_limit
def search_individual(crd_number: str, employee_number: Optional[str] = None, 
                     logger: Logger = logger) -> Optional[Dict]:
    """
    Fetches basic information from FINRA BrokerCheck for an individual using their CRD number.
    Rate limited to one call every 5 seconds.

    This function queries BrokerCheck to retrieve a summary of individuals matching the CRD.
    The response is a search result with a 'hits' structure containing basic details in '_source'.

    Args:
        crd_number (str): The Central Registration Depository (CRD) number of the individual.
        employee_number (Optional[str]): An optional identifier for logging context, e.g., an internal employee ID. Defaults to None.
        logger (Logger): Logger instance for structured logging. Defaults to module logger.

    Returns:
        Optional[Dict]: A dictionary with basic info if successful, None if the fetch fails.
                        Example response structure:
                        {
                            "hits": {
                                "total": int,  # Number of matching records
                                "hits": [
                                    {
                                        "_type": "_doc",
                                        "_source": {
                                            "ind_source_id": str,  # CRD number
                                            "ind_firstname": str,
                                            "ind_middlename": str,
                                            "ind_lastname": str,
                                            "ind_namesuffix": str,
                                            "ind_other_names": List[str],  # Alternate names
                                            "ind_bc_scope": str,  # Broker status (e.g., "InActive")
                                            "ind_ia_scope": str,  # IA status (e.g., "InActive")
                                            "ind_bc_disclosure_fl": str,  # Disclosure flag (Y/N)
                                            "ind_approved_finra_registration_count": int,
                                            "ind_employments_count": int,
                                            "ind_industry_days": str,  # Days in industry
                                            "ind_current_employments": List  # Current employments (may be empty)
                                        },
                                        "highlight": Dict  # Highlighted fields (e.g., CRD)
                                    }
                                ]
                            }
                        }

    Raises:
        RateLimitExceeded: If the API returns a 403 status, indicating too many requests.
    """
    if not crd_number or not isinstance(crd_number, str):
        logger.error("Invalid CRD number", 
                    extra={"crd_number": crd_number, "employee_number": employee_number})
        return None

    service = "brokercheck"
    logger.info("Starting FINRA BrokerCheck basic search", 
               extra={"crd_number": crd_number, "employee_number": employee_number})

    try:
        url = BROKERCHECK_CONFIG["base_search_url"]
        params = dict(BROKERCHECK_CONFIG["default_params"])  # Create an immutable copy of default params
        params["query"] = crd_number  # Set the CRD as the search query
        logger.debug("Fetching basic info from BrokerCheck API", 
                    extra={"url": url, "params": params, "employee_number": employee_number})

        response = requests.get(url, params=params)
        response.raise_for_status()  # Raises an HTTPError for 4xx/5xx responses

        if response.status_code == 200:
            data = response.json()
            logger.info("Basic data fetched successfully", 
                       extra={"crd_number": crd_number, "employee_number": employee_number})
            return data
        else:
            logger.error("Unexpected status code", 
                        extra={"crd_number": crd_number, "status_code": response.status_code, 
                               "employee_number": employee_number})
            return None

    except requests.exceptions.HTTPError as e:
        if response.status_code == 403:
            logger.error("Rate limit exceeded", 
                        extra={"crd_number": crd_number, "employee_number": employee_number})
            raise RateLimitExceeded(f"Rate limit exceeded for CRD {crd_number}.")
        logger.error("HTTP error during fetch", 
                    extra={"crd_number": crd_number, "error": str(e), 
                           "status_code": response.status_code, "employee_number": employee_number})
        return None
    except requests.exceptions.RequestException as e:
        logger.error("Request error during fetch", 
                    extra={"crd_number": crd_number, "error": str(e), "employee_number": employee_number})
        return None

@rate_limit
def search_individual_detailed_info(crd_number: str, employee_number: Optional[str] = None, 
                                   logger: Logger = logger) -> Optional[Dict]:
    """
    Fetches detailed information from FINRA BrokerCheck for an individual using their CRD number.
    Rate limited to one call every 5 seconds.

    This function retrieves a comprehensive profile for the specified CRD from BrokerCheck.
    The response is a 'hits' structure where '_source.content' is a JSON string that must be
    parsed into an object containing detailed profile data.

    Args:
        crd_number (str): The Central Registration Depository (CRD) number of the individual.
        employee_number (Optional[str]): An optional identifier for logging context, e.g., an internal employee ID. Defaults to None.
        logger (Logger): Logger instance for structured logging. Defaults to module logger.

    Returns:
        Optional[Dict]: A dictionary with detailed info if successful, None if the fetch or parsing fails.
                        The returned data is the parsed '_source.content' object.
                        Example response structure (after parsing content):
                        {
                            "basicInformation": {
                                "individualId": int,  # CRD number
                                "firstName": str,
                                "middleName": str,
                                "lastName": str,
                                "nameSuffix": str,
                                "otherNames": List[str],
                                "bcScope": str,  # Broker status
                                "iaScope": str,  # IA status
                                "daysInIndustry": int
                            },
                            "currentEmployments": List[Dict],  # Current broker employments
                            "currentIAEmployments": List[Dict],  # Current IA employments
                            "previousEmployments": List[Dict],  # Past employments with firm details
                            "previousIAEmployments": List[Dict],
                            "disclosureFlag": str,  # Y/N for disclosures
                            "iaDisclosureFlag": str,
                            "disclosures": List[Dict],  # Disciplinary actions
                            "examsCount": Dict,  # Counts of exams passed
                            "stateExamCategory": List[Dict],  # State exams (e.g., Series 63)
                            "principalExamCategory": List[Dict],
                            "productExamCategory": List[Dict],  # Product exams (e.g., Series 6)
                            "registrationCount": Dict,
                            "registeredStates": List[str],
                            "registeredSROs": List[str],
                            "brokerDetails": Dict
                        }

    Raises:
        RateLimitExceeded: If the API returns a 403 status, indicating too many requests.
    """
    if not crd_number or not isinstance(crd_number, str):
        logger.error("Invalid CRD number", 
                    extra={"crd_number": crd_number, "employee_number": employee_number})
        return None

    service = "brokercheck"
    logger.info("Starting FINRA BrokerCheck detailed search", 
               extra={"crd_number": crd_number, "employee_number": employee_number})

    try:
        base_url = f'https://api.brokercheck.finra.org/search/individual/{crd_number}'
        params = dict(BROKERCHECK_CONFIG["default_params"])  # Create an immutable copy of default params
        params["query"] = crd_number  # Include query param for consistency, though possibly redundant here
        logger.debug("Fetching detailed info from BrokerCheck API", 
                    extra={"url": base_url, "params": params, "employee_number": employee_number})

        response = requests.get(base_url, params=params)
        response.raise_for_status()

        if response.status_code == 200:
            raw_data = response.json()
            # Extract and parse the 'content' string from '_source'
            if "hits" in raw_data and raw_data["hits"]["hits"]:
                content_str = raw_data["hits"]["hits"][0]["_source"]["content"]
                try:
                    detailed_data = json.loads(content_str)
                    logger.info("Detailed data fetched and parsed successfully", 
                               extra={"crd_number": crd_number, "employee_number": employee_number})
                    return detailed_data
                except json.JSONDecodeError as e:
                    logger.error("Failed to parse content JSON", 
                                extra={"crd_number": crd_number, "error": str(e), 
                                       "employee_number": employee_number})
                    return None
            else:
                logger.warning("No hits found in detailed response", 
                              extra={"crd_number": crd_number, "employee_number": employee_number})
                return None
        else:
            logger.error("Unexpected status code", 
                        extra={"crd_number": crd_number, "status_code": response.status_code, 
                               "employee_number": employee_number})
            return None

    except requests.exceptions.HTTPError as e:
        if response.status_code == 403:
            logger.error("Rate limit exceeded", 
                        extra={"crd_number": crd_number, "employee_number": employee_number})
            raise RateLimitExceeded(f"Rate limit exceeded for CRD {crd_number}.")
        logger.error("HTTP error during fetch", 
                    extra={"crd_number": crd_number, "error": str(e), 
                           "status_code": response.status_code, "employee_number": employee_number})
        return None
    except requests.exceptions.RequestException as e:
        logger.error("Request error during fetch", 
                    extra={"crd_number": crd_number, "error": str(e), "employee_number": employee_number})
        return None

# Example usage
if __name__ == "__main__":
    crd = "5695141"
    employee = "EMP001"
    try:
        # Fetch basic info
        basic_data = search_individual(crd, employee)
        if basic_data:
            print(f"Basic Data retrieved: {json.dumps(basic_data, indent=2)}")
        else:
            print(f"No basic data retrieved for CRD {crd}")

        # Fetch detailed info
        detailed_data = search_individual_detailed_info(crd, employee)
        if detailed_data:
            print(f"Detailed Data retrieved: {json.dumps(detailed_data, indent=2)}")
        else:
            print(f"No detailed data retrieved for CRD {crd}")
    except RateLimitExceeded as e:
        print(f"Rate limit error: {e}")

# Add this to the __all__ list at the top of the file
__all__ = [
    'search_individual',
    'search_individual_detailed_info',
]