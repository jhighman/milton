import requests
from typing import Dict, Optional, Any, List
import json
import logging
from logging import Logger
import time
from .exceptions import RateLimitExceeded  # Changed to relative import


"""
SEC IAPD Agent


This module provides an agent for interacting with the SEC's Investment Adviser Public Disclosure
(IAPD) API, which offers public access to professional information about investment advisers and
their representatives registered with the SEC or state securities authorities. The agent fetches
data without handling caching, leaving that to the calling client.


Key Features:
- Two services: Basic search and detailed profile retrieval.
- Uses CRD (Central Registration Depository) numbers to identify individuals.
- Structured logging with optional employee number for traceability.
- No caching; clients manage persistence.


Terminology (from SEC IAPD):
- CRD Number: A unique identifier assigned to advisers and representatives by FINRA/SEC.
- Investment Adviser (IA): A person or firm providing investment advice, registered under the Investment Advisers Act.
- IAPD: The SEC's public tool for researching adviser and representative backgrounds.
"""


# Module-level logger configuration
logger = logging.getLogger('sec_iapd_agent')


# Configuration for SEC IAPD API
IAPD_CONFIG: Dict[str, Dict[str, str]] = {
   "base_search_url": "https://api.adviserinfo.sec.gov/search/individual",
   "default_params": {
       "includePrevious": "true",  # Include past registrations
       "hl": "true",  # Highlight search terms in results
       "nrows": "12",  # Number of rows per response
       "start": "0",  # Starting index for pagination
       "r": "25",  # Radius (not typically used for CRD searches, kept for API compatibility)
       "sort": "score+desc",  # Sort by relevance score descending
       "wt": "json"  # Response format (JSON)
   }
}

# Request throttling delay in seconds
REQUEST_DELAY = 5.0




def search_individual_by_firm(individual_name: str, organization_crd: str, employee_number: Optional[str] = None,
                    logger: Logger = logger) -> Optional[Dict]:
    """
    Search for an individual by name within a specific firm using the firm's CRD.
    Args:
        individual_name: Individual's name to search for
        employee_number: Optional identifier for logging
        organization_crd: The firm's CRD number
        logger: Logger instance
    """
    log_context = {
        "individual_name": individual_name,
        "organization_crd": organization_crd,
        "employee_number": employee_number
    }
    
    logger.info("Starting SEC IAPD firm search", extra=log_context)

    # Implement request throttling
    time.sleep(REQUEST_DELAY)

    try:
        url = IAPD_CONFIG["base_search_url"]
        params = {
            'query': individual_name,
            'firm': organization_crd,
            'start': '0',
            'sortField': 'Relevance',
            'sortOrder': 'Desc',
            'type': 'Individual',
            'investmentAdvisors': 'true',
            'brokerDealers': 'false',
            'isNlSearch': 'false',
            'size': '50'
        }
        
        logger.debug("Fetching from IAPD API", 
                    extra={**log_context, "url": url, "params": params})

        full_url = f"{url}?{'&'.join(f'{key}={value}' for key, value in params.items())}"
        logger.debug(f"Fetching correlated firm info with URL: {full_url}")
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            return data

        elif response.status_code == 403:
            raise RateLimitExceeded(f"Rate limit exceeded for individual '{individual_name}' at firm {organization_crd}.")
        else:
            logger.error(f"Error fetching correlated firm info for '{individual_name}' at firm {organization_crd} from SEC API: {response.status_code}")
            return None

    except requests.exceptions.HTTPError as e:
        if response.status_code == 403:
            logger.error("Rate limit exceeded", extra=log_context)
            raise RateLimitExceeded(f"Rate limit exceeded for employee {employee_number}")
        logger.error("HTTP error during fetch", 
                    extra={**log_context, "error": str(e), "status_code": response.status_code})
        return None
    except requests.exceptions.RequestException as e:
        logger.error("Request error during fetch", 
                    extra={**log_context, "error": str(e)})
        return None




def search_individual(crd_number: str, employee_number: Optional[str] = None,
                    logger: Logger = logger) -> Optional[Dict]:
   """
   Fetches basic information from SEC IAPD for an individual using their CRD number.


   This function queries the IAPD API to retrieve a summary of individuals matching the CRD.
   The response is a search result with a 'hits' structure containing basic details in '_source'.


   Args:
       crd_number (str): The Central Registration Depository (CRD) number of the individual.
       employee_number (Optional[str]): An optional identifier for logging context, e.g., an internal employee ID. Defaults to None.
       logger (Logger): Logger instance for structured logging. Defaults to module logger.


   Returns:
       Optional[Dict]: A dictionary with basic info if successful, None if the fetch fails.
                       Response structure:
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
                                           "ind_other_names": List[str],  # Alternate names
                                           "ind_bc_scope": str,  # Broker status (e.g., "NotInScope")
                                           "ind_ia_scope": str,  # IA status (e.g., "Active", "InActive")
                                           "ind_ia_disclosure_fl": str,  # Disclosure flag (Y/N)
                                           "ind_approved_finra_registration_count": int,  # FINRA registrations
                                           "ind_employments_count": int,  # Total employments
                                           "ind_industry_cal_date_iapd": str,  # Date entered IA industry (YYYY-MM-DD)
                                           "ind_ia_current_employments": List[Dict]  # Current IA employments
                                       },
                                       "highlight": Dict  # Highlighted fields (e.g., CRD)
                                   }
                               ]
                           }
                       }
                       Employment Dict example:
                       {
                           "firm_id": str,
                           "firm_name": str,
                           "branch_city": str,
                           "branch_state": str,
                           "branch_zip": str,
                           "ia_only": str  # "Y" if IA-only role
                       }


   Raises:
       RateLimitExceeded: If the API returns a 403 status, indicating too many requests.
   """
   if not crd_number or not isinstance(crd_number, str):
       logger.error("Invalid CRD number",
                   extra={"crd_number": crd_number, "employee_number": employee_number})
       return None


   service = "iapd"
   logger.info("Starting SEC IAPD basic search",
              extra={"crd_number": crd_number, "employee_number": employee_number})


   try:
       url = IAPD_CONFIG["base_search_url"]
       params = dict(IAPD_CONFIG["default_params"])  # Create an immutable copy of default params
       params["query"] = crd_number  # Set the CRD as the search query
       logger.debug("Fetching basic info from IAPD API",
                   extra={"url": url, "params": params, "employee_number": employee_number})


       response = requests.get(url, params=params)
       response.raise_for_status()


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


def search_individual_detailed_info(crd_number: str, employee_number: Optional[str] = None,
                                  logger: Logger = logger) -> Optional[Dict]:
   """
   Fetches detailed information from SEC IAPD for an individual using their CRD number.


   This function retrieves a comprehensive profile for the specified CRD from the IAPD API.
   The response contains an 'iacontent' field with a JSON string that we parse into an object.


   Args:
       crd_number (str): The Central Registration Depository (CRD) number of the individual.
       employee_number (Optional[str]): An optional identifier for logging context.
       logger (Logger): Logger instance for structured logging.


   Returns:
       Optional[Dict]: A dictionary with detailed info if successful, None if the fetch fails.
                      Response structure after parsing:
                      {
                          "basicInformation": {
                              "individualId": int,
                              "firstName": str,
                              "middleName": str,
                              "lastName": str,
                              "otherNames": List[str],
                              "bcScope": str,
                              "iaScope": str,
                              "daysInIndustryCalculatedDateIAPD": str
                          },
                          "currentIAEmployments": List[Dict],
                          "previousIAEmployments": List[Dict],
                          "disclosureFlag": str,
                          "iaDisclosureFlag": str,
                          "disclosures": List,
                          "examsCount": Dict,
                          "registrationCount": Dict,
                          "registeredStates": List[Dict]
                      }
   """
   if not crd_number or not isinstance(crd_number, str):
       logger.error("Invalid CRD number",
                   extra={"crd_number": crd_number, "employee_number": employee_number})
       return None


   logger.info("Starting SEC IAPD detailed search",
              extra={"crd_number": crd_number, "employee_number": employee_number})


   try:
       base_url = f'https://api.adviserinfo.sec.gov/search/individual/{crd_number}'
       params = dict(IAPD_CONFIG["default_params"])
       params["query"] = crd_number
      
       response = requests.get(base_url, params=params)
       response.raise_for_status()


       if response.status_code == 200:
           data = response.json()
           if "hits" in data and data["hits"]["hits"]:
               source_data = data["hits"]["hits"][0]["_source"]
               if "iacontent" in source_data:
                   try:
                       detailed_data = json.loads(source_data["iacontent"])
                       logger.info("Detailed data fetched and parsed successfully",
                                 extra={"crd_number": crd_number, "employee_number": employee_number})
                       return detailed_data
                   except json.JSONDecodeError as e:
                       logger.error("Failed to parse iacontent JSON",
                                  extra={"crd_number": crd_number, "error": str(e)})
                       return None
               else:
                   logger.warning("No iacontent found in response",
                                extra={"crd_number": crd_number})
                   return None
           else:
               logger.warning("No hits found in detailed response",
                            extra={"crd_number": crd_number})
               return None


       logger.error("Unexpected status code",
                   extra={"crd_number": crd_number, "status_code": response.status_code})
       return None


   except requests.exceptions.HTTPError as e:
       if response.status_code == 403:
           raise RateLimitExceeded(f"Rate limit exceeded for CRD {crd_number}.")
       logger.error("HTTP error during fetch",
                   extra={"crd_number": crd_number, "error": str(e)})
       return None
   except requests.exceptions.RequestException as e:
       logger.error("Request error during fetch",
                   extra={"crd_number": crd_number, "error": str(e)})
       return None


# Example usage
if __name__ == "__main__":
   crd = "1438859"
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
           print("\nDetailed Data retrieved:")
           print(json.dumps(detailed_data, indent=2))
       else:
           print(f"No detailed data retrieved for CRD {crd}")
   except RateLimitExceeded as e:
       print(f"Rate limit error: {e}")

