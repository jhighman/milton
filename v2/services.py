import logging
import time
from typing import Optional, Dict, Any, List
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService

# Assuming marshaller.py defines fetcher functions for each agent service
from marshaller import (
    fetch_agent_sec_iapd_search,
    fetch_agent_sec_iapd_detailed,
    fetch_agent_finra_bc_search,
    fetch_agent_finra_bc_detailed,
    fetch_agent_sec_arb_search,
    fetch_agent_finra_disc_search,
    fetch_agent_nfa_search,
    fetch_agent_finra_arb_search,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("FinancialServicesFacade")

# Function to create a Selenium WebDriver instance
def create_driver(headless: bool = True) -> webdriver.Chrome:
    """Create a Selenium WebDriver instance for web-based queries."""
    logger.debug("Initializing Chrome WebDriver", extra={"headless": headless})
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/115.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=ChromeService(), options=options)

class FinancialServicesFacade:
    """Facade providing discrete functions for financial regulator services via the marshaller.

    This class consolidates access to various financial regulator services, including SEC IAPD,
    FINRA BrokerCheck, SEC Arbitration, FINRA Disciplinary, NFA Basic, and FINRA Arbitration.
    It manages a shared Selenium WebDriver for web-based queries and ensures proper resource
    cleanup using a context manager. The marshaller handles caching and data retrieval.

    Notes:
        - Example Responses (JSON assumed):
            - SEC Arbitration:
                - first_name: "Mark"
                - last_name: "Miller"
                - result:
                    - Enforcement Action: "W. MARK MILLER - SEC Administrative Proceeding"
                    - Date Filed: "July 2, 2014"
                    - Documents:
                        - "https://www.sec.gov/litigation/admin/2014/34-72512.pdf"
                - total_actions: 1
            - FINRA Disciplinary:
                - result:
                    - Case ID: "2022076589301"
                    - Case Summary: "The Department of Enforcement alleges: ..."
                    - Document Type: "Complaints"
                    - Firms/Individuals: "Kevin John Herne"
                    - Action Date: "01/30/2025"
                    - ... (additional records possible)
            - NFA Basic:
                - result:
                    - Name: "SMITH, JANET"
                    - NFA ID: "0198019"
                    - Firm: ""
                    - Current NFA Membership Status: "Not an NFA Member"
                    - Current Registration Types: "-"
                    - Regulatory Actions: "No"
                    - Details Available: "Yes"
                    - ... (additional records possible)
            - FINRA Arbitration:
                - result:
                    - Award Document: "09-05842"
                    - PDF URL: "/sites/default/files/aao_documents/09-05842-Award-FINRA-20110217.pdf"
                    - Case Summary:
                        - Claimant(s): "Bob Starnes, The Starnes Family Trust dated 6/25/96"
                        - Claimant Representative(s): "Jeffrey P. Coleman"
                        - Respondent(s): "Merrill Lynch Pierce Fenner & Smith Inc."
                        - Neutral(s): "William H. Fleece, Nicholas G. Dukas, Andrea Bailey"
                        - Hearing Site: "Tampa, FL"
                    - Document Type: "Award"
                    - Forum: "FINRA"
                    - Date of Award: "02/17/2011"
                    - ... (additional records possible)
    """

    def __init__(self):
        """Initialize the facade with a shared WebDriver for Selenium agents."""
        self.driver = create_driver()
        logger.info("WebDriver initialized")

    def __enter__(self):
        """Enter context, returning the facade instance."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context, closing the WebDriver."""
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver closed")

    ### SEC IAPD Agent Functions
    def search_sec_iapd_individual(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        """Search SEC IAPD for individual basic information.

        This function retrieves basic information for an individual from the SEC IAPD database
        using their CRD number. It returns a summary of matching records.

        Args:
            crd_number (str): The Central Registration Depository (CRD) number of the individual.
            employee_number (Optional[str]): An optional identifier for logging context, e.g., an internal employee ID. Defaults to None.

        Returns:
            Optional[Dict]: A dictionary with basic info if successful, None if the fetch fails.
                            Example response structure (JSON assumed):
                            - hits:
                                - total: int  # Number of matching records
                                - hits:
                                    - _source:
                                        - ind_source_id: str  # CRD number
                                        - ind_firstname: str
                                        - ind_middlename: str
                                        - ind_lastname: str
                                        - ind_other_names: List[str]
                                        - ind_bc_scope: str  # Broker status
                                        - ind_ia_scope: str  # IA status
                                        - ind_days_in_industry: str

        Notes:
            - Caching is handled by the marshaller, storing responses for 90 days.
            - If no data is returned, the function logs a warning and returns None.
        """
        logger.info(f"Fetching SEC IAPD basic info for CRD: {crd_number}, Employee: {employee_number}")
        result = fetch_agent_sec_iapd_search(employee_number, {"crd_number": crd_number})
        if result:
            logger.info(f"Successfully fetched SEC IAPD basic data for CRD: {crd_number}")
            return result
        logger.warning(f"No data found for CRD: {crd_number} in SEC IAPD basic search")
        return None

    def search_sec_iapd_detailed(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        """Search SEC IAPD for individual detailed information.

        This function retrieves a comprehensive profile for the specified CRD from the IAPD API.
        The response contains detailed employment, exam, disclosure, and registration information.

        Args:
            crd_number (str): The Central Registration Depository (CRD) number of the individual.
            employee_number (Optional[str]): An optional identifier for logging context. Defaults to None.

        Returns:
            Optional[Dict]: A dictionary with detailed info if successful, None if the fetch fails.
                            Example response structure (JSON assumed):
                            - hits:
                                - total: 1
                                - hits:
                                    - _type: "_doc"
                                    - _source:
                                        - iacontent:
                                            - basicInformation:
                                                - individualId: 6184005
                                                - firstName: "DAVID"
                                                - middleName: "ALAN"
                                                - lastName: "WILLETT"
                                                - nameSuffix: "II"
                                                - otherNames: []
                                                - bcScope: "InActive"
                                                - iaScope: "Active"
                                                - daysInIndustryCalculatedDateIAPD: "2/5/2016"
                                            - currentEmployments: []
                                            - currentIAEmployments:
                                                - firmId: 105348
                                                - firmName: "CREATIVE PLANNING"
                                                - iaOnly: "Y"
                                                - registrationBeginDate: "5/8/2019"
                                                - firmBCScope: "NOTINSCOPE"
                                                - firmIAScope: "ACTIVE"
                                                - iaSECNumber: "18564"
                                                - iaSECNumberType: "801"
                                                - branchOfficeLocations:
                                                    - displayOrder: 2
                                                    - locatedAtFlag: "Y"
                                                    - supervisedFromFlag: "N"
                                                    - privateResidenceFlag: "Y"
                                                    - city: "Palm Beach Gardens"
                                                    - state: "FL"
                                                    - country: "United States"
                                                    - nonRegisteredOfficeFlag: "Y"
                                                    - elaBeginDate: "03/25/2024"
                                                    - displayOrder: 3
                                                    - locatedAtFlag: "Y"
                                                    - supervisedFromFlag: "N"
                                                    - privateResidenceFlag: "N"
                                                    - street1: "2255 Glades Rd, Suite 330W"
                                                    - city: "Boca Raton"
                                                    - cityAlias: ["BOCA RATON"]
                                                    - state: "FL"
                                                    - country: "United States"
                                                    - zipCode: "33431"
                                                    - latitude: "26.378146"
                                                    - longitude: "-80.102036"
                                                    - geoLocation: "26.378146,-80.102036"
                                                    - nonRegisteredOfficeFlag: "Y"
                                                    - elaBeginDate: "07/10/2024"
                                            - previousEmployments:
                                                - iaOnly: "N"
                                                - iaSECNumber: "8095"
                                                - iaSECNumberType: "801"
                                                - bdSECNumber: "14088"
                                                - firmId: 2881
                                                - firmName: "NORTHWESTERN MUTUAL INVESTMENT SERVICES, LLC"
                                                - city: "NASHVILLE"
                                                - state: "TN"
                                                - registrationBeginDate: "9/11/2015"
                                                - registrationEndDate: "12/11/2018"
                                                - firmBCScope: "ACTIVE"
                                                - firmIAScope: "ACTIVE"
                                            - previousIAEmployments: []
                                            - disclosureFlag: "Y"
                                            - iaDisclosureFlag: "Y"
                                            - disclosures:
                                                - eventDate: "3/14/2014"
                                                - disclosureType: "Criminal"
                                                - disclosureResolution: "Final Disposition"
                                                - isIapdExcludedCCFlag: "N"
                                                - isBcExcludedCCFlag: "N"
                                                - bcCtgryType: 11
                                                - iaCtgryType: 10
                                                - disclosureDetail:
                                                    - criminalCharges:
                                                        - Charges: "Misdemeanor-Giving False Information To Law Enforcement Officer"
                                                        - Disposition: "Dismissed"
                                                        - Amended Charges: "Original charge of misdemeanor was dismissed after completion of diversion program. Completed program and charge was officially dismissed 06/05/2014."
                                                        - Amended Charge Type: "OTHER"
                                                        - Amended Charge Disposition: "Dismissed"
                                                    - Broker Comment: "Per the representative: In March of 2014 while on spring break in college, he had presented a fake ID to the bouncer to obtain entry into a bar. He was pulled aside by a police officer and given a ticket, he was released immediately and obtained entry to the same bar as a minor."
                                            - iaDisclosures:
                                                - disclosureType: "Criminal"
                                                - disclosureCount: 1
                                            - examsCount:
                                                - stateExamCount: 2
                                                - principalExamCount: 0
                                                - productExamCount: 2
                                            - stateExamCategory:
                                                - examCategory: "Series 65"
                                                - examName: "Uniform Investment Adviser Law Examination"
                                                - examTakenDate: "3/29/2019"
                                                - examScope: "IA"
                                                - examCategory: "Series 63"
                                                - examName: "Uniform Securities Agent State Law Examination"
                                                - examTakenDate: "11/19/2015"
                                                - examScope: "BC"
                                            - principalExamCategory: []
                                            - productExamCategory:
                                                - examCategory: "SIE"
                                                - examName: "Securities Industry Essentials Examination"
                                                - examTakenDate: "10/1/2018"
                                                - examScope: "BC"
                                                - examCategory: "Series 6"
                                                - examName: "Investment Company Products/Variable Contracts Representative Examination"
                                                - examTakenDate: "9/11/2015"
                                                - examScope: "BC"
                                            - registrationCount:
                                                - hasInactiveRegistration: "N"
                                                - hasSuspendedRegistration: "N"
                                                - approvedSRORegistrationCount: 0
                                                - approvedFinraRegistrationCount: 0
                                                - approvedStateRegistrationCount: 0
                                                - approvedIAStateRegistrationCount: 2
                                            - registeredStates:
                                                - state: "Florida"
                                                - regScope: "IA"
                                                - status: "APPROVED"
                                                - regDate: "5/8/2019"
                                                - state: "Texas"
                                                - regScope: "IA"
                                                - status: "APPROVED_RES"
                                                - regDate: "8/29/2023"
                                            - registeredSROs: []
                                            - brokerDetails:
                                                - hasBCComments: "N"
                                                - hasIAComments: "N"
                                                - legacyReportStatusDescription: "Not Requested"

        Notes:
            - Caching is handled by the marshaller, storing responses for 90 days.
            - If no data is returned, the function logs a warning and returns None.
            - No explicit rate limiting is applied here; any limits are managed by the marshaller.
        """
        logger.info(f"Fetching SEC IAPD detailed info for CRD: {crd_number}, Employee: {employee_number}")
        result = fetch_agent_sec_iapd_detailed(employee_number, {"crd_number": crd_number})
        if result:
            logger.info(f"Successfully fetched SEC IAPD detailed data for CRD: {crd_number}")
            return result
        logger.warning(f"No data found for CRD: {crd_number} in SEC IAPD detailed search")
        return None

    ### FINRA BrokerCheck Agent Functions
    def search_finra_brokercheck_individual(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        """Search FINRA BrokerCheck for individual basic information with rate limiting.

        This function queries FINRA BrokerCheck to retrieve a summary of individuals matching the CRD.
        The response is a search result with a 'hits' structure containing basic details in '_source'.
        Rate limited to one call every 5 seconds.

        Args:
            crd_number (str): The Central Registration Depository (CRD) number of the individual.
            employee_number (Optional[str]): An optional identifier for logging context. Defaults to None.

        Returns:
            Optional[Dict]: A dictionary with basic info if successful, None if the fetch fails.
                            Example response structure (JSON assumed):
                            - hits:
                                - total: int  # Number of matching records
                                - hits:
                                    - _type: "_doc"
                                    - _source:
                                        - ind_source_id: str  # CRD number
                                        - ind_firstname: str
                                        - ind_middlename: str
                                        - ind_lastname: str
                                        - ind_namesuffix: str
                                        - ind_other_names: List[str]  # Alternate names
                                        - ind_bc_scope: str  # Broker status (e.g., "InActive")
                                        - ind_ia_scope: str  # IA status (e.g., "InActive")
                                        - ind_bc_disclosure_fl: str  # Disclosure flag (Y/N)
                                        - ind_approved_finra_registration_count: int
                                        - ind_employments_count: int
                                        - ind_industry_days: str  # Days in industry
                                        - ind_current_employments: List  # Current employments (may be empty)
                                    - highlight:
                                        - ind_source_id: List[str]  # Highlighted CRD

        Notes:
            - Rate limiting is enforced with a 5-second delay between calls to comply with FINRA BrokerCheck restrictions.
            - Caching is handled by the marshaller, storing responses for 90 days.
            - If no data is returned, the function logs a warning and returns None.
        """
        logger.info(f"Fetching FINRA BrokerCheck basic info for CRD: {crd_number}, Employee: {employee_number}")
        time.sleep(5)  # Enforce 5-second delay to respect FINRA BrokerCheck rate limits
        result = fetch_agent_finra_bc_search(employee_number, {"crd_number": crd_number})
        if result:
            logger.info(f"Successfully fetched FINRA BrokerCheck basic data for CRD: {crd_number}")
            return result
        logger.warning(f"No data found for CRD: {crd_number} in FINRA BrokerCheck basic search")
        return None

    def search_finra_brokercheck_detailed(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        """Search FINRA BrokerCheck for individual detailed information with rate limiting.

        This function retrieves a comprehensive profile for an individual from FINRA BrokerCheck
        using their CRD number. Rate limited to one call every 5 seconds.

        Args:
            crd_number (str): The Central Registration Depository (CRD) number of the individual.
            employee_number (Optional[str]): An optional identifier for logging context. Defaults to None.

        Returns:
            Optional[Dict]: A dictionary with detailed info if successful, None if the fetch fails.
                            Example response structure (JSON assumed):
                            - hits:
                                - total: 1
                                - hits:
                                    - _type: "_doc"
                                    - _source:
                                        - content:
                                            - basicInformation:
                                                - individualId: 1555796
                                                - firstName: "DEBRA"
                                                - middleName: "ANN"
                                                - lastName: "GRIEWAHN"
                                                - otherNames:
                                                    - "DEBRA ANN COLLINS"
                                                    - "DEBRA MAIRS COLLINS"
                                                    - "DEBRA ANN MAIRS"
                                                    - "DEBRA ANN MONTANARELLO"
                                                    - "DEBRA MAIRS WOJCHIECHOWSKI"
                                                    - "DEBRA MARIS WOJCIECHOWSKI"
                                                - bcScope: "Active"
                                                - iaScope: "Active"
                                                - daysInIndustryCalculatedDate: "3/14/1993"
                                            - currentEmployments:
                                                - firmId: 7927
                                                - firmName: "NORTHERN TRUST SECURITIES, INC."
                                                - iaOnly: "N"
                                                - registrationBeginDate: "8/20/2004"
                                                - firmBCScope: "ACTIVE"
                                                - firmIAScope: "ACTIVE"
                                                - iaSECNumber: "80781"
                                                - iaSECNumberType: "801"
                                                - bdSECNumber: "23689"
                                                - branchOfficeLocations:
                                                    - displayOrder: 1
                                                    - locatedAtFlag: "Y"
                                                    - supervisedFromFlag: "N"
                                                    - privateResidenceFlag: "N"
                                                    - branchOfficeId: "211492"
                                                    - street1: "333 South Wabash Avenue"
                                                    - street2: "34th Floor"
                                                    - city: "CHICAGO"
                                                    - cityAlias: ["CHICAGO"]
                                                    - state: "IL"
                                                    - country: "United States"
                                                    - zipCode: "60604"
                                                    - latitude: "41.877116"
                                                    - longitude: "-87.624727"
                                                    - geoLocation: "41.877116,-87.624727"
                                                    - nonRegisteredOfficeFlag: "N"
                                                    - elaBeginDate: "01/01/2012"
                                            - currentIAEmployments:
                                                - firmId: 7927
                                                - firmName: "NORTHERN TRUST SECURITIES, INC"
                                                - iaOnly: "Y"
                                                - registrationBeginDate: "1/11/2024"
                                                - firmBCScope: "ACTIVE"
                                                - firmIAScope: "ACTIVE"
                                                - iaSECNumber: "80781"
                                                - iaSECNumberType: "801"
                                                - bdSECNumber: "23689"
                                                - branchOfficeLocations:
                                                    - displayOrder: 1
                                                    - locatedAtFlag: "Y"
                                                    - supervisedFromFlag: "N"
                                                    - privateResidenceFlag: "N"
                                                    - branchOfficeId: "211492"
                                                    - street1: "333 South Wabash Avenue"
                                                    - street2: "34th Floor"
                                                    - city: "CHICAGO"
                                                    - cityAlias: ["CHICAGO"]
                                                    - state: "IL"
                                                    - country: "United States"
                                                    - zipCode: "60604"
                                                    - latitude: "41.877116"
                                                    - longitude: "-87.624727"
                                                    - geoLocation: "41.877116,-87.624727"
                                                    - nonRegisteredOfficeFlag: "N"
                                                    - elaBeginDate: "01/01/2012"
                                                    - displayOrder: 2
                                                    - locatedAtFlag: "Y"
                                                    - supervisedFromFlag: "N"
                                                    - privateResidenceFlag: "Y"
                                                    - city: "Linden"
                                                    - state: "TN"
                                                    - country: "United States"
                                                    - nonRegisteredOfficeFlag: "Y"
                                                    - elaBeginDate: "06/03/2024"
                                            - previousEmployments:
                                                - iaOnly: "N"
                                                - bdSECNumber: "41505"
                                                - firmId: 29767
                                                - firmName: "MELVIN SECURITIES, L.L.C."
                                                - city: "CHICAGO"
                                                - state: "IL"
                                                - country: "UNITED STATES"
                                                - registrationBeginDate: "12/17/1999"
                                                - registrationEndDate: "8/10/2004"
                                                - firmBCScope: "INACTIVE"
                                                - firmIAScope: "NOTINSCOPE"
                                                - expelledDate: "6/21/2024"
                                            - previousIAEmployments:
                                                - iaOnly: "Y"
                                                - iaSECNumber: "80781"
                                                - iaSECNumberType: "801"
                                                - bdSECNumber: "23689"
                                                - firmId: 7927
                                                - firmName: "NORTHERN TRUST SECURITIES, INC"
                                                - city: "WEST PALM BEACH"
                                                - state: "FL"
                                                - country: "United States"
                                                - registrationBeginDate: "4/8/2015"
                                                - registrationEndDate: "1/9/2024"
                                                - firmBCScope: "ACTIVE"
                                                - firmIAScope: "ACTIVE"
                                            - disclosureFlag: "N"
                                            - iaDisclosureFlag: "N"
                                            - disclosures: []
                                            - examsCount:
                                                - stateExamCount: 2
                                                - principalExamCount: 6
                                                - productExamCount: 6
                                            - stateExamCategory:
                                                - examCategory: "Series 66"
                                                - examName: "Uniform Combined State Law Examination"
                                                - examTakenDate: "3/30/2015"
                                                - examScope: "BOTH"
                                                - examCategory: "Series 63"
                                                - examName: "Uniform Securities Agent State Law Examination"
                                                - examTakenDate: "6/27/1992"
                                                - examScope: "BC"
                                            - principalExamCategory:
                                                - examCategory: "Series 14"
                                                - examName: "Compliance Officer Examination"
                                                - examTakenDate: "1/2/2023"
                                                - examScope: "BC"
                                            - productExamCategory:
                                                - examCategory: "Series 99TO"
                                                - examName: "Operations Professional Examination"
                                                - examTakenDate: "1/2/2023"
                                                - examScope: "BC"
                                            - registrationCount:
                                                - approvedSRORegistrationCount: 1
                                                - approvedFinraRegistrationCount: 1
                                                - approvedStateRegistrationCount: 52
                                                - approvedIAStateRegistrationCount: 1
                                            - registeredStates:
                                                - state: "Alabama"
                                                - regScope: "BC"
                                                - status: "APPROVED"
                                                - regDate: "8/20/2004"
                                            - registeredSROs:
                                                - sro: "FINRA"
                                                - status: "APPROVED"
                                                - CategoriesList:
                                                    - "Compliance Officer"
                                                    - "Financial and Operations Principal"
                                            - brokerDetails:
                                                - hasBCComments: "N"
                                                - hasIAComments: "N"
                                                - legacyReportStatusDescription: "Not Requested"

        Notes:
            - Rate limiting is enforced with a 5-second delay between calls to comply with FINRA BrokerCheck restrictions.
            - Caching is handled by the marshaller, storing responses for 90 days.
            - If no data is returned, the function logs a warning and returns None.
        """
        logger.info(f"Fetching FINRA BrokerCheck detailed info for CRD: {crd_number}, Employee: {employee_number}")
        time.sleep(5)  # Rate limit enforcement
        result = fetch_agent_finra_bc_detailed(employee_number, {"crd_number": crd_number})
        if result:
            logger.info(f"Successfully fetched FINRA BrokerCheck detailed data for CRD: {crd_number}")
            return result
        logger.warning(f"No data found for CRD: {crd_number} in FINRA BrokerCheck detailed search")
        return None

    ### SEC Arbitration Agent Functions
    def search_sec_arbitration(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Search SEC Arbitration for individual claims.

        This function queries SEC Arbitration records for enforcement actions involving the specified individual,
        scraped from SEC web sources using Selenium.

        Args:
            first_name (str): First name of the individual.
            last_name (str): Last name of the individual.
            employee_number (Optional[str]): An optional identifier for logging context.

        Returns:
            Dict[str, Any]: A dictionary with enforcement action records or an indication of no results.
                            Example response structure (JSON assumed):
                            - first_name: "Mark"
                            - last_name: "Miller"
                            - result:
                                - Enforcement Action: "W. MARK MILLER - SEC Administrative Proceeding"
                                - Date Filed: "July 2, 2014"
                                - Documents:
                                    - "https://www.sec.gov/litigation/admin/2014/34-72512.pdf"
                            - total_actions: 1

        Notes:
            - Caching is handled by the marshaller, storing responses for 90 days.
            - Selenium WebDriver is used for this web-based query.
            - If no enforcement actions are found, 'result' will be "No Results Found".
            - The example is derived from a test case for "Mark Miller" with one enforcement action dated July 2, 2014.
        """
        logger.info(f"Fetching SEC Arbitration data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        return fetch_agent_sec_arb_search(employee_number, params, self.driver)

    ### FINRA Disciplinary Agent Functions
    def search_finra_disciplinary(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Search FINRA Disciplinary for individual actions.

        This function queries FINRA Disciplinary records for actions involving the specified individual.

        Args:
            first_name (str): First name of the individual.
            last_name (str): Last name of the individual.
            employee_number (Optional[str]): An optional identifier for logging context.

        Returns:
            Dict[str, Any]: A dictionary with a 'result' key containing disciplinary records.
                            Example response structure (JSON assumed):
                            - result:
                                - Case ID: "2022076589301"
                                - Case Summary: "The Department of Enforcement alleges: ..."
                                - Document Type: "Complaints"
                                - Firms/Individuals: "Kevin John Herne"
                                - Action Date: "01/30/2025"
                                - ... (additional records possible)

        Notes:
            - Caching is handled by the marshaller, storing responses for 90 days.
            - This service currently does not require Selenium; WebDriver is passed but may not be used.
        """
        logger.info(f"Fetching FINRA Disciplinary data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        return fetch_agent_finra_disc_search(employee_number, params, self.driver)

    ### NFA Basic Agent Functions
    def search_nfa_basic(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Search NFA Basic for individual profiles.

        This function queries NFA Basic for profiles of individuals matching the specified name.

        Args:
            first_name (str): First name of the individual.
            last_name (str): Last name of the individual.
            employee_number (Optional[str]): An optional identifier for logging context.

        Returns:
            Dict[str, Any]: A dictionary with a 'result' key containing individual profiles.
                            Example response structure (JSON assumed):
                            - result:
                                - Name: "SMITH, JANET"
                                - NFA ID: "0198019"
                                - Firm: ""
                                - Current NFA Membership Status: "Not an NFA Member"
                                - Current Registration Types: "-"
                                - Regulatory Actions: "No"
                                - Details Available: "Yes"
                                - ... (additional records possible)

        Notes:
            - Caching is handled by the marshaller, storing responses for 90 days.
            - Selenium WebDriver is used for this web-based query.
        """
        logger.info(f"Fetching NFA Basic data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        return fetch_agent_nfa_search(employee_number, params, self.driver)

    ### FINRA Arbitration Agent Functions
    def search_finra_arbitration(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Search FINRA Arbitration for individual claims.

        This function queries FINRA Arbitration records for claims involving the specified individual.

        Args:
            first_name (str): First name of the individual.
            last_name (str): Last name of the individual.
            employee_number (Optional[str]): An optional identifier for logging context.

        Returns:
            Dict[str, Any]: A dictionary with a 'result' key containing arbitration records.
                            Example response structure (JSON assumed):
                            - result:
                                - Award Document: "09-05842"
                                - PDF URL: "/sites/default/files/aao_documents/09-05842-Award-FINRA-20110217.pdf"
                                - Case Summary:
                                    - Claimant(s): "Bob Starnes, The Starnes Family Trust dated 6/25/96"
                                    - Claimant Representative(s): "Jeffrey P. Coleman"
                                    - Respondent(s): "Merrill Lynch Pierce Fenner & Smith Inc."
                                    - Neutral(s): "William H. Fleece, Nicholas G. Dukas, Andrea Bailey"
                                    - Hearing Site: "Tampa, FL"
                                - Document Type: "Award"
                                - Forum: "FINRA"
                                - Date of Award: "02/17/2011"
                                - ... (additional records possible)

        Notes:
            - Caching is handled by the marshaller, storing responses for 90 days.
            - Selenium WebDriver is used for this web-based query.
        """
        logger.info(f"Fetching FINRA Arbitration data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        return fetch_agent_finra_arb_search(employee_number, params, self.driver)

    def get_organization_crd(self, organization_name: str) -> str:
        """
        Get the CRD number for an organization.

        Args:
            organization_name (str): Name of the organization to look up

        Returns:
            str: CRD number of the organization

        TODO: Implement organization CRD lookup logic
        - Consider using SEC/FINRA APIs if available
        - May need to implement web scraping from FINRA BrokerCheck
        - Cache results to avoid repeated lookups
        - Handle variations in organization names
        """
        # TODO: Implement organization CRD lookup
        return ""

# Example usage
def main():
    with FinancialServicesFacade() as facade:
        # SEC IAPD
        print("SEC IAPD Individual:", facade.search_sec_iapd_individual("12345", "EMP001"))
        print("SEC IAPD Detailed:", facade.search_sec_iapd_detailed("6184005", "EMP001"))

        # FINRA BrokerCheck
        print("FINRA BrokerCheck Individual:", facade.search_finra_brokercheck_individual("67890", "EMP001"))
        print("FINRA BrokerCheck Detailed:", facade.search_finra_brokercheck_detailed("1555796", "EMP001"))

        # SEC Arbitration
        print("SEC Arbitration:", facade.search_sec_arbitration("Mark", "Miller", "EMP001"))

        # FINRA Disciplinary
        print("FINRA Disciplinary:", facade.search_finra_disciplinary("John", "Doe", "EMP001"))

        # NFA Basic
        print("NFA Basic:", facade.search_nfa_basic("Jane", "Smith", "EMP001"))

        # FINRA Arbitration
        print("FINRA Arbitration:", facade.search_finra_arbitration("Bob", "Smith", "EMP001"))

if __name__ == "__main__":
    main()