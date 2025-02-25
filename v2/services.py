import logging
import os
import json
from typing import Optional, Dict, Any, List
from selenium import webdriver
import argparse

from marshaller import (
    fetch_agent_sec_iapd_search,
    fetch_agent_sec_iapd_detailed,
    fetch_agent_finra_bc_search,
    fetch_agent_finra_bc_detailed,
    fetch_agent_sec_arb_search,
    fetch_agent_finra_disc_search,
    fetch_agent_nfa_search,
    fetch_agent_finra_arb_search,
    fetch_agent_sec_iapd_correlated,
    fetch_agent_sec_disc_search,
    create_driver,
)
from normalizer import create_disciplinary_record, NormalizationError, create_individual_record

# Setup logging with DEBUG level for detailed tracing
logging.basicConfig(
    level=logging.DEBUG,  # Set to INFO later if too verbose
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("FinancialServicesFacade")

RUN_HEADLESS = True  # Default to headless mode for browser automation

class FinancialServicesFacade:
    @staticmethod
    def _load_organizations_cache() -> Optional[List[Dict]]:
        cache_file = os.path.join("input", "organizationsCrd.jsonl")
        if not os.path.exists(cache_file):
            logger.error("Failed to load organizations cache.")
            return None

        try:
            organizations = []
            with open(cache_file, 'r') as f:
                for line in f:
                    if line.strip():
                        organizations.append(json.loads(line))
            return organizations
        except Exception as e:
            logger.error(f"Error loading organizations cache: {e}")
            return None

    @staticmethod
    def _normalize_organization_name(name: str) -> str:
        return name.lower().replace(" ", "")

    @staticmethod
    def _normalize_individual_record(
        data_source: str,
        basic_info: Optional[Dict[str, Any]],
        detailed_info: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        # Delegate to normalizer.py for consistent individual record normalization
        return create_individual_record(data_source, basic_info, detailed_info)

    def get_organization_crd(self, organization_name: str) -> Optional[str]:
        orgs_data = self._load_organizations_cache()
        if not orgs_data:
            logger.error("Failed to load organizations cache.")
            return None

        normalized_search_name = self._normalize_organization_name(organization_name)
        for org in orgs_data:
            stored_name = self._normalize_organization_name(org.get("name", ""))
            if stored_name == normalized_search_name:
                crd = org.get("organizationCRD")
                if crd and crd != "N/A":
                    logger.info(f"Found CRD {crd} for organization '{organization_name}'.")
                    return crd
                else:
                    logger.warning(f"CRD not found for organization '{organization_name}'.")
                    return None
        
        logger.warning(f"Organization '{organization_name}' not found in cache.")
        return "NOT_FOUND"

    # SEC IAPD Agent Functions
    def search_sec_iapd_individual(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.info(f"Fetching SEC IAPD basic info for CRD: {crd_number}, Employee: {employee_number}")
        basic_result = fetch_agent_sec_iapd_search(employee_number, {"crd_number": crd_number})
        detailed_result = fetch_agent_sec_iapd_detailed(employee_number, {"crd_number": crd_number}) if basic_result else None
        if basic_result:
            logger.info(f"Successfully fetched SEC IAPD data for CRD: {crd_number}")
            return self._normalize_individual_record("IAPD", basic_result, detailed_result)
        logger.warning(f"No data found for CRD: {crd_number} in SEC IAPD search")
        return None

    def search_sec_iapd_detailed(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.warning(f"Calling search_sec_iapd_detailed is deprecated; use search_sec_iapd_individual instead for CRD: {crd_number}")
        return self.search_sec_iapd_individual(crd_number, employee_number)

    def search_sec_iapd_correlated(self, individual_name: str, organization_crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.info(f"Fetching SEC IAPD correlated info for {individual_name} at organization {organization_crd_number}, Employee: {employee_number}")
        result = fetch_agent_sec_iapd_correlated(employee_number, {
            "individual_name": individual_name,
            "organization_crd_number": organization_crd_number
        })
        if result:
            logger.info(f"Successfully fetched SEC IAPD correlated data for {individual_name} at organization {organization_crd_number}")
            return self._normalize_individual_record("IAPD", result)
        logger.warning(f"No data found for {individual_name} at organization {organization_crd_number} in SEC IAPD correlated search")
        return None

    # FINRA BrokerCheck Agent Functions
    def search_finra_brokercheck_individual(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.info(f"Fetching FINRA BrokerCheck basic info for CRD: {crd_number}, Employee: {employee_number}")
        basic_result = fetch_agent_finra_bc_search(employee_number, {"crd_number": crd_number})
        detailed_result = fetch_agent_finra_bc_detailed(employee_number, {"crd_number": crd_number}) if basic_result else None
        if basic_result:
            logger.info(f"Successfully fetched FINRA BrokerCheck data for CRD: {crd_number}")
            return self._normalize_individual_record("BrokerCheck", basic_result, detailed_result)
        logger.warning(f"No data found for {crd_number} in FINRA BrokerCheck search")
        return None

    def search_finra_brokercheck_detailed(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.warning(f"Calling search_finra_brokercheck_detailed is deprecated; use search_finra_brokercheck_individual instead for CRD: {crd_number}")
        return self.search_finra_brokercheck_individual(crd_number, employee_number)

    # SEC Arbitration Agent Functions
    def search_sec_arbitration(self, first_name: str, last_name: str, employee_number: Optional[str] = None, driver: Optional[webdriver.Chrome] = None) -> Optional[Dict]:
        logger.info(f"Fetching SEC Arbitration data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        result = fetch_agent_sec_arb_search(employee_number, params, driver)
        if result:
            logger.info(f"Successfully fetched SEC Arbitration data for {first_name} {last_name}")
            return result[0] if isinstance(result, list) and result else result
        logger.warning(f"No data found for {first_name} {last_name} in SEC Arbitration search")
        return None

    # FINRA Disciplinary Agent Functions
    def search_finra_disciplinary(self, first_name: str, last_name: str, employee_number: Optional[str] = None, driver: Optional[webdriver.Chrome] = None) -> Optional[Dict]:
        logger.info(f"Fetching FINRA Disciplinary data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        searched_name = f"{first_name} {last_name}"
        result = fetch_agent_finra_disc_search(employee_number, params, driver)
        if result:
            logger.debug(f"FINRA raw result: {json.dumps(result, indent=2)}")
            normalized = create_disciplinary_record("FINRA_Disciplinary", result, searched_name)
            logger.debug(f"FINRA normalized result: {json.dumps(normalized, indent=2)}")
            logger.info(f"Successfully fetched FINRA Disciplinary data for {first_name} {last_name}")
            return normalized
        logger.warning(f"No data found for {first_name} {last_name} in FINRA Disciplinary search")
        return None

    # NFA Basic Agent Functions
    def search_nfa_basic(self, first_name: str, last_name: str, employee_number: Optional[str] = None, driver: Optional[webdriver.Chrome] = None) -> Dict[str, Any]:
        logger.info(f"Fetching NFA Basic data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        result = fetch_agent_nfa_search(employee_number, params, driver)
        return result[0] if isinstance(result, list) and result else result

    # FINRA Arbitration Agent Functions
    def search_finra_arbitration(self, first_name: str, last_name: str, employee_number: Optional[str] = None, driver: Optional[webdriver.Chrome] = None) -> Optional[Dict]:
        logger.info(f"Fetching FINRA Arbitration data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        result = fetch_agent_finra_arb_search(employee_number, params, driver)
        if result:
            logger.info(f"Successfully fetched FINRA Arbitration data for {first_name} {last_name}")
            return result[0] if isinstance(result, list) and result else result
        logger.warning(f"No data found for {first_name} {last_name} in FINRA Arbitration search")
        return None

    # SEC Disciplinary Agent Functions
    def search_sec_disciplinary(self, first_name: str, last_name: str, employee_number: Optional[str] = None, driver: Optional[webdriver.Chrome] = None) -> Optional[Dict]:
        logger.info(f"Fetching SEC Disciplinary data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        searched_name = f"{first_name} {last_name}"
        result = fetch_agent_sec_disc_search(employee_number, params, driver)
        if result:
            logger.debug(f"SEC raw result: {json.dumps(result, indent=2)}")
            normalized = create_disciplinary_record("SEC_Disciplinary", result, searched_name)
            logger.debug(f"SEC normalized result: {json.dumps(normalized, indent=2)}")
            logger.info(f"Successfully fetched SEC Disciplinary data for {first_name} {last_name}")
            return normalized
        logger.warning(f"No data found for {first_name} {last_name} in SEC Disciplinary search")
        return None

    # Combined Disciplinary Review Function
    def perform_disciplinary_review(self, first_name: str, last_name: str, employee_number: Optional[str] = None, driver: Optional[webdriver.Chrome] = None) -> Dict[str, Any]:
        """
        Perform a combined disciplinary review by searching both SEC and FINRA disciplinary records.

        Args:
            first_name (str): First name to search.
            last_name (str): Last name to search.
            employee_number (Optional[str]): Employee number for caching/logging.
            driver (Optional[webdriver.Chrome]): Selenium WebDriver instance.

        Returns:
            Dict[str, Any]: Combined disciplinary review with filtered actions and detailed due diligence.
        """
        logger.info(f"Performing disciplinary review for {first_name} {last_name}, Employee: {employee_number}")
        searched_name = f"{first_name} {last_name}"
        combined_review = {
            "primary_name": searched_name,
            "disciplinary_actions": [],
            "due_diligence": {
                "searched_name": searched_name,
                "sec_disciplinary": {
                    "records_found": 0,
                    "records_filtered": 0,
                    "names_found": [],
                    "name_scores": {},  # Added to store name scores
                    "exact_match_found": False,
                    "status": "No records fetched"
                },
                "finra_disciplinary": {
                    "records_found": 0,
                    "records_filtered": 0,
                    "names_found": [],
                    "name_scores": {},  # Added to store name scores
                    "exact_match_found": False,
                    "status": "No records fetched"
                }
            }
        }

        # Search SEC Disciplinary
        sec_result = self.search_sec_disciplinary(first_name, last_name, employee_number, driver)
        if sec_result:
            logger.debug(f"SEC result received: {json.dumps(sec_result, indent=2)}")
            sec_actions = sec_result.get("disciplinary_actions", [])
            raw_sec_data = sec_result.get("raw_data", [])
            raw_sec_results = raw_sec_data[0]["result"] if raw_sec_data and isinstance(raw_sec_data, list) and "result" in raw_sec_data[0] else []
            combined_review["due_diligence"]["sec_disciplinary"]["records_found"] = len(raw_sec_results) if isinstance(raw_sec_results, list) else 0
            combined_review["due_diligence"]["sec_disciplinary"]["records_filtered"] = combined_review["due_diligence"]["sec_disciplinary"]["records_found"] - len(sec_actions)
            combined_review["due_diligence"]["sec_disciplinary"]["name_scores"] = sec_result.get("name_scores", {})
            if sec_actions:
                combined_review["disciplinary_actions"].extend(sec_actions)
                combined_review["due_diligence"]["sec_disciplinary"]["exact_match_found"] = True
                combined_review["due_diligence"]["sec_disciplinary"]["status"] = "Exact matches found"
            else:
                combined_review["due_diligence"]["sec_disciplinary"]["status"] = (
                    f"Records found but no matches for '{searched_name}'" if raw_sec_results else "No records found"
                )
            # Extract names from raw data for due diligence
            names_found = []
            if isinstance(raw_sec_results, list):
                for result in raw_sec_results:
                    if isinstance(result, dict):
                        name = result.get("Name", result.get("Firms/Individuals", ""))
                        if name:
                            names_found.append(name)
                            other_names = result.get("Also Known As", "").split("; ")
                            names_found.extend([n for n in other_names if n])
            combined_review["due_diligence"]["sec_disciplinary"]["names_found"] = list(set(names_found))
        else:
            logger.warning(f"SEC Disciplinary search failed for {first_name} {last_name}")

        # Search FINRA Disciplinary
        finra_result = self.search_finra_disciplinary(first_name, last_name, employee_number, driver)
        if finra_result:
            logger.debug(f"FINRA result received: {json.dumps(finra_result, indent=2)}")
            finra_actions = finra_result.get("disciplinary_actions", [])
            raw_finra_data = finra_result.get("raw_data", [])
            raw_finra_results = raw_finra_data[0]["result"] if raw_finra_data and isinstance(raw_finra_data, list) and "result" in raw_finra_data[0] else []
            combined_review["due_diligence"]["finra_disciplinary"]["records_found"] = len(raw_finra_results) if isinstance(raw_finra_results, list) else 0
            combined_review["due_diligence"]["finra_disciplinary"]["records_filtered"] = combined_review["due_diligence"]["finra_disciplinary"]["records_found"] - len(finra_actions)
            combined_review["due_diligence"]["finra_disciplinary"]["name_scores"] = finra_result.get("name_scores", {})
            if finra_actions:
                combined_review["disciplinary_actions"].extend(finra_actions)
                combined_review["due_diligence"]["finra_disciplinary"]["exact_match_found"] = True
                combined_review["due_diligence"]["finra_disciplinary"]["status"] = "Exact matches found"
            else:
                combined_review["due_diligence"]["finra_disciplinary"]["status"] = (
                    f"Records found but no matches for '{searched_name}'" if raw_finra_results else "No records found"
                )
            # Extract names from raw data for due diligence
            names_found = []
            if isinstance(raw_finra_results, list):
                for result in raw_finra_results:
                    if isinstance(result, dict):
                        firms_individuals = result.get("Firms/Individuals", "")
                        if firms_individuals:
                            names_found.extend([name.strip() for name in firms_individuals.split(", ") if name.strip()])
            combined_review["due_diligence"]["finra_disciplinary"]["names_found"] = list(set(names_found))
        else:
            logger.warning(f"FINRA Disciplinary search failed for {first_name} {last_name}")

        # Log the combined result
        if combined_review["disciplinary_actions"]:
            logger.info(f"Combined disciplinary review completed for {combined_review['primary_name']} with {len(combined_review['disciplinary_actions'])} matching actions")
        else:
            logger.info(f"No matching disciplinary actions found for {first_name} {last_name} across SEC and FINRA; due diligence: SEC found {combined_review['due_diligence']['sec_disciplinary']['records_found']}, FINRA found {combined_review['due_diligence']['finra_disciplinary']['records_found']}")

        return combined_review

def main():
    facade = FinancialServicesFacade()
    
    parser = argparse.ArgumentParser(description='Financial Services Facade Interactive Menu')
    parser.add_argument('--employee-number', help='Employee number for the search')
    parser.add_argument('--first-name', help='First name for custom search')
    parser.add_argument('--last-name', help='Last name for custom search')
    parser.add_argument('--crd-number', help='CRD number for custom search')
    parser.add_argument('--headless', action='store_true', default=True, help='Run in headless mode')
    
    args = parser.parse_args()

    def run_search(method: callable, employee_number: str, params: Dict[str, Any], driver: Optional[webdriver.Chrome] = None):
        result = method(**params, employee_number=employee_number, driver=driver)
        method_name = method.__name__.replace('search_', '').replace('perform_', '')
        print(f"\n{method_name} Result for {employee_number}:")
        print(json.dumps(result, indent=2))

    if args.employee_number or args.first_name or args.last_name or args.crd_number:
        employee_number = args.employee_number or "EMP001"
        driver = create_driver(args.headless)
        try:
            if args.crd_number:
                run_search(facade.search_sec_iapd_individual, employee_number, {"crd_number": args.crd_number})
                run_search(facade.search_finra_brokercheck_individual, employee_number, {"crd_number": args.crd_number})
            if args.first_name and args.last_name:
                run_search(facade.search_sec_arbitration, employee_number, {"first_name": args.first_name, "last_name": args.last_name}, driver)
                run_search(facade.search_finra_disciplinary, employee_number, {"first_name": args.first_name, "last_name": args.last_name}, driver)
                run_search(facade.search_nfa_basic, employee_number, {"first_name": args.first_name, "last_name": args.last_name}, driver)
                run_search(facade.search_finra_arbitration, employee_number, {"first_name": args.first_name, "last_name": args.last_name}, driver)
                run_search(facade.perform_disciplinary_review, employee_number, {"first_name": args.first_name, "last_name": args.last_name}, driver)
        finally:
            driver.quit()
            logger.info("WebDriver closed")
    else:
        driver = create_driver(RUN_HEADLESS)
        try:
            while True:
                print("\nFinancial Services Facade Interactive Menu:")
                print("1. Perform combined disciplinary review for 'Matthew Vetto'")
                print("2. Perform combined disciplinary review for 'Mark Miller'")
                print("3. Perform custom disciplinary review")
                print("4. Exit")
                choice = input("Enter your choice (1-4): ").strip()

                if choice == "1":
                    print("\nPerforming combined disciplinary review for 'Matthew Vetto'...")
                    run_search(facade.perform_disciplinary_review, "FIRST_RUN", {"first_name": "Matthew", "last_name": "Vetto"}, driver)
                elif choice == "2":
                    print("\nPerforming combined disciplinary review for 'Mark Miller'...")
                    run_search(facade.perform_disciplinary_review, "EMP_TEST", {"first_name": "Mark", "last_name": "Miller"}, driver)
                elif choice == "3":
                    employee_number = input("Enter employee number (e.g., EMP001): ").strip() or "EMP_CUSTOM"
                    first_name = input("Enter first name (optional, press Enter to skip): ").strip()
                    last_name = input("Enter last name (required): ").strip()
                    if not last_name:
                        print("Last name is required.")
                        continue
                    print(f"\nPerforming combined disciplinary review for '{first_name} {last_name}'...")
                    run_search(facade.perform_disciplinary_review, employee_number, {"first_name": first_name, "last_name": last_name}, driver)
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