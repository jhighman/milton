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
from normalizer import create_disciplinary_record, NormalizationError
from name_matcher import evaluate_name  # Import the new module

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
        extracted_info = {
            "crd_number": "",
            "fetched_name": "",
            "other_names": [],
            "bc_scope": "",
            "ia_scope": "",
            "disclosures": [],
            "arbitrations": [],
            "exams": [],
            "current_ia_employments": []
        }

        valid_sources = {"BrokerCheck", "IAPD"}
        if data_source not in valid_sources:
            logger.error(f"Invalid data_source '{data_source}'. Must be one of {valid_sources}.")
            return extracted_info

        if not basic_info:
            logger.warning("No basic_info provided.")
            return extracted_info

        hits_list = basic_info.get("hits", {}).get("hits", [])
        if not hits_list:
            logger.warning(f"{data_source}: basic_info had no hits.")
            return extracted_info

        individual = hits_list[0].get("_source", {})
        fetched_name = f"{individual.get('ind_firstname', '')} {individual.get('ind_middlename', '')} {individual.get('ind_lastname', '')}".strip()
        
        extracted_info["crd_number"] = individual.get("ind_source_id", individual.get("crd_number", ""))
        extracted_info["fetched_name"] = fetched_name
        extracted_info["other_names"] = individual.get("ind_other_names", [])
        extracted_info["bc_scope"] = individual.get("ind_bc_scope", "")
        extracted_info["ia_scope"] = individual.get("ind_ia_scope", "")

        if detailed_info:
            if "disclosures" in detailed_info or "stateExamCategory" in detailed_info:
                extracted_info["disclosures"] = detailed_info.get("disclosures", [])
                extracted_info["arbitrations"] = detailed_info.get("arbitrations", [])
                extracted_info["exams"] = (
                    detailed_info.get("stateExamCategory", []) +
                    detailed_info.get("principalExamCategory", []) +
                    detailed_info.get("productExamCategory", [])
                )
                extracted_info["current_ia_employments"] = detailed_info.get("currentIAEmployments", [])
                logger.info(f"Normalized {data_source} data from flat JSON structure.")
            elif "hits" in detailed_info and detailed_info["hits"].get("hits"):
                content_str = detailed_info["hits"]["hits"][0]["_source"].get("content", "")
                try:
                    content_json = json.loads(content_str) if content_str else {}
                    extracted_info["disclosures"] = content_json.get("disclosures", [])
                    extracted_info["arbitrations"] = content_json.get("arbitrations", [])
                    extracted_info["exams"] = (
                        content_json.get("stateExamCategory", []) +
                        content_json.get("principalExamCategory", []) +
                        content_json.get("productExamCategory", [])
                    )
                    extracted_info["current_ia_employments"] = content_json.get("currentIAEmployments", [])
                    logger.info(f"Normalized {data_source} data from Elasticsearch-style structure.")
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse {data_source} 'content' JSON: {e}")
            elif data_source == "IAPD" and "iacontent" in individual:
                try:
                    iacontent_data = json.loads(individual.get("iacontent", "{}"))
                    extracted_info["disclosures"] = iacontent_data.get("disclosures", [])
                    extracted_info["arbitrations"] = iacontent_data.get("arbitrations", [])
                    extracted_info["exams"] = (
                        iacontent_data.get("stateExamCategory", []) +
                        iacontent_data.get("principalExamCategory", []) +
                        iacontent_data.get("productExamCategory", [])
                    )
                    extracted_info["current_ia_employments"] = iacontent_data.get("currentIAEmployments", individual.get("ind_ia_current_employments", []))
                except json.JSONDecodeError as e:
                    logger.warning(f"IAPD basic_info iacontent parse error: {e}")
            else:
                logger.info(f"No recognizable detailed_info structure for {data_source}.")
        else:
            logger.info(f"No detailed_info provided for {data_source}.")

        return extracted_info

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
        result = fetch_agent_finra_disc_search(employee_number, params, driver)
        if result:
            logger.debug(f"FINRA raw result: {json.dumps(result, indent=2)}")
            normalized = create_disciplinary_record("FINRA_Disciplinary", result)
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
        result = fetch_agent_sec_disc_search(employee_number, params, driver)
        if result:
            logger.debug(f"SEC raw result: {json.dumps(result, indent=2)}")
            normalized = create_disciplinary_record("SEC_Disciplinary", result)
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
        exact_name = f"{first_name} {last_name}"
        combined_review = {
            "primary_name": exact_name,
            "disciplinary_actions": [],
            "due_diligence": {
                "searched_name": exact_name,
                "sec_disciplinary": {
                    "records_found": 0,
                    "records_filtered": 0,
                    "names_found": [],
                    "exact_match_found": False,
                    "status": "No records fetched"
                },
                "finra_disciplinary": {
                    "records_found": 0,
                    "records_filtered": 0,
                    "names_found": [],
                    "exact_match_found": False,
                    "status": "No records fetched"
                }
            }
        }

        # Search SEC Disciplinary
        try:
            sec_result = self.search_sec_disciplinary(first_name, last_name, employee_number, driver)
            if sec_result:
                logger.debug(f"SEC result received: {json.dumps(sec_result, indent=2)}")
                if sec_result.get("disciplinary_actions"):
                    sec_actions = sec_result["disciplinary_actions"]
                    combined_review["due_diligence"]["sec_disciplinary"]["records_found"] = len(sec_actions)
                    sec_names = list(set(action["associated_names"][0] for action in sec_actions))
                    combined_review["due_diligence"]["sec_disciplinary"]["names_found"] = sec_names
                    filtered_actions = []
                    for action in sec_actions:
                        primary_fetch_name = action["associated_names"][0]
                        other_names = action["associated_names"][1:] if len(action["associated_names"]) > 1 else []
                        eval_details, score = evaluate_name(exact_name, primary_fetch_name, other_names, score_threshold=80.0)
                        logger.debug(f"SEC name evaluation for '{primary_fetch_name}': score={score}, details={json.dumps(eval_details, indent=2)}")
                        if score and score >= 80.0:  # Use similarity threshold from evaluate_name
                            filtered_actions.append(action)
                            logger.debug(f"SEC action included for '{exact_name}': {action['case_id']}, score={score}")
                        else:
                            logger.debug(f"SEC action filtered out for '{exact_name}'; found names: {action['associated_names']}, score={score}")
                    combined_review["due_diligence"]["sec_disciplinary"]["records_filtered"] = len(sec_actions) - len(filtered_actions)
                    if filtered_actions:
                        combined_review["disciplinary_actions"].extend(filtered_actions)
                        combined_review["due_diligence"]["sec_disciplinary"]["exact_match_found"] = True
                        combined_review["due_diligence"]["sec_disciplinary"]["status"] = "Exact matches found"
                    else:
                        combined_review["due_diligence"]["sec_disciplinary"]["status"] = f"Records found but no exact matches for '{exact_name}'"
                else:
                    combined_review["due_diligence"]["sec_disciplinary"]["status"] = "No records found"
            else:
                logger.warning(f"SEC Disciplinary search failed for {first_name} {last_name}")
        except NormalizationError as e:
            logger.error(f"SEC Disciplinary normalization error: {str(e)}")

        # Search FINRA Disciplinary
        try:
            finra_result = self.search_finra_disciplinary(first_name, last_name, employee_number, driver)
            if finra_result:
                logger.debug(f"FINRA result received: {json.dumps(finra_result, indent=2)}")
                if finra_result.get("disciplinary_actions"):
                    finra_actions = finra_result["disciplinary_actions"]
                    combined_review["due_diligence"]["finra_disciplinary"]["records_found"] = len(finra_actions)
                    finra_names = list(set(action["associated_names"][0] for action in finra_actions))
                    combined_review["due_diligence"]["finra_disciplinary"]["names_found"] = finra_names
                    filtered_actions = []
                    for action in finra_actions:
                        primary_fetch_name = action["associated_names"][0]
                        other_names = action["associated_names"][1:] if len(action["associated_names"]) > 1 else []
                        eval_details, score = evaluate_name(exact_name, primary_fetch_name, other_names, score_threshold=80.0)
                        logger.debug(f"FINRA name evaluation for '{primary_fetch_name}': score={score}, details={json.dumps(eval_details, indent=2)}")
                        if score and score >= 80.0:  # Use similarity threshold from evaluate_name
                            filtered_actions.append(action)
                            logger.debug(f"FINRA action included for '{exact_name}': {action['case_id']}, score={score}")
                        else:
                            logger.debug(f"FINRA action filtered out for '{exact_name}'; found names: {action['associated_names']}, score={score}")
                    combined_review["due_diligence"]["finra_disciplinary"]["records_filtered"] = len(finra_actions) - len(filtered_actions)
                    if filtered_actions:
                        combined_review["disciplinary_actions"].extend(filtered_actions)
                        combined_review["due_diligence"]["finra_disciplinary"]["exact_match_found"] = True
                        combined_review["due_diligence"]["finra_disciplinary"]["status"] = "Exact matches found"
                    else:
                        combined_review["due_diligence"]["finra_disciplinary"]["status"] = f"Records found but no exact matches for '{exact_name}'"
                else:
                    combined_review["due_diligence"]["finra_disciplinary"]["status"] = "No records found"
            else:
                logger.warning(f"FINRA Disciplinary search failed for {first_name} {last_name}")
        except NormalizationError as e:
            logger.error(f"FINRA Disciplinary normalization error: {str(e)}")

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
                print("1. Run local test with 'Mark Miller' (SEC Disciplinary)")
                print("2. Perform custom search")
                print("3. Run example searches from original main")
                print("4. Perform combined disciplinary review for 'Mark Miller'")
                print("5. Exit")
                choice = input("Enter your choice (1-5): ").strip()

                if choice == "1":
                    print("\nRunning local test with 'Mark Miller'...")
                    run_search(facade.search_sec_disciplinary, "EMP_TEST", {"first_name": "Mark", "last_name": "Miller"}, driver)
                elif choice == "2":
                    employee_number = input("Enter employee number (e.g., EMP001): ").strip() or "EMP001"
                    search_type = input("Enter search type (1 for CRD, 2 for name, 3 for disciplinary review): ").strip()
                    if search_type == "1":
                        crd_number = input("Enter CRD number: ").strip()
                        if crd_number:
                            run_search(facade.search_sec_iapd_individual, employee_number, {"crd_number": crd_number})
                            run_search(facade.search_finra_brokercheck_individual, employee_number, {"crd_number": crd_number})
                        else:
                            print("CRD number is required for this search type.")
                    elif search_type in ["2", "3"]:
                        first_name = input("Enter first name (optional, press Enter to skip): ").strip()
                        last_name = input("Enter last name (required): ").strip()
                        if not last_name:
                            print("Last name is required.")
                            continue
                        if search_type == "2":
                            run_search(facade.search_sec_arbitration, employee_number, {"first_name": first_name, "last_name": last_name}, driver)
                            run_search(facade.search_finra_disciplinary, employee_number, {"first_name": first_name, "last_name": last_name}, driver)
                            run_search(facade.search_nfa_basic, employee_number, {"first_name": first_name, "last_name": last_name}, driver)
                            run_search(facade.search_finra_arbitration, employee_number, {"first_name": first_name, "last_name": last_name}, driver)
                            run_search(facade.search_sec_disciplinary, employee_number, {"first_name": first_name, "last_name": last_name}, driver)
                        else:  # search_type == "3"
                            run_search(facade.perform_disciplinary_review, employee_number, {"first_name": first_name, "last_name": last_name}, driver)
                    else:
                        print("Invalid search type. Use 1 for CRD, 2 for name, or 3 for disciplinary review.")
                elif choice == "3":
                    print("\nRunning example searches from original main...")
                    claims = [
                        {"individual_name": "Matthew Vetto", "organization_crd": "282563"},
                        {"crd_number": "2112848"},
                        {"crd_number": "2722375"}
                    ]
                    for i, claim in enumerate(claims, 1):
                        employee_number = f"EMP00{i}"
                        logger.info(f"Testing claim {i}: {claim}")
                        if "crd_number" in claim:
                            if "organization_crd" in claim:
                                run_search(facade.search_sec_iapd_individual, employee_number, {"crd_number": claim["crd_number"]})
                            else:
                                run_search(facade.search_finra_brokercheck_individual, employee_number, {"crd_number": claim["crd_number"]})
                        else:
                            run_search(facade.search_sec_iapd_correlated, employee_number, {
                                "individual_name": claim["individual_name"],
                                "organization_crd_number": claim["organization_crd"]
                            })
                elif choice == "4":
                    print("\nPerforming combined disciplinary review for 'Mark Miller'...")
                    run_search(facade.perform_disciplinary_review, "EMP_TEST", {"first_name": "Mark", "last_name": "Miller"}, driver)
                elif choice == "5":
                    print("Exiting...")
                    break
                else:
                    print("Invalid choice. Please enter 1, 2, 3, 4, or 5.")
        finally:
            driver.quit()
            logger.info("WebDriver closed")

if __name__ == "__main__":
    main()