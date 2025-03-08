"""
services.py

This module provides the FinancialServicesFacade class, which consolidates access to
external financial regulatory services (e.g., SEC IAPD, FINRA BrokerCheck, NFA, disciplinary,
and arbitration data) and internal agents for saving data (e.g., compliance reports).
It abstracts away the complexity of interacting with multiple underlying services and
provides a unified interface for business logic to retrieve and store normalized data.
"""

import logging
import json
import os
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
from normalizer import (
    create_disciplinary_record,
    create_arbitration_record,
    create_individual_record,
    create_regulatory_record,
)
from agents.compliance_report_agent import save_compliance_report  # Direct import

logger = logging.getLogger("services")

RUN_HEADLESS = True

class FinancialServicesFacade:
    def __init__(self):
        self.driver = create_driver(RUN_HEADLESS)
        self._is_driver_managed = True

    def __del__(self):
        if self._is_driver_managed and hasattr(self, 'driver') and self.driver:
            self.driver.quit()
            logger.info("WebDriver closed")

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
    def _normalize_individual_record(data_source: str, basic_info: Optional[Dict[str, Any]], detailed_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
        logger.debug(f"Raw result from fetch_agent_sec_iapd_correlated: {json.dumps(result, indent=2)}")
        if result:
            logger.info(f"Successfully fetched SEC IAPD correlated data for {individual_name} at organization {organization_crd_number}")
            normalized = self._normalize_individual_record("IAPD", result)
            logger.debug(f"Normalized basic_result: {json.dumps(normalized, indent=2)}")
            return normalized
        logger.warning(f"No data found for {individual_name} at organization {organization_crd_number} in SEC IAPD correlated search")
        return None

    def search_finra_brokercheck_individual(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.info(f"Fetching FINRA BrokerCheck basic info for CRD: {crd_number}, Employee: {employee_number}")
        basic_result = fetch_agent_finra_bc_search(employee_number, {"crd_number": crd_number})
        detailed_result = fetch_agent_finra_bc_detailed(employee_number, {"crd_number": crd_number}) if basic_result else None
        if basic_result:
            logger.info(f"Successfully fetched FINRA BrokerCheck data for {crd_number}")
            return self._normalize_individual_record("BrokerCheck", basic_result, detailed_result)
        logger.warning(f"No data found for {crd_number} in FINRA BrokerCheck search")
        return None

    def search_finra_brokercheck_detailed(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.warning(f"Calling search_finra_brokercheck_detailed is deprecated; use search_finra_brokercheck_individual instead for CRD: {crd_number}")
        return self.search_finra_brokercheck_individual(crd_number, employee_number)

    def search_sec_arbitration(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.info(f"Fetching SEC Arbitration data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        searched_name = f"{first_name} {last_name}"
        result = fetch_agent_sec_arb_search(employee_number, params, self.driver)
        if result:
            logger.info(f"Successfully fetched SEC Arbitration data for {first_name} {last_name}")
            normalized = create_arbitration_record("SEC_Arbitration", result, searched_name)
            logger.debug(f"SEC Arbitration normalized result: {json.dumps(normalized, indent=2)}")
            return normalized
        logger.warning(f"No data found for {first_name} {last_name} in SEC Arbitration search")
        return None

    def search_finra_disciplinary(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.info(f"Fetching FINRA Disciplinary data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        searched_name = f"{first_name} {last_name}"
        result = fetch_agent_finra_disc_search(employee_number, params, self.driver)
        if result:
            logger.debug(f"FINRA Disciplinary raw result: {json.dumps(result, indent=2)}")
            normalized = create_disciplinary_record("FINRA_Disciplinary", result, searched_name)
            logger.debug(f"FINRA Disciplinary normalized result: {json.dumps(normalized, indent=2)}")
            logger.info(f"Successfully fetched FINRA Disciplinary data for {first_name} {last_name}")
            return normalized
        logger.warning(f"No data found for {first_name} {last_name} in FINRA Disciplinary search")
        return None

    def search_nfa_regulatory(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Fetch and normalize NFA regulatory data."""
        logger.info(f"Fetching NFA regulatory data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        searched_name = f"{first_name} {last_name}"
        result = fetch_agent_nfa_search(employee_number, params, self.driver)
        if result:
            logger.debug(f"NFA regulatory raw result: {json.dumps(result, indent=2)}")
            result_dict = result[0] if isinstance(result, list) and result else result
            normalized = create_regulatory_record("NFA_Regulatory", result_dict, searched_name)
            logger.debug(f"NFA regulatory normalized result: {json.dumps(normalized, indent=2)}")
            logger.info(f"Successfully fetched NFA regulatory data for {first_name} {last_name}")
            return normalized
        logger.warning(f"No data found for {first_name} {last_name} in NFA regulatory search")
        return {
            "actions": [],
            "raw_data": [{"result": "No Results Found"}],
            "name_scores": {}
        }

    def search_finra_arbitration(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.info(f"Fetching FINRA Arbitration data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        searched_name = f"{first_name} {last_name}"
        result = fetch_agent_finra_arb_search(employee_number, params, self.driver)
        if result:
            logger.info(f"Successfully fetched FINRA Arbitration data for {first_name} {last_name}")
            normalized = create_arbitration_record("FINRA_Arbitration", result, searched_name)
            logger.debug(f"FINRA Arbitration normalized result: {json.dumps(normalized, indent=2)}")
            return normalized
        logger.warning(f"No data found for {first_name} {last_name} in FINRA Arbitration search")
        return None

    def search_sec_disciplinary(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.info(f"Fetching SEC Disciplinary data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        searched_name = f"{first_name} {last_name}"
        result = fetch_agent_sec_disc_search(employee_number, params, self.driver)
        if result:
            logger.debug(f"SEC Disciplinary raw result: {json.dumps(result, indent=2)}")
            normalized = create_disciplinary_record("SEC_Disciplinary", result, searched_name)
            logger.debug(f"SEC Disciplinary normalized result: {json.dumps(normalized, indent=2)}")
            logger.info(f"Successfully fetched SEC Disciplinary data for {first_name} {last_name}")
            return normalized
        logger.warning(f"No data found for {first_name} {last_name} in SEC Disciplinary search")
        return None

    def save_compliance_report(self, report: Dict[str, Any], employee_number: Optional[str] = None) -> bool:
        """
        Saves a compliance report directly via the ComplianceReportAgent.

        Args:
            report (Dict[str, Any]): The compliance report to save.
            employee_number (Optional[str]): Identifier for the cache subfolder. Defaults to None.

        Returns:
            bool: True if saved successfully, False otherwise.
        """
        logger.info(f"Saving compliance report for employee_number={employee_number}")
        return save_compliance_report(report, employee_number)

    def perform_disciplinary_review(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        logger.info(f"Performing disciplinary review for {first_name} {last_name}, Employee: {employee_number}")
        searched_name = f"{first_name} {last_name}"
        combined_review = {
            "primary_name": searched_name,
            "actions": [],
            "due_diligence": {
                "searched_name": searched_name,
                "sec_disciplinary": {
                    "records_found": 0,
                    "records_filtered": 0,
                    "names_found": [],
                    "name_scores": {},
                    "exact_match_found": False,
                    "status": "No records fetched"
                },
                "finra_disciplinary": {
                    "records_found": 0,
                    "records_filtered": 0,
                    "names_found": [],
                    "name_scores": {},
                    "exact_match_found": False,
                    "status": "No records fetched"
                }
            }
        }

        sec_result = self.search_sec_disciplinary(first_name, last_name, employee_number)
        if sec_result:
            logger.debug(f"SEC Disciplinary result received: {json.dumps(sec_result, indent=2)}")
            sec_actions = sec_result.get("actions", [])
            combined_review["due_diligence"]["sec_disciplinary"]["records_found"] = len(sec_result.get("raw_data", [{}])[0].get("result", []))
            combined_review["due_diligence"]["sec_disciplinary"]["records_filtered"] = len(sec_result.get("raw_data", [{}])[0].get("result", [])) - len(sec_actions)
            combined_review["due_diligence"]["sec_disciplinary"]["name_scores"] = sec_result.get("name_scores", {})
            combined_review["due_diligence"]["sec_disciplinary"]["names_found"] = list(sec_result.get("name_scores", {}).keys())
            if sec_actions:
                combined_review["actions"].extend(sec_actions)
                combined_review["due_diligence"]["sec_disciplinary"]["exact_match_found"] = True
                combined_review["due_diligence"]["sec_disciplinary"]["status"] = "Exact matches found"
            else:
                combined_review["due_diligence"]["sec_disciplinary"]["status"] = (
                    f"Records found but no matches for '{searched_name}'" if sec_result.get("raw_data") else "No records found"
                )

        finra_result = self.search_finra_disciplinary(first_name, last_name, employee_number)
        if finra_result:
            logger.debug(f"FINRA Disciplinary result received: {json.dumps(finra_result, indent=2)}")
            finra_actions = finra_result.get("actions", [])
            combined_review["due_diligence"]["finra_disciplinary"]["records_found"] = len(finra_result.get("raw_data", [{}])[0].get("result", []))
            combined_review["due_diligence"]["finra_disciplinary"]["records_filtered"] = len(finra_result.get("raw_data", [{}])[0].get("result", [])) - len(finra_actions)
            combined_review["due_diligence"]["finra_disciplinary"]["name_scores"] = finra_result.get("name_scores", {})
            combined_review["due_diligence"]["finra_disciplinary"]["names_found"] = list(finra_result.get("name_scores", {}).keys())
            if finra_actions:
                combined_review["actions"].extend(finra_actions)
                combined_review["due_diligence"]["finra_disciplinary"]["exact_match_found"] = True
                combined_review["due_diligence"]["finra_disciplinary"]["status"] = "Exact matches found"
            else:
                combined_review["due_diligence"]["finra_disciplinary"]["status"] = (
                    f"Records found but no matches for '{searched_name}'" if finra_result.get("raw_data") else "No records found"
                )

        if combined_review["actions"]:
            logger.info(f"Combined disciplinary review completed for {combined_review['primary_name']} with {len(combined_review['actions'])} matching actions")
        else:
            logger.info(f"No matching disciplinary actions found for {first_name} {last_name} across SEC and FINRA; due diligence: SEC found {combined_review['due_diligence']['sec_disciplinary']['records_found']}, FINRA found {combined_review['due_diligence']['finra_disciplinary']['records_found']}")

        return combined_review

    def perform_arbitration_review(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        logger.info(f"Performing arbitration review for {first_name} {last_name}, Employee: {employee_number}")
        searched_name = f"{first_name} {last_name}"
        combined_review = {
            "primary_name": searched_name,
            "actions": [],
            "due_diligence": {
                "searched_name": searched_name,
                "sec_arbitration": {
                    "records_found": 0,
                    "records_filtered": 0,
                    "names_found": [],
                    "name_scores": {},
                    "exact_match_found": False,
                    "status": "No records fetched"
                },
                "finra_arbitration": {
                    "records_found": 0,
                    "records_filtered": 0,
                    "names_found": [],
                    "name_scores": {},
                    "exact_match_found": False,
                    "status": "No records fetched"
                }
            }
        }

        sec_result = self.search_sec_arbitration(first_name, last_name, employee_number)
        if sec_result:
            logger.debug(f"SEC Arbitration result received: {json.dumps(sec_result, indent=2)}")
            sec_actions = sec_result.get("actions", [])
            combined_review["due_diligence"]["sec_arbitration"]["records_found"] = len(sec_result.get("raw_data", [{}])[0].get("result", []))
            combined_review["due_diligence"]["sec_arbitration"]["records_filtered"] = len(sec_result.get("raw_data", [{}])[0].get("result", [])) - len(sec_actions)
            combined_review["due_diligence"]["sec_arbitration"]["name_scores"] = sec_result.get("name_scores", {})
            combined_review["due_diligence"]["sec_arbitration"]["names_found"] = list(sec_result.get("name_scores", {}).keys())
            if sec_actions:
                combined_review["actions"].extend(sec_actions)
                combined_review["due_diligence"]["sec_arbitration"]["exact_match_found"] = True
                combined_review["due_diligence"]["sec_arbitration"]["status"] = "Exact matches found"
            else:
                combined_review["due_diligence"]["sec_arbitration"]["status"] = (
                    f"Records found but no matches for '{searched_name}'" if sec_result.get("raw_data") else "No records found"
                )

        finra_result = self.search_finra_arbitration(first_name, last_name, employee_number)
        if finra_result:
            logger.debug(f"FINRA Arbitration result received: {json.dumps(finra_result, indent=2)}")
            finra_actions = finra_result.get("actions", [])
            combined_review["due_diligence"]["finra_arbitration"]["records_found"] = len(finra_result.get("raw_data", [{}])[0].get("result", []))
            combined_review["due_diligence"]["finra_arbitration"]["records_filtered"] = len(finra_result.get("raw_data", [{}])[0].get("result", [])) - len(finra_actions)
            combined_review["due_diligence"]["finra_arbitration"]["name_scores"] = finra_result.get("name_scores", {})
            combined_review["due_diligence"]["finra_arbitration"]["names_found"] = list(finra_result.get("name_scores", {}).keys())
            if finra_actions:
                combined_review["actions"].extend(finra_actions)
                combined_review["due_diligence"]["finra_arbitration"]["exact_match_found"] = True
                combined_review["due_diligence"]["finra_arbitration"]["status"] = "Exact matches found"
            else:
                combined_review["due_diligence"]["finra_arbitration"]["status"] = (
                    f"Records found but no matches for '{searched_name}'" if finra_result.get("raw_data") else "No records found"
                )

        if combined_review["actions"]:
            logger.info(f"Combined arbitration review completed for {combined_review['primary_name']} with {len(combined_review['actions'])} matching actions")
        else:
            logger.info(f"No matching arbitration actions found for {first_name} {last_name} across SEC and FINRA; due diligence: SEC found {combined_review['due_diligence']['sec_arbitration']['records_found']}, FINRA found {combined_review['due_diligence']['finra_arbitration']['records_found']}")

        return combined_review

    def perform_regulatory_review(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Perform a regulatory review using NFA data."""
        logger.info(f"Performing regulatory review for {first_name} {last_name}, Employee: {employee_number}")
        searched_name = f"{first_name} {last_name}"
        combined_review = {
            "primary_name": searched_name,
            "actions": [],
            "due_diligence": {
                "searched_name": searched_name,
                "nfa_regulatory_actions": {
                    "records_found": 0,
                    "records_filtered": 0,
                    "names_found": [],
                    "name_scores": {},
                    "exact_match_found": False,
                    "status": "No records fetched"
                }
            }
        }

        nfa_result = self.search_nfa_regulatory(first_name, last_name, employee_number)
        if nfa_result:
            logger.debug(f"NFA Regulatory result received: {json.dumps(nfa_result, indent=2)}")
            nfa_actions = nfa_result.get("actions", [])
            combined_review["due_diligence"]["nfa_regulatory_actions"]["records_found"] = len(nfa_result.get("raw_data", [{}])[0].get("result", []))
            combined_review["due_diligence"]["nfa_regulatory_actions"]["records_filtered"] = len(nfa_result.get("raw_data", [{}])[0].get("result", [])) - len(nfa_actions)
            combined_review["due_diligence"]["nfa_regulatory_actions"]["name_scores"] = nfa_result.get("name_scores", {})
            combined_review["due_diligence"]["nfa_regulatory_actions"]["names_found"] = list(nfa_result.get("name_scores", {}).keys())
            if nfa_actions:
                combined_review["actions"].extend(nfa_actions)
                combined_review["due_diligence"]["nfa_regulatory_actions"]["exact_match_found"] = True
                combined_review["due_diligence"]["nfa_regulatory_actions"]["status"] = "Exact matches found"
            else:
                combined_review["due_diligence"]["nfa_regulatory_actions"]["status"] = (
                    f"Records found but no matches for '{searched_name}'" if nfa_result.get("raw_data") else "No records found"
                )

        if combined_review["actions"]:
            logger.info(f"Combined regulatory review completed for {combined_review['primary_name']} with {len(combined_review['actions'])} matching actions")
        else:
            logger.info(f"No matching regulatory actions found for {first_name} {last_name} in NFA; due diligence: NFA found {combined_review['due_diligence']['nfa_regulatory_actions']['records_found']}")

        return combined_review

def main():
    facade = FinancialServicesFacade()
    
    parser = argparse.ArgumentParser(description='Financial Services Facade Interactive Menu')
    parser.add_argument('--employee-number', help='Employee number for the search')
    parser.add_argument('--first-name', help='First name for custom search')
    parser.add_argument('--last-name', help='Last name for custom search')
    parser.add_argument('--crd-number', help='CRD number for custom search')
    
    args = parser.parse_args()

    def run_search(method: callable, employee_number: str, params: Dict[str, Any]):
        result = method(**params, employee_number=employee_number)
        method_name = method.__name__.replace('search_', '').replace('perform_', '')
        print(f"\n{method_name} Result for {employee_number}:")
        print(json.dumps(result, indent=2))

    if args.employee_number or args.first_name or args.last_name or args.crd_number:
        employee_number = args.employee_number or "EMP001"
        if args.crd_number:
            run_search(facade.search_sec_iapd_individual, employee_number, {"crd_number": args.crd_number})
            run_search(facade.search_finra_brokercheck_individual, employee_number, {"crd_number": args.crd_number})
        if args.first_name and args.last_name:
            run_search(facade.search_sec_arbitration, employee_number, {"first_name": args.first_name, "last_name": args.last_name})
            run_search(facade.search_finra_disciplinary, employee_number, {"first_name": args.first_name, "last_name": args.last_name})
            run_search(facade.search_nfa_regulatory, employee_number, {"first_name": args.first_name, "last_name": args.last_name})
            run_search(facade.search_finra_arbitration, employee_number, {"first_name": args.first_name, "last_name": args.last_name})
            run_search(facade.perform_disciplinary_review, employee_number, {"first_name": args.first_name, "last_name": args.last_name})
            run_search(facade.perform_arbitration_review, employee_number, {"first_name": args.first_name, "last_name": args.last_name})
            run_search(facade.perform_regulatory_review, employee_number, {"first_name": args.first_name, "last_name": args.last_name})
    else:
        while True:
            print("\nFinancial Services Facade Interactive Menu:")
            print("1. Perform combined disciplinary review for 'Matthew Vetto'")
            print("2. Perform combined arbitration review for 'Matthew Vetto'")
            print("3. Perform combined regulatory review for 'Matthew Vetto'")
            print("4. Perform custom disciplinary, arbitration, or regulatory review")
            print("5. Exit")
            choice = input("Enter your choice (1-5): ").strip()

            if choice == "1":
                print("\nPerforming combined disciplinary review for 'Matthew Vetto'...")
                run_search(facade.perform_disciplinary_review, "FIRST_RUN", {"first_name": "Matthew", "last_name": "Vetto"})
            elif choice == "2":
                print("\nPerforming combined arbitration review for 'Matthew Vetto'...")
                run_search(facade.perform_arbitration_review, "FIRST_RUN", {"first_name": "Matthew", "last_name": "Vetto"})
            elif choice == "3":
                print("\nPerforming combined regulatory review for 'Matthew Vetto'...")
                run_search(facade.perform_regulatory_review, "FIRST_RUN", {"first_name": "Matthew", "last_name": "Vetto"})
            elif choice == "4":
                employee_number = input("Enter employee number (e.g., EMP001): ").strip() or "EMP_CUSTOM"
                first_name = input("Enter first name (optional, press Enter to skip): ").strip()
                last_name = input("Enter last name (required): ").strip()
                if not last_name:
                    print("Last name is required.")
                    continue
                review_type = input("Enter review type (1 for disciplinary, 2 for arbitration, 3 for regulatory): ").strip()
                if review_type == "1":
                    print(f"\nPerforming combined disciplinary review for '{first_name} {last_name}'...")
                    run_search(facade.perform_disciplinary_review, employee_number, {"first_name": first_name, "last_name": last_name})
                elif review_type == "2":
                    print(f"\nPerforming combined arbitration review for '{first_name} {last_name}'...")
                    run_search(facade.perform_arbitration_review, employee_number, {"first_name": first_name, "last_name": last_name})
                elif review_type == "3":
                    print(f"\nPerforming combined regulatory review for '{first_name} {last_name}'...")
                    run_search(facade.perform_regulatory_review, employee_number, {"first_name": first_name, "last_name": last_name})
                else:
                    print("Invalid review type. Use 1 for disciplinary, 2 for arbitration, or 3 for regulatory.")
            elif choice == "5":
                print("Exiting...")
                break
            else:
                print("Invalid choice. Please enter 1, 2, 3, 4, or 5.")

if __name__ == "__main__":
    main()