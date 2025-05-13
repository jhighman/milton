"""
services.py

This module provides the FinancialServicesFacade class, which consolidates access to
external financial regulatory services (e.g., SEC IAPD, FINRA BrokerCheck, NFA, disciplinary,
and arbitration data) and internal agents for saving data (e.g., compliance reports).
It abstracts away the complexity of interacting with multiple underlying services and
provides a unified interface for business logic to retrieve and store normalized data.
"""

import json
import os
from typing import Dict, Any, List, Optional
import argparse
import logging

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
    fetch_agent_finra_bc_search_by_firm,
    create_driver,
    Marshaller,
)
from normalizer import (
    create_disciplinary_record,
    create_arbitration_record,
    create_individual_record,
    create_regulatory_record,
)
from agents.compliance_report_agent import save_compliance_report
from logger_config import setup_logging, reconfigure_logging
from evaluation_processor import Alert

# Set up logging using logger_config
loggers = setup_logging(debug=True)  # Enable debug mode for detailed logs
logger = loggers["services"]

RUN_HEADLESS = True

class AlertEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Alert objects."""
    def default(self, obj):
        if isinstance(obj, Alert):
            return obj.to_dict()
        return super().default(obj)

def json_dumps_with_alerts(obj: Any, **kwargs) -> str:
    """Helper function to serialize objects that may contain Alert instances."""
    return json.dumps(obj, cls=AlertEncoder, **kwargs)

class FinancialServicesFacade:
    def __init__(self, headless: bool = True, storage_manager=None):
        """Initialize the facade with configurable headless mode and storage manager."""
        self.headless = headless
        self.driver = None
        self._is_driver_managed = False
        self.storage_manager = storage_manager
        self.logger = logging.getLogger("services")
        self.logger.debug(f"FinancialServicesFacade initialized with headless={headless}")

    def _ensure_driver(self):
        """Ensure WebDriver is initialized with current headless setting."""
        if not self.driver:
            self.driver = create_driver(headless=self.headless)
            self._is_driver_managed = True
            self.logger.debug("Created new WebDriver instance")

    def cleanup(self):
        """Explicitly close the WebDriver."""
        if self._is_driver_managed and self.driver:
            try:
                self.driver.quit()
                self.logger.info("WebDriver closed successfully")
            except Exception as e:
                self.logger.error(f"Failed to close WebDriver: {str(e)}")
            finally:
                self.driver = None
                self._is_driver_managed = False

    def _load_organizations_cache(self) -> Optional[List[Dict]]:
        """Load organizations cache using storage manager if available, fallback to direct file operations."""
        cache_file = os.path.join("input", "organizationsCrd.jsonl")
        
        if self.storage_manager:
            try:
                content = self.storage_manager.read_file(cache_file)
                organizations = []
                for line in content.decode('utf-8').splitlines():
                    if line.strip():
                        organizations.append(json.loads(line))
                self.logger.debug(f"Loaded {len(organizations)} organizations from cache using storage manager")
                return organizations
            except Exception as e:
                self.logger.error(f"Error loading organizations cache using storage manager: {e}", exc_info=True)
        
        # Fallback to direct file operations
        if not os.path.exists(cache_file):
            self.logger.error("Failed to load organizations cache.")
            return None
        try:
            organizations = []
            with open(cache_file, 'r') as f:
                for line in f:
                    if line.strip():
                        organizations.append(json.loads(line))
            self.logger.debug(f"Loaded {len(organizations)} organizations from cache using direct file operations")
            return organizations
        except Exception as e:
            self.logger.error(f"Error loading organizations cache: {e}", exc_info=True)
            return None

    @staticmethod
    def _normalize_organization_name(name: str) -> str:
        """Normalize organization name for comparison."""
        return name.lower().replace(" ", "")

    @staticmethod
    def _normalize_individual_record(data_source: str, basic_info: Optional[Dict[str, Any]], detailed_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Normalize individual record using the normalizer module."""
        result = create_individual_record(data_source, basic_info, detailed_info)
        logger.debug(f"Normalized individual record from {data_source}: {json.dumps(result, indent=2)}")
        return result

    def get_organization_crd(self, organization_name: str) -> Optional[str]:
        """Retrieve organization CRD from cache."""
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

    def search_sec_iapd_individual(self, crd_number: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Search SEC IAPD for an individual by CRD number."""
        logger.info(f"Fetching SEC IAPD basic info for CRD: {crd_number}, Employee: {employee_number}")
        if not crd_number or not crd_number.isdigit():
            logger.error(f"Invalid CRD number: {crd_number}")
            return self._normalize_individual_record("IAPD", None, None)
        basic_result = fetch_agent_sec_iapd_search(employee_number, {"crd_number": crd_number})
        detailed_result = fetch_agent_sec_iapd_detailed(employee_number, {"crd_number": crd_number}) if basic_result else None
        if basic_result:
            logger.info(f"Successfully fetched SEC IAPD data for CRD: {crd_number}")
        else:
            logger.warning(f"No data found for CRD: {crd_number} in SEC IAPD search")
        return self._normalize_individual_record("IAPD", basic_result, detailed_result)

    def search_sec_iapd_detailed(self, crd_number: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Deprecated: Use search_sec_iapd_individual instead."""
        logger.warning(f"Calling search_sec_iapd_detailed is deprecated; use search_sec_iapd_individual instead for CRD: {crd_number}")
        return self.search_sec_iapd_individual(crd_number, employee_number)

    def search_sec_iapd_correlated(self, individual_name: str, organization_crd_number: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Search SEC IAPD for an individual correlated with an organization."""
        logger.info(f"Fetching SEC IAPD correlated info for {individual_name} at organization {organization_crd_number}, Employee: {employee_number}")
        result = fetch_agent_sec_iapd_correlated(employee_number, {
            "individual_name": individual_name,
            "organization_crd_number": organization_crd_number
        })
        logger.debug(f"Raw result from fetch_agent_sec_iapd_correlated: {json.dumps(result, indent=2) if result else 'None'}")
        if result:
            logger.info(f"Successfully fetched SEC IAPD correlated data for {individual_name} at organization {organization_crd_number}")
        else:
            logger.warning(f"No data found for {individual_name} at organization {organization_crd_number} in SEC IAPD correlated search")
        return self._normalize_individual_record("IAPD", result)

    def search_finra_correlated(self, individual_name: str, organization_crd_number: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Search FINRA BrokerCheck for an individual correlated with an organization."""
        logger.info(f"Fetching FINRA correlated info for {individual_name} at organization {organization_crd_number}, Employee: {employee_number}")
        result = fetch_agent_finra_bc_search_by_firm(employee_number, {
            "individual_name": individual_name,
            "organization_crd": organization_crd_number
        })
        logger.debug(f"Raw result from fetch_agent_finra_bc_search_by_firm: {json.dumps(result, indent=2) if result else 'None'}")
        if result:
            logger.info(f"Successfully fetched FINRA correlated data for {individual_name} at organization {organization_crd_number}")
        else:
            logger.warning(f"No data found for {individual_name} at organization {organization_crd_number} in FINRA correlated search")
        return self._normalize_individual_record("FINRA_BrokerCheck", result)

    def search_finra_brokercheck_individual(self, crd_number: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Search FINRA BrokerCheck for an individual by CRD number."""
        logger.info(f"Fetching FINRA BrokerCheck basic info for CRD: {crd_number}, Employee: {employee_number}")
        if not crd_number or not crd_number.isdigit():
            logger.error(f"Invalid CRD number: {crd_number}")
            return self._normalize_individual_record("FINRA_BrokerCheck", None, None)
        basic_result = fetch_agent_finra_bc_search(employee_number, {"crd_number": crd_number})
        detailed_result = fetch_agent_finra_bc_detailed(employee_number, {"crd_number": crd_number}) if basic_result else None
        if basic_result:
            logger.info(f"Successfully fetched FINRA BrokerCheck data for CRD: {crd_number}")
        else:
            logger.warning(f"No data found for CRD: {crd_number} in FINRA BrokerCheck search")
        return self._normalize_individual_record("FINRA_BrokerCheck", basic_result, detailed_result)

    def search_finra_brokercheck_detailed(self, crd_number: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Deprecated: Use search_finra_brokercheck_individual instead."""
        logger.warning(f"Calling search_finra_brokercheck_detailed is deprecated; use search_finra_brokercheck_individual instead for CRD: {crd_number}")
        return self.search_finra_brokercheck_individual(crd_number, employee_number)

    def search_sec_arbitration(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Search SEC arbitration records by name."""
        self._ensure_driver()
        logger.info(f"Fetching SEC Arbitration data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        searched_name = f"{first_name} {last_name}"
        result = fetch_agent_sec_arb_search(employee_number, params, self.driver)
        normalized = create_arbitration_record("SEC_Arbitration", result, searched_name)
        logger.debug(f"SEC Arbitration normalized result: {json.dumps(normalized, indent=2)}")
        if result:
            logger.info(f"Successfully fetched SEC Arbitration data for {searched_name}")
        else:
            logger.warning(f"No data found for {searched_name} in SEC Arbitration search")
        return normalized

    def search_finra_disciplinary(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Search FINRA disciplinary records by name."""
        self._ensure_driver()
        logger.info(f"Fetching FINRA Disciplinary data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        searched_name = f"{first_name} {last_name}"
        result = fetch_agent_finra_disc_search(employee_number, params, self.driver)
        normalized = create_disciplinary_record("FINRA_Disciplinary", result, searched_name)
        logger.debug(f"FINRA Disciplinary raw result: {json.dumps(result, indent=2) if result else 'None'}")
        logger.debug(f"FINRA Disciplinary normalized result: {json.dumps(normalized, indent=2)}")
        if result:
            logger.info(f"Successfully fetched FINRA Disciplinary data for {searched_name}")
        else:
            logger.warning(f"No data found for {searched_name} in FINRA Disciplinary search")
        return normalized

    def search_nfa_regulatory(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Search NFA regulatory records by name."""
        self._ensure_driver()
        logger.info(f"Fetching NFA regulatory data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        searched_name = f"{first_name} {last_name}"
        result = fetch_agent_nfa_search(employee_number, params, self.driver)
        result_dict = result[0] if isinstance(result, list) and result else result
        normalized = create_regulatory_record("NFA_Regulatory", result_dict, searched_name)
        logger.debug(f"NFA regulatory raw result: {json.dumps(result, indent=2) if result else 'None'}")
        logger.debug(f"NFA regulatory normalized result: {json.dumps(normalized, indent=2)}")
        if result:
            logger.info(f"Successfully fetched NFA regulatory data for {searched_name}")
        else:
            logger.warning(f"No data found for {searched_name} in NFA regulatory search")
        return normalized

    def search_finra_arbitration(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Search FINRA arbitration records by name."""
        self._ensure_driver()
        logger.info(f"Fetching FINRA Arbitration data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        searched_name = f"{first_name} {last_name}"
        result = fetch_agent_finra_arb_search(employee_number, params, self.driver)
        normalized = create_arbitration_record("FINRA_Arbitration", result, searched_name)
        logger.debug(f"FINRA Arbitration normalized result: {json.dumps(normalized, indent=2)}")
        if result:
            logger.info(f"Successfully fetched FINRA Arbitration data for {searched_name}")
        else:
            logger.warning(f"No data found for {searched_name} in FINRA Arbitration search")
        return normalized

    def search_sec_disciplinary(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Search SEC disciplinary records by name."""
        self._ensure_driver()
        logger.info(f"Fetching SEC Disciplinary data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        searched_name = f"{first_name} {last_name}"
        result = fetch_agent_sec_disc_search(employee_number, params, self.driver)
        normalized = create_disciplinary_record("SEC_Disciplinary", result, searched_name)
        logger.debug(f"SEC Disciplinary raw result: {json.dumps(result, indent=2) if result else 'None'}")
        logger.debug(f"SEC Disciplinary normalized result: {json.dumps(normalized, indent=2)}")
        if result:
            logger.info(f"Successfully fetched SEC Disciplinary data for {searched_name}")
        else:
            logger.warning(f"No data found for {searched_name} in SEC Disciplinary search")
        return normalized

    def save_compliance_report(self, report: Dict[str, Any], employee_number: Optional[str] = None) -> bool:
        """Save a compliance report."""
        logger.info(f"Saving compliance report for employee_number={employee_number}")
        success = save_compliance_report(report, employee_number)
        if success:
            logger.debug(f"Compliance report saved: {json_dumps_with_alerts(report, indent=2)}")
        else:
            logger.error("Failed to save compliance report")
        return success

    def evaluate_individual(self, claim: Dict[str, Any]) -> Dict[str, Any]:
        """Evaluate an individual based on claim data, respecting packageName."""
        logger.info(f"Evaluating individual for claim: {claim.get('reference_id')}")
        crd_number = claim.get("crd_number")
        employee_number = claim.get("employee_number")
        package_name = claim.get("packageName", "FULL")
        result = {
            "search_evaluation": {
                "source": [],
                "basic_result": None,
                "detailed_result": None,
                "search_strategy": "search_with_crd_only",
                "crd_number": crd_number,
                "compliance": False,
                "compliance_explanation": ""
            }
        }
        sources = []
        compliance_explanations = []

        # Handle packageName to determine search scope
        if package_name == "BROKERCHECK":
            logger.info("Restricting search to FINRA BrokerCheck due to packageName='BROKERCHECK'")
            bc_result = self.search_finra_brokercheck_individual(crd_number, employee_number)
            if bc_result.get("fetched_name"):
                result["search_evaluation"]["basic_result"] = bc_result
                result["search_evaluation"]["compliance"] = True
                sources.append("FINRA_BrokerCheck")
            else:
                compliance_explanations.append("No data found in FINRA BrokerCheck")
        else:
            # Try BrokerCheck first
            bc_result = self.search_finra_brokercheck_individual(crd_number, employee_number)
            if bc_result.get("fetched_name"):
                result["search_evaluation"]["basic_result"] = bc_result
                result["search_evaluation"]["compliance"] = True
                sources.append("FINRA_BrokerCheck")
            else:
                compliance_explanations.append("No data found in FINRA BrokerCheck")

            # Try IAPD if BrokerCheck fails or packageName allows
            if not sources or package_name in ["IAPD", "FULL"]:
                iapd_result = self.search_sec_iapd_individual(crd_number, employee_number)
                if iapd_result.get("fetched_name"):
                    result["search_evaluation"]["basic_result"] = iapd_result
                    result["search_evaluation"]["compliance"] = True
                    sources.append("IAPD")
                else:
                    compliance_explanations.append("No data found in SEC IAPD")

        result["search_evaluation"]["source"] = sources
        result["search_evaluation"]["compliance_explanation"] = "; ".join(compliance_explanations) or "Search completed successfully"
        logger.debug(f"Evaluation result: {json.dumps(result, indent=2)}")
        return result

    def perform_disciplinary_review(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Perform a combined disciplinary review (SEC and FINRA)."""
        logger.info(f"Performing disciplinary review for {first_name} {last_name}, Employee: {employee_number}")
        searched_name = f"{first_name} {last_name}"
        combined_review = {
            "source": ["FINRA_Disciplinary", "SEC_Disciplinary"],
            "primary_name": searched_name,
            "actions": [],
            "due_diligence": {
                "searched_name": searched_name,
                "sec_disciplinary": {
                    "source": "SEC_Disciplinary",
                    "records_found": 0,
                    "records_filtered": 0,
                    "names_found": [],
                    "name_scores": {},
                    "exact_match_found": False,
                    "status": "No records fetched"
                },
                "finra_disciplinary": {
                    "source": "FINRA_Disciplinary",
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
            sec_dd = sec_result.get("due_diligence", {})
            sec_actions = sec_result.get("actions", [])
            combined_review["due_diligence"]["sec_disciplinary"]["records_found"] = sec_dd.get("records_found", 0)
            combined_review["due_diligence"]["sec_disciplinary"]["records_filtered"] = sec_dd.get("records_filtered", 0)
            combined_review["due_diligence"]["sec_disciplinary"]["names_found"] = sec_dd.get("names_found", [])
            combined_review["due_diligence"]["sec_disciplinary"]["name_scores"] = sec_dd.get("name_scores", {})
            combined_review["due_diligence"]["sec_disciplinary"]["exact_match_found"] = sec_dd.get("exact_match_found", False)
            combined_review["due_diligence"]["sec_disciplinary"]["status"] = sec_dd.get("status", "Records processed")
            if sec_actions:
                combined_review["actions"].extend(sec_actions)
                logger.debug(f"Added {len(sec_actions)} SEC disciplinary actions")

        finra_result = self.search_finra_disciplinary(first_name, last_name, employee_number)
        if finra_result:
            logger.debug(f"FINRA Disciplinary result received: {json.dumps(finra_result, indent=2)}")
            finra_dd = finra_result.get("due_diligence", {})
            finra_actions = finra_result.get("actions", [])
            combined_review["due_diligence"]["finra_disciplinary"]["records_found"] = finra_dd.get("records_found", 0)
            combined_review["due_diligence"]["finra_disciplinary"]["records_filtered"] = finra_dd.get("records_filtered", 0)
            combined_review["due_diligence"]["finra_disciplinary"]["names_found"] = finra_dd.get("names_found", [])
            combined_review["due_diligence"]["finra_disciplinary"]["name_scores"] = finra_dd.get("name_scores", {})
            combined_review["due_diligence"]["finra_disciplinary"]["exact_match_found"] = finra_dd.get("exact_match_found", False)
            combined_review["due_diligence"]["finra_disciplinary"]["status"] = finra_dd.get("status", "Records processed")
            if finra_actions:
                combined_review["actions"].extend(finra_actions)
                logger.debug(f"Added {len(finra_actions)} FINRA disciplinary actions")

        logger.debug(f"Combined disciplinary review result: {json.dumps(combined_review, indent=2)}")
        if combined_review["actions"]:
            logger.info(f"Combined disciplinary review completed for {combined_review['primary_name']} with {len(combined_review['actions'])} matching actions")
        else:
            logger.info(f"No matching disciplinary actions found for {first_name} {last_name} across SEC and FINRA; due diligence: SEC found {combined_review['due_diligence']['sec_disciplinary']['records_found']}, FINRA found {combined_review['due_diligence']['finra_disciplinary']['records_found']}")
        return combined_review

    def perform_arbitration_review(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Perform a combined arbitration review (SEC and FINRA)."""
        logger.info(f"Performing arbitration review for {first_name} {last_name}, Employee: {employee_number}")
        searched_name = f"{first_name} {last_name}"
        combined_review = {
            "source": ["FINRA_Arbitration", "SEC_Arbitration"],
            "primary_name": searched_name,
            "actions": [],
            "due_diligence": {
                "searched_name": searched_name,
                "sec_arbitration": {
                    "source": "SEC_Arbitration",
                    "records_found": 0,
                    "records_filtered": 0,
                    "names_found": [],
                    "name_scores": {},
                    "exact_match_found": False,
                    "status": "No records fetched"
                },
                "finra_arbitration": {
                    "source": "FINRA_Arbitration",
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
            sec_dd = sec_result.get("due_diligence", {})
            sec_actions = sec_result.get("actions", [])
            combined_review["due_diligence"]["sec_arbitration"]["records_found"] = sec_dd.get("records_found", 0)
            combined_review["due_diligence"]["sec_arbitration"]["records_filtered"] = sec_dd.get("records_filtered", 0)
            combined_review["due_diligence"]["sec_arbitration"]["names_found"] = sec_dd.get("names_found", [])
            combined_review["due_diligence"]["sec_arbitration"]["name_scores"] = sec_dd.get("name_scores", {})
            combined_review["due_diligence"]["sec_arbitration"]["exact_match_found"] = sec_dd.get("exact_match_found", False)
            combined_review["due_diligence"]["sec_arbitration"]["status"] = sec_dd.get("status", "Records processed")
            if sec_actions:
                combined_review["actions"].extend(sec_actions)
                logger.debug(f"Added {len(sec_actions)} SEC arbitration actions")

        finra_result = self.search_finra_arbitration(first_name, last_name, employee_number)
        if finra_result:
            logger.debug(f"FINRA Arbitration result received: {json.dumps(finra_result, indent=2)}")
            finra_dd = finra_result.get("due_diligence", {})
            finra_actions = finra_result.get("actions", [])
            combined_review["due_diligence"]["finra_arbitration"]["records_found"] = finra_dd.get("records_found", 0)
            combined_review["due_diligence"]["finra_arbitration"]["records_filtered"] = finra_dd.get("records_filtered", 0)
            combined_review["due_diligence"]["finra_arbitration"]["names_found"] = finra_dd.get("names_found", [])
            combined_review["due_diligence"]["finra_arbitration"]["name_scores"] = finra_dd.get("name_scores", {})
            combined_review["due_diligence"]["finra_arbitration"]["exact_match_found"] = finra_dd.get("exact_match_found", False)
            combined_review["due_diligence"]["finra_arbitration"]["status"] = finra_dd.get("status", "Records processed")
            if finra_actions:
                combined_review["actions"].extend(finra_actions)
                logger.debug(f"Added {len(finra_actions)} FINRA arbitration actions")

        logger.debug(f"Combined arbitration review result: {json.dumps(combined_review, indent=2)}")
        if combined_review["actions"]:
            logger.info(f"Combined arbitration review completed for {combined_review['primary_name']} with {len(combined_review['actions'])} matching actions")
        else:
            logger.info(f"No matching arbitration actions found for {first_name} {last_name} across SEC and FINRA; due diligence: SEC found {combined_review['due_diligence']['sec_arbitration']['records_found']}, FINRA found {combined_review['due_diligence']['finra_arbitration']['records_found']}")
        return combined_review

    def perform_regulatory_review(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        """Perform a regulatory review (NFA)."""
        logger.info(f"Performing regulatory review for {first_name} {last_name}, Employee: {employee_number}")
        searched_name = f"{first_name} {last_name}"
        combined_review = {
            "source": ["NFA_Regulatory"],
            "primary_name": searched_name,
            "actions": [],
            "due_diligence": {
                "searched_name": searched_name,
                "nfa_regulatory_actions": {
                    "source": "NFA_Regulatory",
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
            nfa_dd = nfa_result.get("due_diligence", {})
            nfa_actions = nfa_result.get("actions", [])
            combined_review["due_diligence"]["nfa_regulatory_actions"]["records_found"] = nfa_dd.get("records_found", 0)
            combined_review["due_diligence"]["nfa_regulatory_actions"]["records_filtered"] = nfa_dd.get("records_filtered", 0)
            combined_review["due_diligence"]["nfa_regulatory_actions"]["names_found"] = nfa_dd.get("names_found", [])
            combined_review["due_diligence"]["nfa_regulatory_actions"]["name_scores"] = nfa_dd.get("name_scores", {})
            combined_review["due_diligence"]["nfa_regulatory_actions"]["exact_match_found"] = nfa_dd.get("exact_match_found", False)
            combined_review["due_diligence"]["nfa_regulatory_actions"]["status"] = nfa_dd.get("status", "Records processed")
            if nfa_actions:
                combined_review["actions"].extend(nfa_actions)
                logger.debug(f"Added {len(nfa_actions)} NFA regulatory actions")

        logger.debug(f"Combined regulatory review result: {json.dumps(combined_review, indent=2)}")
        if combined_review["actions"]:
            logger.info(f"Combined regulatory review completed for {combined_review['primary_name']} with {len(combined_review['actions'])} matching actions")
        else:
            logger.info(f"No matching regulatory actions found for {first_name} {last_name} in NFA; due diligence: NFA found {combined_review['due_diligence']['nfa_regulatory_actions']['records_found']}")
        return combined_review

def main():
    """Interactive CLI for testing FinancialServicesFacade methods."""
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