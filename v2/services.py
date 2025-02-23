import logging
import os
import json
from typing import Optional, Dict, Any, List

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
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("FinancialServicesFacade")


class FinancialServicesFacade:
    """
    Facade providing discrete functions for financial regulator services.

    This class encapsulates helper functions such as loading the organization cache,
    normalizing organization names, and normalizing BrokerCheck/IAPD data into a unified structure.
    It provides methods to access various financial services via the marshaller and returns normalized data.
    """

    @staticmethod
    def _load_organizations_cache() -> Optional[List[Dict]]:
        """Loads the organizationsCrd.jsonl file from the input directory."""
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
        """
        Normalizes an organization name by converting it to lowercase and removing spaces.
        """
        return name.lower().replace(" ", "")

    @staticmethod
    def _normalize_individual_record(
        data_source: str,
        basic_info: Optional[Dict[str, Any]],
        detailed_info: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Creates a unified 'individual record' from BrokerCheck or IAPD data.

        :param data_source: "BrokerCheck" or "IAPD"
        :param basic_info: JSON from basic search
        :param detailed_info: JSON from detailed search (optional)
        :return: Normalized dictionary with individual data
        """
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
            logger.error(f"Invalid data_source '{data_source}'. Must be one of {valid_sources}. Returning minimal extracted_info.")
            return extracted_info

        if not basic_info:
            logger.warning("No basic_info provided. Returning empty extracted_info.")
            return extracted_info

        hits_list = basic_info.get("hits", {}).get("hits", [])
        if hits_list:
            individual = hits_list[0].get("_source", {})
        else:
            logger.warning(f"{data_source}: basic_info had no hits. Returning mostly empty extracted_info.")
            return extracted_info

        fetched_name = f"{individual.get('ind_firstname', '')} {individual.get('ind_middlename', '')} {individual.get('ind_lastname', '')}".strip()
        extracted_info["crd_number"] = individual.get("crd_number", "")
        extracted_info["fetched_name"] = fetched_name
        extracted_info["other_names"] = individual.get("ind_other_names", [])
        extracted_info["bc_scope"] = individual.get("ind_bc_scope", "")
        extracted_info["ia_scope"] = individual.get("ind_ia_scope", "")

        if data_source == "BrokerCheck":
            if detailed_info and "hits" in detailed_info:
                detailed_hits = detailed_info["hits"].get("hits", [])
                if detailed_hits:
                    content_str = detailed_hits[0]["_source"].get("content", "")
                    try:
                        content_json = json.loads(content_str)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse BrokerCheck 'content' JSON: {e}")
                        content_json = {}
                    extracted_info["disclosures"] = content_json.get("disclosures", [])
                else:
                    logger.info("BrokerCheck detailed_info had no hits. No disclosures extracted.")
            else:
                logger.info("No BrokerCheck detailed_info provided or empty, skipping disclosures parsing.")

        elif data_source == "IAPD":
            iacontent_str = individual.get("iacontent", "{}")
            try:
                iacontent_data = json.loads(iacontent_str)
            except json.JSONDecodeError as e:
                logger.warning(f"IAPD basic_info iacontent parse error: {e}")
                iacontent_data = {}

            current_employments = []
            for emp in iacontent_data.get("currentIAEmployments", []):
                current_employments.append({
                    "firm_crd": emp.get("firmId"),
                    "firm_name": emp.get("firmName"),
                    "registration_begin_date": emp.get("registrationBeginDate"),
                    "branch_offices": [
                        {
                            "street": office.get("street1"),
                            "city": office.get("city"),
                            "state": office.get("state"),
                            "zip_code": office.get("zipCode")
                        }
                        for office in emp.get("branchOfficeLocations", [])
                    ]
                })
            extracted_info["current_ia_employments"] = current_employments

            if detailed_info and "hits" in detailed_info:
                detailed_hits = detailed_info["hits"].get("hits", [])
                if detailed_hits:
                    iapd_detailed_content_str = detailed_hits[0]["_source"].get("iacontent", "{}")
                    try:
                        iapd_detailed_content_data = json.loads(iapd_detailed_content_str)
                    except json.JSONDecodeError as e:
                        logger.warning(f"IAPD detailed_info iacontent parse error: {e}")
                        iapd_detailed_content_data = {}

                    state_exams = iapd_detailed_content_data.get("stateExamCategory", [])
                    principal_exams = iapd_detailed_content_data.get("principalExamCategory", [])
                    product_exams = iapd_detailed_content_data.get("productExamCategory", [])
                    extracted_info["exams"] = state_exams + principal_exams + product_exams
                    extracted_info["disclosures"] = iapd_detailed_content_data.get("disclosures", [])
                    extracted_info["arbitrations"] = iapd_detailed_content_data.get("arbitrations", [])
                else:
                    logger.info("IAPD detailed_info had no hits. Using only basic_info's iacontent if available.")
                    extracted_info["disclosures"] = iacontent_data.get("disclosures", [])
                    extracted_info["arbitrations"] = iacontent_data.get("arbitrations", [])
            else:
                extracted_info["disclosures"] = iacontent_data.get("disclosures", [])
                extracted_info["arbitrations"] = iacontent_data.get("arbitrations", [])

        return extracted_info

    def get_organization_crd(self, organization_name: str) -> Optional[str]:
        """
        Looks up the CRD for a given organization name using normalized name matching.
        
        Args:
            organization_name: Name of the organization to look up.
            
        Returns:
            The organization's CRD number if found; None if not found or on error;
            or "NOT_FOUND" if the organization was searched but not found.
        """
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
        """
        Search SEC IAPD for an individual using their CRD number, returning normalized data.
        """
        logger.info(f"Fetching SEC IAPD basic info for CRD: {crd_number}, Employee: {employee_number}")
        basic_result = fetch_agent_sec_iapd_search(employee_number, {"crd_number": crd_number})
        detailed_result = fetch_agent_sec_iapd_detailed(employee_number, {"crd_number": crd_number}) if basic_result else None
        if basic_result:
            logger.info(f"Successfully fetched SEC IAPD data for CRD: {crd_number}")
            return self._normalize_individual_record("IAPD", basic_result, detailed_result)
        logger.warning(f"No data found for CRD: {crd_number} in SEC IAPD search")
        return None

    def search_sec_iapd_detailed(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        """
        Search SEC IAPD for detailed information about an individual using their CRD number.
        Note: This method is now redundant as search_sec_iapd_individual includes detailed data.
        """
        logger.warning(f"Calling search_sec_iapd_detailed is deprecated; use search_sec_iapd_individual instead for CRD: {crd_number}")
        return self.search_sec_iapd_individual(crd_number, employee_number)

    def search_sec_iapd_correlated(self, individual_name: str, organization_crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        """
        Search SEC IAPD for an individual by name within a specific firm, returning normalized data.
        """
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
        """
        Search FINRA BrokerCheck for an individual using their CRD number, returning normalized data.
        """
        logger.info(f"Fetching FINRA BrokerCheck basic info for CRD: {crd_number}, Employee: {employee_number}")
        basic_result = fetch_agent_finra_bc_search(employee_number, {"crd_number": crd_number})
        detailed_result = fetch_agent_finra_bc_detailed(employee_number, {"crd_number": crd_number}) if basic_result else None
        if basic_result:
            logger.info(f"Successfully fetched FINRA BrokerCheck data for CRD: {crd_number}")
            return self._normalize_individual_record("BrokerCheck", basic_result, detailed_result)
        logger.warning(f"No data found for CRD: {crd_number} in FINRA BrokerCheck search")
        return None

    def search_finra_brokercheck_detailed(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        """
        Search FINRA BrokerCheck for detailed information about an individual using their CRD number.
        Note: This method is now redundant as search_finra_brokercheck_individual includes detailed data.
        """
        logger.warning(f"Calling search_finra_brokercheck_detailed is deprecated; use search_finra_brokercheck_individual instead for CRD: {crd_number}")
        return self.search_finra_brokercheck_individual(crd_number, employee_number)

    # SEC Arbitration Agent Functions
    def search_sec_arbitration(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        logger.info(f"Fetching SEC Arbitration data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        return fetch_agent_sec_arb_search(employee_number, params)

    # FINRA Disciplinary Agent Functions
    def search_finra_disciplinary(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        logger.info(f"Fetching FINRA Disciplinary data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        return fetch_agent_finra_disc_search(employee_number, params)

    # NFA Basic Agent Functions
    def search_nfa_basic(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        logger.info(f"Fetching NFA Basic data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        return fetch_agent_nfa_search(employee_number, params)

    # FINRA Arbitration Agent Functions
    def search_finra_arbitration(self, first_name: str, last_name: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
        logger.info(f"Fetching FINRA Arbitration data for {first_name} {last_name}, Employee: {employee_number}")
        params = {"first_name": first_name, "last_name": last_name}
        return fetch_agent_finra_arb_search(employee_number, params)


# Example usage in a batch process
def main():
    facade = FinancialServicesFacade()
    # SEC IAPD
    print("SEC IAPD Individual:", facade.search_sec_iapd_individual("12345", "EMP001"))
    # Note: search_sec_iapd_detailed is now redundant
    print("SEC IAPD Correlated:", facade.search_sec_iapd_correlated("Matthew Vetto", "282563", "EMP001"))

    # FINRA BrokerCheck
    print("FINRA BrokerCheck Individual:", facade.search_finra_brokercheck_individual("67890", "EMP001"))
    # Note: search_finra_brokercheck_detailed is now redundant

    # Other services (unchanged)
    print("SEC Arbitration:", facade.search_sec_arbitration("Mark", "Miller", "EMP001"))
    print("FINRA Disciplinary:", facade.search_finra_disciplinary("John", "Doe", "EMP001"))
    print("NFA Basic:", facade.search_nfa_basic("Jane", "Smith", "EMP001"))
    print("FINRA Arbitration:", facade.search_finra_arbitration("Bob", "Smith", "EMP001"))


if __name__ == "__main__":
    main()