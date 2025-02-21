import logging
import time
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

    This class encapsulates helper functions such as loading the organization cache
    and normalizing organization names, as well as providing methods to access various
    financial services via the marshaller.
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
            if org.get("normalizedName") == normalized_search_name:
                crd = org.get("organizationCRD")
                if crd and crd != "N/A":
                    logger.info(f"Found CRD {crd} for organization '{organization_name}'.")
                    return crd
                else:
                    logger.warning(f"CRD not found for organization '{organization_name}'.")
                    return None
        return "NOT_FOUND"

    # SEC IAPD Agent Functions
    def search_sec_iapd_individual(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.info(f"Fetching SEC IAPD basic info for CRD: {crd_number}, Employee: {employee_number}")
        result = fetch_agent_sec_iapd_search(employee_number, {"crd_number": crd_number})
        if result:
            logger.info(f"Successfully fetched SEC IAPD basic data for CRD: {crd_number}")
            return result
        logger.warning(f"No data found for CRD: {crd_number} in SEC IAPD basic search")
        return None

    def search_sec_iapd_detailed(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.info(f"Fetching SEC IAPD detailed info for CRD: {crd_number}, Employee: {employee_number}")
        result = fetch_agent_sec_iapd_detailed(employee_number, {"crd_number": crd_number})
        if result:
            logger.info(f"Successfully fetched SEC IAPD detailed data for CRD: {crd_number}")
            return result
        logger.warning(f"No data found for CRD: {crd_number} in SEC IAPD detailed search")
        return None

    # FINRA BrokerCheck Agent Functions
    def search_finra_brokercheck_individual(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.info(f"Fetching FINRA BrokerCheck basic info for CRD: {crd_number}, Employee: {employee_number}")
        time.sleep(5)  # Enforce rate limiting for FINRA BrokerCheck
        result = fetch_agent_finra_bc_search(employee_number, {"crd_number": crd_number})
        if result:
            logger.info(f"Successfully fetched FINRA BrokerCheck basic data for CRD: {crd_number}")
            return result
        logger.warning(f"No data found for CRD: {crd_number} in FINRA BrokerCheck basic search")
        return None

    def search_finra_brokercheck_detailed(self, crd_number: str, employee_number: Optional[str] = None) -> Optional[Dict]:
        logger.info(f"Fetching FINRA BrokerCheck detailed info for CRD: {crd_number}, Employee: {employee_number}")
        time.sleep(5)  # Enforce rate limiting
        result = fetch_agent_finra_bc_detailed(employee_number, {"crd_number": crd_number})
        if result:
            logger.info(f"Successfully fetched FINRA BrokerCheck detailed data for CRD: {crd_number}")
            return result
        logger.warning(f"No data found for CRD: {crd_number} in FINRA BrokerCheck detailed search")
        return None

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
