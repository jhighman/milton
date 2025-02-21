from typing import Dict, Any, Callable
import logging
from services import FinancialServicesFacade

# Logger setup
logger = logging.getLogger("business")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s'))
logger.addHandler(handler)

def determine_search_strategy(claim: Dict[str, Any]) -> Callable[[Dict[str, Any], FinancialServicesFacade, str], Dict[str, Any]]:
    """Determine the search strategy based on claim fields.

    Args:
        claim (Dict[str, Any]): Claim with optional 'crd', 'org_crd', 'organization_name'.

    Returns:
        Callable: A function that executes the chosen search strategy.
    """
    crd = claim.get("crd", "")
    org_crd = claim.get("org_crd", "")
    org_name = claim.get("organization_name", "")

    if crd and org_crd:
        logger.info("Claim has both crd and org_crd, selecting search_with_org_crd")
        return search_with_org_crd
    elif org_crd and not crd:
        logger.info("Claim has only org_crd, selecting search_with_entity")
        return search_with_entity
    elif crd and not org_crd and org_name:
        logger.info("Claim has crd and organization_name, selecting search_with_crd_and_org_name")
        return search_with_crd_and_org_name
    elif crd and not org_crd:
        logger.info("Claim has only crd, selecting search_with_crd_only")
        return search_with_crd_only
    else:
        logger.info("Claim lacks sufficient fields, selecting default strategy")
        return search_default

def search_with_org_crd(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Search SEC IAPD using org_crd when both crd and org_crd are present.

    Args:
        claim (Dict[str, Any]): Claim with 'crd' and 'org_crd'.
        facade (FinancialServicesFacade): Service facade.
        employee_number (str): Logging identifier.

    Returns:
        Dict[str, Any]: Updated claim with SEC IAPD result.
    """
    org_crd = claim.get("org_crd", "")
    logger.info(f"Searching SEC IAPD with org_crd: {org_crd}, Employee: {employee_number}")
    result = facade.search_sec_iapd_individual(org_crd, employee_number)
    return {**claim, "source": "SEC_IAPD_Organization", "result": result}

def search_with_entity(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Handle entity search (org_crd only), returning an error as it's not supported yet.

    Args:
        claim (Dict[str, Any]): Claim with 'org_crd' but no 'crd'.
        facade (FinancialServicesFacade): Service facade (unused here).
        employee_number (str): Logging identifier.

    Returns:
        Dict[str, Any]: Updated claim with error message.
    """
    org_crd = claim.get("org_crd", "")
    logger.info(f"Detected entity search with org_crd: {org_crd}, Employee: {employee_number}")
    error_msg = "Entity search using org_crd is not supported at this time. Please provide an individual crd."
    logger.warning(error_msg)
    return {**claim, "source": "Entity_Search", "result": {"error": error_msg}}

def search_with_crd_only(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Search BrokerCheck with crd, fall back to SEC IAPD if no hits.

    Args:
        claim (Dict[str, Any]): Claim with 'crd' only.
        facade (FinancialServicesFacade): Service facade.
        employee_number (str): Logging identifier.

    Returns:
        Dict[str, Any]: Updated claim with search result.
    """
    crd = claim.get("crd", "")
    logger.info(f"Searching with crd only: {crd}, Employee: {employee_number}")

    broker_result = facade.search_finra_brokercheck_individual(crd, employee_number)
    if broker_result and broker_result.get("hits", {}).get("total", 0) > 0:
        logger.info(f"Found in BrokerCheck for CRD: {crd}")
        return {**claim, "source": "BrokerCheck", "result": broker_result}

    logger.info(f"No BrokerCheck hits, searching SEC IAPD with crd: {crd}")
    sec_result = facade.search_sec_iapd_individual(crd, employee_number)
    return {**claim, "source": "SEC_IAPD", "result": sec_result}

def search_with_crd_and_org_name(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Search BrokerCheck with crd, fall back to SEC IAPD using org_name-derived CRD.

    Args:
        claim (Dict[str, Any]): Claim with 'crd' and 'organization_name'.
        facade (FinancialServicesFacade): Service facade.
        employee_number (str): Logging identifier.

    Returns:
        Dict[str, Any]: Updated claim with search result or error.
    """
    crd = claim.get("crd", "")
    org_name = claim.get("organization_name", "")
    logger.info(f"Searching with crd: {crd} and org_name: {org_name}, Employee: {employee_number}")

    broker_result = facade.search_finra_brokercheck_individual(crd, employee_number)
    if broker_result and broker_result.get("hits", {}).get("total", 0) > 0:
        logger.info(f"Found in BrokerCheck for CRD: {crd}")
        return {**claim, "source": "BrokerCheck", "result": broker_result}

    logger.info(f"No BrokerCheck hits, checking organization: {org_name}")
    org_crd = facade.get_organization_crd(org_name, employee_number)
    if org_crd and org_crd != "NOT_FOUND":
        sec_result = facade.search_sec_iapd_individual(org_crd, employee_number)
        logger.info(f"SEC IAPD result for org CRD: {org_crd}")
        return {**claim, "source": "SEC_IAPD_Organization", "result": sec_result}

    error_msg = "Organization supplied was not found in our index, and no org_crd was included in the search please supply a CRD"
    logger.warning(error_msg)
    return {**claim, "source": "SEC_IAPD_Organization", "result": {"error": error_msg}}

def search_default(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Default strategy when no usable fields are present.

    Args:
        claim (Dict[str, Any]): Claim object.
        facade (FinancialServicesFacade): Service facade (unused here).
        employee_number (str): Logging identifier.

    Returns:
        Dict[str, Any]: Updated claim with empty result.
    """
    logger.info(f"No crd or org_crd, defaulting, Employee: {employee_number}")
    return {**claim, "source": "Default", "result": {"hits": {"total": 0, "hits": []}}}

def process_claim(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str = None) -> Dict[str, Any]:
    """Process a claim using the determined search strategy.

    Args:
        claim (Dict[str, Any]): The claim object.
        facade (FinancialServicesFacade): Service facade.
        employee_number (str, optional): Logging identifier.

    Returns:
        Dict[str, Any]: Updated claim with search results.
    """
    strategy = determine_search_strategy(claim)
    return strategy(claim, facade, employee_number)