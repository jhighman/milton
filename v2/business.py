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
    individual_name = claim.get("individual_name", "")
    crd_number = claim.get("crd_number", "")
    organization_crd_number = claim.get("organization_crd_number", "")
    organization_name = claim.get("organization_name", "")

    if individual_name and organization_crd_number:
        logger.info("Claim has individual_name and organization_crd_number, selecting search_with_correlated")
        return search_with_correlated
    elif crd_number and organization_crd_number:
        logger.info("Claim has both crd_number and organization_crd_number, selecting search_with_both_crds")
        return search_with_both_crds
    elif crd_number and organization_name and not organization_crd_number:
        logger.info("Claim has crd_number and organization_name, selecting search_with_crd_and_org_name")
        return search_with_crd_and_org_name
    elif crd_number and not organization_crd_number and not organization_name:
        logger.info("Claim has only crd_number, selecting search_with_crd_only")
        return search_with_crd_only
    elif organization_crd_number and not crd_number:
        logger.info("Claim has only organization_crd_number, selecting search_with_entity")
        return search_with_entity
    elif organization_name and not crd_number and not organization_crd_number:
        logger.info("Claim has only organization_name, selecting search_with_org_name_only")
        return search_with_org_name_only
    else:
        logger.info("Claim lacks sufficient fields, selecting default strategy")
        return search_default

def search_with_both_crds(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    crd_number = claim.get("crd_number", "")
    logger.info(f"Searching SEC IAPD with crd_number='{crd_number}', Employee='{employee_number}'")

    basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
    detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None

    return {
        "source": "SEC_IAPD",
        "basic_result": basic_result,
        "detailed_result": detailed_result,
        "search_strategy": "search_with_both_crds",
        "crd_number": crd_number
    }

def search_with_crd_and_org_name(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    crd_number = claim.get("crd_number", "")
    org_name = claim.get("organization_name", "")
    logger.info(f"Searching with crd_number='{crd_number}', organization_name='{org_name}', Employee='{employee_number}'")

    broker_result = facade.search_finra_brokercheck_individual(crd_number, employee_number)
    if broker_result and broker_result.get("fetched_name") != "":
        logger.info(f"Found in BrokerCheck for CRD: {crd_number}")
        detailed_result = facade.search_finra_brokercheck_detailed(crd_number, employee_number)
        return {
            "source": "BrokerCheck",
            "basic_result": broker_result,
            "detailed_result": detailed_result,
            "search_strategy": "search_with_crd_and_org_name",
            "crd_number": crd_number
        }

    if org_name.strip():
        org_crd_number = facade.get_organization_crd(org_name)
        if org_crd_number is None or org_crd_number == "NOT_FOUND":
            logger.warning("Unknown organization by lookup")
            return {
                "source": "Entity_Search",
                "basic_result": None,
                "detailed_result": None,
                "search_strategy": "search_with_crd_and_org_name",
                "crd_number": crd_number
            }
        logger.info(f"No BrokerCheck hits, searching SEC IAPD with crd_number='{crd_number}'")
        basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
        detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
        return {
            "source": "SEC_IAPD",
            "basic_result": basic_result,
            "detailed_result": detailed_result,
            "search_strategy": "search_with_crd_and_org_name",
            "crd_number": crd_number
        }
    else:
        logger.info(f"No BrokerCheck hits, searching SEC IAPD with crd_number='{crd_number}'")
        basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
        detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
        return {
            "source": "SEC_IAPD",
            "basic_result": basic_result,
            "detailed_result": detailed_result,
            "search_strategy": "search_with_crd_and_org_name",
            "crd_number": crd_number
        }

def search_with_crd_only(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    crd_number = claim.get("crd_number", "")
    logger.info(f"Searching with crd_number only='{crd_number}', Employee='{employee_number}'")

    broker_result = facade.search_finra_brokercheck_individual(crd_number, employee_number)
    if broker_result and broker_result.get("fetched_name") != "":
        logger.info(f"Found in BrokerCheck for CRD: {crd_number}")
        detailed_result = facade.search_finra_brokercheck_detailed(crd_number, employee_number)
        return {
            "source": "BrokerCheck",
            "basic_result": broker_result,
            "detailed_result": detailed_result,
            "search_strategy": "search_with_crd_only",
            "crd_number": crd_number
        }

    logger.info(f"No BrokerCheck hits => searching SEC IAPD with crd_number='{crd_number}'")
    basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
    detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
    return {
        "source": "SEC_IAPD",
        "basic_result": basic_result,
        "detailed_result": detailed_result,
        "search_strategy": "search_with_crd_only",
        "crd_number": crd_number
    }

def search_with_entity(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    organization_crd_number = claim.get("organization_crd_number", "")
    logger.info(f"Detected entity search with organization_crd_number='{organization_crd_number}', Employee='{employee_number}'")

    logger.warning("Entity search using organization_crd_number is not supported at this time.")
    return {
        "source": "Entity_Search",
        "basic_result": None,
        "detailed_result": None,
        "search_strategy": "search_with_entity",
        "crd_number": None
    }

def search_with_org_name_only(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    organization_name = claim.get("organization_name", "")
    logger.info(f"Detected org_name-only search with organization_name='{organization_name}', Employee='{employee_number}'")

    logger.warning("Entity search using organization_name is not supported at this time.")
    return {
        "source": "Entity_Search",
        "basic_result": None,
        "detailed_result": None,
        "search_strategy": "search_with_org_name_only",
        "crd_number": None
    }

def search_default(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    logger.info(f"No usable fields => defaulting, Employee='{employee_number}'")
    logger.warning("Insufficient identifiers to perform search")
    return {
        "source": "Default",
        "basic_result": None,
        "detailed_result": None,
        "search_strategy": "search_default",
        "crd_number": None
    }

def search_with_correlated(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    individual_name = claim.get("individual_name", "")
    organization_crd_number = claim.get("organization_crd_number", "")
    logger.info(f"Searching SEC IAPD with individual_name='{individual_name}', organization_crd_number='{organization_crd_number}', Employee='{employee_number}'")

    basic_result = facade.search_sec_iapd_correlated(individual_name, organization_crd_number, employee_number)
    crd_number = basic_result.get("crd_number", basic_result.get("ind_source_id", None)) if basic_result else None
    detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if crd_number else None

    return {
        "source": "SEC_IAPD",
        "basic_result": basic_result,
        "detailed_result": detailed_result,
        "search_strategy": "search_with_correlated",
        "crd_number": crd_number
    }

def process_claim(
    claim: Dict[str, Any],
    facade: FinancialServicesFacade,
    employee_number: str = None
) -> Dict[str, Any]:
    """Process a claim and return raw search results."""
    strategy_func = determine_search_strategy(claim)
    logger.debug(f"Selected strategy: {strategy_func.__name__} for employee_number={employee_number}")
    result = strategy_func(claim, facade, employee_number)
    if result is None:
        logger.error(f"Strategy {strategy_func.__name__} returned None for claim: {claim}, employee_number={employee_number}")
        return {
            "source": "Unknown",
            "basic_result": None,
            "detailed_result": None,
            "search_strategy": strategy_func.__name__,
            "crd_number": None
        }
    return result

if __name__ == "__main__":
    from services import FinancialServicesFacade
    facade = FinancialServicesFacade()

    claims = [
        {"individual_name": "Matthew Vetto", "organization_crd_number": "282563"},
        {"crd_number": "2112848"},
        {"crd_number": "2722375"}
    ]

    for i, claim in enumerate(claims, 1):
        employee_number = f"EMP00{i}"
        logger.info(f"Processing claim {i}: {claim}")
        result = process_claim(claim, facade, employee_number)
        print(f"\nResult {i} for {employee_number}:")
        print(result)