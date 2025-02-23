from typing import Dict, Any, Callable
import logging
from services import FinancialServicesFacade
from datetime import datetime
from collections import OrderedDict

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

    # Prioritize individual_name and organization_crd_number as per the scenario outline
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
    compliance = basic_result and basic_result.get("hits", {}).get("total", 0) > 0

    return {
        "compliance": compliance,
        "search_outcome": "SEC_IAPD hit" if compliance else "No records found",
        "compliance_explanation": f"Record found via SEC_IAPD for employee_number='{employee_number}'" if compliance else "No records found",
        "source": "SEC_IAPD",
        "basic_result": basic_result,
        "detailed_result": detailed_result
    }

def search_with_crd_and_org_name(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    crd_number = claim.get("crd_number", "")
    org_name = claim.get("organization_name", "")
    logger.info(f"Searching with crd_number='{crd_number}', organization_name='{org_name}', Employee='{employee_number}'")

    broker_result = facade.search_finra_brokercheck_individual(crd_number, employee_number)
    if broker_result and broker_result.get("hits", {}).get("total", 0) > 0:
        logger.info(f"Found in BrokerCheck for CRD: {crd_number}")
        detailed_result = facade.search_finra_brokercheck_detailed(crd_number, employee_number)
        return {
            "compliance": True,
            "search_outcome": "BrokerCheck hit",
            "compliance_explanation": f"Record found via BrokerCheck for employee_number='{employee_number}'",
            "source": "BrokerCheck",
            "basic_result": broker_result,
            "detailed_result": detailed_result
        }

    if org_name.strip():
        org_crd_number = facade.get_organization_crd(org_name)
        if org_crd_number is None or org_crd_number == "NOT_FOUND":
            error_msg = "unknown organization by lookup"
            logger.warning(error_msg)
            return {
                "compliance": False,
                "search_outcome": error_msg,
                "compliance_explanation": error_msg,
                "source": "Entity_Search",
                "basic_result": None,
                "detailed_result": None
            }
        # Fallback to SEC IAPD if org_crd found but entity search unsupported
        logger.info(f"No BrokerCheck hits, searching SEC IAPD with crd_number='{crd_number}'")
        basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
        detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
        compliance = basic_result and basic_result.get("hits", {}).get("total", 0) > 0
        return {
            "compliance": compliance,
            "search_outcome": "SEC_IAPD hit" if compliance else "No records found",
            "compliance_explanation": f"Record found via SEC_IAPD for employee_number='{employee_number}'" if compliance else "No records found",
            "source": "SEC_IAPD",
            "basic_result": basic_result,
            "detailed_result": detailed_result
        }
    else:
        logger.info(f"No BrokerCheck hits, searching SEC IAPD with crd_number='{crd_number}'")
        basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
        detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
        compliance = basic_result and basic_result.get("hits", {}).get("total", 0) > 0
        return {
            "compliance": compliance,
            "search_outcome": "SEC_IAPD hit" if compliance else "No records found",
            "compliance_explanation": f"Record found via SEC_IAPD for employee_number='{employee_number}'" if compliance else "No records found",
            "source": "SEC_IAPD",
            "basic_result": basic_result,
            "detailed_result": detailed_result
        }

def search_with_crd_only(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    crd_number = claim.get("crd_number", "")
    logger.info(f"Searching with crd_number only='{crd_number}', Employee='{employee_number}'")

    broker_result = facade.search_finra_brokercheck_individual(crd_number, employee_number)
    if broker_result and broker_result.get("hits", {}).get("total", 0) > 0:
        logger.info(f"Found in BrokerCheck for CRD: {crd_number}")
        detailed_result = facade.search_finra_brokercheck_detailed(crd_number, employee_number)
        return {
            "compliance": True,
            "search_outcome": "BrokerCheck hit",
            "compliance_explanation": f"Record found via BrokerCheck for employee_number='{employee_number}'",
            "source": "BrokerCheck",
            "basic_result": broker_result,
            "detailed_result": detailed_result
        }

    logger.info(f"No BrokerCheck hits => searching SEC IAPD with crd_number='{crd_number}'")
    basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
    detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
    compliance = basic_result and basic_result.get("hits", {}).get("total", 0) > 0
    return {
        "compliance": compliance,
        "search_outcome": "SEC_IAPD hit" if compliance else "No records found",
        "compliance_explanation": f"Record found via SEC_IAPD for employee_number='{employee_number}'" if compliance else "No records found",
        "source": "SEC_IAPD",
        "basic_result": basic_result,
        "detailed_result": detailed_result
    }

def search_with_entity(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    organization_crd_number = claim.get("organization_crd_number", "")
    logger.info(f"Detected entity search with organization_crd_number='{organization_crd_number}', Employee='{employee_number}'")

    error_msg = (
        "Entity search using organization_crd_number is not supported at this time. "
        "Please provide an individual crd_number."
    )
    logger.warning(error_msg)
    return {
        "compliance": False,
        "search_outcome": error_msg,
        "compliance_explanation": error_msg,
        "source": "Entity_Search",
        "basic_result": None,
        "detailed_result": None
    }

def search_with_org_name_only(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    organization_name = claim.get("organization_name", "")
    logger.info(f"Detected org_name-only search with organization_name='{organization_name}', Employee='{employee_number}'")

    error_msg = (
        "Entity search using organization_name is not supported at this time. "
        "Please provide an individual crd_number."
    )
    logger.warning(error_msg)
    return {
        "compliance": False,
        "search_outcome": error_msg,
        "compliance_explanation": error_msg,
        "source": "Entity_Search",
        "basic_result": None,
        "detailed_result": None
    }

def search_default(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    logger.info(f"No usable fields => defaulting, Employee='{employee_number}'")
    error_msg = "Insufficient identifiers to perform search"
    logger.warning(error_msg)
    return {
        "compliance": False,
        "search_outcome": error_msg,
        "compliance_explanation": error_msg,
        "source": "Default",
        "basic_result": None,
        "detailed_result": None
    }

def search_with_correlated(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    individual_name = claim.get("individual_name", "")
    organization_crd = claim.get("organization_crd", "")
    logger.info(f"Searching SEC IAPD with individual_name='{individual_name}', organization_crd='{organization_crd}', Employee='{employee_number}'")

    result = facade.search_sec_iapd_correlated(individual_name, organization_crd, employee_number)
    compliance = result and result.get("hits", {}).get("total", 0) > 0
    return {
        "compliance": compliance,
        "search_outcome": "SEC_IAPD hit" if compliance else "No records found",
        "compliance_explanation": f"Record found via SEC_IAPD for employee_number='{employee_number}'" if compliance else "No records found",
        "source": "SEC_IAPD",
        "basic_result": result,
        "detailed_result": None
    }

def process_claim(
    claim: Dict[str, Any], 
    facade: FinancialServicesFacade, 
    employee_number: str = None
) -> Dict[str, Any]:
    from collections import OrderedDict

    # 1) Create compliance structure
    search_evaluation = OrderedDict([
        ('compliance', False),
        ('compliance_explanation', ''),
        ('search_strategy', None),
        ('search_outcome', None),
        ('search_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
        ('alerts', []),
    ])

    # 2) Pick the strategy
    strategy_func = determine_search_strategy(claim)
    search_evaluation['search_strategy'] = strategy_func.__name__
    logger.debug(f"Selected strategy: {strategy_func.__name__} for employee_number={employee_number}")

    # 3) Execute it
    strategy_result = strategy_func(claim, facade, employee_number)
    if strategy_result is None:
        logger.error(f"Strategy {strategy_func.__name__} returned None for claim: {claim}, employee_number={employee_number}")
        search_evaluation.update({
            "compliance": False,
            "search_outcome": "Search failed - no data returned",
            "compliance_explanation": "Strategy returned no result"
        })
    else:
        logger.debug(f"Strategy result: {strategy_result}")
        search_evaluation.update({
            "compliance": strategy_result.get("compliance", False),
            "search_outcome": strategy_result.get("search_outcome", "Unknown"),
            "compliance_explanation": strategy_result.get("compliance_explanation", "No explanation provided")
        })

    # 4) Merge and return
    final_output = {
        "search_evaluation": search_evaluation,
        "source": strategy_result.get("source", "Unknown"),
        "basic_result": strategy_result.get("basic_result"),
        "detailed_result": strategy_result.get("detailed_result")
    }
    return final_output