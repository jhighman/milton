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
    firm_crd = claim.get("firm_crd", "")
    crd_number = claim.get("crd_number", "")
    organization_crd_number = claim.get("organization_crd_number", "")
    organization_name = claim.get("organization_name", "")

    if individual_name and firm_crd:
        logger.info("Claim has individual_name and firm_crd, selecting search_with_correlated")
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
        **claim,
        "source": "SEC_IAPD",
        "basic_result": basic_result,
        "detailed_result": detailed_result
    }

def search_with_crd_and_org_name(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    crd_number = claim.get("crd_number", "")
    org_name = claim.get("organization_name", "")
    logger.info(f"Searching with crd_number='{crd_number}', organization_name='{org_name}', Employee='{employee_number}'")

    # Try BrokerCheck first
    broker_result = facade.search_finra_brokercheck_individual(crd_number, employee_number)
    if broker_result and broker_result.get("hits", {}).get("total", 0) > 0:
        logger.info(f"Found in BrokerCheck for CRD: {crd_number}")
        detailed_result = facade.search_finra_brokercheck_detailed(crd_number, employee_number)
        return {
            **claim,
            "source": "BrokerCheck",
            "basic_result": broker_result,
            "detailed_result": detailed_result
        }

    # If no BrokerCheck hits, attempt to look up the organization CRD
    if org_name.strip():
        org_crd_number = facade.get_organization_crd(org_name)
        if org_crd_number is None or org_crd_number == "NOT_FOUND":
            error_msg = "unknown organization by lookup"
            logger.warning(error_msg)
            return {
                **claim,
                "source": "Entity_Search",
                "result": {"error": error_msg}
            }
        else:
            # Found an org CRD, but let's assume entity searching is still unsupported
            error_msg = (
                "Entity search using the derived org_crd_number is not supported "
                "even though we found one for this organization."
            )
            logger.warning(error_msg)
            return {
                **claim,
                "source": "Entity_Search",
                "result": {"error": error_msg}
            }
    else:
        # If the org_name is empty, fallback to SEC IAPD with the crd_number
        logger.info(f"No BrokerCheck hits, searching SEC IAPD with crd_number='{crd_number}'")
        basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
        detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
        return {
            **claim,
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
            **claim,
            "source": "BrokerCheck",
            "basic_result": broker_result,
            "detailed_result": detailed_result
        }

    logger.info(f"No BrokerCheck hits => searching SEC IAPD with crd_number='{crd_number}'")
    basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
    detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
    return {
        **claim,
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
        **claim,
        "source": "Entity_Search",
        "result": {"error": error_msg}
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
        **claim,
        "source": "Entity_Search",
        "result": {"error": error_msg}
    }

def search_default(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    logger.info(f"No usable fields => defaulting, Employee='{employee_number}'")
    error_msg = "Insufficient identifiers to perform search"
    logger.warning(error_msg)
    return {
        **claim,
        "source": "Default",
        "result": {"error": error_msg}
    }

def search_with_correlated(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    individual_name = claim.get("individual_name", "")
    firm_crd = claim.get("firm_crd", "")
    logger.info(f"Searching SEC IAPD with individual_name='{individual_name}', firm_crd='{firm_crd}', Employee='{employee_number}'")

    result = facade.search_sec_iapd_correlated(individual_name, firm_crd, employee_number)
    compliance = result and result.get("hits", {}).get("total", 0) > 0

    return {
        "source": "SEC_IAPD",
        "basic_result": result,
        "search_evaluation": {
            "search_strategy": "search_with_correlated",
            "compliance": compliance,
            "search_outcome": "SEC_IAPD hit" if compliance else "No records found",
            "compliance_explanation": "Record found via SEC_IAPD." if compliance else "No records found"
        }
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

    # 3) Execute it
    strategy_result = strategy_func(claim, facade, employee_number)

    # 4) Inspect for error vs. record found
    error_msg = None
    if 'result' in strategy_result and isinstance(strategy_result['result'], dict):
        error_msg = strategy_result['result'].get('error')

    record_found = False
    basic_result = strategy_result.get('basic_result', {})
    if basic_result and isinstance(basic_result, dict):
        hits = basic_result.get('hits', {})
        total_hits = hits.get('total', 0)
        if total_hits > 0:
            record_found = True

    source = strategy_result.get('source', '')

    if error_msg:
        search_evaluation['search_outcome'] = error_msg
        search_evaluation['compliance'] = False
        search_evaluation['compliance_explanation'] = error_msg
        search_evaluation['alerts'].append({
            "alert_type": "SearchStrategyError",
            "message": error_msg,
            "severity": "HIGH",
            "alert_category": "SearchEvaluation"
        })
    elif record_found:
        search_evaluation['search_outcome'] = "Record found"
        search_evaluation['compliance'] = True
        
        # If we have a known source, reference it explicitly
        if source:
            explanation = f"Record found via {source}."
        else:
            explanation = "Record found via an unspecified data source."
        search_evaluation['compliance_explanation'] = explanation
    else:
        not_found_msg = "No records found"
        search_evaluation['search_outcome'] = not_found_msg
        search_evaluation['compliance'] = False
        search_evaluation['compliance_explanation'] = not_found_msg

    # 5) Merge and return
    final_output = {
        **strategy_result,
        "search_evaluation": search_evaluation
    }
    return final_output
