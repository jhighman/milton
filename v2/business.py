import json
from typing import Dict, Any, Callable
import logging
from services import FinancialServicesFacade
from evaluation_report_builder import EvaluationReportBuilder
from evaluation_report_director import EvaluationReportDirector
from evaluation_processor import Alert

# Configure logging with detailed format
logger = logging.getLogger("business")

class AlertEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Alert objects."""
    def default(self, obj):
        if isinstance(obj, Alert):
            return obj.to_dict()
        return super().default(obj)

def json_dumps_with_alerts(obj: Any, **kwargs) -> str:
    """Helper function to serialize objects that may contain Alert instances."""
    return json.dumps(obj, cls=AlertEncoder, **kwargs)

def determine_search_strategy(claim: Dict[str, Any]) -> Callable[[Dict[str, Any], FinancialServicesFacade, str], Dict[str, Any]]:
    """Determine the appropriate search strategy based on claim data."""
    # Get individual identifiers
    individual_name = claim.get("individual_name") or ""
    first_name = claim.get("first_name") or ""
    last_name = claim.get("last_name") or ""
    if not individual_name and (first_name or last_name):
        individual_name = f"{first_name} {last_name}".strip()

    # Get organization identifiers
    crd_number = (claim.get("crd_number") or "").strip()
    organization_crd_number = (claim.get("organization_crd_number") or claim.get("organization_crd") or "").strip()
    organization_name = (claim.get("organization_name") or "").strip()

    claim_summary = f"claim={json_dumps_with_alerts(claim)}"
    logger.debug(f"Determining search strategy for {claim_summary}")

    if crd_number and organization_crd_number:
        logger.info(f"Selected search_with_crd_and_org_crd for {claim_summary} with crd_number='{crd_number}' and organization_crd_number='{organization_crd_number}'")
        return search_with_crd_and_org_crd
    elif crd_number:
        logger.info(f"Selected search_with_crd_only for {claim_summary} due to crd_number='{crd_number}'")
        return search_with_crd_only
    elif individual_name and organization_crd_number:
        logger.info(f"Selected search_with_correlated for {claim_summary} with individual_name='{individual_name}' and organization_crd_number='{organization_crd_number}'")
        return search_with_correlated
    elif individual_name and organization_name and not organization_crd_number:
        logger.info(f"Selected search_with_correlated for {claim_summary} with individual_name='{individual_name}' and organization_name='{organization_name}'")
        return search_with_correlated
    elif organization_crd_number:
        logger.info(f"Selected search_with_entity for {claim_summary} with organization_crd_number='{organization_crd_number}'")
        return search_with_entity
    elif organization_name:
        logger.info(f"Selected search_with_org_name_only for {claim_summary} with organization_name='{organization_name}'")
        return search_with_org_name_only
    else:
        logger.info(f"Selected search_default for {claim_summary} as fallback")
        return search_default

def search_with_both_crds(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Search using both individual and organization CRDs."""
    crd_number = claim.get("crd_number", "")
    claim_summary = f"claim={json_dumps_with_alerts(claim)}, employee_number={employee_number}"
    logger.info(f"Executing search_with_both_crds for {claim_summary} with crd_number='{crd_number}'")

    try:
        basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
        logger.debug(f"SEC IAPD basic_result: {json_dumps_with_alerts(basic_result)}")
        if basic_result and (basic_result.get("fetched_name", "").strip() or basic_result.get("crd_number")):
            detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number)
            logger.debug(f"SEC IAPD detailed_result: {json_dumps_with_alerts(detailed_result)}")
            logger.info(f"SEC IAPD returned valid data for {claim_summary}")
            return {
                "source": "SEC_IAPD",
                "basic_result": basic_result,
                "detailed_result": detailed_result,
                "search_strategy": "search_with_both_crds",
                "crd_number": crd_number,
                "compliance": True,
                "compliance_explanation": "Search completed successfully with SEC IAPD data, individual found."
            }
        else:
            logger.warning(f"SEC IAPD search returned no meaningful data for {claim_summary}")
            return {
                "source": "SEC_IAPD",
                "basic_result": basic_result or {},
                "detailed_result": None,
                "search_strategy": "search_with_both_crds",
                "crd_number": crd_number,
                "compliance": False,
                "compliance_explanation": "Search completed but no individual found in SEC IAPD data."
            }
    except Exception as e:
        logger.error(f"Failed to search SEC IAPD for {claim_summary}: {str(e)}", exc_info=True)
        return {
            "source": "SEC_IAPD",
            "basic_result": None,
            "detailed_result": None,
            "search_strategy": "search_with_both_crds",
            "crd_number": crd_number,
            "compliance": False,
            "compliance_explanation": f"SEC IAPD search failed: {str(e)}",
            "error": str(e)
        }

def search_with_crd_and_org_name(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Search using CRD and organization name."""
    crd_number = claim.get("crd_number", "")
    org_name = claim.get("organization_name", "")
    claim_summary = f"claim={json_dumps_with_alerts(claim)}, employee_number={employee_number}"
    logger.info(f"Executing search_with_crd_and_org_name for {claim_summary} with crd_number='{crd_number}', org_name='{org_name}'")

    try:
        broker_result = facade.search_finra_brokercheck_individual(crd_number, employee_number)
        logger.debug(f"BrokerCheck result: {json_dumps_with_alerts(broker_result)}")
        if broker_result and broker_result.get("fetched_name", "").strip():
            logger.info(f"BrokerCheck returned valid data for {claim_summary}")
            return {
                "source": "BrokerCheck",
                "basic_result": broker_result,
                "detailed_result": broker_result,
                "search_strategy": "search_with_crd_and_org_name",
                "crd_number": crd_number,
                "compliance": True,
                "compliance_explanation": "Search completed successfully with BrokerCheck data, individual found."
            }
    except Exception as e:
        logger.error(f"BrokerCheck search failed for {claim_summary}: {str(e)}", exc_info=True)

    if org_name.strip():
        try:
            org_crd_number = facade.get_organization_crd(org_name)
            if not org_crd_number or org_crd_number == "NOT_FOUND":
                logger.warning(f"Unknown organization '{org_name}' for {claim_summary}")
                return {
                    "source": "Entity_Search",
                    "basic_result": None,
                    "detailed_result": None,
                    "search_strategy": "search_with_crd_and_org_name",
                    "crd_number": crd_number,
                    "compliance": False,
                    "compliance_explanation": f"Unable to resolve organization CRD from name '{org_name}'",
                    "skip_reasons": [f"Unable to resolve organization CRD from name '{org_name}'"]
                }
        except Exception as e:
            logger.error(f"Failed to resolve org CRD for '{org_name}' in {claim_summary}: {str(e)}", exc_info=True)
            return {
                "source": "Entity_Search",
                "basic_result": None,
                "detailed_result": None,
                "search_strategy": "search_with_crd_and_org_name",
                "crd_number": crd_number,
                "compliance": False,
                "compliance_explanation": f"Organization CRD resolution failed for '{org_name}': {str(e)}",
                "error": str(e)
            }

    logger.info(f"No valid BrokerCheck hits, falling back to SEC IAPD for {claim_summary}")
    try:
        basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
        logger.debug(f"SEC IAPD basic_result: {json_dumps_with_alerts(basic_result)}")
        if basic_result and (basic_result.get("fetched_name", "").strip() or basic_result.get("crd_number")):
            detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number)
            logger.debug(f"SEC IAPD detailed_result: {json_dumps_with_alerts(detailed_result)}")
            logger.info(f"SEC IAPD returned valid data for {claim_summary}")
            return {
                "source": "SEC_IAPD",
                "basic_result": basic_result,
                "detailed_result": detailed_result,
                "search_strategy": "search_with_crd_and_org_name",
                "crd_number": crd_number,
                "compliance": True,
                "compliance_explanation": "Search completed successfully with SEC IAPD data, individual found."
            }
        else:
            logger.warning(f"SEC IAPD search returned no meaningful data for {claim_summary}")
            return {
                "source": "SEC_IAPD",
                "basic_result": basic_result or {},
                "detailed_result": None,
                "search_strategy": "search_with_crd_and_org_name",
                "crd_number": crd_number,
                "compliance": False,
                "compliance_explanation": "Search completed but no individual found in SEC IAPD data."
            }
    except Exception as e:
        logger.error(f"SEC IAPD search failed for {claim_summary}: {str(e)}", exc_info=True)
        return {
            "source": "SEC_IAPD",
            "basic_result": None,
            "detailed_result": None,
            "search_strategy": "search_with_crd_and_org_name",
            "crd_number": crd_number,
            "compliance": False,
            "compliance_explanation": f"SEC IAPD search failed: {str(e)}",
            "error": str(e)
        }

def search_with_crd_and_org_crd(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Search using individual CRD and organization CRD, or name and organization CRD if individual CRD is missing."""
    crd_number = claim.get("crd_number", "")
    individual_name = claim.get("individual_name", "")
    organization_crd_number = claim.get("organization_crd_number", claim.get("organization_crd", ""))
    claim_summary = f"claim={json_dumps_with_alerts(claim)}, employee_number={employee_number}"
    
    logger.info(f"Executing search_with_crd_and_org_crd for {claim_summary} with crd_number='{crd_number}' and organization_crd_number='{organization_crd_number}'")

    try:
        if crd_number:
            # Full correlated search with both CRDs
            broker_result = facade.search_finra_correlated(individual_name or crd_number, organization_crd_number, employee_number)
            logger.debug(f"BrokerCheck correlated result: {json_dumps_with_alerts(broker_result)}")
            if broker_result and broker_result.get("fetched_name", "").strip():
                logger.info(f"BrokerCheck correlated search returned valid data for {employee_number}")
                detailed_broker_result = facade.search_finra_brokercheck_detailed(crd_number, employee_number)
                return {
                    "source": "BrokerCheck",
                    "basic_result": broker_result,
                    "detailed_result": detailed_broker_result if detailed_broker_result else None,
                    "search_strategy": "search_with_crd_and_org_crd",
                    "crd_number": crd_number,
                    "compliance": True,
                    "compliance_explanation": "Search completed successfully with BrokerCheck correlated data."
                }
            iapd_result = facade.search_sec_iapd_correlated(individual_name or crd_number, organization_crd_number, employee_number)
            if iapd_result and iapd_result.get("fetched_name", "").strip():
                logger.info(f"IAPD correlated search returned valid data for {employee_number}")
                detailed_iapd_result = facade.search_sec_iapd_detailed(crd_number, employee_number)
                return {
                    "source": "IAPD",
                    "basic_result": iapd_result,
                    "detailed_result": detailed_iapd_result if detailed_iapd_result else None,
                    "search_strategy": "search_with_crd_and_org_crd",
                    "crd_number": crd_number,
                    "compliance": True,
                    "compliance_explanation": "Search completed successfully with IAPD correlated data."
                }
        elif individual_name:
            # Fallback to name and organization CRD search
            logger.info(f"No individual CRD, searching with name '{individual_name}' and org_crd '{organization_crd_number}' for {claim_summary}")
            broker_result = facade.search_finra_correlated(individual_name, organization_crd_number, employee_number)
            logger.debug(f"BrokerCheck name/org result: {json_dumps_with_alerts(broker_result)}")
            if broker_result and broker_result.get("fetched_name", "").strip():
                crd_number = broker_result.get("crd_number", "")
                detailed_broker_result = facade.search_finra_brokercheck_detailed(crd_number, employee_number) if crd_number else None
                return {
                    "source": "BrokerCheck",
                    "basic_result": broker_result,
                    "detailed_result": detailed_broker_result if detailed_broker_result else None,
                    "search_strategy": "search_with_crd_and_org_crd",
                    "crd_number": crd_number,
                    "compliance": True,
                    "compliance_explanation": "Search completed successfully with BrokerCheck using name and org CRD."
                }
            iapd_result = facade.search_sec_iapd_correlated(individual_name, organization_crd_number, employee_number)
            logger.debug(f"IAPD name/org result: {json_dumps_with_alerts(iapd_result)}")
            if iapd_result and iapd_result.get("fetched_name", "").strip():
                crd_number = iapd_result.get("crd_number", "")
                detailed_iapd_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if crd_number else None
                return {
                    "source": "IAPD",
                    "basic_result": iapd_result,
                    "detailed_result": detailed_iapd_result if detailed_iapd_result else None,
                    "search_strategy": "search_with_crd_and_org_crd",
                    "crd_number": crd_number,
                    "compliance": True,
                    "compliance_explanation": "Search completed successfully with IAPD using name and org CRD."
                }

        # No valid results
        logger.info(f"No valid results found for {claim_summary} from BrokerCheck or IAPD")
        return {
            "source": None,
            "basic_result": None,
            "detailed_result": None,
            "search_strategy": "search_with_crd_and_org_crd",
            "crd_number": crd_number,
            "compliance": False,
            "compliance_explanation": "No valid data found in BrokerCheck or IAPD searches."
        }

    except Exception as e:
        logger.error(f"Search failed for {claim_summary}: {str(e)}", exc_info=True)
        return {
            "source": None,
            "basic_result": None,
            "detailed_result": None,
            "search_strategy": "search_with_crd_and_org_crd",
            "crd_number": crd_number,
            "compliance": False,
            "compliance_explanation": f"Search failed due to an error: {str(e)}"
        }

def search_with_crd_only(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Search using only CRD number, ensuring compliance reflects data retrieval."""
    crd_number = claim.get("crd_number", "")
    claim_summary = f"claim={json_dumps_with_alerts(claim)}, employee_number={employee_number}"
    logger.info(f"Executing search_with_crd_only for {claim_summary} with crd_number='{crd_number}'")

    try:
        broker_result = facade.search_finra_brokercheck_individual(crd_number, employee_number)
        logger.debug(f"BrokerCheck result: {json_dumps_with_alerts(broker_result)}")
    except Exception as e:
        logger.error(f"Failed to search BrokerCheck for {claim_summary}: {str(e)}", exc_info=True)
        return {
            "source": "BrokerCheck",
            "basic_result": None,
            "detailed_result": None,
            "search_strategy": "search_with_crd_only",
            "crd_number": crd_number,
            "compliance": False,
            "compliance_explanation": f"BrokerCheck search failed: {str(e)}",
            "error": str(e)
        }

    if broker_result and broker_result.get("fetched_name", "").strip():
        if broker_result.get("employments", []):
            logger.info(f"BrokerCheck returned valid data with employments for {claim_summary}")
            return {
                "source": "BrokerCheck",
                "basic_result": broker_result,
                "detailed_result": broker_result,
                "search_strategy": "search_with_crd_only",
                "crd_number": crd_number,
                "compliance": True,
                "compliance_explanation": "Search completed successfully with BrokerCheck data, individual found with employments."
            }
        else:
            logger.info(f"BrokerCheck hit but no employments found for {claim_summary}, falling back to SEC IAPD")

    logger.info(f"No valid BrokerCheck hits, searching SEC IAPD for {claim_summary}")
    try:
        basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
        logger.debug(f"SEC IAPD basic_result: {json_dumps_with_alerts(basic_result)}")
        if basic_result and (basic_result.get("fetched_name", "").strip() or basic_result.get("crd_number")):
            detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number)
            logger.debug(f"SEC IAPD detailed_result: {json_dumps_with_alerts(detailed_result)}")
            logger.info(f"SEC IAPD returned valid data for {claim_summary}")
            return {
                "source": "SEC_IAPD",
                "basic_result": basic_result,
                "detailed_result": detailed_result,
                "search_strategy": "search_with_crd_only",
                "crd_number": crd_number,
                "compliance": True,
                "compliance_explanation": "Search completed successfully with SEC IAPD data, individual found."
            }
        else:
            logger.warning(f"SEC IAPD search returned no meaningful data for {claim_summary}")
            return {
                "source": "SEC_IAPD",
                "basic_result": basic_result or {},
                "detailed_result": None,
                "search_strategy": "search_with_crd_only",
                "crd_number": crd_number,
                "compliance": False,
                "compliance_explanation": "Search completed but no individual found in SEC IAPD data."
            }
    except Exception as e:
        logger.error(f"Failed to search SEC IAPD for {claim_summary}: {str(e)}", exc_info=True)
        return {
            "source": "SEC_IAPD",
            "basic_result": None,
            "detailed_result": None,
            "search_strategy": "search_with_crd_only",
            "crd_number": crd_number,
            "compliance": False,
            "compliance_explanation": f"SEC IAPD search failed: {str(e)}",
            "error": str(e)
        }

def search_with_entity(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Search using only organization CRD number."""
    organization_crd_number = claim.get("organization_crd_number", claim.get("organization_crd", ""))
    claim_summary = f"claim={json_dumps_with_alerts(claim)}, employee_number={employee_number}"
    logger.info(f"Executing search_with_entity for {claim_summary} with organization_crd_number='{organization_crd_number}'")
    logger.warning(f"Entity search not supported for {claim_summary}")
    return {
        "source": "Entity_Search",
        "basic_result": None,
        "detailed_result": None,
        "search_strategy": "search_with_entity",
        "crd_number": None,
        "compliance": False,
        "compliance_explanation": "Entity search using organization_crd_number is not supported."
    }

def search_with_org_name_only(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Search using only organization name."""
    organization_name = claim.get("organization_name", "")
    claim_summary = f"claim={json_dumps_with_alerts(claim)}, employee_number={employee_number}"
    logger.info(f"Executing search_with_org_name_only for {claim_summary} with organization_name='{organization_name}'")
    logger.warning(f"Entity search not supported for {claim_summary}")
    return {
        "source": "Entity_Search",
        "basic_result": None,
        "detailed_result": None,
        "search_strategy": "search_with_org_name_only",
        "crd_number": None,
        "compliance": False,
        "compliance_explanation": "Entity search using organization_name is not supported."
    }

def search_default(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Default search when no usable fields are provided."""
    claim_summary = f"claim={json_dumps_with_alerts(claim)}, employee_number={employee_number}"
    logger.info(f"Executing search_default for {claim_summary}")
    logger.warning(f"Insufficient identifiers to perform search for {claim_summary}")
    return {
        "source": "Default",
        "basic_result": None,
        "detailed_result": None,
        "search_strategy": "search_default",
        "crd_number": None,
        "compliance": False,
        "compliance_explanation": "Insufficient identifiers to perform search."
    }

def search_with_correlated(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Search using correlated individual and organization data, delegating to search_with_crd_only if CRD is resolved."""
    individual_name = claim.get("individual_name", "")
    organization_name = claim.get("organization_name", "")
    organization_crd_number = claim.get("organization_crd_number", claim.get("organization_crd", ""))
    claim_summary = f"claim={json_dumps_with_alerts(claim)}, employee_number={employee_number}"
    
    logger.info(f"Executing search_with_correlated for {claim_summary} with individual_name='{individual_name}', "
                f"organization_name='{organization_name}', organization_crd_number='{organization_crd_number}'")

    resolved_crd_number = None
    if organization_crd_number.strip():
        resolved_crd_number = organization_crd_number
        logger.debug(f"Using provided organization_crd_number='{resolved_crd_number}' for {claim_summary}")
    elif organization_name.strip():
        try:
            resolved_crd_number = facade.get_organization_crd(organization_name)
            if not resolved_crd_number or resolved_crd_number == "NOT_FOUND":
                logger.info(f"Skipping record - unable to resolve CRD for organization_name='{organization_name}' in {claim_summary}")
                return {
                    "source": "SEC_IAPD",
                    "basic_result": None,
                    "detailed_result": None,
                    "search_strategy": "search_with_correlated",
                    "crd_number": None,
                    "compliance": False,
                    "compliance_explanation": f"Unable to resolve organization CRD from name '{organization_name}'",
                    "skip_reasons": [f"Unable to resolve organization CRD from name '{organization_name}'"]
                }
            logger.debug(f"Resolved CRD '{resolved_crd_number}' from organization_name='{organization_name}' for {claim_summary}")
        except Exception as e:
            logger.error(f"Error resolving CRD for organization_name='{organization_name}' in {claim_summary}: {str(e)}", exc_info=True)
            return {
                "source": "SEC_IAPD",
                "basic_result": None,
                "detailed_result": None,
                "search_strategy": "search_with_correlated",
                "crd_number": None,
                "compliance": False,
                "compliance_explanation": f"Organization CRD resolution failed for '{organization_name}': {str(e)}",
                "skip_reasons": [f"Organization CRD resolution failed for '{organization_name}'"]
            }
    else:
        logger.info(f"Skipping record - no organization_name or organization_crd_number provided for {claim_summary}")
        return {
            "source": "SEC_IAPD",
            "basic_result": None,
            "detailed_result": None,
            "search_strategy": "search_with_correlated",
            "crd_number": None,
            "compliance": False,
            "compliance_explanation": "No organization name or CRD provided for correlated search",
            "skip_reasons": ["No organization name or CRD provided"]
        }

    claim["crd_number"] = resolved_crd_number
    logger.info(f"Resolved CRD number '{resolved_crd_number}' for {claim_summary}, delegating to search_with_crd_only")
    try:
        result = search_with_crd_only(claim, facade, employee_number)
        logger.debug(f"search_with_crd_only returned: {json_dumps_with_alerts(result)} for {claim_summary}")
        # Preserve the original search strategy
        result["search_strategy"] = "search_with_correlated"
        return result
    except Exception as e:
        logger.error(f"Delegation to search_with_crd_only failed for {claim_summary} with crd_number='{resolved_crd_number}': {str(e)}", exc_info=True)
        return {
            "source": "Unknown",
            "basic_result": None,
            "detailed_result": None,
            "search_strategy": "search_with_correlated",
            "crd_number": resolved_crd_number,
            "compliance": False,
            "compliance_explanation": f"Search failed: Delegation to search_with_crd_only failed: {str(e)}",
            "error": str(e)
        }

def process_claim(
    claim: Dict[str, Any],
    facade: FinancialServicesFacade,
    employee_number: str = None,
    skip_disciplinary: bool = False,
    skip_arbitration: bool = False,
    skip_regulatory: bool = False
) -> Dict[str, Any]:
    """Process a claim with enhanced error handling and logging."""
    claim_summary = f"claim={json_dumps_with_alerts(claim)}"
    employee_number = claim.get("employee_number", employee_number or "EMP_DEFAULT")
    logger.info(f"Starting claim processing for {claim_summary}, employee_number={employee_number}, "
                f"skip_disciplinary={skip_disciplinary}, skip_arbitration={skip_arbitration}, skip_regulatory={skip_regulatory}")

    strategy_func = determine_search_strategy(claim)
    logger.debug(f"Selected strategy: {strategy_func.__name__} for {claim_summary}")

    try:
        search_evaluation = strategy_func(claim, facade, employee_number)
    except Exception as e:
        logger.error(f"Search strategy {strategy_func.__name__} failed for {claim_summary}: {str(e)}", exc_info=True)
        search_evaluation = {
            "source": "Unknown",
            "search_strategy": strategy_func.__name__,
            "crd_number": None,
            "basic_result": None,
            "detailed_result": None,
            "compliance": False,
            "compliance_explanation": f"Search strategy execution failed: {str(e)}",
            "skip_reasons": [f"Search strategy execution failed: {str(e)}"]
        }

    # Prepare extracted_info
    extracted_info = {
        "search_evaluation": search_evaluation,
        "individual": {},
        "fetched_name": "",
        "other_names": [],
        "bc_scope": "NotInScope",
        "ia_scope": "NotInScope",
        "exams": [],
        "disclosures": [],
        "disciplinary_evaluation": {"actions": [], "due_diligence": {"status": "Skipped"}},
        "arbitration_evaluation": {"actions": [], "due_diligence": {"status": "Skipped"}},
        "regulatory_evaluation": {"actions": [], "due_diligence": {"status": "Skipped"}}
    }

    if "skip_reasons" not in search_evaluation and search_evaluation.get("compliance", False):
        # Only perform detailed evaluations if search succeeds
        first_name = claim.get("first_name", "")
        last_name = claim.get("last_name", "")
        individual_name = claim.get("individual_name", "")
        if not (first_name and last_name) and individual_name:
            first_name, *last_name_parts = individual_name.split()
            last_name = " ".join(last_name_parts) if last_name_parts else ""

        if skip_disciplinary:
            logger.info(f"Skipping disciplinary review for {claim_summary}")
            extracted_info["disciplinary_evaluation"] = {"actions": [], "due_diligence": {"status": "Skipped per configuration"}}
        else:
            try:
                extracted_info["disciplinary_evaluation"] = facade.perform_disciplinary_review(first_name, last_name, employee_number) if first_name and last_name else {
                    "actions": [], "due_diligence": {"status": "No name provided"}
                }
            except Exception as e:
                logger.error(f"Disciplinary review failed for {claim_summary}: {str(e)}", exc_info=True)
                extracted_info["disciplinary_evaluation"] = {"actions": [], "due_diligence": {"status": f"Failed: {str(e)}"}}

        if skip_arbitration:
            logger.info(f"Skipping arbitration review for {claim_summary}")
            extracted_info["arbitration_evaluation"] = {"actions": [], "due_diligence": {"status": "Skipped per configuration"}}
        else:
            try:
                extracted_info["arbitration_evaluation"] = facade.perform_arbitration_review(first_name, last_name, employee_number) if first_name and last_name else {
                    "actions": [], "due_diligence": {"status": "No name provided"}
                }
            except Exception as e:
                logger.error(f"Arbitration review failed for {claim_summary}: {str(e)}", exc_info=True)
                extracted_info["arbitration_evaluation"] = {"actions": [], "due_diligence": {"status": f"Failed: {str(e)}"}}

        if skip_regulatory:
            logger.info(f"Skipping regulatory review for {claim_summary}")
            extracted_info["regulatory_evaluation"] = {"actions": [], "due_diligence": {"status": "Skipped per configuration"}}
        else:
            try:
                extracted_info["regulatory_evaluation"] = facade.perform_regulatory_review(first_name, last_name, employee_number) if first_name and last_name else {
                    "actions": [], "due_diligence": {"status": "No name provided"}
                }
            except Exception as e:
                logger.error(f"Regulatory review failed for {claim_summary}: {str(e)}", exc_info=True)
                extracted_info["regulatory_evaluation"] = {"actions": [], "due_diligence": {"status": f"Failed: {str(e)}"}}

        extracted_info.update({
            "individual": search_evaluation.get("basic_result", {}),
            "fetched_name": search_evaluation.get("basic_result", {}).get("fetched_name", ""),
            "other_names": search_evaluation.get("basic_result", {}).get("other_names", []),
            "bc_scope": search_evaluation.get("basic_result", {}).get("bc_scope", "NotInScope"),
            "ia_scope": search_evaluation.get("basic_result", {}).get("ia_scope", "NotInScope"),
            "exams": search_evaluation.get("detailed_result", {}).get("exams", []) if search_evaluation.get("detailed_result") else [],
            "disclosures": search_evaluation.get("detailed_result", {}).get("disclosures", []) if search_evaluation.get("detailed_result") else []
        })

    # Construct report via director
    builder = EvaluationReportBuilder(claim.get("reference_id", "UNKNOWN"))
    director = EvaluationReportDirector(builder)
    report = director.construct_evaluation_report(claim, extracted_info)

    try:
        if facade.save_compliance_report(report, employee_number):
            logger.info(f"Compliance report saved for {claim_summary}, reference_id={report['reference_id']}")
        else:
            logger.error(f"Failed to save compliance report for {claim_summary}, reference_id={report['reference_id']}")
    except Exception as e:
        logger.error(f"Exception while saving compliance report for {claim_summary}, reference_id={report['reference_id']}: {str(e)}", exc_info=True)

    logger.info(f"Claim processing completed for {claim_summary}")
    return report

if __name__ == "__main__":
    facade = FinancialServicesFacade()

    def run_process_claim(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str = None, skip_disciplinary: bool = False, skip_arbitration: bool = False, skip_regulatory: bool = False):
        result = process_claim(claim, facade, employee_number, skip_disciplinary, skip_arbitration, skip_regulatory)
        print(f"\nResult for {claim.get('reference_id', 'Custom Claim')}:")
        print(json_dumps_with_alerts(result, indent=2))

    while True:
        print("\nBusiness Process Interactive Menu:")
        print("1. Process predefined Matthew Vetto claim")
        print("2. Process predefined Mark Miller claim")
        print("3. Process custom claim")
        print("4. Exit")
        choice = input("Enter your choice (1-4): ").strip()

        if choice == "1":
            claim = {
                "crd_number": "",
                "first_name": "Matthew",
                "last_name": "Vetto",
                "organization_crd": "282563",
                "employee_number": "FIRST_RUN",
                "reference_id": "S987-97987",
                "individual_name": "Matthew Vetto"
            }
            print("\nRunning Matthew Vetto claim...")
            run_process_claim(claim, facade)
        elif choice == "2":
            claim = {
                "first_name": "Mark",
                "last_name": "Miller",
                "employee_number": "EMP_TEST",
                "reference_id": "S123-45678"
            }
            print("\nRunning Mark Miller claim...")
            run_process_claim(claim, facade)
        elif choice == "3":
            print("\nEnter custom claim details (press Enter to skip optional fields):")
            reference_id = input("Reference ID (e.g., S123-45678): ").strip() or "CUSTOM-" + str(id({}))
            employee_number = input("Employee Number (e.g., EMP001): ").strip() or "EMP_CUSTOM"
            first_name = input("First Name: ").strip()
            last_name = input("Last Name: ").strip()
            individual_name = input("Individual Name (if different, e.g., 'John Doe'): ").strip() or (f"{first_name} {last_name}".strip() if first_name and last_name else "")
            crd_number = input("CRD Number: ").strip()
            organization_crd = input("Organization CRD Number: ").strip()
            organization_name = input("Organization Name: ").strip()
            skip_disc = input("Skip Disciplinary Review? (y/n): ").strip().lower() == 'y'
            skip_arb = input("Skip Arbitration Review? (y/n): ").strip().lower() == 'y'
            skip_reg = input("Skip Regulatory Review? (y/n): ").strip().lower() == 'y'

            claim = {
                "reference_id": reference_id,
                "employee_number": employee_number
            }
            if first_name:
                claim["first_name"] = first_name
            if last_name:
                claim["last_name"] = last_name
            if individual_name:
                claim["individual_name"] = individual_name
            if crd_number:
                claim["crd_number"] = crd_number
            if organization_crd:
                claim["organization_crd"] = organization_crd
            if organization_name:
                claim["organization_name"] = organization_name

            print("\nRunning custom claim...")
            run_process_claim(claim, facade, skip_disciplinary=skip_disc, skip_arbitration=skip_arb, skip_regulatory=skip_reg)
        elif choice == "4":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please enter 1, 2, 3, or 4.")