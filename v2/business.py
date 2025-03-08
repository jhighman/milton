import json
from typing import Dict, Any, Callable
import logging
from services import FinancialServicesFacade
from evaluation_report_builder import EvaluationReportBuilder
from evaluation_report_director import EvaluationReportDirector

# Configure logging with detailed format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("business")

def determine_search_strategy(claim: Dict[str, Any]) -> Callable[[Dict[str, Any], FinancialServicesFacade, str], Dict[str, Any]]:
    """Determine the appropriate search strategy based on claim data."""
    individual_name = claim.get("individual_name", "")
    crd_number = claim.get("crd_number", "")
    organization_crd_number = claim.get("organization_crd_number", claim.get("organization_crd", ""))
    organization_name = claim.get("organization_name", "")

    claim_summary = f"claim={json.dumps(claim, default=str)}"
    logger.debug(f"Determining search strategy for {claim_summary}")

    if crd_number:
        logger.info(f"Selected search_with_crd_only for {claim_summary} due to crd_number='{crd_number}'")
        return search_with_crd_only
    elif individual_name and organization_name and not organization_crd_number:
        logger.info(f"Selected search_with_correlated for {claim_summary} with individual_name='{individual_name}' and organization_name='{organization_name}'")
        return search_with_correlated
    elif individual_name and organization_crd_number:
        logger.info(f"Selected search_with_correlated for {claim_summary} with individual_name='{individual_name}' and organization_crd_number='{organization_crd_number}'")
        return search_with_correlated
    elif not individual_name and not organization_crd_number and not organization_name:
        logger.info(f"Selected search_default for {claim_summary} due to no key identifiers")
        return search_default
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
    claim_summary = f"claim={json.dumps(claim, default=str)}, employee_number={employee_number}"
    logger.info(f"Executing search_with_both_crds for {claim_summary} with crd_number='{crd_number}'")

    try:
        basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
        detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
        compliance = bool(basic_result)
        explanation = "Search completed successfully with SEC IAPD data." if compliance else "Search failed to retrieve data from SEC IAPD."
        logger.debug(f"SEC IAPD result: compliance={compliance}, basic_result={json.dumps(basic_result, default=str)}")
        return {
            "source": "SEC_IAPD",
            "basic_result": basic_result,
            "detailed_result": detailed_result,
            "search_strategy": "search_with_both_crds",
            "crd_number": crd_number,
            "compliance": compliance,
            "compliance_explanation": explanation
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
    claim_summary = f"claim={json.dumps(claim, default=str)}, employee_number={employee_number}"
    logger.info(f"Executing search_with_crd_and_org_name for {claim_summary} with crd_number='{crd_number}', org_name='{org_name}'")

    try:
        broker_result = facade.search_finra_brokercheck_individual(crd_number, employee_number)
        if broker_result and broker_result.get("fetched_name", "").strip():
            detailed_result = facade.search_finra_brokercheck_detailed(crd_number, employee_number)
            logger.debug(f"BrokerCheck result: {json.dumps(broker_result, default=str)}")
            return {
                "source": "BrokerCheck",
                "basic_result": broker_result,
                "detailed_result": detailed_result,
                "search_strategy": "search_with_crd_and_org_name",
                "crd_number": crd_number,
                "compliance": True,
                "compliance_explanation": "Search completed successfully with BrokerCheck data."
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
                    "compliance_explanation": "Unable to resolve organization CRD from name",
                    "skip_reasons": ["Unable to resolve organization CRD from name"]
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
                "compliance_explanation": f"Organization CRD resolution failed: {str(e)}",
                "error": str(e)
            }

    logger.info(f"No BrokerCheck hits, falling back to SEC IAPD for {claim_summary}")
    try:
        basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
        detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
        compliance = bool(basic_result)
        explanation = "Search completed successfully with SEC IAPD data." if compliance else "Search failed to retrieve data from SEC IAPD."
        return {
            "source": "SEC_IAPD",
            "basic_result": basic_result,
            "detailed_result": detailed_result,
            "search_strategy": "search_with_crd_and_org_name",
            "crd_number": crd_number,
            "compliance": compliance,
            "compliance_explanation": explanation
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

def search_with_crd_only(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    """Search using only CRD number."""
    crd_number = claim.get("crd_number", "")
    claim_summary = f"claim={json.dumps(claim, default=str)}, employee_number={employee_number}"
    logger.info(f"Executing search_with_crd_only for {claim_summary} with crd_number='{crd_number}'")

    try:
        broker_result = facade.search_finra_brokercheck_individual(crd_number, employee_number)
        logger.debug(f"BrokerCheck result: {json.dumps(broker_result, default=str)}")
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
        try:
            detailed_result = facade.search_finra_brokercheck_detailed(crd_number, employee_number)
            if detailed_result and detailed_result.get("employments", []):
                logger.info(f"BrokerCheck returned valid data with employments for {claim_summary}")
                return {
                    "source": "BrokerCheck",
                    "basic_result": broker_result,
                    "detailed_result": detailed_result,
                    "search_strategy": "search_with_crd_only",
                    "crd_number": crd_number,
                    "compliance": True,
                    "compliance_explanation": "Search completed successfully with BrokerCheck data."
                }
            else:
                logger.info(f"BrokerCheck hit but no employments found for {claim_summary}, falling back to SEC IAPD")
        except Exception as e:
            logger.error(f"Failed to fetch detailed BrokerCheck data for {claim_summary}: {str(e)}", exc_info=True)
            return {
                "source": "BrokerCheck",
                "basic_result": broker_result,
                "detailed_result": None,
                "search_strategy": "search_with_crd_only",
                "crd_number": crd_number,
                "compliance": False,
                "compliance_explanation": f"Detailed BrokerCheck search failed: {str(e)}",
                "error": str(e)
            }

    logger.info(f"No valid BrokerCheck hits, searching SEC IAPD for {claim_summary}")
    try:
        basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
        detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
        compliance = bool(basic_result)
        explanation = "Search completed successfully with SEC IAPD data." if compliance else "Search failed to retrieve data from SEC IAPD."
        logger.debug(f"SEC IAPD result - compliance={compliance}, basic_result={json.dumps(basic_result, default=str)}")
        return {
            "source": "SEC_IAPD",
            "basic_result": basic_result,
            "detailed_result": detailed_result,
            "search_strategy": "search_with_crd_only",
            "crd_number": crd_number,
            "compliance": compliance,
            "compliance_explanation": explanation
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
    claim_summary = f"claim={json.dumps(claim, default=str)}, employee_number={employee_number}"
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
    claim_summary = f"claim={json.dumps(claim, default=str)}, employee_number={employee_number}"
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
    claim_summary = f"claim={json.dumps(claim, default=str)}, employee_number={employee_number}"
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
    claim_summary = f"claim={json.dumps(claim, default=str)}, employee_number={employee_number}"
    
    logger.info(f"Executing search_with_correlated for {claim_summary} with individual_name='{individual_name}', "
                f"organization_name='{organization_name}', organization_crd_number='{organization_crd_number}'")

    # Attempt to resolve CRD
    resolved_crd_number = None
    if organization_crd_number.strip():
        resolved_crd_number = organization_crd_number
        logger.debug(f"Using provided organization_crd_number='{resolved_crd_number}' for {claim_summary}")
    elif organization_name.strip():
        try:
            resolved_crd_number = facade.get_organization_crd(organization_name)
            if not resolved_crd_number or resolved_crd_number == "NOT_FOUND":
                logger.warning(f"Failed to resolve CRD for organization_name='{organization_name}' in {claim_summary}")
                return {
                    "source": "SEC_IAPD",
                    "basic_result": None,
                    "detailed_result": None,
                    "search_strategy": "search_with_correlated",
                    "crd_number": None,
                    "compliance": False,
                    "compliance_explanation": "Unable to resolve organization CRD from name",
                    "skip_reasons": ["Unable to resolve organization CRD from name"]
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
                "compliance_explanation": f"Organization CRD resolution failed: {str(e)}",
                "error": str(e)
            }
    else:
        logger.warning(f"No organization_name or organization_crd_number provided for {claim_summary}")
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

    # Add the resolved CRD number into the claim for delegation to search_with_crd_only
    claim["crd_number"] = resolved_crd_number
    logger.info(f"Resolved CRD number '{resolved_crd_number}' for {claim_summary}, delegating to search_with_crd_only")

    # Delegate to search_with_crd_only with error handling
    try:
        result = search_with_crd_only(claim, facade, employee_number)
        logger.debug(f"search_with_crd_only returned: {json.dumps(result, default=str)} for {claim_summary}")
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
            "compliance_explanation": f"Delegation to search_with_crd_only failed: {str(e)}",
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
    claim_summary = f"claim={json.dumps(claim, default=str)}"
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
            "error": str(e)
        }

    if search_evaluation is None or "skip_reasons" in search_evaluation or not search_evaluation.get("compliance", False):
        logger.warning(f"Search failed or skipped for {claim_summary}: {search_evaluation.get('compliance_explanation', 'Unknown issue')}")
        extracted_info = {
            "search_evaluation": search_evaluation,
            "skip_reasons": search_evaluation.get("skip_reasons", ["Search failure"]) if search_evaluation else ["Search returned None"],
            "individual": {},
            "fetched_name": "",
            "other_names": [],
            "bc_scope": "NotInScope",
            "ia_scope": "NotInScope",
            "exams": [],
            "disclosures": [],
            "disciplinary_evaluation": {"actions": [], "due_diligence": {"status": "Skipped due to search failure"}},
            "arbitration_evaluation": {"actions": [], "due_diligence": {"status": "Skipped due to search failure"}},
            "regulatory_evaluation": {"actions": [], "due_diligence": {"status": "Skipped due to search failure"}}
        }
    else:
        first_name = claim.get("first_name", "")
        last_name = claim.get("last_name", "")
        individual_name = claim.get("individual_name", "")
        if not (first_name and last_name) and individual_name:
            first_name, *last_name_parts = individual_name.split()
            last_name = " ".join(last_name_parts) if last_name_parts else ""

        disciplinary_evaluation = arbitration_evaluation = regulatory_evaluation = None

        if skip_disciplinary:
            logger.info(f"Skipping disciplinary review for {claim_summary}")
            disciplinary_evaluation = {"actions": [], "due_diligence": {"status": "Skipped per configuration"}}
        else:
            try:
                disciplinary_evaluation = facade.perform_disciplinary_review(first_name, last_name, employee_number) if first_name and last_name else {
                    "actions": [], "due_diligence": {"status": "No name provided"}
                }
            except Exception as e:
                logger.error(f"Disciplinary review failed for {claim_summary}: {str(e)}", exc_info=True)
                disciplinary_evaluation = {"actions": [], "due_diligence": {"status": f"Failed: {str(e)}"}}

        if skip_arbitration:
            logger.info(f"Skipping arbitration review for {claim_summary}")
            arbitration_evaluation = {"actions": [], "due_diligence": {"status": "Skipped per configuration"}}
        else:
            try:
                arbitration_evaluation = facade.perform_arbitration_review(first_name, last_name, employee_number) if first_name and last_name else {
                    "actions": [], "due_diligence": {"status": "No name provided"}
                }
            except Exception as e:
                logger.error(f"Arbitration review failed for {claim_summary}: {str(e)}", exc_info=True)
                arbitration_evaluation = {"actions": [], "due_diligence": {"status": f"Failed: {str(e)}"}}

        if skip_regulatory:
            logger.info(f"Skipping regulatory review for {claim_summary}")
            regulatory_evaluation = {"actions": [], "due_diligence": {"status": "Skipped per configuration"}}
        else:
            try:
                regulatory_evaluation = facade.perform_regulatory_review(first_name, last_name, employee_number) if first_name and last_name else {
                    "actions": [], "due_diligence": {"status": "No name provided"}
                }
            except Exception as e:
                logger.error(f"Regulatory review failed for {claim_summary}: {str(e)}", exc_info=True)
                regulatory_evaluation = {"actions": [], "due_diligence": {"status": f"Failed: {str(e)}"}}

        extracted_info = {
            "search_evaluation": search_evaluation,
            "individual": search_evaluation.get("basic_result", {}),
            "fetched_name": search_evaluation.get("basic_result", {}).get("fetched_name", ""),
            "other_names": search_evaluation.get("basic_result", {}).get("other_names", []),
            "bc_scope": search_evaluation.get("basic_result", {}).get("bc_scope", "NotInScope"),
            "ia_scope": search_evaluation.get("basic_result", {}).get("ia_scope", "NotInScope"),
            "exams": search_evaluation.get("detailed_result", {}).get("exams", []) if search_evaluation.get("detailed_result") else [],
            "disclosures": search_evaluation.get("detailed_result", {}).get("disclosures", []) if search_evaluation.get("detailed_result") else [],
            "disciplinary_evaluation": disciplinary_evaluation,
            "arbitration_evaluation": arbitration_evaluation,
            "regulatory_evaluation": regulatory_evaluation
        }

    reference_id = claim.get("reference_id", "UNKNOWN")
    builder = EvaluationReportBuilder(reference_id)
    director = EvaluationReportDirector(builder)
    report = director.construct_evaluation_report(claim, extracted_info)

    try:
        if facade.save_compliance_report(report, employee_number):
            logger.info(f"Compliance report saved for {claim_summary}, reference_id={reference_id}")
        else:
            logger.error(f"Failed to save compliance report for {claim_summary}, reference_id={reference_id}")
    except Exception as e:
        logger.error(f"Exception while saving compliance report for {claim_summary}, reference_id={reference_id}: {str(e)}", exc_info=True)

    logger.info(f"Claim processing completed for {claim_summary}")
    return report

if __name__ == "__main__":
    facade = FinancialServicesFacade()

    def run_process_claim(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str = None, skip_disciplinary: bool = False, skip_arbitration: bool = False, skip_regulatory: bool = False):
        result = process_claim(claim, facade, employee_number, skip_disciplinary, skip_arbitration, skip_regulatory)
        print(f"\nResult for {claim.get('reference_id', 'Custom Claim')}:")
        print(json.dumps(result, indent=2, default=str))

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