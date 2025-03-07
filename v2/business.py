import json
from typing import Dict, Any, Callable
import logging
from services import FinancialServicesFacade
from evaluation_report_builder import EvaluationReportBuilder
from evaluation_report_director import EvaluationReportDirector

logger = logging.getLogger("business")

def determine_search_strategy(claim: Dict[str, Any]) -> Callable[[Dict[str, Any], FinancialServicesFacade, str], Dict[str, Any]]:
    individual_name = claim.get("individual_name") or ""
    crd_number = claim.get("crd_number", "")
    organization_crd_number = claim.get("organization_crd_number", claim.get("organization_crd", ""))
    organization_name = claim.get("organization_name", "")

    # First order of precedence: if crd_number is present, use search_with_crd_only
    if crd_number:
        logger.info(f"Claim has crd_number='{crd_number}', selecting search_with_crd_only as highest priority")
        return search_with_crd_only

    # Subsequent conditions for cases without crd_number
    if individual_name and organization_name and not organization_crd_number:
        logger.info("Claim has individual_name and organization_name but no organization_crd_number, selecting search_with_correlated")
        return search_with_correlated
    elif individual_name and organization_crd_number:
        logger.info("Claim has individual_name and organization_crd_number, selecting search_with_correlated")
        return search_with_correlated
    elif not individual_name and not organization_crd_number and not organization_name:
        logger.info("Claim has no individual_name, organization_crd_number, or organization_name, selecting search_default")
        return search_default
    elif organization_crd_number:
        logger.info("Claim has only organization_crd_number, selecting search_with_entity")
        return search_with_entity
    elif organization_name:
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
    compliance = bool(basic_result)
    explanation = "Search completed successfully with SEC IAPD data." if compliance else "Search failed to retrieve data from SEC IAPD."
    return {
        "source": "SEC_IAPD",
        "basic_result": basic_result,
        "detailed_result": detailed_result,
        "search_strategy": "search_with_both_crds",
        "crd_number": crd_number,
        "compliance": compliance,
        "compliance_explanation": explanation
    }

def search_with_crd_and_org_name(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    crd_number = claim.get("crd_number", "")
    org_name = claim.get("organization_name", "")
    logger.info(f"Searching with crd_number='{crd_number}', organization_name='{org_name}', Employee='{employee_number}'")
    broker_result = facade.search_finra_brokercheck_individual(crd_number, employee_number)
    if broker_result and broker_result.get("fetched_name") != "":
        detailed_result = facade.search_finra_brokercheck_detailed(crd_number, employee_number)
        return {
            "source": "BrokerCheck",
            "basic_result": broker_result,
            "detailed_result": detailed_result,
            "search_strategy": "search_with_crd_and_org_name",
            "crd_number": crd_number,
            "compliance": True,
            "compliance_explanation": "Search completed successfully with BrokerCheck data."
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
                "crd_number": crd_number,
                "compliance": False,
                "compliance_explanation": "Unable to resolve organization CRD from name",
                "skip_reasons": ["Unable to resolve organization CRD from name"]
            }
        logger.info(f"No BrokerCheck hits, searching SEC IAPD with crd_number='{crd_number}'")
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
    logger.info(f"No BrokerCheck hits, searching SEC IAPD with crd_number='{crd_number}'")
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

def search_with_crd_only(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    crd_number = claim.get("crd_number", "")
    logger.info(f"Searching with crd_number only='{crd_number}', Employee='{employee_number}'")
    broker_result = facade.search_finra_brokercheck_individual(crd_number, employee_number)
    
    if broker_result and broker_result.get("fetched_name", "").strip():
        detailed_result = facade.search_finra_brokercheck_detailed(crd_number, employee_number)
        # Check if detailed_result exists and employments is not empty
        if detailed_result and detailed_result.get("employments", []):
            logger.info(f"BrokerCheck returned valid data with employments for CRD='{crd_number}'")
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
            logger.info(f"BrokerCheck hit but no employments found for CRD='{crd_number}', falling back to SEC IAPD")
    
    logger.info(f"No valid BrokerCheck hits (either no result, no name, or no employments) => searching SEC IAPD with crd_number='{crd_number}'")
    basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
    detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
    compliance = bool(basic_result)
    explanation = "Search completed successfully with SEC IAPD data." if compliance else "Search failed to retrieve data from SEC IAPD."
    return {
        "source": "SEC_IAPD",
        "basic_result": basic_result,
        "detailed_result": detailed_result,
        "search_strategy": "search_with_crd_only",
        "crd_number": crd_number,
        "compliance": compliance,
        "compliance_explanation": explanation
    }

def search_with_entity(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    organization_crd_number = claim.get("organization_crd_number", claim.get("organization_crd", ""))
    logger.info(f"Detected entity search with organization_crd_number='{organization_crd_number}', Employee='{employee_number}'")
    logger.warning("Entity search using organization_crd_number is not supported at this time.")
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
    organization_name = claim.get("organization_name", "")
    logger.info(f"Detected org_name-only search with organization_name='{organization_name}', Employee='{employee_number}'")
    logger.warning("Entity search using organization_name is not supported at this time.")
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
    logger.info(f"No usable fields => defaulting, Employee='{employee_number}'")
    logger.warning("Insufficient identifiers to perform search")
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
    individual_name = claim.get("individual_name") or ""
    organization_name = claim.get("organization_name", "")
    logger.info(f"Searching SEC IAPD with individual_name='{individual_name}', organization_name='{organization_name}', Employee='{employee_number}'")
    
    if not organization_name.strip():
        logger.warning("No organization_name provided for correlated search")
        return {
            "source": "SEC_IAPD",
            "basic_result": None,
            "detailed_result": None,
            "search_strategy": "search_with_correlated",
            "crd_number": None,
            "compliance": False,
            "compliance_explanation": "No organization name provided for correlated search",
            "skip_reasons": ["No organization name provided"]
        }
    
    org_crd_number = facade.get_organization_crd(organization_name)
    if org_crd_number is None or org_crd_number == "NOT_FOUND":
        logger.warning(f"Failed to resolve CRD for organization '{organization_name}'")
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
    
    logger.info(f"Resolved organization CRD '{org_crd_number}' for '{organization_name}', proceeding with correlated search")
    basic_result = facade.search_sec_iapd_correlated(individual_name, org_crd_number, employee_number)
    logger.debug(f"Raw basic_result from SEC IAPD correlated search: {json.dumps(basic_result, indent=2)}")
    crd_number = basic_result.get("crd_number", None) if basic_result else None
    if basic_result:
        logger.info(f"Found basic_result with crd_number='{crd_number}'")
    else:
        logger.warning("No basic_result from SEC IAPD correlated search")
    detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if crd_number else None
    compliance = bool(basic_result)
    explanation = "Search completed successfully with SEC IAPD correlated data." if compliance else "Search failed to retrieve correlated data from SEC IAPD."
    return {
        "source": "SEC_IAPD",
        "basic_result": basic_result,
        "detailed_result": detailed_result,
        "search_strategy": "search_with_correlated",
        "crd_number": crd_number,
        "compliance": compliance,
        "compliance_explanation": explanation
    }

def process_claim(
    claim: Dict[str, Any],
    facade: FinancialServicesFacade,
    employee_number: str = None,
    skip_disciplinary: bool = False,
    skip_arbitration: bool = False,
    skip_regulatory: bool = False
) -> Dict[str, Any]:
    """Process a claim by collecting data and building a report."""
    logger.info(f"Starting claim processing for {claim}, Employee='{employee_number}', skip_disciplinary={skip_disciplinary}, skip_arbitration={skip_arbitration}, skip_regulatory={skip_regulatory}")
    
    employee_number = claim.get("employee_number", employee_number or "EMP_DEFAULT")
    
    strategy_func = determine_search_strategy(claim)
    logger.debug(f"Selected primary strategy: {strategy_func.__name__}")
    search_evaluation = strategy_func(claim, facade, employee_number)
    
    if search_evaluation is None or "skip_reasons" in search_evaluation:
        logger.warning(f"Primary strategy {strategy_func.__name__} failed or skipped: {search_evaluation.get('compliance_explanation', 'Unknown issue') if search_evaluation else 'Returned None'}")
        extracted_info = {
            "search_evaluation": search_evaluation or {
                "source": "Unknown",
                "search_strategy": strategy_func.__name__,
                "crd_number": None,
                "basic_result": None,
                "detailed_result": None,
                "compliance": False,
                "compliance_explanation": "Search strategy returned None",
                "skip_reasons": ["Search strategy failure"]
            },
            "skip_reasons": search_evaluation.get("skip_reasons", ["Search strategy failure"]) if search_evaluation else ["Search strategy returned None"],
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
        
        if skip_disciplinary:
            logger.info(f"Skipping disciplinary review for Employee='{employee_number}' as per configuration")
            disciplinary_evaluation = {
                "primary_name": individual_name or "Unknown",
                "actions": [],
                "due_diligence": {"status": "Skipped per configuration"}
            }
        else:
            disciplinary_evaluation = (
                facade.perform_disciplinary_review(first_name, last_name, employee_number)
                if first_name and last_name
                else {
                    "primary_name": individual_name or "Unknown",
                    "actions": [],
                    "due_diligence": {"status": "No name provided for search"}
                }
            )

        if skip_arbitration:
            logger.info(f"Skipping arbitration review for Employee='{employee_number}' as per configuration")
            arbitration_evaluation = {
                "primary_name": individual_name or "Unknown",
                "actions": [],
                "due_diligence": {"status": "Skipped per configuration"}
            }
        else:
            arbitration_evaluation = (
                facade.perform_arbitration_review(first_name, last_name, employee_number)
                if first_name and last_name
                else {
                    "primary_name": individual_name or "Unknown",
                    "actions": [],
                    "due_diligence": {"status": "No name provided for search"}
                }
            )

        if skip_regulatory:
            logger.info(f"Skipping regulatory review for Employee='{employee_number}' as per configuration")
            regulatory_evaluation = {
                "primary_name": individual_name or "Unknown",
                "actions": [],
                "due_diligence": {"status": "Skipped per configuration"}
            }
        else:
            regulatory_evaluation = (
                facade.perform_regulatory_review(first_name, last_name, employee_number)
                if first_name and last_name
                else {
                    "primary_name": individual_name or "Unknown",
                    "actions": [],
                    "due_diligence": {"status": "No name provided for search"}
                }
            )

        extracted_info = {
            "search_evaluation": {
                "source": search_evaluation.get("source", "Unknown"),
                "search_strategy": search_evaluation.get("search_strategy", "Unknown"),
                "crd_number": search_evaluation.get("crd_number"),
                "basic_result": search_evaluation.get("basic_result", {}),
                "detailed_result": search_evaluation.get("detailed_result"),
                "compliance": search_evaluation.get("compliance", False),
                "compliance_explanation": search_evaluation.get("compliance_explanation", "No compliance status provided.")
            },
            "individual": search_evaluation.get("basic_result", {}),
            "fetched_name": search_evaluation.get("basic_result", {}).get("fetched_name", ""),
            "other_names": search_evaluation.get("basic_result", {}).get("other_names", []),
            "bc_scope": search_evaluation.get("basic_result", {}).get("bc_scope", ""),
            "ia_scope": search_evaluation.get("basic_result", {}).get("ia_scope", ""),
            "exams": search_evaluation.get("detailed_result", {}).get("exams", []) if search_evaluation.get("detailed_result") is not None else [],
            "disclosures": search_evaluation.get("detailed_result", {}).get("disclosures", []) if search_evaluation.get("detailed_result") is not None else [],
            "disciplinary_evaluation": disciplinary_evaluation,
            "arbitration_evaluation": arbitration_evaluation,
            "regulatory_evaluation": regulatory_evaluation
        }

    reference_id = claim.get("reference_id", "UNKNOWN")
    builder = EvaluationReportBuilder(reference_id)
    director = EvaluationReportDirector(builder)
    report = director.construct_evaluation_report(claim, extracted_info)
    
    # Serialize the report to the cache
    if not facade.save_compliance_report(report, employee_number):
        logger.error(f"Failed to serialize report for employee_number={employee_number}")
    
    logger.info(f"Claim processing completed for {claim} with report built")
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