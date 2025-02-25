from typing import Dict, Any, Callable
import logging
import json
from services import FinancialServicesFacade
from evaluation_report_builder import EvaluationReportBuilder
from evaluation_report_director import EvaluationReportDirector

# Logger setup
logger = logging.getLogger("business")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s'))
logger.addHandler(handler)

# Existing search strategy functions (unchanged)
def determine_search_strategy(claim: Dict[str, Any]) -> Callable[[Dict[str, Any], FinancialServicesFacade, str], Dict[str, Any]]:
    individual_name = claim.get("individual_name", "")
    crd_number = claim.get("crd_number", "")
    organization_crd_number = claim.get("organization_crd_number", claim.get("organization_crd", ""))
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

# Existing search functions (unchanged)
def search_with_both_crds(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    crd_number = claim.get("crd_number", "")
    logger.info(f"Searching SEC IAPD with crd_number='{crd_number}', Employee='{employee_number}'")
    basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
    detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
    return {"source": "SEC_IAPD", "basic_result": basic_result, "detailed_result": detailed_result, "search_strategy": "search_with_both_crds", "crd_number": crd_number}

def search_with_crd_and_org_name(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    crd_number = claim.get("crd_number", "")
    org_name = claim.get("organization_name", "")
    logger.info(f"Searching with crd_number='{crd_number}', organization_name='{org_name}', Employee='{employee_number}'")
    broker_result = facade.search_finra_brokercheck_individual(crd_number, employee_number)
    if broker_result and broker_result.get("fetched_name") != "":
        detailed_result = facade.search_finra_brokercheck_detailed(crd_number, employee_number)
        return {"source": "BrokerCheck", "basic_result": broker_result, "detailed_result": detailed_result, "search_strategy": "search_with_crd_and_org_name", "crd_number": crd_number}
    if org_name.strip():
        org_crd_number = facade.get_organization_crd(org_name)
        if org_crd_number is None or org_crd_number == "NOT_FOUND":
            logger.warning("Unknown organization by lookup")
            return {"source": "Entity_Search", "basic_result": None, "detailed_result": None, "search_strategy": "search_with_crd_and_org_name", "crd_number": crd_number}
        logger.info(f"No BrokerCheck hits, searching SEC IAPD with crd_number='{crd_number}'")
        basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
        detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
        return {"source": "SEC_IAPD", "basic_result": basic_result, "detailed_result": detailed_result, "search_strategy": "search_with_crd_and_org_name", "crd_number": crd_number}
    logger.info(f"No BrokerCheck hits, searching SEC IAPD with crd_number='{crd_number}'")
    basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
    detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
    return {"source": "SEC_IAPD", "basic_result": basic_result, "detailed_result": detailed_result, "search_strategy": "search_with_crd_and_org_name", "crd_number": crd_number}

def search_with_crd_only(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    crd_number = claim.get("crd_number", "")
    logger.info(f"Searching with crd_number only='{crd_number}', Employee='{employee_number}'")
    broker_result = facade.search_finra_brokercheck_individual(crd_number, employee_number)
    if broker_result and broker_result.get("fetched_name") != "":
        detailed_result = facade.search_finra_brokercheck_detailed(crd_number, employee_number)
        return {"source": "BrokerCheck", "basic_result": broker_result, "detailed_result": detailed_result, "search_strategy": "search_with_crd_only", "crd_number": crd_number}
    logger.info(f"No BrokerCheck hits => searching SEC IAPD with crd_number='{crd_number}'")
    basic_result = facade.search_sec_iapd_individual(crd_number, employee_number)
    detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if basic_result else None
    return {"source": "SEC_IAPD", "basic_result": basic_result, "detailed_result": detailed_result, "search_strategy": "search_with_crd_only", "crd_number": crd_number}

def search_with_entity(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    organization_crd_number = claim.get("organization_crd_number", claim.get("organization_crd", ""))
    logger.info(f"Detected entity search with organization_crd_number='{organization_crd_number}', Employee='{employee_number}'")
    logger.warning("Entity search using organization_crd_number is not supported at this time.")
    return {"source": "Entity_Search", "basic_result": None, "detailed_result": None, "search_strategy": "search_with_entity", "crd_number": None}

def search_with_org_name_only(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    organization_name = claim.get("organization_name", "")
    logger.info(f"Detected org_name-only search with organization_name='{organization_name}', Employee='{employee_number}'")
    logger.warning("Entity search using organization_name is not supported at this time.")
    return {"source": "Entity_Search", "basic_result": None, "detailed_result": None, "search_strategy": "search_with_org_name_only", "crd_number": None}

def search_default(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    logger.info(f"No usable fields => defaulting, Employee='{employee_number}'")
    logger.warning("Insufficient identifiers to perform search")
    return {"source": "Default", "basic_result": None, "detailed_result": None, "search_strategy": "search_default", "crd_number": None}

def search_with_correlated(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str) -> Dict[str, Any]:
    individual_name = claim.get("individual_name", "")
    organization_crd_number = claim.get("organization_crd_number", claim.get("organization_crd", ""))
    logger.info(f"Searching SEC IAPD with individual_name='{individual_name}', organization_crd_number='{organization_crd_number}', Employee='{employee_number}'")
    basic_result = facade.search_sec_iapd_correlated(individual_name, organization_crd_number, employee_number)
    crd_number = basic_result.get("crd_number", None) if basic_result else None
    detailed_result = facade.search_sec_iapd_detailed(crd_number, employee_number) if crd_number else None
    return {"source": "SEC_IAPD", "basic_result": basic_result, "detailed_result": detailed_result, "search_strategy": "search_with_correlated", "crd_number": crd_number}

def process_claim(
    claim: Dict[str, Any],
    facade: FinancialServicesFacade,
    employee_number: str = None
) -> Dict[str, Any]:
    """Process a claim by collecting data and delegating report building to EvaluationReportDirector."""
    logger.info(f"Starting claim processing for {claim}, Employee='{employee_number}'")
    
    # Default employee_number if not provided in claim or args
    employee_number = claim.get("employee_number", employee_number or "EMP_DEFAULT")
    
    # Step 1: Collect primary search data
    strategy_func = determine_search_strategy(claim)
    logger.debug(f"Selected primary strategy: {strategy_func.__name__}")
    search_evaluation = strategy_func(claim, facade, employee_number)
    if search_evaluation is None or (search_evaluation.get("basic_result") is None and search_evaluation.get("detailed_result") is None):
        logger.warning(f"Primary strategy {strategy_func.__name__} returned no usable data")

    # Step 2: Perform disciplinary review
    first_name = claim.get("first_name", "")
    last_name = claim.get("last_name", "")
    individual_name = claim.get("individual_name", "")
    if not (first_name and last_name) and individual_name:
        first_name, *last_name_parts = individual_name.split()
        last_name = " ".join(last_name_parts) if last_name_parts else ""
    
    disciplinary_evaluation = (
        facade.perform_disciplinary_review(first_name, last_name, employee_number)
        if first_name and last_name
        else {
            "primary_name": individual_name or "Unknown",
            "disciplinary_actions": [],
            "due_diligence": {"status": "No name provided for search"}
        }
    )

    # Build extracted_info for the director
    extracted_info = {
        "search_evaluation": {
            "source": search_evaluation.get("source", "Unknown"),
            "search_strategy": search_evaluation.get("search_strategy", "Unknown"),
            "crd_number": search_evaluation.get("crd_number"),
            "basic_result": search_evaluation.get("basic_result", {}),
            "detailed_result": search_evaluation.get("detailed_result")
        },
        "individual": search_evaluation.get("basic_result", {}),
        "fetched_name": search_evaluation.get("basic_result", {}).get("fetched_name", ""),
        "other_names": search_evaluation.get("basic_result", {}).get("other_names", []),
        "bc_scope": search_evaluation.get("basic_result", {}).get("bc_scope", ""),
        "ia_scope": search_evaluation.get("basic_result", {}).get("ia_scope", ""),
        "exams": search_evaluation.get("detailed_result", {}).get("exams", []),
        "disclosures": search_evaluation.get("detailed_result", {}).get("disclosures", []),
        "arbitrations": search_evaluation.get("detailed_result", {}).get("arbitrations", []),
        "disciplinary_evaluation": disciplinary_evaluation  # Pass full disciplinary_evaluation
    }

    # Delegate report building with reference_id
    reference_id = claim.get("reference_id", "UNKNOWN")
    builder = EvaluationReportBuilder(reference_id)
    director = EvaluationReportDirector(builder)
    report = director.construct_evaluation_report(claim, extracted_info)
    
    logger.info(f"Claim processing completed for {claim} with report built")
    return report

if __name__ == "__main__":
    facade = FinancialServicesFacade()

    def run_process_claim(claim: Dict[str, Any], facade: FinancialServicesFacade, employee_number: str = None):
        result = process_claim(claim, facade, employee_number)
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
            run_process_claim(claim, facade)
        elif choice == "4":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please enter 1, 2, 3, or 4.")