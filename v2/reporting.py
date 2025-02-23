from typing import Dict, Any
from datetime import datetime
from collections import OrderedDict
from evaluation_report_builder import EvaluationReportBuilder
from evaluation_report_director import EvaluationReportDirector

def generate_evaluation_report(claim: Dict[str, Any], search_result: Dict[str, Any], reference_id: str) -> Dict[str, Any]:
    """Generate a detailed evaluation report from claim and search results."""
    builder = EvaluationReportBuilder(reference_id=reference_id)
    director = EvaluationReportDirector(builder)

    # Extract data from search_result
    basic_result = search_result.get("basic_result", {})
    detailed_result = search_result.get("detailed_result", {})
    source = search_result.get("source", "Unknown")
    search_strategy = search_result.get("search_strategy", "unknown")
    crd_number = search_result.get("crd_number") or basic_result.get("crd_number", basic_result.get("ind_source_id", None))

    # Determine search outcome and basic compliance info
    search_outcome = "Record found" if basic_result and basic_result.get("fetched_name") else "No records found"
    compliance_explanation = "Record found" if search_outcome == "Record found" else "No records found in search"

    # Build individual data from basic_result
    individual = {}
    if basic_result and "fetched_name" in basic_result:
        individual = {
            "ind_source_id": basic_result.get("crd_number", basic_result.get("ind_source_id", "")),
            "ind_firstname": basic_result.get("ind_firstname", ""),
            "ind_middlename": basic_result.get("ind_middlename", ""),
            "ind_lastname": basic_result.get("ind_lastname", ""),
            "ind_other_names": basic_result.get("other_names", []),
            "ind_bc_scope": basic_result.get("bc_scope", ""),
            "ind_ia_scope": basic_result.get("ia_scope", ""),
            "ind_bc_disclosure_fl": basic_result.get("ind_bc_disclosure_fl", ""),
            "ind_approved_finra_registration_count": basic_result.get("ind_approved_finra_registration_count", 0),
            "ind_employments_count": basic_result.get("ind_employments_count", 0),
            "ind_industry_cal_date": basic_result.get("ind_industry_cal_date", ""),
            "ind_current_employments": basic_result.get("ind_current_employments", basic_result.get("current_ia_employments", []))
        }

    # Construct search_evaluation
    search_evaluation = OrderedDict([
        ("compliance", None),  # Defer to final_evaluation
        ("compliance_explanation", compliance_explanation),
        ("search_strategy", search_strategy),
        ("search_outcome", search_outcome),
        ("search_date", datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
        ("crd_number", crd_number),
        ("data_source", source),
        ("individual", individual),
        ("detailed_info", detailed_result if detailed_result else {})
    ])
    if search_outcome == "No records found":
        search_evaluation["alerts"] = [{
            "alert_type": "Search Failure",
            "severity": "Medium",
            "metadata": {"source": source},
            "description": "No records found in search"
        }]
    else:
        search_evaluation["alerts"] = []

    # Prepare extracted_info for the director
    extracted_info = {
        "search_evaluation": search_evaluation,
        "fetched_name": basic_result.get("fetched_name", "") if basic_result else "",
        "other_names": basic_result.get("other_names", []) if basic_result else [],
        "bc_scope": basic_result.get("bc_scope", "") if basic_result else "",
        "ia_scope": basic_result.get("ia_scope", "") if basic_result else "",
        "exams": basic_result.get("exams", []) if basic_result else [],
        "disclosures": basic_result.get("disclosures", []) if basic_result else [],
        "arbitrations": basic_result.get("arbitrations", []) if basic_result else [],
        "disciplinary_records": [],  # Placeholder until facade provides this
        "individual": individual if basic_result else {}
    }

    # Construct and return the report
    return director.construct_evaluation_report(claim, extracted_info)