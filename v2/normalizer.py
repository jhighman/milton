"""
normalizer.py

This module provides functions to normalize raw data from various financial service sources
into consistent structures suitable for evaluation in the `evaluation_processor.py` module.
It handles:
- Individual records (e.g., IAPD, BrokerCheck)
- Disciplinary records (e.g., SEC, FINRA)
- Arbitration records (e.g., SEC, FINRA)
- Regulatory records (e.g., NFA)
"""

import json
import logging
from typing import Dict, Any, List, Optional
from evaluation_processor import evaluate_name, parse_name
from datetime import datetime

logger = logging.getLogger("normalizer")

class NormalizationError(Exception):
    """Exception raised for errors during normalization."""
    pass

def create_individual_record(
    data_source: str,
    basic_info: Optional[Dict[str, Any]],
    detailed_info: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Creates a unified "individual record" from BrokerCheck or IAPD data, handling both flat and nested structures.

    :param data_source: Either "BrokerCheck" or "IAPD".
    :param basic_info: JSON structure from basic search (e.g., name, scopes).
    :param detailed_info: JSON structure from detailed search (e.g., disclosures, employments).
    :return: A normalized dictionary with keys like:
        {
          "fetched_name": str,
          "other_names": list,
          "bc_scope": str,
          "ia_scope": str,
          "disclosures": list,
          "arbitrations": list,
          "exams": list,
          "employments": list,  # Consolidated employment section
          "crd_number": str
        }
    """
    extracted_info = {
        "fetched_name": "",
        "other_names": [],
        "bc_scope": "",
        "ia_scope": "",
        "disclosures": [],
        "arbitrations": [],
        "exams": [],
        "employments": [],  # Single section for all employments
        "crd_number": None
    }

    if data_source not in ["BrokerCheck", "IAPD"]:
        logger.error(f"Unknown data source '{data_source}'. Returning minimal extracted_info.")
        return extracted_info

    if not basic_info:
        logger.warning("No basic_info provided. Returning empty extracted_info.")
        return extracted_info

    hits_list = basic_info.get("hits", {}).get("hits", [])
    if not hits_list:
        logger.warning(f"{data_source}: basic_info had no hits. Returning mostly empty extracted_info.")
        return extracted_info

    individual = hits_list[0].get("_source", {})

    # Name fields
    first_name = individual.get('ind_firstname', '').upper()
    middle_name = individual.get('ind_middlename', '').upper()
    last_name = individual.get('ind_lastname', '').upper()
    fetched_name = " ".join(filter(None, [first_name, middle_name, last_name]))
    extracted_info["fetched_name"] = fetched_name
    extracted_info["other_names"] = individual.get("ind_other_names", [])

    # Scopes and CRD
    extracted_info["bc_scope"] = individual.get("ind_bc_scope", "")
    extracted_info["ia_scope"] = individual.get("ind_ia_scope", "")
    extracted_info["crd_number"] = str(individual.get("ind_source_id", "")) if individual.get("ind_source_id") else None

    if not detailed_info:
        logger.warning(f"No detailed_info provided for {data_source}. Skipping detailed fields.")
        return extracted_info

    # Helper function to normalize employments with firm_id as string
    def normalize_employment(emp: Dict[str, Any], status: str, emp_type: str) -> Dict[str, Any]:
        firm_id = emp.get("firmId")
        normalized = {
            "firm_id": str(firm_id) if firm_id is not None else None,  # Convert firm_id to string if present
            "firm_name": emp.get("firmName"),
            "registration_begin_date": emp.get("registrationBeginDate"),
            "branch_offices": [
                {
                    "street": office.get("street1"),
                    "city": office.get("city"),
                    "state": office.get("state"),
                    "zip_code": office.get("zipCode")
                }
                for office in emp.get("branchOfficeLocations", [])
            ],
            "status": status,  # "current" or "previous"
            "type": emp_type   # "registered_firm" or "employment_history"
        }
        if status == "previous":
            normalized["registration_end_date"] = emp.get("registrationEndDate")
        return normalized

    # 10-year cutoff for IAPD employment history
    current_date = datetime(2025, 3, 5)  # Current date per system info
    ten_years_ago = current_date.replace(year=current_date.year - 10)

    # Source-specific parsing
    if data_source == "BrokerCheck":
        # Handle flat structure
        if "disclosures" in detailed_info:
            logger.debug("Detected flat BrokerCheck structure in detailed_info")
            extracted_info["disclosures"] = detailed_info.get("disclosures", [])
            extracted_info["arbitrations"] = detailed_info.get("arbitrations", [])
            extracted_info["exams"] = (
                detailed_info.get("stateExamCategory", []) +
                detailed_info.get("principalExamCategory", []) +
                detailed_info.get("productExamCategory", [])
            )
            employments = []
            for emp in detailed_info.get("currentEmployments", []):
                employments.append(normalize_employment(emp, "current", "registered_firm"))
            for emp in detailed_info.get("previousEmployments", []):
                employments.append(normalize_employment(emp, "previous", "registered_firm"))
            extracted_info["employments"] = employments

        # Handle nested structure
        elif "hits" in detailed_info:
            logger.debug("Detected nested structure in BrokerCheck detailed_info")
            detailed_hits = detailed_info["hits"].get("hits", [])
            if detailed_hits:
                content_str = detailed_hits[0]["_source"].get("content", "")
                try:
                    content_json = json.loads(content_str)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse BrokerCheck 'content' JSON: {e}")
                    content_json = {}
                extracted_info["disclosures"] = content_json.get("disclosures", [])
                employments = []
                for emp in content_json.get("currentEmployments", []):
                    employments.append(normalize_employment(emp, "current", "registered_firm"))
                for emp in content_json.get("previousEmployments", []):
                    employments.append(normalize_employment(emp, "previous", "registered_firm"))
                extracted_info["employments"] = employments
            else:
                logger.info("BrokerCheck detailed_info had no hits. No employments extracted.")
        else:
            logger.info("No valid BrokerCheck detailed_info provided, skipping employments.")

    elif data_source == "IAPD":
        # Parse employments from basic_info iacontent (if nested)
        iacontent_str = individual.get("iacontent", "{}")
        try:
            iacontent_data = json.loads(iacontent_str)
        except json.JSONDecodeError as e:
            logger.warning(f"IAPD basic_info iacontent parse error: {e}")
            iacontent_data = {}

        employments = []
        for emp in iacontent_data.get("currentIAEmployments", []):
            employments.append(normalize_employment(emp, "current", "registered_firm"))

        # Handle flat structure
        if "disclosures" in detailed_info:
            logger.debug("Detected flat IAPD structure in detailed_info")
            extracted_info["disclosures"] = detailed_info.get("disclosures", [])
            extracted_info["arbitrations"] = detailed_info.get("arbitrations", [])
            extracted_info["exams"] = (
                detailed_info.get("stateExamCategory", []) +
                detailed_info.get("principalExamCategory", []) +
                detailed_info.get("productExamCategory", [])
            )
            employments = []
            for emp in detailed_info.get("currentIAEmployments", []):
                employments.append(normalize_employment(emp, "current", "registered_firm"))
            for emp in detailed_info.get("previousIAEmployments", []):
                employments.append(normalize_employment(emp, "previous", "registered_firm"))
            for emp in detailed_info.get("previousEmployments", []):
                normalized_emp = normalize_employment(emp, "previous", "employment_history")
                end_date_str = emp.get("registrationEndDate")
                if end_date_str:
                    try:
                        end_date = datetime.strptime(end_date_str, "%m/%d/%Y")
                        if end_date >= ten_years_ago:
                            employments.append(normalized_emp)
                    except ValueError:
                        logger.warning(f"Invalid registrationEndDate format: {end_date_str}")
                        employments.append(normalized_emp)  # Include if parsing fails
                else:
                    employments.append(normalized_emp)  # Include if no end date
            extracted_info["employments"] = employments

        # Handle nested structure
        elif "hits" in detailed_info:
            logger.debug("Detected nested structure in IAPD detailed_info")
            detailed_hits = detailed_info["hits"].get("hits", [])
            if detailed_hits:
                iapd_detailed_content_str = detailed_hits[0]["_source"].get("iacontent", "{}")
                try:
                    iapd_detailed_content_data = json.loads(iapd_detailed_content_str)
                except json.JSONDecodeError as e:
                    logger.warning(f"IAPD detailed_info iacontent parse error: {e}")
                    iapd_detailed_content_data = {}
                extracted_info["exams"] = (
                    iapd_detailed_content_data.get("stateExamCategory", []) +
                    iapd_detailed_content_data.get("principalExamCategory", []) +
                    iapd_detailed_content_data.get("productExamCategory", [])
                )
                extracted_info["disclosures"] = iapd_detailed_content_data.get("disclosures", [])
                extracted_info["arbitrations"] = iapd_detailed_content_data.get("arbitrations", [])
                employments = []
                for emp in iapd_detailed_content_data.get("currentIAEmployments", []):
                    employments.append(normalize_employment(emp, "current", "registered_firm"))
                for emp in iapd_detailed_content_data.get("previousIAEmployments", []):
                    employments.append(normalize_employment(emp, "previous", "registered_firm"))
                for emp in iapd_detailed_content_data.get("previousEmployments", []):
                    normalized_emp = normalize_employment(emp, "previous", "employment_history")
                    end_date_str = emp.get("registrationEndDate")
                    if end_date_str:
                        try:
                            end_date = datetime.strptime(end_date_str, "%m/%d/%Y")
                            if end_date >= ten_years_ago:
                                employments.append(normalized_emp)
                        except ValueError:
                            logger.warning(f"Invalid registrationEndDate format: {end_date_str}")
                            employments.append(normalized_emp)
                    else:
                        employments.append(normalized_emp)
                extracted_info["employments"] = employments
            else:
                logger.info("IAPD detailed_info had no hits. Using basic_info's iacontent.")
                extracted_info["disclosures"] = iacontent_data.get("disclosures", [])
                extracted_info["arbitrations"] = iacontent_data.get("arbitrations", [])
                extracted_info["employments"] = employments
        else:
            extracted_info["disclosures"] = iacontent_data.get("disclosures", [])
            extracted_info["arbitrations"] = iacontent_data.get("arbitrations", [])
            extracted_info["employments"] = employments

    return extracted_info

def create_disciplinary_record(data_source: str, data: Any, searched_name: str) -> Dict[str, Any]:
    result = {
        "actions": [],
        "raw_data": [data] if not isinstance(data, list) else data,
        "name_scores": {}
    }
    logger.debug(f"Normalizing {data_source} disciplinary data for {searched_name}")

    if isinstance(data, dict) and "error" in data:
        logger.warning(f"Error in {data_source} data: {data['error']}")
        return result

    raw_results = data.get("result", []) if isinstance(data, dict) else data
    if raw_results == "No Results Found" or not raw_results:
        logger.info(f"No results found in {data_source} for {searched_name}")
        return result

    if not isinstance(raw_results, list):
        logger.warning(f"Unexpected result format in {data_source}: {raw_results}")
        return result

    searched_name_dict = parse_name(searched_name)
    for record in raw_results:
        if not isinstance(record, dict):
            logger.warning(f"Skipping malformed record in {data_source}: {record}")
            continue

        normalized_record = {}
        respondent_name = None
        if data_source == "FINRA_Disciplinary":
            normalized_record["case_id"] = record.get("Case ID", "Unknown")
            normalized_record["date"] = record.get("Action Date", "Unknown")
            normalized_record["details"] = {
                "action_type": record.get("Action Type", ""),
                "firms_individuals": record.get("Firms/Individuals", ""),
                "description": record.get("Description", "")
            }
            respondent_name = record.get("Firms/Individuals", "")
        elif data_source == "SEC_Disciplinary":
            normalized_record["case_id"] = record.get("Case ID", "Unknown")
            normalized_record["date"] = record.get("Action Date", "Unknown")
            normalized_record["details"] = {
                "action_type": "Disciplinary",
                "firms_individuals": record.get("Name", ""),
                "description": record.get("Description", "")
            }
            respondent_name = record.get("Name", "")

        if respondent_name:
            name_eval, _ = evaluate_name(searched_name, respondent_name, [])
            score = name_eval["best_match"]["score"]
            result["name_scores"][respondent_name] = score
            if score >= 80.0:
                result["actions"].append(normalized_record)
                logger.debug(f"Matched disciplinary record {normalized_record['case_id']} with {respondent_name} (score: {score})")
        else:
            logger.debug(f"Skipped disciplinary record {normalized_record['case_id']} - no respondent name")

    return result

def create_arbitration_record(data_source: str, data: Any, searched_name: str) -> Dict[str, Any]:
    result = {
        "actions": [],
        "raw_data": [data] if not isinstance(data, list) else data,
        "name_scores": {}
    }
    logger.debug(f"Normalizing {data_source} arbitration data for {searched_name}")

    if isinstance(data, dict) and "error" in data:
        logger.warning(f"Error in {data_source} data: {data['error']}")
        return result

    if isinstance(data, dict):
        raw_results = data.get("result", [])
    elif isinstance(data, list):
        logger.debug(f"{data_source} data is a raw list, treating as results")
        raw_results = data
    else:
        logger.warning(f"Unexpected {data_source} data format: {type(data)}")
        return result

    if raw_results == "No Results Found" or not raw_results:
        logger.info(f"No results found in {data_source} for {searched_name}")
        return result

    if not isinstance(raw_results, list):
        logger.warning(f"Unexpected {data_source} result format: {raw_results}")
        return result

    searched_name_dict = parse_name(searched_name)
    for record in raw_results:
        if not isinstance(record, dict):
            logger.warning(f"Skipping malformed record in {data_source}: {record}")
            continue

        normalized_record = {}
        respondent_names = []

        if data_source == "FINRA_Arbitration":
            normalized_record["case_id"] = record.get("Case Summary", {}).get("Case Number", record.get("Award Document", "Unknown"))
            normalized_record["date"] = record.get("Date of Award", "Unknown")
            normalized_record["details"] = {
                "action_type": "Award" if "Award" in record.get("Document Type", "") else "Unknown",
                "firms_individuals": record.get("Case Summary", {}).get("Respondent(s):", ""),
                "description": f"Case {normalized_record['case_id']} closed with outcome: {'Award against individual' if 'Award' in record.get('Document Type', '') else 'Unknown'}",
                "award_document": record.get("Award Document"),
                "pdf_url": record.get("PDF URL"),
                "case_summary": record.get("Case Summary", {}),
                "forum": record.get("Forum")
            }
            respondents_str = record.get("Case Summary", {}).get("Respondent(s):", "")
            if respondents_str:
                respondent_names = [name.strip() for name in respondents_str.split(",") if name.strip()]
            else:
                logger.debug(f"No respondents found in {normalized_record['case_id']}, skipping match")
                continue
        elif data_source == "SEC_Arbitration":
            normalized_record["case_id"] = record.get("Enforcement Action", "Unknown")
            normalized_record["date"] = record.get("Date Filed", "Unknown")
            normalized_record["details"] = {
                "action_type": "Award against individual",
                "firms_individuals": searched_name,
                "description": f"Case {normalized_record['case_id']} closed with outcome: Award against individual",
                "enforcement_action": record.get("Enforcement Action"),
                "documents": record.get("Documents", [])
            }
            respondent_names = []

        for respondent_name in respondent_names:
            name_eval, _ = evaluate_name(searched_name, respondent_name, [])
            score = name_eval["best_match"]["score"]
            result["name_scores"][respondent_name] = score
            if score >= 80.0:
                normalized_record["matched_name"] = respondent_name
                result["actions"].append(normalized_record)
                logger.debug(f"Matched arbitration record {normalized_record['case_id']} with {respondent_name} (score: {score})")
                break
        if respondent_names and not any(a["case_id"] == normalized_record["case_id"] for a in result["actions"]):
            logger.debug(f"Filtered arbitration record {normalized_record['case_id']} - no respondent matched {searched_name} (best score: {max([result['name_scores'].get(r, 0) for r in respondent_names], default=0)})")

    return result

def create_regulatory_record(data_source: str, data: Any, searched_name: str) -> Dict[str, Any]:
    """Normalize NFA regulatory data into a consistent structure."""
    result = {
        "actions": [],
        "raw_data": [data] if not isinstance(data, list) else data,
        "name_scores": {}
    }
    logger.debug(f"Normalizing {data_source} regulatory data for {searched_name}")

    if isinstance(data, dict) and "error" in data:
        logger.warning(f"Error in {data_source} data: {data['error']}")
        return result

    raw_results = data.get("result", []) if isinstance(data, dict) else data
    if raw_results == "No Results Found" or not raw_results:
        logger.info(f"No results found in {data_source} for {searched_name}")
        return result

    if not isinstance(raw_results, list):
        logger.warning(f"Unexpected {data_source} result format: {raw_results}")
        return result

    searched_name_dict = parse_name(searched_name)
    for record in raw_results:
        if not isinstance(record, dict):
            logger.warning(f"Skipping malformed record in {data_source}: {record}")
            continue

        normalized_record = {
            "case_id": record.get("NFA ID", "Unknown"),
            "date": "Unknown",  # NFA data lacks a specific date field
            "details": {
                "action_type": "Regulatory" if record.get("Regulatory Actions") == "Yes" else "Registration",
                "firms_individuals": record.get("Name", ""),
                "description": (
                    f"NFA ID {record.get('NFA ID', 'Unknown')} status: {record.get('Current NFA Membership Status', 'Unknown')}, "
                    f"Registration: {record.get('Current Registration Types', '-')}, "
                    f"Regulatory Actions: {record.get('Regulatory Actions', 'No')}"
                ),
                "firm_name": record.get("Firm", ""),
                "details_available": record.get("Details Available", "No")
            }
        }
        respondent_name = record.get("Name", "").strip()

        if respondent_name:
            name_eval, _ = evaluate_name(searched_name, respondent_name, [])
            score = name_eval["best_match"]["score"]
            result["name_scores"][respondent_name] = score
            if score >= 80.0 and record.get("Regulatory Actions") == "Yes":
                result["actions"].append(normalized_record)
                logger.debug(f"Matched regulatory record {normalized_record['case_id']} with {respondent_name} (score: {score})")
        else:
            logger.debug(f"Skipped regulatory record {normalized_record['case_id']} - no respondent name")

    return result