"""
normalizer.py

This module provides functions to normalize raw data from various financial service sources
into consistent structures suitable for evaluation in the `evaluation_processor.py` module.
It handles:
- Individual records (e.g., IAPD, BrokerCheck)
- Disciplinary records (e.g., SEC, FINRA)
- Arbitration records (e.g., SEC, FINRA)

Normalization ensures that downstream evaluation logic operates on abstract, uniform data,
independent of the specifics of the originating service.
"""

import json
import logging
from typing import Dict, Any, List, Optional
from evaluation_processor import evaluate_name, parse_name

logger = logging.getLogger(__name__)

class NormalizationError(Exception):
    """Exception raised for errors during normalization."""
    pass

def create_individual_record(
    data_source: str,
    basic_info: Optional[Dict[str, Any]],
    detailed_info: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Creates a unified "individual record" from BrokerCheck or IAPD data.

    :param data_source: A string indicating the source of data. Accepted values:
        - "BrokerCheck": For FINRA BrokerCheck data
        - "IAPD": For SEC IAPD data
    :param basic_info: The JSON structure returned by your "basic" search function.
                       Typically has partial fields (like name, CRD, etc.).
    :param detailed_info: The JSON structure returned by your "detailed" search function.
                          Typically holds disclosures, exam details, or other extended info.
                          Optional, defaults to None.
    :return: A dictionary (extracted_info) with normalized keys:
        {
          "fetched_name": str,
          "other_names": list,
          "bc_scope": str,
          "ia_scope": str,
          "disclosures": list,
          "arbitrations": list,
          "exams": list,
          "current_ia_employments": list,
          ...
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
        "current_ia_employments": [],
        "crd_number": None
    }

    # Return empty info for invalid data sources
    if data_source not in ["BrokerCheck", "IAPD"]:
        return extracted_info

    if not basic_info:
        logger.warning("No basic_info provided. Returning empty extracted_info.")
        return extracted_info

    hits_list = basic_info.get("hits", {}).get("hits", [])
    if not hits_list:
        logger.warning(f"{data_source}: basic_info had no hits. Returning mostly empty extracted_info.")
        return extracted_info

    individual = hits_list[0].get("_source", {})

    # Always convert names to uppercase
    first_name = individual.get('ind_firstname', '').upper()
    middle_name = individual.get('ind_middlename', '').upper()
    last_name = individual.get('ind_lastname', '').upper()
    fetched_name = " ".join(filter(None, [first_name, middle_name, last_name]))
    extracted_info["fetched_name"] = fetched_name
    extracted_info["other_names"] = individual.get("ind_other_names", [])

    extracted_info["bc_scope"] = individual.get("ind_bc_scope", "")
    extracted_info["ia_scope"] = individual.get("ind_ia_scope", "")
    extracted_info["crd_number"] = str(individual.get("ind_source_id", ""))

    if data_source == "BrokerCheck":
        # BrokerCheck data should never populate current_ia_employments
        extracted_info["current_ia_employments"] = []
        
        if detailed_info and "hits" in detailed_info:
            detailed_hits = detailed_info["hits"].get("hits", [])
            if detailed_hits:
                bc_content_str = detailed_hits[0]["_source"].get("content", "{}")
                try:
                    bc_content_data = json.loads(bc_content_str)
                    extracted_info["disclosures"] = bc_content_data.get("disclosures", [])
                    extracted_info["arbitrations"] = bc_content_data.get("arbitrations", [])

                    # Combine exam categories
                    state_exams = bc_content_data.get("stateExamCategory", [])
                    principal_exams = bc_content_data.get("principalExamCategory", [])
                    product_exams = bc_content_data.get("productExamCategory", [])
                    extracted_info["exams"] = state_exams + principal_exams + product_exams

                except json.JSONDecodeError as e:
                    logger.warning(f"BrokerCheck detailed_info content parse error: {e}")

    elif data_source == "IAPD":
        # Extract current IA employments from basic info
        current_employments = []
        for emp in individual.get("ind_ia_current_employments", []):
            firm_id = emp.get("firm_id")
            # Keep firm_id as string if present
            if firm_id is not None:
                firm_id = str(firm_id)

            current_employments.append({
                "firm_id": firm_id,
                "firm_name": emp.get("firm_name"),
                "registration_begin_date": None,  # Will be updated from detailed info if available
                "branch_offices": [{
                    "street": None,  # Basic info doesn't have street
                    "city": emp.get("branch_city"),
                    "state": emp.get("branch_state"),
                    "zip_code": emp.get("branch_zip")
                }]
            })

        # Update with detailed info if available
        if detailed_info and "hits" in detailed_info:
            detailed_hits = detailed_info["hits"].get("hits", [])
            if detailed_hits:
                iapd_content_str = detailed_hits[0]["_source"].get("iacontent", "{}")
                try:
                    iapd_content_data = json.loads(iapd_content_str)
                    
                    # Update employments with detailed info
                    for emp_idx, emp in enumerate(iapd_content_data.get("currentIAEmployments", [])):
                        if emp_idx < len(current_employments):
                            # Add registration begin date
                            current_employments[emp_idx]["registration_begin_date"] = emp.get("registrationBeginDate")

                            # Update branch offices
                            branch_offices = emp.get("branchOfficeLocations", [])
                            if branch_offices:
                                current_employments[emp_idx]["branch_offices"] = [{
                                    "street": office.get("street1"),
                                    "city": office.get("city"),
                                    "state": office.get("state"),
                                    "zip_code": office.get("zipCode")
                                } for office in branch_offices]

                    # Combine exam categories
                    state_exams = iapd_content_data.get("stateExamCategory", [])
                    principal_exams = iapd_content_data.get("principalExamCategory", [])
                    product_exams = iapd_content_data.get("productExamCategory", [])
                    extracted_info["exams"] = state_exams + principal_exams + product_exams

                    extracted_info["disclosures"] = iapd_content_data.get("disclosures", [])
                    extracted_info["arbitrations"] = iapd_content_data.get("arbitrations", [])
                except json.JSONDecodeError as e:
                    logger.warning(f"IAPD detailed_info content parse error: {e}")

        extracted_info["current_ia_employments"] = current_employments

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

        if respondent_name:  # Only process if respondent_name exists
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

        # Score all respondent names, even if no match
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