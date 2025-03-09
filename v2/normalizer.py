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
from evaluation_processor import evaluate_name, MatchThreshold  # Use MatchThreshold from evaluation_processor
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
    Note: 'fetched_name' and 'other_names' are designed to be passed to evaluate_name for matching
    against an expected name, producing names_found, name_scores, exact_match_found, and status.
    """
    logger.debug(f"Entering create_individual_record for {data_source} with basic_info={json.dumps(basic_info, indent=2)}")
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
    logger.debug(f"Extracted individual data: {json.dumps(individual, indent=2)}")

    # Name fields
    first_name = individual.get('ind_firstname', '').upper()
    middle_name = individual.get('ind_middlename', '').upper()
    last_name = individual.get('ind_lastname', '').upper()
    fetched_name = " ".join(filter(None, [first_name, middle_name, last_name]))
    extracted_info["fetched_name"] = fetched_name
    extracted_info["other_names"] = individual.get("ind_other_names", [])
    logger.debug(f"Parsed fetched_name: '{fetched_name}', other_names: {extracted_info['other_names']}")

    # Scopes and CRD
    extracted_info["bc_scope"] = individual.get("ind_bc_scope", "")
    extracted_info["ia_scope"] = individual.get("ind_ia_scope", "")
    extracted_info["crd_number"] = str(individual.get("ind_source_id", "")) if individual.get("ind_source_id") else None
    logger.debug(f"Scopes: bc_scope='{extracted_info['bc_scope']}', ia_scope='{extracted_info['ia_scope']}', crd_number='{extracted_info['crd_number']}'")

    if not detailed_info:
        logger.warning(f"No detailed_info provided for {data_source}. Skipping detailed fields.")
        return extracted_info

    logger.debug(f"Processing detailed_info: {json.dumps(detailed_info, indent=2)}")

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
        logger.debug(f"Normalized employment: {json.dumps(normalized, indent=2)}")
        return normalized

    # 10-year cutoff for IAPD employment history
    current_date = datetime(2025, 3, 9)  # Updated to match current date from your context
    ten_years_ago = current_date.replace(year=current_date.year - 10)
    logger.debug(f"Using 10-year cutoff: {ten_years_ago}")

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
            logger.debug(f"Flat structure employments: {len(employments)} entries")

        # Handle nested structure
        elif "hits" in detailed_info:
            logger.debug("Detected nested structure in BrokerCheck detailed_info")
            detailed_hits = detailed_info["hits"].get("hits", [])
            if detailed_hits:
                content_str = detailed_hits[0]["_source"].get("content", "")
                try:
                    content_json = json.loads(content_str)
                    logger.debug(f"Parsed nested content: {json.dumps(content_json, indent=2)}")
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
                logger.debug(f"Nested structure employments: {len(employments)} entries")
            else:
                logger.info("BrokerCheck detailed_info had no hits. No employments extracted.")
        else:
            logger.info("No valid BrokerCheck detailed_info provided, skipping employments.")

    elif data_source == "IAPD":
        # Parse employments from basic_info iacontent (if nested)
        iacontent_str = individual.get("iacontent", "{}")
        try:
            iacontent_data = json.loads(iacontent_str)
            logger.debug(f"Parsed iacontent from basic_info: {json.dumps(iacontent_data, indent=2)}")
        except json.JSONDecodeError as e:
            logger.warning(f"IAPD basic_info iacontent parse error: {e}")
            iacontent_data = {}

        employments = []
        for emp in iacontent_data.get("currentIAEmployments", []):
            employments.append(normalize_employment(emp, "current", "registered_firm"))
        logger.debug(f"Basic_info iacontent employments: {len(employments)} entries")

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
                            logger.debug(f"Included previous employment ending {end_date_str} (within 10 years)")
                        else:
                            logger.debug(f"Excluded previous employment ending {end_date_str} (older than 10 years)")
                    except ValueError:
                        logger.warning(f"Invalid registrationEndDate format: {end_date_str}")
                        employments.append(normalized_emp)  # Include if parsing fails
                else:
                    employments.append(normalized_emp)  # Include if no end date
            extracted_info["employments"] = employments
            logger.debug(f"Flat structure employments: {len(employments)} entries")

        # Handle nested structure
        elif "hits" in detailed_info:
            logger.debug("Detected nested structure in IAPD detailed_info")
            detailed_hits = detailed_info["hits"].get("hits", [])
            if detailed_hits:
                iapd_detailed_content_str = detailed_hits[0]["_source"].get("iacontent", "{}")
                try:
                    iapd_detailed_content_data = json.loads(iapd_detailed_content_str)
                    logger.debug(f"Parsed nested iacontent: {json.dumps(iapd_detailed_content_data, indent=2)}")
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
                                logger.debug(f"Included previous employment ending {end_date_str} (within 10 years)")
                            else:
                                logger.debug(f"Excluded previous employment ending {end_date_str} (older than 10 years)")
                        except ValueError:
                            logger.warning(f"Invalid registrationEndDate format: {end_date_str}")
                            employments.append(normalized_emp)
                    else:
                        employments.append(normalized_emp)
                extracted_info["employments"] = employments
                logger.debug(f"Nested structure employments: {len(employments)} entries")
            else:
                logger.info("IAPD detailed_info had no hits. Using basic_info's iacontent.")
                extracted_info["disclosures"] = iacontent_data.get("disclosures", [])
                extracted_info["arbitrations"] = iacontent_data.get("arbitrations", [])
                extracted_info["employments"] = employments
        else:
            extracted_info["disclosures"] = iacontent_data.get("disclosures", [])
            extracted_info["arbitrations"] = iacontent_data.get("arbitrations", [])
            extracted_info["employments"] = employments
            logger.debug(f"Defaulted to iacontent employments: {len(employments)} entries")

    logger.debug(f"Normalized individual record from {data_source}: {json.dumps(extracted_info, indent=2)}")
    return extracted_info

def create_disciplinary_record(
    data_source: str,
    data: Any,
    searched_name: str,
    threshold: MatchThreshold = MatchThreshold.STRICT
) -> Dict[str, Any]:
    """
    Normalize disciplinary data from SEC or FINRA sources into a consistent structure.
    Includes due_diligence section aligning with FINRA name handling convention.

    :param data_source: Source of the data ("FINRA_Disciplinary" or "SEC_Disciplinary").
    :param data: Raw data from the source.
    :param searched_name: Name to match against respondent names.
    :param threshold: MatchThreshold enum value defining the minimum score for a match (default: STRICT).
    :return: Normalized dictionary with actions and due_diligence.
    """
    logger.debug(f"Entering create_disciplinary_record for {data_source} with searched_name='{searched_name}', threshold={threshold.name}")
    result = {
        "actions": [],
        "due_diligence": {
            "searched_name": searched_name,
            "records_found": 0,
            "records_filtered": 0,
            "names_found": [],
            "name_scores": {},
            "exact_match_found": False,
            "status": "No records found"
        },
        "raw_data": [data] if not isinstance(data, list) else data
    }
    logger.debug(f"Raw data: {json.dumps(result['raw_data'], indent=2)}")

    if isinstance(data, dict) and "error" in data:
        logger.warning(f"Error in {data_source} data: {data['error']}")
        return result

    # Handle nested structure: extract "result" if present
    raw_results = data
    if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict) and "result" in data[0]:
        raw_results = data[0]["result"]
        logger.debug(f"{data_source} data is a nested list, using 'result' key")
    elif isinstance(data, dict) and "result" in data:
        raw_results = data["result"]
    elif not isinstance(data, list):
        logger.warning(f"Unexpected {data_source} data format: {type(data)} - {data}")
        return result

    if raw_results == "No Results Found" or not raw_results:
        logger.info(f"No results found in {data_source} for {searched_name}")
        return result

    if not isinstance(raw_results, list):
        logger.warning(f"Unexpected {data_source} result format: {raw_results}")
        return result

    due_diligence = result["due_diligence"]
    due_diligence["records_found"] = len(raw_results)
    logger.debug(f"Found {due_diligence['records_found']} records")

    for record in raw_results:
        if not isinstance(record, dict):
            logger.warning(f"Skipping malformed record in {data_source}: {record}")
            due_diligence["records_filtered"] += 1
            continue

        logger.debug(f"Processing record: {json.dumps(record, indent=2)}")
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

        logger.debug(f"Extracted respondent_name: '{respondent_name}'")
        if respondent_name:
            try:
                name_eval, _ = evaluate_name(searched_name, respondent_name, [], score_threshold=threshold.value)
                dd = name_eval["due_diligence"]
                logger.debug(f"Name evaluation result for '{respondent_name}': {json.dumps(dd, indent=2)}")
                due_diligence["names_found"].append(respondent_name)
                due_diligence["name_scores"][respondent_name] = dd["name_scores"][respondent_name]
                if dd["exact_match_found"]:
                    result["actions"].append(normalized_record)
                    due_diligence["exact_match_found"] = True
                    logger.debug(f"Matched disciplinary record {normalized_record['case_id']} with {respondent_name} (score: {dd['name_scores'][respondent_name]})")
            except Exception as e:
                logger.error(f"Failed to evaluate name '{respondent_name}' against '{searched_name}': {str(e)}")
                due_diligence["names_found"].append(respondent_name)
                due_diligence["name_scores"][respondent_name] = 0.0
                due_diligence["status"] = f"Partial failure: Error processing '{respondent_name}'"
        else:
            due_diligence["records_filtered"] += 1
            logger.debug(f"Skipped disciplinary record {normalized_record['case_id']} - no respondent name")

    due_diligence["records_filtered"] = due_diligence["records_found"] - len(result["actions"])
    if not due_diligence["status"].startswith("Partial failure"):
        due_diligence["status"] = "Exact matches found" if due_diligence["exact_match_found"] else f"Records found but no matches for '{searched_name}'"

    logger.debug(f"Final due_diligence for {data_source}: {json.dumps(due_diligence, indent=2)}")
    return result

def create_arbitration_record(
    data_source: str,
    data: Any,
    searched_name: str,
    threshold: MatchThreshold = MatchThreshold.STRICT
) -> Dict[str, Any]:
    """
    Normalize arbitration data from SEC or FINRA sources into a consistent structure.
    Includes due_diligence section aligning with FINRA name handling convention.

    :param data_source: Source of the data ("FINRA_Arbitration" or "SEC_Arbitration").
    :param data: Raw data from the source.
    :param searched_name: Name to match against respondent names.
    :param threshold: MatchThreshold enum value defining the minimum score for a match (default: STRICT).
    :return: Normalized dictionary with actions and due_diligence.
    """
    logger.debug(f"Entering create_arbitration_record for {data_source} with searched_name='{searched_name}', threshold={threshold.name}")
    result = {
        "actions": [],
        "due_diligence": {
            "searched_name": searched_name,
            "records_found": 0,
            "records_filtered": 0,
            "names_found": [],
            "name_scores": {},
            "exact_match_found": False,
            "status": "No records found"
        },
        "raw_data": [data] if not isinstance(data, list) else data
    }
    logger.debug(f"Raw data: {json.dumps(result['raw_data'], indent=2)}")

    if isinstance(data, dict) and "error" in data:
        logger.warning(f"Error in {data_source} data: {data['error']}")
        return result

    # Handle nested structure: extract "result" if present
    raw_results = data
    if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict) and "result" in data[0]:
        raw_results = data[0]["result"]
        logger.debug(f"{data_source} data is a nested list, using 'result' key")
    elif isinstance(data, dict) and "result" in data:
        raw_results = data["result"]
    elif isinstance(data, list):
        logger.debug(f"{data_source} data is a raw list, treating as results")
        raw_results = data
    else:
        logger.warning(f"Unexpected {data_source} data format: {type(data)} - {data}")
        return result

    if raw_results == "No Results Found" or not raw_results:
        logger.info(f"No results found in {data_source} for {searched_name}")
        return result

    if not isinstance(raw_results, list):
        logger.warning(f"Unexpected {data_source} result format: {raw_results}")
        return result

    due_diligence = result["due_diligence"]
    due_diligence["records_found"] = len(raw_results)
    logger.debug(f"Found {due_diligence['records_found']} records")

    for record in raw_results:
        if not isinstance(record, dict):
            logger.warning(f"Skipping malformed record in {data_source}: {record}")
            due_diligence["records_filtered"] += 1
            continue

        logger.debug(f"Processing record: {json.dumps(record, indent=2)}")
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
            respondent_names = [searched_name]  # Explicitly score searched_name

        logger.debug(f"Extracted respondent_names: {respondent_names}")
        if respondent_names:
            matched = False
            for respondent_name in respondent_names:
                try:
                    name_eval, _ = evaluate_name(searched_name, respondent_name, [], score_threshold=threshold.value)
                    dd = name_eval["due_diligence"]
                    logger.debug(f"Name evaluation result for '{respondent_name}': {json.dumps(dd, indent=2)}")
                    due_diligence["names_found"].append(respondent_name)
                    due_diligence["name_scores"][respondent_name] = dd["name_scores"][respondent_name]
                    if dd["exact_match_found"]:
                        normalized_record["matched_name"] = respondent_name
                        result["actions"].append(normalized_record)
                        due_diligence["exact_match_found"] = True
                        matched = True
                        logger.debug(f"Matched arbitration record {normalized_record['case_id']} with {respondent_name} (score: {dd['name_scores'][respondent_name]})")
                        break
                except Exception as e:
                    logger.error(f"Failed to evaluate name '{respondent_name}' against '{searched_name}': {str(e)}")
                    due_diligence["names_found"].append(respondent_name)
                    due_diligence["name_scores"][respondent_name] = 0.0
                    due_diligence["status"] = f"Partial failure: Error processing '{respondent_name}'"
            if not matched:
                due_diligence["records_filtered"] += 1
                logger.debug(f"Record {normalized_record['case_id']} not matched")
        else:
            due_diligence["records_filtered"] += 1
            logger.debug(f"No respondents found in {normalized_record['case_id']}, skipping match")

    due_diligence["status"] = "Exact matches found" if due_diligence["exact_match_found"] else f"Records found but no matches for '{searched_name}'"
    logger.debug(f"Final due_diligence for {data_source}: {json.dumps(due_diligence, indent=2)}")
    return result

def create_regulatory_record(
    data_source: str,
    data: Any,
    searched_name: str,
    threshold: MatchThreshold = MatchThreshold.STRICT
) -> Dict[str, Any]:
    logger.debug(f"Entering create_regulatory_record for {data_source} with searched_name='{searched_name}', threshold={threshold.name}")
    result = {
        "actions": [],
        "due_diligence": {
            "searched_name": searched_name,
            "records_found": 0,
            "records_filtered": 0,
            "names_found": [],
            "name_scores": {},
            "exact_match_found": False,
            "status": "No records found"
        },
        "raw_data": [data] if not isinstance(data, list) else data
    }
    logger.debug(f"Raw data: {json.dumps(result['raw_data'], indent=2)}")

    if isinstance(data, dict) and "error" in data:
        logger.warning(f"Error in {data_source} data: {data['error']}")
        return result

    raw_results = data
    if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict) and "result" in data[0]:
        raw_results = data[0]["result"]
        logger.debug(f"{data_source} data is a nested list, using 'result' key")
    elif isinstance(data, dict) and "result" in data:
        raw_results = data["result"]
    elif not isinstance(data, list):
        logger.warning(f"Unexpected {data_source} data format: {type(data)} - {data}")
        return result

    if raw_results == "No Results Found" or not raw_results:
        logger.info(f"No results found in {data_source} for {searched_name}")
        return result

    if not isinstance(raw_results, list):
        logger.warning(f"Unexpected {data_source} result format: {raw_results}")
        return result

    due_diligence = result["due_diligence"]
    due_diligence["records_found"] = len(raw_results)
    logger.debug(f"Found {due_diligence['records_found']} records")

    for record in raw_results:
        if not isinstance(record, dict):
            logger.warning(f"Skipping malformed record in {data_source}: {record}")
            due_diligence["records_filtered"] += 1
            continue

        logger.debug(f"Processing record: {json.dumps(record, indent=2)}")
        normalized_record = {
            "case_id": record.get("NFA ID", "Unknown"),
            "date": "Unknown",
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
        logger.debug(f"Extracted respondent_name: '{respondent_name}'")

        if respondent_name:
            try:
                name_eval, _ = evaluate_name(searched_name, respondent_name, [], score_threshold=threshold.value)
                dd = name_eval["due_diligence"]
                logger.debug(f"Name evaluation result for '{respondent_name}': {json.dumps(dd, indent=2)}")
                # Use the raw respondent_name as the key, but fetch the normalized score
                normalized_name = dd["names_found"][0]  # First name in names_found is the main fetched name
                score = dd["name_scores"].get(normalized_name, 0.0)
                due_diligence["names_found"].append(respondent_name)
                due_diligence["name_scores"][respondent_name] = score
                if dd["exact_match_found"] and record.get("Regulatory Actions") == "Yes":
                    result["actions"].append(normalized_record)
                    due_diligence["exact_match_found"] = True
                    logger.debug(f"Matched regulatory record {normalized_record['case_id']} with {respondent_name} (score: {score})")
            except Exception as e:
                logger.error(f"Failed to evaluate name '{respondent_name}' against '{searched_name}': {str(e)}")
                due_diligence["names_found"].append(respondent_name)
                due_diligence["name_scores"][respondent_name] = 0.0
                due_diligence["status"] = f"Partial failure: Error processing '{respondent_name}'"
        else:
            due_diligence["records_filtered"] += 1
            logger.debug(f"Skipped regulatory record {normalized_record['case_id']} - no respondent name")

    due_diligence["records_filtered"] = due_diligence["records_found"] - len(result["actions"])
    if not due_diligence["status"].startswith("Partial failure"):
        due_diligence["status"] = "Exact matches found" if due_diligence["exact_match_found"] else f"Records found but no matches for '{searched_name}'"

    logger.debug(f"Final due_diligence for {data_source}: {json.dumps(due_diligence, indent=2)}")
    return result

if __name__ == "__main__":
    # Configure logging for testing
    logging.basicConfig(level=logging.DEBUG)
    test_data = [{"result": [{"Name": "LANEY, DANNY", "NFA ID": "0569081", "Regulatory Actions": "Yes"}]}]
    result = create_regulatory_record("NFA_Regulatory", test_data, "Danny La")
    print(json.dumps(result, indent=2))