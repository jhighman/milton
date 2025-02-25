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

def create_individual_record(data_source: str, basic_info: Optional[Dict[str, Any]], detailed_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    logger.debug(f"Normalizing individual record from {data_source}")
    record = {
        "crd_number": None,
        "fetched_name": "",
        "other_names": [],
        "bc_scope": "NotInScope",
        "ia_scope": "NotInScope",
        "disclosures": [],
        "arbitrations": [],
        "exams": [],
        "current_ia_employments": []
    }

    if not basic_info:
        logger.warning(f"No basic info provided for {data_source}")
        return record

    basic = basic_info.copy()
    detailed = detailed_info.copy() if detailed_info else {}

    # Handle nested SEC IAPD data structure
    if "hits" in basic and "hits" in basic["hits"] and basic["hits"]["hits"]:
        source_data = basic["hits"]["hits"][0]["_source"]
    else:
        source_data = basic

    # Extract crd_number from ind_source_id or other fields
    record["crd_number"] = (source_data.get("ind_source_id") or 
                           source_data.get("crd_number") or 
                           source_data.get("CRD") or 
                           detailed.get("crd_number"))
    record["fetched_name"] = (source_data.get("ind_other_names", [source_data.get("ind_firstname", "") + " " + source_data.get("ind_lastname", "")])[0].strip() or 
                             detailed.get("fetched_name", ""))
    record["other_names"] = source_data.get("ind_other_names", detailed.get("other_names", []))
    record["bc_scope"] = source_data.get("ind_bc_scope", detailed.get("bc_scope", "NotInScope")).capitalize()
    record["ia_scope"] = source_data.get("ind_ia_scope", detailed.get("ia_scope", "NotInScope")).capitalize()
    record["current_ia_employments"] = source_data.get("ind_ia_current_employments", detailed.get("current_ia_employments", []))

    if detailed:
        record["disclosures"] = detailed.get("disclosures", [])
        record["arbitrations"] = detailed.get("arbitrations", [])
        record["exams"] = detailed.get("exams", [])

    logger.debug(f"Normalized individual record: {json.dumps(record, indent=2)}")
    return record

def create_disciplinary_record(data_source: str, data: Any, searched_name: str) -> Dict[str, Any]:
    """
    Normalize disciplinary data from sources like SEC or FINRA into a common structure.

    Args:
        data_source (str): Source identifier (e.g., "SEC_Disciplinary", "FINRA_Disciplinary").
        data (Any): Raw data from the source, can be a dict or list.
        searched_name (str): Name searched for filtering.

    Returns:
        Dict[str, Any]: Normalized disciplinary data with actions and due diligence details.
    """
    result = {
        "disciplinary_actions": [],
        "raw_data": [data] if not isinstance(data, list) else data,
        "name_scores": {}
    }
    logger.debug(f"Normalizing {data_source} disciplinary data for {searched_name}")

    if isinstance(data, dict) and "error" in data:
        logger.warning(f"Error in {data_source} data: {data['error']}")
        return result

    # Handle both dict with "result" and direct list inputs
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
        if data_source == "FINRA_Disciplinary":
            normalized_record["case_id"] = record.get("Case ID", "Unknown")
            normalized_record["date"] = record.get("Action Date", "Unknown")
            normalized_record["details"] = {
                "action_type": record.get("Action Type", ""),
                "firms_individuals": record.get("Firms/Individuals", ""),
                "description": record.get("Description", "")
            }
            respondent_name = record.get("Firms/Individuals", searched_name)

        elif data_source == "SEC_Disciplinary":
            normalized_record["case_id"] = record.get("Case ID", "Unknown")
            normalized_record["date"] = record.get("Action Date", "Unknown")
            normalized_record["details"] = {
                "name": record.get("Name", ""),
                "description": record.get("Description", "")
            }
            respondent_name = record.get("Name", searched_name)

        name_eval, _ = evaluate_name(searched_name, respondent_name, [])
        score = name_eval["best_match"]["score"]
        result["name_scores"][respondent_name] = score
        if score >= 80.0:  # Consistent threshold
            result["disciplinary_actions"].append(normalized_record)

    return result

def create_arbitration_record(data_source: str, data: Any, searched_name: str) -> Dict[str, Any]:
    result = {
        "arbitration_actions": [],
        "raw_data": [data] if not isinstance(data, list) else data,
        "name_scores": {}
    }
    logger.debug(f"Normalizing {data_source} arbitration data for {searched_name}")

    if isinstance(data, dict) and "error" in data:
        logger.warning(f"Error in {data_source} data: {data['error']}")
        return result

    # Handle dict with "result" (FINRA/SEC) or direct list (cached SEC)
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
        normalized_record = {}
        respondent_name = None

        if data_source == "FINRA_Arbitration":
            normalized_record["case_id"] = record.get("Case Summary", {}).get("Case Number", record.get("Award Document", "Unknown"))
            normalized_record["status"] = "closed"
            normalized_record["outcome"] = "Award against individual" if "Award" in record.get("Document Type", "") else "Unknown"
            normalized_record["date"] = record.get("Date of Award", "Unknown")
            normalized_record["details"] = {
                "award_document": record.get("Award Document"),
                "pdf_url": record.get("PDF URL"),
                "case_summary": record.get("Case Summary", {}),
                "forum": record.get("Forum")
            }
            respondent_name = record.get("Case Summary", {}).get("Respondent", searched_name)
        elif data_source == "SEC_Arbitration":
            normalized_record["case_id"] = record.get("Enforcement Action", "Unknown")
            normalized_record["status"] = "closed"
            normalized_record["outcome"] = "Award against individual"
            normalized_record["date"] = record.get("Date Filed", "Unknown")
            normalized_record["details"] = {
                "enforcement_action": record.get("Enforcement Action"),
                "documents": record.get("Documents", [])
            }
            respondent_name = searched_name  # SEC doesn't list respondents explicitly

        if respondent_name:
            name_eval, _ = evaluate_name(searched_name, respondent_name, [])
            score = name_eval["best_match"]["score"]
            result["name_scores"][respondent_name] = score
            if score >= 80.0:
                result["arbitration_actions"].append(normalized_record)

    return result