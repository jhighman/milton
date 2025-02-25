"""
normalizer.py

This module provides utilities to extract and normalize individual (person) records
from two major data sources:
   1) FINRA BrokerCheck (aka "BrokerCheck")
   2) SEC IAPD (Investment Adviser Public Disclosure)
   3) SEC Disciplinary (Enforcement Actions)
   4) FINRA Disciplinary (Enforcement Actions)
"""

import json
import logging
from typing import Dict, Any, Optional, List, Union
from name_matcher import evaluate_name

logger = logging.getLogger(__name__)

def create_individual_record(
    data_source: str,
    basic_info: Optional[Dict[str, Any]],
    detailed_info: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    extracted_info = {
        "fetched_name": "",
        "other_names": [],
        "bc_scope": "",
        "ia_scope": "",
        "disclosures": [],
        "arbitrations": [],
        "exams": [],
        "current_ia_employments": []
    }

    if not basic_info:
        logger.warning("No basic_info provided. Returning empty extracted_info.")
        return extracted_info

    hits_list = basic_info.get("hits", {}).get("hits", [])
    if hits_list:
        individual = hits_list[0].get("_source", {})
    else:
        logger.warning(f"{data_source}: basic_info had no hits. Returning mostly empty extracted_info.")
        return extracted_info

    fetched_name = " ".join(filter(None, [
        individual.get('ind_firstname', ''),
        individual.get('ind_middlename', ''),
        individual.get('ind_lastname', '')
    ])).strip()
    extracted_info["fetched_name"] = fetched_name
    extracted_info["other_names"] = individual.get("ind_other_names", [])
    extracted_info["bc_scope"] = individual.get("ind_bc_scope", "")
    extracted_info["ia_scope"] = individual.get("ind_ia_scope", "")

    if detailed_info:
        if "disclosures" in detailed_info or "stateExamCategory" in detailed_info:
            extracted_info["disclosures"] = detailed_info.get("disclosures", [])
            extracted_info["arbitrations"] = detailed_info.get("arbitrations", [])
            extracted_info["exams"] = (
                detailed_info.get("stateExamCategory", []) +
                detailed_info.get("principalExamCategory", []) +
                detailed_info.get("productExamCategory", [])
            )
            extracted_info["current_ia_employments"] = detailed_info.get("currentIAEmployments", [])
            logger.info(f"Normalized {data_source} data from flat JSON structure.")
        elif "hits" in detailed_info and detailed_info["hits"].get("hits"):
            content_str = detailed_info["hits"]["hits"][0]["_source"].get("content", "")
            try:
                content_json = json.loads(content_str) if content_str else {}
                extracted_info["disclosures"] = content_json.get("disclosures", [])
                extracted_info["arbitrations"] = content_json.get("arbitrations", [])
                extracted_info["exams"] = (
                    content_json.get("stateExamCategory", []) +
                    content_json.get("principalExamCategory", []) +
                    content_json.get("productExamCategory", [])
                )
                extracted_info["current_ia_employments"] = content_json.get("currentIAEmployments", [])
                logger.info(f"Normalized {data_source} data from Elasticsearch-style structure.")
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse {data_source} 'content' JSON: {e}")
        elif data_source == "IAPD" and "iacontent" in individual:
            try:
                iacontent_data = json.loads(individual.get("iacontent", "{}"))
                extracted_info["disclosures"] = iacontent_data.get("disclosures", [])
                extracted_info["arbitrations"] = iacontent_data.get("arbitrations", [])
                extracted_info["exams"] = (
                    iacontent_data.get("stateExamCategory", []) +
                    iacontent_data.get("principalExamCategory", []) +
                    iacontent_data.get("productExamCategory", [])
                )
                extracted_info["current_ia_employments"] = iacontent_data.get("currentIAEmployments", individual.get("ind_ia_current_employments", []))
            except json.JSONDecodeError as e:
                logger.warning(f"IAPD basic_info iacontent parse error: {e}")
        else:
            logger.info(f"No recognizable detailed_info structure for {data_source}.")
    else:
        logger.info(f"No detailed_info provided for {data_source}.")

    return extracted_info

VALID_DISCIPLINARY_SOURCES = {"SEC_Disciplinary", "FINRA_Disciplinary"}

class NormalizationError(Exception):
    """Exception raised when normalization fails unexpectedly."""
    pass

def create_disciplinary_record(
    source: str, 
    data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]],
    searched_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Normalize disciplinary records from SEC or FINRA into a unified structure, optionally filtering by name match.

    Args:
        source (str): The source of the data ("SEC_Disciplinary" or "FINRA_Disciplinary").
        data (Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]): Raw data from the disciplinary agent.
        searched_name (Optional[str]): The name to match against for filtering actions (e.g., "Mark Miller").

    Returns:
        Dict[str, Any]: Normalized disciplinary record with actions, name scores, and raw data.
    """
    normalized_record = {
        "source": source,
        "primary_name": "",
        "disciplinary_actions": [],
        "name_scores": {},  # Added to store scores for all names
        "raw_data": data  # Preserve raw data
    }

    if source not in VALID_DISCIPLINARY_SOURCES:
        logger.error(f"Invalid source '{source}'. Must be one of {VALID_DISCIPLINARY_SOURCES}.")
        return normalized_record

    if isinstance(data, list):
        if not data:
            logger.warning(f"Empty list provided for {source} data.")
            return normalized_record
        data = data[0]
        logger.debug(f"Unwrapped list to first item for {source}: {json.dumps(data, indent=2)}")

    if not data or "result" not in data or data["result"] == "No Results Found":
        logger.warning(f"No results found in {source} data.")
        return normalized_record

    results = data["result"] if isinstance(data["result"], list) else [data["result"]]
    if not results:
        logger.warning(f"Empty result list in {source} data.")
        return normalized_record

    if results and not any(result.get("Name") or result.get("Firms/Individuals") for result in results):
        logger.error(f"Normalization failed for {source}: data provided but no actionable records found: {json.dumps(results, indent=2)}")
        raise NormalizationError(f"Normalization failed for {source}: data provided but no actionable records found")

    if source == "SEC_Disciplinary":
        first_result = results[0]
        normalized_record["primary_name"] = first_result.get("Name", "")
        for result in results:
            if result.get("Name", "") != normalized_record["primary_name"]:
                logger.warning(f"Inconsistent names in SEC Disciplinary results: {result.get('Name')} vs {normalized_record['primary_name']}")
            
            primary_name = result.get("Name", "")
            other_names = result.get("Also Known As", "").split("; ") if result.get("Also Known As") else []
            associated_names = [primary_name] + [name for name in other_names if name and name != primary_name]

            action = {
                "source": source,
                "case_id": result.get("Enforcement Action", ""),
                "description": result.get("Enforcement Action", ""),
                "date": result.get("Date Filed", ""),
                "documents": result.get("Documents", []),
                "associated_names": associated_names,
                "additional_info": {
                    "state": result.get("State", ""),
                    "current_age": result.get("Current Age", "")
                }
            }

            # Apply name matching and store scores
            if searched_name:
                eval_details, score = evaluate_name(searched_name, primary_name, other_names, score_threshold=80.0)
                logger.debug(f"SEC name evaluation for '{primary_name}': score={score}, details={json.dumps(eval_details, indent=2)}")
                normalized_record["name_scores"][primary_name] = score if score is not None else 0.0
                if score and score >= 80.0:
                    normalized_record["disciplinary_actions"].append(action)
            else:
                normalized_record["disciplinary_actions"].append(action)

        logger.info(f"Normalized {source} data for {normalized_record['primary_name']}")

    elif source == "FINRA_Disciplinary":
        first_result = results[0]
        normalized_record["primary_name"] = first_result.get("Firms/Individuals", "")
        for result in results:
            firms_individuals = result.get("Firms/Individuals", "").split(", ")
            associated_names = [name.strip() for name in firms_individuals if name.strip()]

            action = {
                "source": source,
                "case_id": result.get("Case ID", ""),
                "description": result.get("Case Summary", ""),
                "date": result.get("Action Date", ""),
                "documents": [{"title": result.get("Document Type", ""), "link": "", "date": result.get("Action Date", "")}] if result.get("Document Type") else [],
                "associated_names": associated_names,
                "additional_info": {
                    "document_type": result.get("Document Type", "")
                }
            }

            # Apply name matching and store scores
            if searched_name:
                primary_name = associated_names[0] if associated_names else ""
                other_names = associated_names[1:] if len(associated_names) > 1 else []
                eval_details, score = evaluate_name(searched_name, primary_name, other_names, score_threshold=80.0)
                logger.debug(f"FINRA name evaluation for '{primary_name}': score={score}, details={json.dumps(eval_details, indent=2)}")
                normalized_record["name_scores"][primary_name] = score if score is not None else 0.0
                if score and score >= 80.0:
                    normalized_record["disciplinary_actions"].append(action)
            else:
                normalized_record["disciplinary_actions"].append(action)

        logger.info(f"Normalized {source} data for {normalized_record['primary_name']}")

    # Final validation: warn if no actions matched searched_name, but don’t raise error to preserve data
    if results and not normalized_record["disciplinary_actions"] and searched_name:
        logger.warning(f"No actions matched searched name '{searched_name}' for {source}, but data preserved.")
    elif results and not normalized_record["disciplinary_actions"]:
        logger.warning(f"No actions normalized for {source}, but no searched_name provided to filter.")

    return normalized_record