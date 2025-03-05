"""
brokercheck_iapd_extractor.py

This module provides utilities to extract and normalize individual (person) records
from two major data sources:
    1) FINRA BrokerCheck (aka "BrokerCheck")
    2) SEC IAPD (Investment Adviser Public Disclosure)

Why Separate Logic for Each Source?
-----------------------------------
- BrokerCheck and IAPD both contain similar information—names, disclosures, exams, etc.—
  but they present data in different JSON structures.
- BrokerCheck often stores details within a key called "content" in the detailed results,
  while IAPD references "iacontent" for both basic and detailed info.
- We combine "basic info" and "detailed info" responses into a single record here,
  ensuring downstream code can work with consistent keys like "fetched_name" and "disclosures".

What This Module Does
---------------------
- Defines a function `create_individual_record` that:
  1) Accepts a data source (either "BrokerCheck" or "IAPD").
  2) Accepts a "basic_info" structure and a "detailed_info" structure,
     as returned by your fetching functions (e.g., `search_finra_brokercheck_individual`, 
     `search_finra_brokercheck_detailed`, `search_sec_iapd_individual`, etc.).
  3) Extracts fields like first/last names, registration scopes, disclosures, 
     exams, and employments, placing them into a single dictionary ("extracted_info").
  4) Ensures that for each data source, we parse the relevant JSON fields correctly.

Why This Is Done
----------------
- Having a single "extracted_info" structure simplifies evaluation steps like:
  - Name matching
  - Disclosures checks
  - Exam verifications
  - Arbitration checks
- Without normalizing them, each part of your code would need to handle both 
  BrokerCheck and IAPD’s quirks. Centralizing that logic here keeps the rest of 
  the application simpler and more maintainable.
"""

import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

def create_individual_record(
    data_source: str,
    basic_info: Optional[Dict[str, Any]],
    detailed_info: Optional[Dict[str, Any]]
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
    :return: A dictionary (extracted_info) with normalized keys:
        {
          "fetched_name": str,
          "other_names": list,
          "bc_scope": str,
          "ia_scope": str,
          "disclosures": list,
          "arbitrations": list,
          "exams": list,
          "current_employments": list,
          ...
        }

    For BrokerCheck:
    - 'basic_info' usually contains a "hits" -> "hits"[0] -> "_source" with fields like
      ind_firstname, ind_middlename, ind_lastname, ind_other_names, ind_bc_scope, ind_ia_scope.
    - 'detailed_info' typically contains a "content" JSON that includes 'disclosures' and 'currentEmployments'.

    For IAPD:
    - 'basic_info' also has a "hits" -> "hits"[0] -> "_source" structure, with the relevant
      fields plus an 'iacontent' key containing partial detail (like "currentIAEmployments").
    - 'detailed_info' typically references 'iacontent' again to retrieve disclosures, arbitrations,
      and exam categories ("stateExamCategory", "principalExamCategory", "productExamCategory").
    """
    extracted_info = {
        "fetched_name": "",
        "other_names": [],
        "bc_scope": "",
        "ia_scope": "",
        "disclosures": [],
        "arbitrations": [],
        "exams": [],
        "current_employments": []  # Normalized for both sources
    }

    # If we have no basic_info, we can't parse anything. Return an empty structure.
    if not basic_info:
        logger.warning("No basic_info provided. Returning empty extracted_info.")
        return extracted_info

    # Parse out the individual (top-level) from "basic_info"
    hits_list = basic_info.get("hits", {}).get("hits", [])
    if hits_list:
        individual = hits_list[0].get("_source", {})
    else:
        logger.warning(f"{data_source}: basic_info had no hits. Returning mostly empty extracted_info.")
        return extracted_info

    # Common name fields (both sources have ind_firstname / middlename / lastname)
    fetched_name = f"{individual.get('ind_firstname', '')} {individual.get('ind_middlename', '')} {individual.get('ind_lastname', '')}".strip()
    extracted_info["fetched_name"] = fetched_name
    extracted_info["other_names"] = individual.get("ind_other_names", [])

    # Registration scopes (some individuals may have both bc_scope and ia_scope)
    extracted_info["bc_scope"] = individual.get("ind_bc_scope", "")
    extracted_info["ia_scope"] = individual.get("ind_ia_scope", "")

    # Helper function to normalize employments
    def normalize_employment(emp: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "firm_id": emp.get("firmId"),
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
            ]
        }

    # Different parsing logic depending on source
    if data_source == "BrokerCheck":
        # For BrokerCheck, we typically rely on 'detailed_info' to get disclosures and employments
        if detailed_info and "hits" in detailed_info:
            detailed_hits = detailed_info["hits"].get("hits", [])
            if detailed_hits:
                content_str = detailed_hits[0]["_source"].get("content", "")
                try:
                    content_json = json.loads(content_str)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse BrokerCheck 'content' JSON: {e}")
                    content_json = {}

                extracted_info["disclosures"] = content_json.get("disclosures", [])
                # Parse and normalize BrokerCheck employments
                current_employments = []
                for emp in content_json.get("currentEmployments", []):
                    current_employments.append(normalize_employment(emp))
                extracted_info["current_employments"] = current_employments
            else:
                logger.info("BrokerCheck detailed_info had no hits. No disclosures extracted.")
        else:
            logger.info("No BrokerCheck detailed_info provided or empty, skipping disclosures parsing.")

    elif data_source == "IAPD":
        # IAPD: Parse partial data from basic_info (iacontent) for current employments
        iacontent_str = individual.get("iacontent", "{}")
        try:
            iacontent_data = json.loads(iacontent_str)
        except json.JSONDecodeError as e:
            logger.warning(f"IAPD basic_info iacontent parse error: {e}")
            iacontent_data = {}

        # Extract and normalize current employments
        current_employments = []
        for emp in iacontent_data.get("currentIAEmployments", []):
            current_employments.append(normalize_employment(emp))
        extracted_info["current_employments"] = current_employments

        # Now parse the 'detailed_info' to get full disclosures, exams, arbitrations, etc.
        if detailed_info and "hits" in detailed_info:
            detailed_hits = detailed_info["hits"].get("hits", [])
            if detailed_hits:
                iapd_detailed_content_str = detailed_hits[0]["_source"].get("iacontent", "{}")
                try:
                    iapd_detailed_content_data = json.loads(iapd_detailed_content_str)
                except json.JSONDecodeError as e:
                    logger.warning(f"IAPD detailed_info iacontent parse error: {e}")
                    iapd_detailed_content_data = {}

                # Combine exam categories
                state_exams = iapd_detailed_content_data.get("stateExamCategory", [])
                principal_exams = iapd_detailed_content_data.get("principalExamCategory", [])
                product_exams = iapd_detailed_content_data.get("productExamCategory", [])
                extracted_info["exams"] = state_exams + principal_exams + product_exams

                # Disclosures & arbitrations
                extracted_info["disclosures"] = iapd_detailed_content_data.get("disclosures", [])
                extracted_info["arbitrations"] = iapd_detailed_content_data.get("arbitrations", [])
            else:
                logger.info("IAPD detailed_info had no hits. Using only basic_info's iacontent if available.")
                extracted_info["disclosures"] = iacontent_data.get("disclosures", [])
                extracted_info["arbitrations"] = iacontent_data.get("arbitrations", [])
        else:
            # If no 'detailed_info' was provided, at least use the basic iacontent
            extracted_info["disclosures"] = iacontent_data.get("disclosures", [])
            extracted_info["arbitrations"] = iacontent_data.get("arbitrations", [])

    else:
        logger.error(f"Unknown data source '{data_source}'. Returning minimal extracted_info.")

    return extracted_info