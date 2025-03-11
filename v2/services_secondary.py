"""
services_secondary.py

This module contains secondary service functions extracted from services.py,
specifically for performing regulatory action reviews, fully independent of FinancialServicesFacade.
"""

import json
import logging
from typing import Optional, Dict, Any, List

from logger_config import setup_logging
from marshaller import fetch_agent_nfa_id_search, create_driver

loggers = setup_logging(debug=True)
logger = loggers["services"]

RUN_HEADLESS = True

def perform_regulatory_action_review(nfa_id: str, employee_number: Optional[str] = None) -> Dict[str, Any]:
    """
    Performs a consolidated regulatory action review for a specific NFA ID using NFA data.
    Manages its own WebDriver instance independently.
    
    :param nfa_id: The NFA ID to search for.
    :param employee_number: Optional employee identifier.
    :return: A dictionary containing combined regulatory actions and due diligence metadata.
    """
    call_id = id(object())  # Unique ID for tracing
    logger.debug(f"[{call_id}] Starting regulatory action review for NFA ID {nfa_id}, Employee: {employee_number}")

    if not nfa_id:
        logger.error(f"[{call_id}] NFA ID is required")
        return {
            "primary_id": nfa_id,
            "actions": [],
            "due_diligence": {
                "searched_id": nfa_id,
                "nfa_regulatory_actions": {
                    "records_found": 0,
                    "records_filtered": 0,
                    "exact_match_found": False,
                    "status": "No records fetched due to invalid NFA ID"
                }
            },
            "raw_data": [{"result": "No Results Found"}]
        }

    combined_review = {
        "primary_id": nfa_id,
        "actions": [],
        "due_diligence": {
            "searched_id": nfa_id,
            "nfa_regulatory_actions": {
                "records_found": 0,
                "records_filtered": 0,
                "exact_match_found": False,
                "status": "No records fetched"
            }
        },
        "raw_data": []
    }

    driver = None
    try:
        logger.debug(f"[{call_id}] Creating new WebDriver for secondary NFA search")
        driver = create_driver(RUN_HEADLESS)
        logger.info(f"[{call_id}] Fetching NFA regulatory data by ID {nfa_id}, Employee: {employee_number}")
        params = {"nfa_id": nfa_id}
        result = fetch_agent_nfa_id_search(employee_number, params, driver)
        if result:
            logger.debug(f"[{call_id}] NFA regulatory by ID raw result: {json.dumps(result, indent=2)}")
            result_dict = result[0] if isinstance(result, list) and result else result
            data_source = "NFA_Regulatory_Actions"
            base_url = "https://www.nfa.futures.org/BasicNet/"

            normalized_result = {
                "actions": [],
                "due_diligence": {
                    "searched_id": nfa_id,
                    "records_found": 0,
                    "records_filtered": 0,
                    "status": "No records found"
                }
            }

            if isinstance(result_dict, dict) and "error" in result_dict:
                logger.warning(f"[{call_id}] Error in {data_source} data: {result_dict['error']}")
                nfa_result = normalized_result
            else:
                raw_results = result_dict.get("result") if isinstance(result_dict, dict) else None
                if not raw_results or raw_results == "No Results Found":
                    logger.info(f"[{call_id}] No results found in {data_source} for NFA ID {nfa_id}")
                    nfa_result = normalized_result
                else:
                    if isinstance(raw_results, dict) and "regulatory_actions" in raw_results:
                        actions = raw_results["regulatory_actions"]
                    else:
                        actions = []

                    due_diligence = normalized_result["due_diligence"]
                    due_diligence["records_found"] = len(actions)
                    if actions:
                        for row in actions:
                            required_fields = ["Effective Date", "Contributor", "Action Type", "Case Outcome", "Case #"]
                            missing_fields = [field for field in required_fields if not row.get(field)]
                            if missing_fields:
                                logger.warning(f"[{call_id}] Skipping action with missing fields {missing_fields}: {row}")
                                due_diligence["records_filtered"] += 1
                                continue

                            effective_date = row.get("Effective Date", "")
                            contributor = row.get("Contributor", "")
                            action_type = row.get("Action Type", [])
                            case_outcome = row.get("Case Outcome", [])
                            case_num = row.get("Case #", "")
                            relative_link = row.get("Case Link", "")
                            full_link = f"{base_url}{relative_link}" if relative_link else ""

                            normalized_action = {
                                "data_source": data_source,
                                "effective_date": effective_date,
                                "contributor": contributor,
                                "action_type": action_type,
                                "case_outcome": case_outcome,
                                "case_number": case_num,
                                "case_link": full_link,
                                "nfa_id": raw_results.get("nfa_id", nfa_id)
                            }
                            normalized_result["actions"].append(normalized_action)
                            logger.debug(f"[{call_id}] Normalized action for NFA ID {nfa_id}: {normalized_action}")

                        appended_count = len(normalized_result["actions"])
                        due_diligence["records_filtered"] = due_diligence["records_found"] - appended_count
                        due_diligence["status"] = "Actions appended" if appended_count > 0 else "No records found"
                        logger.debug(f"[{call_id}] Final due_diligence for {data_source}: {json.dumps(due_diligence, indent=2)}")
                    nfa_result = normalized_result
        else:
            logger.warning(f"[{call_id}] No data found for NFA ID {nfa_id} in NFA regulatory search")
            nfa_result = {
                "actions": [],
                "due_diligence": {
                    "searched_id": nfa_id,
                    "records_found": 0,
                    "records_filtered": 0,
                    "exact_match_found": False,
                    "status": "No records found"
                },
                "raw_data": [{"result": "No Results Found"}]
            }
    except Exception as e:
        logger.error(f"[{call_id}] Error during NFA search: {str(e)}")
        nfa_result = {
            "actions": [],
            "due_diligence": {
                "searched_id": nfa_id,
                "records_found": 0,
                "records_filtered": 0,
                "exact_match_found": False,
                "status": f"Search failed: {str(e)}"
            },
            "raw_data": [{"result": "Search Failed"}]
        }
    finally:
        if driver:
            try:
                driver.quit()
                logger.info(f"[{call_id}] WebDriver closed for secondary NFA search")
            except Exception as e:
                logger.warning(f"[{call_id}] Failed to close WebDriver: {str(e)}")

    combined_review["raw_data"] = nfa_result if nfa_result else [{"result": "No Results Found"}]
    if nfa_result and isinstance(nfa_result, dict) and "due_diligence" in nfa_result:
        logger.debug(f"[{call_id}] NFA Regulatory Action result received: {json.dumps(nfa_result, indent=2)}")
        nfa_dd = nfa_result.get("due_diligence", {})
        nfa_actions = nfa_result.get("actions", [])
        combined_review["due_diligence"]["nfa_regulatory_actions"]["records_found"] = nfa_dd.get("records_found", 0)
        combined_review["due_diligence"]["nfa_regulatory_actions"]["records_filtered"] = nfa_dd.get("records_filtered", 0)
        combined_review["due_diligence"]["nfa_regulatory_actions"]["exact_match_found"] = False
        combined_review["due_diligence"]["nfa_regulatory_actions"]["status"] = nfa_dd.get("status", "Records processed")
        if nfa_actions:
            combined_review["actions"].extend(nfa_actions)
            logger.debug(f"[{call_id}] Added {len(nfa_actions)} NFA regulatory actions")
    else:
        logger.warning(f"[{call_id}] Malformed NFA result: {nfa_result}")

    logger.debug(f"[{call_id}] Combined regulatory action review result: {json.dumps(combined_review, indent=2)}")
    if combined_review["actions"]:
        logger.info(f"[{call_id}] Combined regulatory action review completed for NFA ID {nfa_id} with {len(combined_review['actions'])} actions")
    else:
        logger.info(f"[{call_id}] No regulatory actions found for NFA ID {nfa_id}; due diligence: NFA found {combined_review['due_diligence']['nfa_regulatory_actions']['records_found']}")
    return combined_review

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    while True:
        print("\nServices Secondary Interactive Menu:")
        print("1. Perform NFA ID-based regulatory action review")
        print("2. Perform NFA ID-based review with sample data (NFA ID: 0569081)")
        print("3. Exit")
        choice = input("Enter your choice (1-3): ").strip()
        if choice == "1":
            nfa_id = input("Enter NFA ID: ").strip()
            if not nfa_id:
                print("NFA ID is required.")
                continue
            employee_number = input("Enter employee number (optional, press Enter to skip): ").strip() or None
            print(f"\nPerforming regulatory action review for NFA ID '{nfa_id}'...")
            result = perform_regulatory_action_review(nfa_id, employee_number)
            print("\nResult:")
            print(json.dumps(result, indent=2))
        elif choice == "2":
            nfa_id = "0569081"
            employee_number = "EN-046143"
            print(f"\nPerforming regulatory action review for NFA ID '{nfa_id}' with employee number '{employee_number}'...")
            result = perform_regulatory_action_review(nfa_id, employee_number)
            print("\nResult:")
            print(json.dumps(result, indent=2))
        elif choice == "3":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")