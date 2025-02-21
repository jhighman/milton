from typing import Dict, Any, Tuple
import logging
from services import FinancialServicesFacade

# Logger setup for functional use
logger = logging.getLogger("business")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s'))
logger.addHandler(handler)

def search_sec_with_broker_fallback(
    claim: Dict[str, Any],
    facade: FinancialServicesFacade,
    employee_number: str = None
) -> Dict[str, Any]:
    """Associate an SEC Arbitration result with a claim, using BrokerCheck or claim name.

    Searches FINRA BrokerCheck with the claim's CRD to get an individual's name. If hits are found,
    uses that name for SEC Arbitration; otherwise, uses the claim's provided name. Returns a new
    claim with the SEC result and source flag.

    Args:
        claim (Dict[str, Any]): The claim object with 'crd', 'first_name', and 'last_name'.
        facade (FinancialServicesFacade): The facade instance for service calls.
        employee_number (str, optional): Identifier for logging context. Defaults to None.

    Returns:
        Dict[str, Any]: Updated claim with:
            - 'source': 'BrokerCheck' or 'Provided' indicating name origin.
            - 'sec_result': SEC Arbitration result.
            - Original claim fields preserved.

    Notes:
        - Example claim input:
            - crd: "12345"
            - first_name: "John"
            - last_name: "Doe"
        - Example output if BrokerCheck finds a hit:
            - crd: "12345"
            - first_name: "John"
            - last_name: "Doe"
            - source: "BrokerCheck"
            - sec_result:
                - first_name: "Jane"
                - last_name: "Smith"
                - result: [...]  # SEC Arbitration result
        - Example output if BrokerCheck has no hits:
            - crd: "12345"
            - first_name: "John"
            - last_name: "Doe"
            - source: "Provided"
            - sec_result:
                - first_name: "John"
                - last_name: "Doe"
                - result: "No Results Found"
    """
    crd = claim.get("crd", "")
    fallback_first_name = claim.get("first_name", "")
    fallback_last_name = claim.get("last_name", "")

    logger.info(f"Processing claim with CRD: {crd}, Fallback Name: {fallback_first_name} {fallback_last_name}")

    # Search FINRA BrokerCheck
    broker_result = facade.search_finra_brokercheck_individual(crd, employee_number)

    # Determine name and source for SEC search
    if broker_result and "hits" in broker_result and broker_result["hits"]["total"] > 0:
        hit = broker_result["hits"]["hits"][0]["_source"]
        first_name = hit.get("ind_firstname", "")
        last_name = hit.get("ind_lastname", "")
        source = "BrokerCheck"
        logger.info(f"Extracted name from BrokerCheck: {first_name} {last_name}")
    else:
        first_name = fallback_first_name
        last_name = fallback_last_name
        source = "Provided"
        logger.warning(f"No hits in BrokerCheck for CRD: {crd}, using claim name: {first_name} {last_name}")

    # Search SEC Arbitration
    sec_result = facade.search_sec_arbitration(first_name, last_name, employee_number)

    # Return a new claim with SEC result and source
    return {
        **claim,  # Preserve original claim fields
        "source": source,
        "sec_result": sec_result
    }