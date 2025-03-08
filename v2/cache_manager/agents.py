"""
==============================================
📌 AGENT NAME MODULE OVERVIEW
==============================================

🗂 PURPOSE
This module provides a class `AgentName` to enumerate valid agent names, ensuring consistency
across cache operations and preventing typos.

🗂 USAGE
Use agent names in cache operations:
    from cache_manager.agents import AgentName
    agent = AgentName.SEC_IAPD

🗂 AGENTS SUPPORTED
- SEC_IAPD: SEC Investment Adviser Public Disclosure
- FINRA_BROKERCHECK: FINRA BrokerCheck
- COMPLIANCE_REPORT: Special handling for compliance reports
==============================================
"""

class AgentName:
    """
    Enumerates valid agent names for cache operations.

    Attributes:
        SEC_IAPD (str): SEC Investment Adviser Public Disclosure agent.
        FINRA_BROKERCHECK (str): FINRA BrokerCheck agent.
        COMPLIANCE_REPORT (str): Compliance report agent (special handling).
        # Additional agents as per original list
    """
    SEC_IAPD = "SEC_IAPD_Agent"
    FINRA_BROKERCHECK = "FINRA_BrokerCheck_Agent"
    SEC_ARBITRATION = "SEC_Arbitration_Agent"
    FINRA_DISCIPLINARY = "FINRA_Disciplinary_Agent"
    NFA_BASIC = "NFA_Basic_Agent"
    FINRA_ARBITRATION = "FINRA_Arbitration_Agent"
    SEC_DISCIPLINARY = "SEC_Disciplinary_Agent"
    COMPLIANCE_REPORT = "ComplianceReportAgent"