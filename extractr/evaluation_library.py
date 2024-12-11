# evaluation_library.py

import json
import re
import logging
from typing import Dict, Any, List, Set, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

class AlertSeverity(Enum):
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"

@dataclass
class Alert:
    alert_type: str
    severity: AlertSeverity
    metadata: Dict[str, Any]
    description: str

    def to_dict(self):
        return {
            "alert_type": self.alert_type,
            "severity": self.severity.value,
            "metadata": self.metadata,
            "description": self.description
        }

VALID_EXAM_PATTERNS = [
    'Series 86/87', 'Series 9/10', 'Series 7TO', 'Series 99', 'Series 57',
    'Series 66', 'Series 65', 'Series 63', 'Series 82', 'Series 52', 'Series 53',
    'Series 51', 'Series 31', 'Series 28', 'Series 27', 'Series 26', 'Series 24',
    'Series 22', 'Series 50', 'Series 4', 'Series 3', 'Series 7', 'Series 6', 'SIE'
]

# Sort patterns by length in descending order
VALID_EXAM_PATTERNS.sort(key=len, reverse=True)

def get_passed_exams(exams: List[Dict[str, Any]]) -> Set[str]:
    """Accumulate a set of passed exams."""
    passed_exams = set()
    for exam in exams:
        exam_category = exam.get('examCategory', '')
        for pattern in VALID_EXAM_PATTERNS:
            if re.search(pattern, exam_category, re.IGNORECASE):
                passed_exams.add(pattern)
                break
    return passed_exams

def evaluate_name(expected_name: str, fetched_name: str, other_names: List[str]) -> Tuple[bool, Optional[Alert]]:
    """Compare the expected name with the fetched name and generate an alert if they don't match."""
    match = compare_names(expected_name, fetched_name, other_names)
    if not match:
        alert = Alert(
            alert_type="Name Mismatch",
            severity=AlertSeverity.MEDIUM,
            metadata={"expected_name": expected_name, "fetched_name": fetched_name, "other_names": other_names},
            description=f"Expected name '{expected_name}' did not match fetched name '{fetched_name}'."
        )
        return False, alert
    else:
        return True, None

def evaluate_license(csv_license: str, bc_scope: str, ia_scope: str, name: str) -> Tuple[bool, Optional[Alert]]:
    """Evaluate license compliance."""
    api_broker_active = bc_scope.lower() == 'active'
    api_ia_active = ia_scope.lower() == 'active'
    
    if not csv_license:
        # No license_type provided in the CSV
        if not api_broker_active and not api_ia_active:
            # No active licenses found
            alert = Alert(
                alert_type="No Active Licenses Found",
                severity=AlertSeverity.HIGH,
                metadata={"bc_scope": bc_scope, "ia_scope": ia_scope},
                description=f"No active licenses found for {name}."
            )
            return False, alert
        else:
            # At least one active license found
            return True, None
    else:
        # License type is provided; proceed to compare
        compliant = compare_license_types(csv_license, bc_scope, ia_scope)
        if not compliant:
            alert = Alert(
                alert_type="License Compliance Alert",
                severity=AlertSeverity.HIGH,
                metadata={"csv_license": csv_license, "bc_scope": bc_scope, "ia_scope": ia_scope},
                description=f"License compliance failed for {name}."
            )
            return False, alert
        else:
            return True, None

def evaluate_exams(passed_exams: Set[str], license_type: str, name: str) -> Tuple[bool, Optional[Alert]]:
    """Evaluate exam compliance."""
    requirements_met = check_exam_requirements(passed_exams)
    csv_broker, csv_ia = interpret_license_type(license_type)
    exam_compliance = True
    missing_roles = []
    if csv_broker and not requirements_met.get('Broker', False):
        exam_compliance = False
        missing_roles.append('Broker')
    if csv_ia and not requirements_met.get('Investment Advisor', False):
        exam_compliance = False
        missing_roles.append('Investment Advisor')
    if not exam_compliance:
        alert = Alert(
            alert_type="Exam Requirement Alert",
            severity=AlertSeverity.MEDIUM,
            metadata={"passed_exams": list(passed_exams), "missing_roles": missing_roles},
            description=f"{name} has not passed the required exams for the {', '.join(missing_roles)} role(s)."
        )
        return False, alert
    else:
        return True, None

def evaluate_registration_status(individual_info: dict) -> Tuple[bool, List[Alert]]:
    """
    Evaluate the registration status of the individual and generate alerts if the status is concerning.
    """
    alerts = []
    status_compliant = True

    bc_status = individual_info.get('ind_bc_scope') or individual_info.get('bcScope', '').lower()
    ia_status = individual_info.get('ind_ia_scope') or individual_info.get('iaScope', '').lower()

    if not bc_status or not ia_status:
        content = individual_info.get('content')
        if content:
            try:
                content_data = json.loads(content)
                basic_info = content_data.get('basicInformation', {})
                bc_status = bc_status or basic_info.get('bcScope', '').lower()
                ia_status = ia_status or basic_info.get('iaScope', '').lower()
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse `content`: {e}")

    concerning_statuses = ['inactive', 'temp_wd', 'pending', 't_noreg', 'tempreg', 'restricted']

    if bc_status in concerning_statuses:
        alert = Alert(
            alert_type="Registration Status Alert",
            severity=AlertSeverity.HIGH,
            metadata={"bc_status": bc_status},
            description=f"Broker registration status is {bc_status}."
        )
        alerts.append(alert)
        status_compliant = False

    if ia_status in concerning_statuses:
        alert = Alert(
            alert_type="Registration Status Alert",
            severity=AlertSeverity.HIGH,
            metadata={"ia_status": ia_status},
            description=f"Investment Advisor registration status is {ia_status}."
        )
        alerts.append(alert)
        status_compliant = False

    return status_compliant, alerts

def evaluate_disclosures(disclosures: List[Dict[str, Any]], name: str) -> Tuple[bool, Optional[str], List[Alert]]:
    """Evaluate disclosures and return compliance status, explanation, and any alerts."""
    alerts = []
    disclosure_counts = {}
    for disclosure in disclosures:
        disclosure_type = disclosure.get('disclosureType', 'Unknown')
        disclosure_counts[disclosure_type] = disclosure_counts.get(disclosure_type, 0) + 1
        alert = generate_disclosure_alert(disclosure)
        if alert:
            alerts.append(alert)
    if disclosure_counts:
        summary_parts = [f"{count} {dtype.lower()} disclosure{'s' if count > 1 else ''}" for dtype, count in disclosure_counts.items()]
        summary = f"{name} has {', '.join(summary_parts)}."
        disclosure_compliance = False
    else:
        summary = f"No disclosures found for {name}."
        disclosure_compliance = True
    return disclosure_compliance, summary, alerts

def compare_names(input_name: str, api_name: str, other_names: List[str]) -> bool:
    """Compare names for equality, checking alternate names as well."""
    def normalize_name(name: str) -> str:
        return ' '.join(name.lower().split())

    def match_names(name1: str, name2: str) -> bool:
        parts1 = name1.split()
        parts2 = name2.split()
        if name1 == name2:
            return True
        if len(parts1) >= 2 and len(parts2) >= 2:
            if (parts1[0] == parts2[0] and parts1[-1] == parts2[-1]) or \
               (parts1[0] == parts2[-1] and parts1[-1] == parts2[0]):
                return True
        return False

    input_name = normalize_name(input_name)
    api_name = normalize_name(api_name)
    if match_names(input_name, api_name):
        return True
    for other_name in other_names:
        other_name = normalize_name(other_name)
        if match_names(input_name, other_name):
            return True
    return False

def interpret_license_type(license_type: str) -> Tuple[bool, bool]:
    """Interpret the license type from the CSV."""
    license_type = license_type.upper()
    is_broker = 'B' in license_type
    is_ia = 'IA' in license_type
    return is_broker, is_ia

def compare_license_types(csv_license: str, bc_scope: str, ia_scope: str) -> bool:
    """Compare license types from the CSV with API data."""
    csv_broker, csv_ia = interpret_license_type(csv_license)
    api_broker = bc_scope.lower() == 'active'
    api_ia = ia_scope.lower() == 'active'
    return (csv_broker == api_broker) and (csv_ia == api_ia)

def check_exam_requirements(passed_exams: Set[str]) -> Dict[str, bool]:
    """Check if passed exams meet the requirements for the roles."""
    ia_exam_requirement = 'Series 65' in passed_exams or 'Series 66' in passed_exams
    broker_exam_requirement = 'Series 7' in passed_exams and ('Series 63' in passed_exams or 'Series 66' in passed_exams)
    return {
        'Investment Advisor': ia_exam_requirement,
        'Broker': broker_exam_requirement
    }

def generate_disclosure_alert(disclosure: Dict[str, Any]) -> Optional[Alert]:
    """Generate an alert based on the type of disclosure."""
    disclosure_type = disclosure.get('disclosureType', 'Unknown')
    event_date = disclosure.get('eventDate', 'Unknown')
    resolution = disclosure.get('disclosureResolution', 'Unknown')
    details = disclosure.get('disclosureDetail', {})
    description = ""
    severity = AlertSeverity.HIGH  # Assuming disclosures are high severity by default

    if disclosure_type == 'Regulatory':
        description = generate_regulatory_alert_description(event_date, resolution, details)
    elif disclosure_type == 'Customer Dispute':
        description = generate_customer_dispute_alert_description(event_date, resolution, details)
    elif disclosure_type == 'Criminal':
        description = generate_criminal_alert_description(event_date, resolution, details)
    elif disclosure_type == 'Civil':
        description = generate_civil_alert_description(event_date, resolution, details)
    else:
        description = f"Unknown disclosure type {disclosure_type} on {event_date}."
    
    if description:
        alert = Alert(
            alert_type=f"{disclosure_type} Disclosure",
            severity=severity,
            metadata={"event_date": event_date, "resolution": resolution, "details": details},
            description=description
        )
        return alert
    else:
        return None

def generate_regulatory_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    initiated_by = details.get('Initiated By', 'Unknown')
    allegations = details.get('Allegations', 'Not specified')
    sanctions_list = details.get('SanctionDetails', [])
    sanctions = ', '.join([s.get('Sanctions', '') for s in sanctions_list])
    
    return f"Regulatory action on {event_date} initiated by {initiated_by}. Resolution: {resolution}. Allegations: {allegations}. Sanctions: {sanctions}"

def generate_customer_dispute_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    allegations = details.get('Allegations', 'Not specified')
    damage_requested = details.get('Damage Amount Requested', 'Not specified')
    settlement_amount = details.get('Settlement Amount', 'Not specified')
    
    return f"Customer dispute on {event_date}. Resolution: {resolution}. Allegations: {allegations}. Damage requested: {damage_requested}. Settlement: {settlement_amount}"

def generate_criminal_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    charges_list = details.get('criminalCharges', [])
    charges = ', '.join([charge.get('Charges', '') for charge in charges_list])
    disposition = ', '.join([charge.get('Disposition', '') for charge in charges_list])
    
    return f"Criminal disclosure on {event_date}. Resolution: {resolution}. Charges: {charges}. Disposition: {disposition}"

def generate_civil_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    allegations = details.get('Allegations', 'Not specified')
    disposition = details.get('Disposition', 'Not specified')
    
    return f"Civil disclosure on {event_date}. Resolution: {resolution}. Allegations: {allegations}. Disposition: {disposition}"

######################
# New Arbitration Evaluation
######################

def evaluate_arbitration(arbitrations: List[Dict[str, Any]], name: str) -> Tuple[bool, Optional[str], List[Alert]]:
    """
    Evaluate arbitration records and return compliance status, explanation, and any alerts.
    According to specification:
    - If no arbitrations: compliant = True, no alert.
    - If pending or adverse outcome arbitrations: compliant = False, high severity alert.
    - Else: arbitrations exist but no concerns, compliant = True, no alert.
    """
    if not arbitrations:
        # No arbitrations found
        return True, f"No arbitrations found for {name}.", []

    pending_cases = []
    adverse_cases = []
    for arb in arbitrations:
        status = arb.get('status', '').lower()
        outcome = (arb.get('outcome', '') or '').lower()
        case_number = arb.get('case_number', 'Unknown')

        if status == 'pending':
            pending_cases.append(case_number)
        if outcome in ['award against individual', 'adverse finding']:
            adverse_cases.append(case_number)

    if pending_cases or adverse_cases:
        # Non-compliant scenario
        metadata = {}
        if pending_cases:
            metadata["pending_cases"] = pending_cases
        if adverse_cases:
            metadata["adverse_outcomes"] = adverse_cases

        alert_description = f"{name} has arbitration issues: "
        if pending_cases:
            alert_description += f"Pending cases: {', '.join(pending_cases)}. "
        if adverse_cases:
            alert_description += f"Adverse outcomes: {', '.join(adverse_cases)}."

        alert = Alert(
            alert_type="Arbitration Alert",
            severity=AlertSeverity.HIGH,
            metadata=metadata,
            description=alert_description.strip()
        )

        return False, f"Arbitration issues found for {name}.", [alert]
    else:
        # Arbitrations present but no pending/adverse issues
        return True, f"{name} has arbitration history but no pending or adverse outcomes.", []

def evaluate_disciplinary(disciplinary_records: List[Dict[str, Any]], name: str) -> Tuple[bool, Optional[str], List[Alert]]:
    """
    Evaluate disciplinary records and return compliance status, explanation, and any alerts.
    According to specification:
    - If no disciplinary records: compliant = True, no alert.
    - If any records with concerning outcomes: compliant = False, high severity alert.
    - Else: disciplinary records exist but no concerning outcomes, compliant = True, no alert.
    """
    if not disciplinary_records:
        # No disciplinary records found
        return True, f"No disciplinary records found for {name}.", []

    concerning_cases = []
    for record in disciplinary_records:
        document_type = record.get('Document Type', '').lower()
        case_summary = record.get('Case Summary', '').lower()
        case_id = record.get('Case ID', 'Unknown')

        if any(keyword in case_summary for keyword in ['suspension', 'revocation', 'barred', 'fine']):
            concerning_cases.append(case_id)

    if concerning_cases:
        # Non-compliant scenario
        metadata = {"concerning_cases": concerning_cases}
        alert_description = f"{name} has disciplinary issues: Cases {', '.join(concerning_cases)} with concerning outcomes."

        alert = Alert(
            alert_type="Disciplinary Alert",
            severity=AlertSeverity.HIGH,
            metadata=metadata,
            description=alert_description.strip()
        )

        return False, f"Disciplinary issues found for {name}.", [alert]
    else:
        # Disciplinary records present but no concerning outcomes
        return True, f"{name} has disciplinary history but no concerning outcomes.", []