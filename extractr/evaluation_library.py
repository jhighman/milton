# evaluation_library.py

import re
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
    """Evaluate license compliance.

    If csv_license is provided, compare it with bc_scope and ia_scope.
    If csv_license is not provided, check if the individual has any active license (bc_scope or ia_scope is 'active').
    If no active license is found, return False and generate an alert.
    """
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
    """Compare the passed exams against the requirements for the license type and generate an alert if exams don't meet the requirements."""
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

def evaluate_registration_status(individual_info: Dict[str, Any]) -> Tuple[bool, List[Alert]]:
    """Evaluate the registration status of the individual and generate alerts if the status is concerning."""
    bc_status = individual_info.get('ind_bc_scope', '').lower()
    ia_status = individual_info.get('ind_ia_scope', '').lower()

    concerning_statuses = ['inactive', 'temp_wd', 'pending', 't_noreg', 'tempreg', 'restricted']

    alerts = []
    status_compliant = True  # Renamed from status_ok

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
    """Generate a description for a regulatory disclosure."""
    initiated_by = details.get('Initiated By', 'Unknown')
    allegations = details.get('Allegations', 'Not specified')
    sanctions_list = details.get('SanctionDetails', [])
    sanctions = ', '.join([s.get('Sanctions', '') for s in sanctions_list])
    
    return f"Regulatory action on {event_date} initiated by {initiated_by}. Resolution: {resolution}. Allegations: {allegations}. Sanctions: {sanctions}"

def generate_customer_dispute_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    """Generate a description for a customer dispute disclosure."""
    allegations = details.get('Allegations', 'Not specified')
    damage_requested = details.get('Damage Amount Requested', 'Not specified')
    settlement_amount = details.get('Settlement Amount', 'Not specified')
    
    return f"Customer dispute on {event_date}. Resolution: {resolution}. Allegations: {allegations}. Damage requested: {damage_requested}. Settlement: {settlement_amount}"

def generate_criminal_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    """Generate a description for a criminal disclosure."""
    charges_list = details.get('criminalCharges', [])
    charges = ', '.join([charge.get('Charges', '') for charge in charges_list])
    disposition = ', '.join([charge.get('Disposition', '') for charge in charges_list])
    
    return f"Criminal disclosure on {event_date}. Resolution: {resolution}. Charges: {charges}. Disposition: {disposition}"

def generate_civil_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    """Generate a description for a civil disclosure."""
    allegations = details.get('Allegations', 'Not specified')
    disposition = details.get('Disposition', 'Not specified')
    
    return f"Civil disclosure on {event_date}. Resolution: {resolution}. Allegations: {allegations}. Disposition: {disposition}"
