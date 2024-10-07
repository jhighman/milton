import requests
import csv
import json
import re
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Dict, Any, List, Set, Optional, Tuple

class AlertSeverity(Enum):
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"

    def to_dict(self):
        return self.value

@dataclass
class Alert:
    alert_type: str
    severity: AlertSeverity
    metadata: Dict[str, Any]
    description: str

    def to_dict(self):
        return {
            "alert_type": self.alert_type,
            "severity": self.severity.value,  # Use .value instead of the enum itself
            "metadata": self.metadata,
            "description": self.description
        }

VALID_EXAM_PATTERNS = [
    'Series 86/87',
    'Series 9/10',
    'Series 7TO',
    'Series 99',
    'Series 57',
    'Series 66',
    'Series 65',
    'Series 63',
    'Series 82',
    'Series 52',
    'Series 53',
    'Series 51',
    'Series 31',
    'Series 28',
    'Series 27',
    'Series 26',
    'Series 24',
    'Series 22',
    'Series 50',
    'Series 4',
    'Series 3',
    'Series 7',
    'Series 6',
    'SIE',
]

# Sort patterns by length in descending order
VALID_EXAM_PATTERNS.sort(key=len, reverse=True)

def get_passed_exams(exams: List[Dict[str, Any]]) -> Set[str]:
    """
    Accumulate a set of passed exams.
    """
    passed_exams = set()
    for exam in exams:
        exam_category = exam.get('examCategory', '')
        for pattern in VALID_EXAM_PATTERNS:
            if re.search(pattern, exam_category, re.IGNORECASE):
                passed_exams.add(pattern)
                break
    return passed_exams

def ia_exam_requirement(passed_exams: Set[str]) -> bool:
    return 'Series 65' in passed_exams or 'Series 66' in passed_exams

def broker_exam_requirement(passed_exams: Set[str]) -> bool:
    return 'Series 7' in passed_exams and ('Series 63' in passed_exams or 'Series 66' in passed_exams)

EXAM_REQUIREMENTS = {
    'Investment Advisor': ia_exam_requirement,
    'Broker': broker_exam_requirement
}

def check_exam_requirements(passed_exams: Set[str]) -> Dict[str, bool]:
    """
    Check if the passed exams meet the requirements for each role.
    """
    return {role: requirement(passed_exams) for role, requirement in EXAM_REQUIREMENTS.items()}

def is_valid_exam_category(category: str) -> bool:
    return any(re.search(pattern, category, re.IGNORECASE) for pattern in VALID_EXAM_PATTERNS)

def validate_exams(exams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    invalid_exams = []
    for exam in exams:
        exam_category = exam.get('examCategory', '')
        if not is_valid_exam_category(exam_category):
            invalid_exams.append(exam)
    return invalid_exams

def generate_disclosure_alert(disclosure: Dict[str, Any]) -> Optional[Alert]:
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

def evaluate_name(expected_name: str, fetched_name: str, other_names: List[str]) -> Tuple[bool, Optional[Alert]]:
    """
    Compare the expected name with the fetched name and generate an alert if they don't match.
    """
    match = compare_names(expected_name, fetched_name, other_names)
    if not match:
        alert = Alert(
            alert_type="Name Mismatch",
            severity=AlertSeverity.MEDIUM,
            metadata={"expected_name": expected_name, "fetched_name": fetched_name, "other_names": other_names},
            description=f"Expected name {expected_name} did not match fetched name {fetched_name}."
        )
        return False, alert
    else:
        return True, None

def evaluate_license(csv_license: str, bc_scope: str, ia_scope: str, name: str) -> Tuple[bool, Optional[Alert]]:
    """
    Compare the CSV license type with the API license status and generate an alert if they don't match.
    """
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
    """
    Compare the passed exams against the requirements for the license type and generate an alert if exams don't meet the requirements.
    """
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

def evaluate_disclosures(disclosures: List[Dict[str, Any]], name: str) -> Tuple[List[Alert], Optional[str]]:
    """
    Process disclosures and generate alerts.
    Return a list of alerts and a summary string.
    """
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
    else:
        summary = None
    return alerts, summary

def evaluate_registration_status(individual_info: Dict[str, Any]) -> Tuple[bool, List[Alert]]:
    bc_status = individual_info.get('ind_bc_scope', '').lower()
    ia_status = individual_info.get('ind_ia_scope', '').lower()
    
    concerning_statuses = ['inactive', 'temp_wd', 'pending', 't_noreg', 'tempreg', 'restricted']
    
    alerts = []
    status_ok = True
    
    if bc_status in concerning_statuses:
        alert = Alert(
            alert_type="Registration Status Alert",
            severity=AlertSeverity.HIGH,
            metadata={"bc_status": bc_status},
            description=f"Broker registration status is {bc_status}."
        )
        alerts.append(alert)
        status_ok = False

    if ia_status in concerning_statuses:
        alert = Alert(
            alert_type="Registration Status Alert",
            severity=AlertSeverity.HIGH,
            metadata={"ia_status": ia_status},
            description=f"Investment Advisor registration status is {ia_status}."
        )
        alerts.append(alert)
        status_ok = False

    return status_ok, alerts

def get_individual_basic_info(crd_number):
    url = 'https://api.brokercheck.finra.org/search/individual'
    
    params = {
        'query': crd_number,
        'filter': 'active=true,prev=true,bar=true,broker=true,ia=true,brokeria=true',
        'includePrevious': 'true',
        'hl': 'true',
        'nrows': '12',
        'start': '0',
        'r': '25',
        'sort': 'score+desc',
        'wt': 'json'
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

def get_individual_detailed_info(crd_number):
    url = f'https://api.brokercheck.finra.org/search/individual/{crd_number}'
    
    params = {
        'hl': 'true',
        'includePrevious': 'true',
        'nrows': '12',
        'query': 'john',  # Placeholder
        'r': '25',
        'sort': 'bc_lastname_sort asc,bc_firstname_sort asc,bc_middlename_sort asc,score desc',
        'wt': 'json'
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

def compare_names(input_name: str, api_name: str, other_names: List[str]) -> bool:
    def normalize_name(name: str) -> str:
        return ' '.join(name.lower().split())
    
    def match_names(name1: str, name2: str) -> bool:
        parts1 = name1.split()
        parts2 = name2.split()
        
        # Exact match
        if name1 == name2:
            return True
        
        # Check if names match in either order (ignoring middle name)
        if len(parts1) >= 2 and len(parts2) >= 2:
            if (parts1[0] == parts2[0] and parts1[-1] == parts2[-1]) or \
               (parts1[0] == parts2[-1] and parts1[-1] == parts2[0]):
                return True
        
        return False

    input_name = normalize_name(input_name)
    api_name = normalize_name(api_name)

    # Check main name
    if match_names(input_name, api_name):
        return True

    # Check other names
    for other_name in other_names:
        other_name = normalize_name(other_name)
        if match_names(input_name, other_name):
            return True

    return False

def interpret_license_type(license_type: str) -> Tuple[bool, bool]:
    license_type = license_type.upper()
    is_broker = 'B' in license_type
    is_ia = 'IA' in license_type
    return is_broker, is_ia

def compare_license_types(csv_license: str, bc_scope: str, ia_scope: str) -> bool:
    csv_broker, csv_ia = interpret_license_type(csv_license)
    api_broker = bc_scope.lower() == 'active'
    api_ia = ia_scope.lower() == 'active'
    return (csv_broker == api_broker) and (csv_ia == api_ia)

def main(csv_file_path: str):
    final_reports = []
    with open(csv_file_path, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            alerts = []
            evaluation_report = {}
            try:
                crd_number = row['crd_number']
                last_name = row['last_name']
                first_name = row['first_name']
                name = f"{first_name} {last_name}"
                license_type = row['license_type']
            except KeyError as e:
                missing_key = str(e).strip("'")
                print(f"Warning: '{missing_key}' not found in row: {row}")
                continue  # Skip this row

            basic_info = get_individual_basic_info(crd_number)
            detailed_info = get_individual_detailed_info(crd_number)

            if basic_info and detailed_info:
                # Parse basic info
                if basic_info['hits']['hits']:
                    individual = basic_info['hits']['hits'][0]['_source']
                    fetched_name = f"{individual.get('ind_firstname', '')} {individual.get('ind_middlename', '')} {individual.get('ind_lastname', '')}".strip()
                    other_names = individual.get('ind_other_names', [])
                    bc_scope = individual.get('ind_bc_scope', '')
                    ia_scope = individual.get('ind_ia_scope', '')
                    # Name Verification
                    name_match, name_alert = evaluate_name(name, fetched_name, other_names)
                    evaluation_report['name'] = {
                        'expected_name': name,
                        'fetched_name': fetched_name,
                        'name_match': name_match,
                        'name_match_explanation': "" if name_match else "Expected name did not match fetched name."
                    }
                    if name_alert:
                        alerts.append(name_alert)
                    # License Compliance
                    license_compliant, license_alert = evaluate_license(license_type, bc_scope, ia_scope, name)
                    evaluation_report['license_verification'] = {
                        'license_compliance': license_compliant,
                        'license_compliance_explanation': "The individual holds an active license." if license_compliant else "License compliance failed."
                    }
                    if license_alert:
                        alerts.append(license_alert)
                    # Registration Status Check
                    status_ok, status_alerts = evaluate_registration_status(individual)
                    evaluation_report['registration_status'] = {
                        'status_alerts': not status_ok,
                        'status_summary': "Active registration found." if status_ok else "Registration status is concerning."
                    }
                    alerts.extend(status_alerts)
                    # Exam Evaluation
                    # Get passed exams
                    detailed_content = detailed_info['hits']['hits'][0]['_source'].get('content', '{}')
                    detailed_data = json.loads(detailed_content)
                    exams = detailed_data.get('stateExamCategory', []) + detailed_data.get('productExamCategory', [])
                    passed_exams = get_passed_exams(exams)
                    exam_compliant, exam_alert = evaluate_exams(passed_exams, license_type, name)
                    evaluation_report['exam_evaluation'] = {
                        'exam_compliance': exam_compliant,
                        'exam_compliance_explanation': "The individual has passed all required exams." if exam_compliant else "Exam compliance failed."
                    }
                    if exam_alert:
                        alerts.append(exam_alert)
                    # Disclosures Review
                    disclosures = detailed_data.get('disclosures', [])
                    disclosure_alerts, disclosure_summary = evaluate_disclosures(disclosures, name)
                    evaluation_report['disclosure_review'] = {
                        'disclosure_alerts': bool(disclosure_alerts),
                        'disclosure_review_summary': disclosure_summary if disclosure_summary else "No disclosures found."
                    }
                    alerts.extend(disclosure_alerts)
                    # Final Evaluation
                    overall_compliance = name_match and license_compliant and exam_compliant and status_ok
                    # Determine overall risk level based on alerts
                    if any(alert.severity == AlertSeverity.HIGH for alert in alerts):
                        overall_risk_level = "High"
                    elif any(alert.severity == AlertSeverity.MEDIUM for alert in alerts):
                        overall_risk_level = "Medium"
                    else:
                        overall_risk_level = "Low"
                    # Recommendations
                    if overall_risk_level == "High":
                        recommendations = "Immediate action required due to critical compliance issues."
                    elif overall_risk_level == "Medium":
                        recommendations = "Further review recommended due to potential compliance issues."
                    else:
                        recommendations = "No action needed."
                    evaluation_report['final_evaluation'] = {
                        'overall_compliance': overall_compliance,
                        'overall_risk_level': overall_risk_level,
                        'recommendations': recommendations,
                        'alerts': [alert.to_dict() for alert in alerts]
                    }
                    # Add to final reports
                    final_reports.append({
                        'crd_number': crd_number,
                        **evaluation_report
                    })
                else:
                    print(f"No basic information available for CRD number {crd_number}.")
            else:
                print(f"Failed to fetch information for CRD number {crd_number}")
    # Output the final reports
    print(json.dumps(final_reports, indent=2))

if __name__ == "__main__":
    csv_file_path = 'crd_numbers.csv'  # Update this to your CSV file path
    main(csv_file_path)
