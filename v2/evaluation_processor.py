"""
evaluation_processor.py

This module provides a cohesive set of functions for evaluating an individual's
financial regulatory compliance. It is designed to integrate with a Builder/Director pattern.

The module includes functions for:
  - Parsing and comparing names
  - Evaluating name match quality
  - Evaluating license compliance
  - Evaluating exam requirements
  - Evaluating registration status
  - Evaluating disclosures
  - Evaluating arbitrations
  - Evaluating disciplinary records
  - Mapping alert types to standardized alert categories

All functions return standardized data structures, and Alert objects are defined
as dataclasses with a to_dict() method.
"""

import json
import re
import logging
from typing import Dict, Any, List, Set, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import jellyfish

logger = logging.getLogger(__name__)

# Alert and Severity Definitions
class AlertSeverity(Enum):
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"
    INFO = "INFO"

@dataclass
class Alert:
    alert_type: str
    severity: AlertSeverity
    metadata: Dict[str, Any]
    description: str
    alert_category: Optional[str] = field(default=None)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_type": self.alert_type,
            "alert_category": self.alert_category,
            "severity": self.severity.value,
            "metadata": self.metadata,
            "description": self.description
        }

# Constants and Helpers
VALID_EXAM_PATTERNS = [
    'Series 86/87', 'Series 9/10', 'Series 7TO', 'Series 99', 'Series 57',
    'Series 66', 'Series 65', 'Series 63', 'Series 82', 'Series 52', 'Series 53',
    'Series 51', 'Series 31', 'Series 28', 'Series 27', 'Series 26', 'Series 24',
    'Series 22', 'Series 50', 'Series 4', 'Series 3', 'Series 7', 'Series 6', 'SIE'
]
VALID_EXAM_PATTERNS.sort(key=len, reverse=True)

nickname_dict = {
    "john": {"jon", "johnny", "jack"},
    "robert": {"bob", "rob", "bobby", "bert"},
    "elizabeth": {"liz", "beth", "lizzy", "eliza"},
}
reverse_nickname_dict = {}
for formal_name, nicknames in nickname_dict.items():
    for nickname in nicknames:
        reverse_nickname_dict.setdefault(nickname, set()).add(formal_name)

def parse_name(name_input: Any) -> Dict[str, Optional[str]]:
    if isinstance(name_input, dict):
        return {
            "first": name_input.get("first"),
            "middle": name_input.get("middle"),
            "last": name_input.get("last")
        }
    if isinstance(name_input, str):
        parts = name_input.strip().split()
        if len(parts) == 0:
            return {"first": None, "middle": None, "last": None}
        elif len(parts) == 1:
            return {"first": parts[0], "middle": None, "last": None}
        elif len(parts) == 2:
            return {"first": parts[0], "middle": None, "last": parts[1]}
        else:
            return {"first": parts[0], "middle": " ".join(parts[1:-1]), "last": parts[-1]}
    return {"first": None, "middle": None, "last": None}

def get_passed_exams(exams: List[Dict[str, Any]]) -> Set[str]:
    passed_exams = set()
    for exam in exams:
        exam_category = exam.get('examCategory', '')
        for pattern in VALID_EXAM_PATTERNS:
            if re.search(pattern, exam_category, re.IGNORECASE):
                passed_exams.add(pattern)
                break
    return passed_exams

def get_name_variants(name: str) -> Set[str]:
    variants = {name.lower()}
    if name.lower() in nickname_dict:
        variants.update({n.lower() for n in nickname_dict[name.lower()]})
    if name.lower() in reverse_nickname_dict:
        variants.update({n.lower() for n in reverse_nickname_dict[name.lower()]})
    return variants

def are_nicknames(name1: str, name2: str) -> bool:
    variants1 = get_name_variants(name1)
    variants2 = get_name_variants(name2)
    return not variants1.isdisjoint(variants2)

def match_name_part(claim_part: Optional[str], fetched_part: Optional[str], name_type: str) -> float:
    if not claim_part and not fetched_part:
        return 1.0
    if not claim_part or not fetched_part:
        return 0.5 if name_type == "middle" else 0.0

    claim_part = claim_part.strip().lower()
    fetched_part = fetched_part.strip().lower()

    if claim_part == fetched_part:
        return 1.0

    if name_type == "first" and are_nicknames(claim_part, fetched_part):
        return 1.0

    if name_type in ("first", "middle"):
        if len(claim_part) == 1 and len(fetched_part) == 1 and claim_part[0] == fetched_part[0]:
            return 1.0

    if name_type == "last":
        distance = jellyfish.damerau_levenshtein_distance(claim_part, fetched_part)
        max_len = max(len(claim_part), len(fetched_part))
        similarity = 1.0 - (distance / max_len) if max_len else 0.0
        return similarity if similarity >= 0.8 else 0.0
    elif name_type == "first":
        distance = jellyfish.levenshtein_distance(claim_part, fetched_part)
        similarity = 1.0 - (distance / max(len(claim_part), len(fetched_part)))
        return similarity if similarity >= 0.85 else 0.0
    else:
        try:
            code1 = jellyfish.nysiis(claim_part)
            code2 = jellyfish.nysiis(fetched_part)
        except Exception:
            code1 = code2 = ""
        return 0.8 if code1 == code2 and code1 != "" else 0.0

def evaluate_name(expected_name: Any, fetched_name: Any, other_names: List[Any], score_threshold: float = 80.0) -> Tuple[Dict[str, Any], Optional[Alert]]:
    claim_name = parse_name(expected_name)

    def score_single_name(claim: Dict[str, Any], fetched: Any) -> Dict[str, Any]:
        fetched_parsed = parse_name(fetched)
        weights = {"first": 40, "middle": 10, "last": 50}
        if not claim["middle"] and not fetched_parsed["middle"]:
            weights["middle"] = 0
        first_score = match_name_part(claim["first"], fetched_parsed["first"], "first")
        middle_score = match_name_part(claim["middle"], fetched_parsed["middle"], "middle") if weights["middle"] else 0.0
        last_score = match_name_part(claim["last"], fetched_parsed["last"], "last")
        total_weight = sum(weights.values())
        total_score = (first_score * weights["first"] +
                       middle_score * weights["middle"] +
                       last_score * weights["last"])
        normalized_score = (total_score / total_weight) * 100.0 if total_weight else 0.0
        return {
            "fetched_name": fetched_parsed,
            "score": round(normalized_score, 2),
            "first_score": round(first_score, 2),
            "middle_score": round(middle_score, 2),
            "last_score": round(last_score, 2),
        }

    matches = []
    main_match = score_single_name(claim_name, fetched_name)
    matches.append({"name_source": "main_fetched_name", **main_match})
    for idx, alt in enumerate(other_names):
        alt_match = score_single_name(claim_name, alt)
        matches.append({"name_source": f"other_names[{idx}]", **alt_match})

    best_match = max(matches, key=lambda x: x["score"])
    best_score = best_match["score"]
    compliance = best_score >= score_threshold

    evaluation_details = {
        "claimed_name": claim_name,
        "all_matches": matches,
        "best_match": best_match,
        "compliance": compliance
    }

    alert = None
    if not compliance:
        alert = Alert(
            alert_type="Name Mismatch",
            severity=AlertSeverity.MEDIUM,
            metadata={"expected_name": claim_name, "best_score": best_score, "best_match": best_match},
            description=f"Name match score {best_score} is below threshold {score_threshold}."
        )

    return evaluation_details, alert

def interpret_license_type(license_type: str) -> Tuple[bool, bool]:
    license_type = license_type.upper() if license_type else ""
    is_broker = 'B' in license_type
    is_ia = 'IA' in license_type
    return is_broker, is_ia

def compare_license_types(csv_license: str, bc_scope: str, ia_scope: str) -> bool:
    csv_broker, csv_ia = interpret_license_type(csv_license)
    api_broker = bc_scope.lower() == 'active'
    api_ia = ia_scope.lower() == 'active'
    return (csv_broker == api_broker) and (csv_ia == api_ia)

def evaluate_license(csv_license: str, bc_scope: str, ia_scope: str, name: str) -> Tuple[bool, Optional[Alert]]:
    api_broker_active = bc_scope.lower() == 'active'
    api_ia_active = ia_scope.lower() == 'active'
    if not csv_license:
        if not api_broker_active and not api_ia_active:
            alert = Alert(
                alert_type="No Active Licenses Found",
                severity=AlertSeverity.HIGH,
                metadata={"bc_scope": bc_scope, "ia_scope": ia_scope},
                description=f"No active licenses found for {name}."
            )
            return False, alert
        return True, None
    else:
        compliant = compare_license_types(csv_license, bc_scope, ia_scope)
        if not compliant:
            alert = Alert(
                alert_type="License Compliance Alert",
                severity=AlertSeverity.HIGH,
                metadata={"csv_license": csv_license, "bc_scope": bc_scope, "ia_scope": ia_scope},
                description=f"License compliance failed for {name}."
            )
            return False, alert
        return True, None

def check_exam_requirements(passed_exams: Set[str]) -> Dict[str, bool]:
    ia_requirement = ('Series 65' in passed_exams) or ('Series 66' in passed_exams)
    broker_requirement = ('Series 7' in passed_exams) and (('Series 63' in passed_exams) or ('Series 66' in passed_exams))
    return {"Investment Advisor": ia_requirement, "Broker": broker_requirement}

def evaluate_exams(passed_exams: Set[str], license_type: str, name: str) -> Tuple[bool, Optional[Alert]]:
    requirements = check_exam_requirements(passed_exams)
    csv_broker, csv_ia = interpret_license_type(license_type)
    exam_compliant = True
    missing_roles = []
    if csv_broker and not requirements.get("Broker", False):
        exam_compliant = False
        missing_roles.append("Broker")
    if csv_ia and not requirements.get("Investment Advisor", False):
        exam_compliant = False
        missing_roles.append("Investment Advisor")
    if not exam_compliant:
        alert = Alert(
            alert_type="Exam Requirement Alert",
            severity=AlertSeverity.MEDIUM,
            metadata={"passed_exams": list(passed_exams), "missing_roles": missing_roles},
            description=f"{name} is missing required exams for: {', '.join(missing_roles)}."
        )
        return False, alert
    return True, None

def evaluate_registration_status(individual_info: Dict[str, Any]) -> Tuple[bool, List[Alert]]:
    alerts = []
    status_compliant = True

    bc_status = (individual_info.get('ind_bc_scope') or individual_info.get('bcScope', '')).lower()
    ia_status = (individual_info.get('ind_ia_scope') or individual_info.get('iaScope', '')).lower()

    if not bc_status or not ia_status:
        content = individual_info.get('content')
        if content:
            try:
                content_data = json.loads(content)
                basic_info = content_data.get('basicInformation', {})
                bc_status = bc_status or basic_info.get('bcScope', '').lower()
                ia_status = ia_status or basic_info.get('iaScope', '').lower()
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse content for registration status: {e}")

    concerning_statuses = ['inactive', 'temp_wd', 'pending', 't_noreg', 'tempreg', 'restricted']

    if bc_status in concerning_statuses:
        alerts.append(Alert(
            alert_type="Registration Status Alert",
            severity=AlertSeverity.HIGH,
            metadata={"bc_status": bc_status},
            description=f"Broker registration status is {bc_status}."
        ))
        status_compliant = False

    if ia_status in concerning_statuses:
        alerts.append(Alert(
            alert_type="Registration Status Alert",
            severity=AlertSeverity.HIGH,
            metadata={"ia_status": ia_status},
            description=f"Investment Advisor registration status is {ia_status}."
        ))
        status_compliant = False

    return status_compliant, alerts

def generate_regulatory_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    initiated_by = details.get('Initiated By', 'Unknown')
    allegations = details.get('Allegations', 'Not specified')
    sanctions_list = details.get('SanctionDetails', [])
    sanctions = ', '.join([s.get('Sanctions', '') for s in sanctions_list])
    return (f"Regulatory action on {event_date} initiated by {initiated_by}. "
            f"Resolution: {resolution}. Allegations: {allegations}. Sanctions: {sanctions}")

def generate_customer_dispute_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    allegations = details.get('Allegations', 'Not specified')
    damage_requested = details.get('Damage Amount Requested', 'Not specified')
    settlement_amount = details.get('Settlement Amount', 'Not specified')
    return (f"Customer dispute on {event_date}. Resolution: {resolution}. Allegations: {allegations}. "
            f"Damage requested: {damage_requested}. Settlement: {settlement_amount}")

def generate_criminal_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    charges_list = details.get('criminalCharges', [])
    charges = ', '.join([charge.get('Charges', '') for charge in charges_list])
    disposition = ', '.join([charge.get('Disposition', '') for charge in charges_list])
    return (f"Criminal disclosure on {event_date}. Resolution: {resolution}. Charges: {charges}. "
            f"Disposition: {disposition}")

def generate_civil_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    allegations = details.get('Allegations', 'Not specified')
    disposition = details.get('Disposition', 'Not specified')
    return (f"Civil disclosure on {event_date}. Resolution: {resolution}. Allegations: {allegations}. "
            f"Disposition: {disposition}")

def generate_disclosure_alert(disclosure: Dict[str, Any]) -> Optional[Alert]:
    disclosure_type = disclosure.get('disclosureType', 'Unknown')
    event_date = disclosure.get('eventDate', 'Unknown')
    resolution = disclosure.get('disclosureResolution', 'Unknown')
    details = disclosure.get('disclosureDetail', {})
    description = ""
    severity = AlertSeverity.HIGH

    if disclosure_type == 'Regulatory':
        description = generate_regulatory_alert_description(event_date, resolution, details)
        sanctions_list = details.get('SanctionDetails', [])
        combined_sanctions = " ".join([s.get('Sanctions', '') for s in sanctions_list]).lower()
        if "civil" in combined_sanctions:
            severity = AlertSeverity.HIGH
            description += " [Civil penalty detected.]"
    elif disclosure_type == 'Customer Dispute':
        description = generate_customer_dispute_alert_description(event_date, resolution, details)
    elif disclosure_type == 'Criminal':
        description = generate_criminal_alert_description(event_date, resolution, details)
    elif disclosure_type == 'Civil':
        description = generate_civil_alert_description(event_date, resolution, details)
    else:
        description = f"Unknown disclosure type {disclosure_type} on {event_date}."
    if description:
        return Alert(
            alert_type=f"{disclosure_type} Disclosure",
            severity=severity,
            metadata={"event_date": event_date, "resolution": resolution, "details": details},
            description=description
        )
    return None

def evaluate_disclosures(disclosures: List[Dict[str, Any]], name: str) -> Tuple[bool, Optional[str], List[Alert]]:
    alerts = []
    disclosure_counts = {}
    for disclosure in disclosures:
        dtype = disclosure.get('disclosureType', 'Unknown')
        disclosure_counts[dtype] = disclosure_counts.get(dtype, 0) + 1
        alert = generate_disclosure_alert(disclosure)
        if alert:
            alert.alert_category = determine_alert_category(alert.alert_type)
            alerts.append(alert)
    if disclosure_counts:
        summary_parts = [
            f"{count} {dtype.lower()} disclosure{'s' if count > 1 else ''}"
            for dtype, count in disclosure_counts.items()
        ]
        summary = f"{name} has {', '.join(summary_parts)}."
        return False, summary, alerts
    else:
        summary = f"No disclosures found for {name}."
        return True, summary, alerts

def evaluate_arbitration(actions: List[Dict[str, Any]], name: str, due_diligence: Optional[Dict[str, Any]] = None) -> Tuple[bool, str, List[Alert]]:
    alerts = []
    
    if actions:
        for arb in actions:
            case_id = arb.get('case_id', 'Unknown')
            status = arb.get('details', {}).get('status', arb.get('status', 'Unknown')).lower()
            outcome = arb.get('details', {}).get('action_type', arb.get('outcome', 'Unknown')).lower()
            if status == 'pending' or 'award' in outcome or 'adverse' in outcome:
                alert = Alert(
                    alert_type="Arbitration Alert",
                    severity=AlertSeverity.HIGH,
                    metadata={"arbitration": arb},
                    description=f"Arbitration issue found: Case {case_id} for {name}, status: {status}, outcome: {outcome}."
                )
                alert.alert_category = determine_alert_category(alert.alert_type)
                alerts.append(alert)
        if alerts:
            explanation = f"Arbitration issues found for {name}."
            return False, explanation, alerts

    if due_diligence:
        sec_dd = due_diligence.get("sec_arbitration", {})
        finra_dd = due_diligence.get("finra_arbitration", {})
        total_records = sec_dd.get("records_found", 0) + finra_dd.get("records_found", 0)
        total_filtered = sec_dd.get("records_filtered", 0) + finra_dd.get("records_filtered", 0)
        
        if total_records > 10 and total_filtered == total_records:
            alert = Alert(
                alert_type="Arbitration Search Info",
                severity=AlertSeverity.MEDIUM,
                metadata={"due_diligence": due_diligence},
                description=f"Found {total_records} arbitration records for {name}, all filtered out due to name mismatch. Potential review needed."
            )
            alert.alert_category = determine_alert_category(alert.alert_type)
            alerts.append(alert)
            explanation = f"No matching arbitration records found for {name}, but {total_records} records were reviewed and filtered, suggesting possible alias or data issues."
            return True, explanation, alerts
        elif total_records > 0:
            alert = Alert(
                alert_type="Arbitration Search Info",
                severity=AlertSeverity.INFO,
                metadata={"due_diligence": due_diligence},
                description=f"Found {total_records} arbitration records for {name}, {total_filtered} filtered out."
            )
            alert.alert_category = determine_alert_category(alert.alert_type)
            alerts.append(alert)
            explanation = f"No matching arbitration records found for {name}, {total_records} records reviewed with {total_filtered} filtered."
            return True, explanation, alerts

    explanation = f"No arbitration records found for {name}."
    return True, explanation, alerts

def evaluate_disciplinary(actions: List[Dict[str, Any]], name: str, due_diligence: Optional[Dict[str, Any]] = None) -> Tuple[bool, str, List[Alert]]:
    alerts = []
    
    if actions:
        for record in actions:
            case_id = record.get('case_id', 'Unknown')
            alert = Alert(
                alert_type="Disciplinary Alert",
                severity=AlertSeverity.HIGH,
                metadata={"record": record},
                description=f"Disciplinary record found: Case ID {case_id} for {name}."
            )
            alert.alert_category = determine_alert_category(alert.alert_type)
            alerts.append(alert)
        if alerts:
            explanation = f"Disciplinary records found for {name}."
            return False, explanation, alerts

    if due_diligence:
        sec_dd = due_diligence.get("sec_disciplinary", {})
        finra_dd = due_diligence.get("finra_disciplinary", {})
        total_records = sec_dd.get("records_found", 0) + finra_dd.get("records_found", 0)
        total_filtered = sec_dd.get("records_filtered", 0) + finra_dd.get("records_filtered", 0)
        
        if total_records > 10 and total_filtered == total_records:
            alert = Alert(
                alert_type="Disciplinary Search Info",
                severity=AlertSeverity.MEDIUM,
                metadata={"due_diligence": due_diligence},
                description=f"Found {total_records} disciplinary records for {name}, all filtered out due to name mismatch. Potential review needed."
            )
            alert.alert_category = determine_alert_category(alert.alert_type)
            alerts.append(alert)
            explanation = f"No matching disciplinary records found for {name}, but {total_records} records were reviewed and filtered, suggesting possible alias or data issues."
            return True, explanation, alerts
        elif total_records > 0:
            alert = Alert(
                alert_type="Disciplinary Search Info",
                severity=AlertSeverity.INFO,
                metadata={"due_diligence": due_diligence},
                description=f"Found {total_records} disciplinary records for {name}, {total_filtered} filtered out."
            )
            alert.alert_category = determine_alert_category(alert.alert_type)
            alerts.append(alert)
            explanation = f"No matching disciplinary records found for {name}, {total_records} records reviewed with {total_filtered} filtered."
            return True, explanation, alerts

    explanation = f"No disciplinary records found for {name}."
    return True, explanation, alerts

def determine_alert_category(alert_type: str) -> str:
    alert_type_lower = alert_type.lower()
    if 'exam' in alert_type_lower:
        return "EXAM"
    elif 'license' in alert_type_lower or 'registration' in alert_type_lower:
        return "LICENSE"
    elif 'disclosure' in alert_type_lower:
        return "DISCLOSURE"
    elif 'disciplinary' in alert_type_lower:
        return "DISCIPLINARY"
    elif 'arbitration' in alert_type_lower:
        return "ARBITRATION"
    elif 'name mismatch' in alert_type_lower:
        return "STATUS"
    else:
        return "STATUS"