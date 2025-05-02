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
  - Evaluating regulatory records (e.g., NFA)
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
from common_types import MatchThreshold
from services_secondary import perform_regulatory_action_review

logger = logging.getLogger("evaluation_processor")

# Alert and Severity Definitions
class AlertSeverity(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
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

def extract_suffix(name_part: str) -> Tuple[str, Optional[str]]:
    """Extract suffix from a name part if present."""
    suffixes = ["SR", "JR", "II", "III", "IV", "V"]
    name_part = name_part.strip()
    
    # Check for suffix at the end with space
    for suffix in suffixes:
        if name_part.upper().endswith(f" {suffix}"):
            return name_part[:-len(suffix)-1].strip(), suffix
    
    # Check for suffix at the end without space
    for suffix in suffixes:
        if name_part.upper().endswith(suffix) and len(name_part) > len(suffix):
            return name_part[:-len(suffix)].strip(), suffix
            
    return name_part, None

def parse_name(name: str) -> Dict[str, Optional[str]]:
    name = name.strip()
    parts = [part.strip() for part in name.split(",")] if "," in name else name.split()
    result = {"first": None, "middle": None, "last": None, "suffix": None}
    
    if len(parts) == 1:
        result["first"] = parts[0]
    elif len(parts) == 2:
        if "," in name:
            result["first"] = parts[1]
            last_part, suffix = extract_suffix(parts[0])
            result["last"] = last_part
            result["suffix"] = suffix
        else:
            result["first"] = parts[0]
            last_part, suffix = extract_suffix(parts[1])
            result["last"] = last_part
            result["suffix"] = suffix
    elif len(parts) >= 3:
        if "," in name:
            result["first"] = parts[1]
            result["middle"] = " ".join(parts[2:])
            last_part, suffix = extract_suffix(parts[0])
            result["last"] = last_part
            result["suffix"] = suffix
        else:
            result["first"] = parts[0]
            
            # Check if the last part contains a suffix
            last_part, suffix = extract_suffix(parts[-1])
            
            if suffix:
                result["last"] = last_part
                result["suffix"] = suffix
                if len(parts) > 2:
                    result["middle"] = " ".join(parts[1:-1])
            else:
                result["last"] = parts[-1]
                if len(parts) > 2:
                    # Check if the second-to-last part is a suffix
                    if len(parts) > 3 and parts[-2].upper() in ["SR", "JR", "II", "III", "IV", "V"]:
                        result["middle"] = " ".join(parts[1:-2])
                        result["suffix"] = parts[-2]
                    else:
                        result["middle"] = " ".join(parts[1:-1])
    
    return result

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
    # Include suffix in the searched name if present
    suffix_str = f" {claim_name['suffix']}" if claim_name["suffix"] else ""
    searched_name = " ".join(filter(None, [claim_name["first"], claim_name["middle"], claim_name["last"]])).strip() + suffix_str

    def score_single_name(claim: Dict[str, Any], fetched: Any) -> Tuple[str, float]:
        fetched_parsed = parse_name(fetched)
        weights = {"first": 40, "middle": 10, "last": 50, "suffix": 5}
        
        # Adjust weights if middle name is not present
        if not claim["middle"] and not fetched_parsed["middle"]:
            weights["middle"] = 0
            
        # Calculate scores for each name part
        first_score = match_name_part(claim["first"], fetched_parsed["first"], "first")
        middle_score = match_name_part(claim["middle"], fetched_parsed["middle"], "middle") if weights["middle"] else 0.0
        last_score = match_name_part(claim["last"], fetched_parsed["last"], "last")
        
        # Add suffix matching - exact match or both null gets 1.0, otherwise 0.0
        suffix_score = 1.0 if (claim["suffix"] == fetched_parsed["suffix"] or
                              (not claim["suffix"] and not fetched_parsed["suffix"])) else 0.0
        
        # Calculate total score with weights
        total_weight = sum(weights.values())
        total_score = (first_score * weights["first"] +
                      middle_score * weights["middle"] +
                      last_score * weights["last"] +
                      suffix_score * weights["suffix"])
        
        normalized_score = (total_score / total_weight) * 100.0 if total_weight else 0.0
        
        # Construct full name including suffix
        fetched_suffix = f" {fetched_parsed['suffix']}" if fetched_parsed["suffix"] else ""
        fetched_full_name = " ".join(filter(None, [fetched_parsed["first"],
                                                  fetched_parsed["middle"],
                                                  fetched_parsed["last"]])).strip() + fetched_suffix
        
        # Boost score if suffixes match and are present
        if claim["suffix"] and fetched_parsed["suffix"] and claim["suffix"] == fetched_parsed["suffix"]:
            normalized_score = min(100.0, normalized_score + 5.0)
            
        return fetched_full_name, round(normalized_score, 2)

    names_found = []
    name_scores = {}
    main_name, main_score = score_single_name(claim_name, fetched_name)
    names_found.append(main_name)
    name_scores[main_name] = main_score
    for alt in other_names:
        alt_name, alt_score = score_single_name(claim_name, alt)
        names_found.append(alt_name)
        name_scores[alt_name] = alt_score

    exact_match_found = any(score >= score_threshold for score in name_scores.values())
    status = "Exact matches found" if exact_match_found else f"Records found but no matches for '{searched_name}'"
    compliance = exact_match_found

    evaluation_details = {
        "searched_name": searched_name,
        "records_found": len(names_found),
        "records_filtered": 0,
        "names_found": names_found,
        "name_scores": name_scores,
        "exact_match_found": exact_match_found,
        "status": status
    }

    alert = None
    if not compliance:
        best_match_name = max(name_scores, key=name_scores.get)
        best_score = name_scores[best_match_name]
        alert = Alert(
            alert_type="Name Mismatch",
            severity=AlertSeverity.MEDIUM,
            metadata={"expected_name": searched_name, "best_score": best_score, "best_match": best_match_name},
            description=f"Name match score {best_score} for '{best_match_name}' is below threshold {score_threshold}.",
            alert_category=determine_alert_category("Name Mismatch")
        )

    return {
        "compliance": compliance,
        "compliance_explanation": "Name matches fetched record." if compliance else "Name does not match fetched record.",
        "due_diligence": evaluation_details,
        "alerts": [alert] if alert else []
    }, alert

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
                alert_type="License Compliance",
                severity=AlertSeverity.HIGH,
                metadata={"bc_scope": bc_scope, "csv_license": csv_license, "ia_scope": ia_scope},
                description=f"No active licenses found for {name}.",
                alert_category=determine_alert_category("License Compliance")
            )
            return False, alert
        return True, None
    else:
        compliant = compare_license_types(csv_license, bc_scope, ia_scope)
        if not compliant:
            alert = Alert(
                alert_type="License Compliance",
                severity=AlertSeverity.HIGH,
                metadata={"bc_scope": bc_scope, "csv_license": csv_license, "ia_scope": ia_scope},
                description=f"License compliance failed for {name}.",
                alert_category=determine_alert_category("License Compliance")
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
            alert_type="Exam Requirement",
            severity=AlertSeverity.MEDIUM,
            metadata={"passed_exams": list(passed_exams), "missing_roles": missing_roles},
            description=f"{name} is missing required exams for: {', '.join(missing_roles)}.",
            alert_category=determine_alert_category("Exam Requirement")
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
            alert_type="Registration Status",
            severity=AlertSeverity.HIGH,
            metadata={"bc_status": bc_status, "ia_status": ia_status},
            description=f"Broker registration status is {bc_status}.",
            alert_category=determine_alert_category("Registration Status")
        ))
        status_compliant = False
    if ia_status in concerning_statuses:
        alerts.append(Alert(
            alert_type="Registration Status",
            severity=AlertSeverity.HIGH,
            metadata={"bc_status": bc_status, "ia_status": ia_status},
            description=f"Investment Advisor registration status is {ia_status}.",
            alert_category=determine_alert_category("Registration Status")
        ))
        status_compliant = False
    return status_compliant, alerts

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

def generate_judgment_lien_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    amount = details.get('Judgment/Lien Amount', 'Not specified')
    lien_type = details.get('Judgment/Lien Type', 'Not specified')
    return f"Judgment/Lien disclosure on {event_date}. Resolution: {resolution}. Amount: {amount}. Type: {lien_type}"

def generate_financial_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    disposition = details.get('Disposition', 'Not specified')
    fin_type = details.get('Type', 'Not specified')
    return f"Financial disclosure on {event_date}. Resolution: {resolution}. Disposition: {disposition}. Type: {fin_type}"

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
            description += " [Civil penalty detected.]"
        return Alert(
            alert_type="Regulatory Disclosure",
            severity=severity,
            metadata={"details": details, "event_date": event_date, "resolution": resolution},
            description=description,
            alert_category=determine_alert_category("Regulatory Disclosure")
        )
    elif disclosure_type == 'Customer Dispute':
        description = generate_customer_dispute_alert_description(event_date, resolution, details)
        return Alert(
            alert_type="Customer Dispute Disclosure",
            severity=severity,
            metadata={"details": details, "event_date": event_date, "resolution": resolution},
            description=description,
            alert_category=determine_alert_category("Customer Dispute Disclosure")
        )
    elif disclosure_type == 'Criminal':
        description = generate_criminal_alert_description(event_date, resolution, details)
        return Alert(
            alert_type="Criminal Disclosure",
            severity=severity,
            metadata={"details": details, "event_date": event_date, "resolution": resolution},
            description=description,
            alert_category=determine_alert_category("Criminal Disclosure")
        )
    elif disclosure_type == 'Civil':
        description = generate_civil_alert_description(event_date, resolution, details)
        return Alert(
            alert_type="Civil Disclosure",
            severity=severity,
            metadata={"details": details, "event_date": event_date, "resolution": resolution},
            description=description,
            alert_category=determine_alert_category("Civil Disclosure")
        )
    elif disclosure_type == 'Judgment / Lien':
        description = generate_judgment_lien_alert_description(event_date, resolution, details)
        return Alert(
            alert_type="Judgment / Lien Disclosure",
            severity=severity,
            metadata={"details": details, "event_date": event_date, "resolution": resolution},
            description=description,
            alert_category=determine_alert_category("Judgment / Lien Disclosure")
        )
    elif disclosure_type == 'Financial':
        description = generate_financial_alert_description(event_date, resolution, details)
        return Alert(
            alert_type="Financial Disclosure",
            severity=severity,
            metadata={"details": details, "event_date": event_date, "resolution": resolution},
            description=description,
            alert_category=determine_alert_category("Financial Disclosure")
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
            alerts.append(alert)
    if disclosure_counts:
        summary_parts = [f"{count} {dtype.lower()} disclosure{'s' if count > 1 else ''}" for dtype, count in disclosure_counts.items()]
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
                    alert_type="Arbitration Compliance",
                    severity=AlertSeverity.HIGH,
                    metadata={"arbitration": arb},
                    description=f"Arbitration issue found: Case {case_id} for {name}, status: {status}, outcome: {outcome}.",
                    alert_category=determine_alert_category("Arbitration Compliance")
                )
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
                severity=AlertSeverity.INFO,
                metadata={"due_diligence": due_diligence},
                description=f"Found {total_records} arbitration records for {name}, all filtered out due to name mismatch. Potential review needed.",
                alert_category=determine_alert_category("Arbitration Search Info")
            )
            alerts.append(alert)
            explanation = f"No matching arbitration records found for {name}, but {total_records} records were reviewed and filtered, suggesting possible alias or data issues."
            return True, explanation, alerts
        elif total_records > 0:
            severity = AlertSeverity.MEDIUM if total_records > total_filtered else AlertSeverity.INFO
            alert = Alert(
                alert_type="Arbitration Search Info",
                severity=severity,
                metadata={"due_diligence": due_diligence},
                description=f"Found {total_records} arbitration records for {name}, {total_filtered} filtered out.",
                alert_category=determine_alert_category("Arbitration Search Info")
            )
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
                alert_type="Disciplinary Proceeding",
                severity=AlertSeverity.HIGH,
                metadata={"record": record},
                description=f"Disciplinary record found: Case ID {case_id} for {name}.",
                alert_category=determine_alert_category("Disciplinary Proceeding")
            )
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
                severity=AlertSeverity.INFO,
                metadata={"due_diligence": due_diligence},
                description=f"Found {total_records} disciplinary records for {name}, all filtered out due to name mismatch. Potential review needed.",
                alert_category=determine_alert_category("Disciplinary Search Info")
            )
            alerts.append(alert)
            explanation = f"No matching disciplinary records found for {name}, but {total_records} records were reviewed and filtered, suggesting possible alias or data issues."
            return True, explanation, alerts
        elif total_records > 0:
            severity = AlertSeverity.MEDIUM if total_records > total_filtered else AlertSeverity.INFO
            alert = Alert(
                alert_type="Disciplinary Search Info",
                severity=severity,
                metadata={"due_diligence": due_diligence},
                description=f"Found {total_records} disciplinary records for {name}, {total_filtered} filtered out.",
                alert_category=determine_alert_category("Disciplinary Search Info")
            )
            alerts.append(alert)
            explanation = f"No matching disciplinary records found for {name}, {total_records} records reviewed with {total_filtered} filtered."
            return True, explanation, alerts
    explanation = f"No disciplinary records found for {name}."
    return True, explanation, alerts

def evaluate_regulatory(actions: List[Dict[str, Any]], name: str, due_diligence: Optional[Dict[str, Any]] = None, employee_number: Optional[str] = None) -> Tuple[bool, str, List[Alert]]:
    logger.debug(f"evaluate_regulatory called with: name={name}, actions={actions}, due_diligence={due_diligence}, employee_number={employee_number}")
    alerts = []
    regulatory_found = False
    nfa_id_to_alert_index = {}

    if actions:
        for idx, record in enumerate(actions):
            nfa_id = record.get('nfa_id', 'Unknown')
            action_type = record.get('details', {}).get('action_type', 'Unknown')
            if action_type == "Regulatory":
                regulatory_found = True
                alert = Alert(
                    alert_type="Regulatory Disclosure",
                    severity=AlertSeverity.HIGH,
                    metadata={"record": record},
                    description=f"Regulatory action found: NFA ID {nfa_id} for {name}.",
                    alert_category=determine_alert_category("Regulatory Disclosure")
                )
                alerts.append(alert)
                nfa_id_to_alert_index[nfa_id] = len(alerts) - 1

    if regulatory_found:
        for nfa_id, alert_idx in nfa_id_to_alert_index.items():
            try:
                logger.info(f"Performing secondary NFA search for NFA ID {nfa_id} for {name}")
                if not employee_number:
                    logger.warning(f"employee_number is None for NFA ID {nfa_id}, using default 'UNKNOWN'")
                    employee_number = "UNKNOWN"  # TODO: Remove this once we have a way to get the employee number  
                secondary_result = perform_regulatory_action_review(nfa_id, employee_number)
                logger.debug(f"Secondary NFA search result for NFA ID {nfa_id}: {json_dumps_with_alerts(secondary_result, indent=2)}")
                if secondary_result and isinstance(secondary_result, dict):
                    secondary_actions = secondary_result.get("actions", [])
                    if secondary_actions:
                        alerts[alert_idx].metadata["secondary_regulatory_actions"] = secondary_actions
                        actions_summary = "; ".join(
                            f"Case {action['case_number']} on {action['effective_date']}: {', '.join(action['case_outcome'])}"
                            for action in secondary_actions
                        )
                        alerts[alert_idx].description += f" Details: {actions_summary}."
                    else:
                        logger.info(f"No secondary regulatory actions found for NFA ID {nfa_id}")
                        alerts[alert_idx].metadata["secondary_regulatory_actions"] = []
                else:
                    logger.warning(f"Secondary NFA search for NFA ID {nfa_id} returned malformed result: {secondary_result}")
                    alerts[alert_idx].metadata["secondary_regulatory_actions"] = []
            except Exception as e:
                logger.error(f"Failed to perform secondary NFA search for NFA ID {nfa_id}: {str(e)}")
                alerts[alert_idx].metadata["secondary_regulatory_actions"] = []
                alerts[alert_idx].description += " [Failed to retrieve detailed regulatory actions.]"

    if regulatory_found:
        explanation = f"Regulatory actions found for {name}."
        return False, explanation, alerts
    else:
        explanation = f"No regulatory actions found for {name}; only registration records present."
        if due_diligence:
            nfa_dd = due_diligence.get("nfa_regulatory_actions", due_diligence if "searched_name" in due_diligence else {})
            total_records = nfa_dd.get("records_found", 0)
            total_filtered = nfa_dd.get("records_filtered", 0)
            status = nfa_dd.get("status", "No records found")
            if "failure" in status.lower():
                alert = Alert(
                    alert_type="Regulatory Search Info",
                    severity=AlertSeverity.MEDIUM,
                    metadata={"due_diligence": due_diligence},
                    description=f"Regulatory record processing failed for {name}: {status}. Review raw data.",
                    alert_category=determine_alert_category("Regulatory Search Info")
                )
                alerts.append(alert)
                explanation = f"No matching regulatory records found for {name}, processing failed: {status}."
                return True, explanation, alerts
            if total_records > 10 and total_filtered == total_records:
                alert = Alert(
                    alert_type="Regulatory Search Info",
                    severity=AlertSeverity.INFO,
                    metadata={"due_diligence": due_diligence},
                    description=f"Found {total_records} regulatory records for {name}, all filtered out due to name mismatch. Potential review needed.",
                    alert_category=determine_alert_category("Regulatory Search Info")
                )
                alerts.append(alert)
                explanation = f"No matching regulatory records found for {name}, but {total_records} records were reviewed and filtered, suggesting possible alias or data issues."
                return True, explanation, alerts
            elif total_records > 0:
                severity = AlertSeverity.MEDIUM if total_records > total_filtered else AlertSeverity.INFO
                alert = Alert(
                    alert_type="Regulatory Search Info",
                    severity=severity,
                    metadata={"due_diligence": due_diligence},
                    description=f"Found {total_records} regulatory records for {name}, {total_filtered} filtered out.",
                    alert_category=determine_alert_category("Regulatory Search Info")
                )
                alerts.append(alert)
                explanation = f"No matching regulatory records found for {name}, {total_records} records reviewed with {total_filtered} filtered."
                return True, explanation, alerts
        return True, explanation, alerts

def determine_alert_category(alert_type: str) -> str:
    alert_type_to_category = {
        "Exam Requirement": "EXAM",
        "Registration Status": "REGISTRATION",
        "Criminal Disclosure": "DISCLOSURE",
        "Customer Dispute Disclosure": "DISCLOSURE",
        "Regulatory Disclosure": "DISCLOSURE",
        "Judgment / Lien Disclosure": "DISCLOSURE",
        "Financial Disclosure": "DISCLOSURE",
        "Civil Disclosure": "DISCLOSURE",
        "License Compliance": "LICENSE",
        "Disciplinary Proceeding": "DISCIPLINARY",
        "Arbitration Compliance": "ARBITRATION",
        "DueDiligenceNotPerformed": "status_evaluation",
        "IndividualNotFound": "status_evaluation",
        "Arbitration Search Info": "ARBITRATION",
        "Disciplinary Search Info": "DISCIPLINARY",
        "Regulatory Search Info": "REGULATORY",
        "Name Mismatch": "status_evaluation"
    }
    return alert_type_to_category.get(alert_type, "status_evaluation")

class AlertEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Alert objects."""
    def default(self, obj):
        if isinstance(obj, Alert):
            return obj.to_dict()
        return super().default(obj)

def json_dumps_with_alerts(obj: Any, **kwargs) -> str:
    """Helper function to serialize objects that may contain Alert instances."""
    return json.dumps(obj, cls=AlertEncoder, **kwargs)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    def print_result(result):
        print(json_dumps_with_alerts(result, indent=2))

    while True:
        print("\nEvaluation Processor Interactive Menu:")
        print("1. Evaluate Name Matching")
        print("2. Evaluate License Compliance")
        print("3. Evaluate Exam Requirements")
        print("4. Evaluate Registration Status")
        print("5. Evaluate Disclosures")
        print("6. Evaluate Arbitration Records")
        print("7. Evaluate Disciplinary Records")
        print("8. Evaluate Regulatory Records (with secondary NFA search)")
        print("9. Exit")
        choice = input("Enter your choice (1-9): ").strip()

        if choice == "1":
            expected_name = input("Enter expected name (e.g., 'John Doe'): ").strip()
            fetched_name = input("Enter fetched name (e.g., 'John Doe'): ").strip()
            other_names_input = input("Enter other names (comma-separated, e.g., 'Jon Doe, Johnny Doe') or press Enter to skip: ").strip()
            other_names = [name.strip() for name in other_names_input.split(",")] if other_names_input else []
            result, alert = evaluate_name(expected_name, fetched_name, other_names)
            print_result({"name_evaluation": result})

        elif choice == "2":
            csv_license = input("Enter CSV license type (e.g., 'B', 'IA', 'BIA', or empty): ").strip()
            bc_scope = input("Enter BrokerCheck scope (e.g., 'active', 'inactive'): ").strip()
            ia_scope = input("Enter IAPD scope (e.g., 'active', 'inactive'): ").strip()
            name = input("Enter individual name (e.g., 'John Doe'): ").strip()
            compliance, alert = evaluate_license(csv_license, bc_scope, ia_scope, name)
            result = {"license_evaluation": {"compliance": compliance, "alerts": [alert.to_dict()] if alert else []}}
            print_result(result)

        elif choice == "3":
            exams_input = input("Enter passed exams (comma-separated, e.g., 'Series 7, Series 63') or press Enter for none: ").strip()
            passed_exams = set(exams_input.split(",")) if exams_input else set()
            license_type = input("Enter license type (e.g., 'B', 'IA', 'BIA'): ").strip()
            name = input("Enter individual name (e.g., 'John Doe'): ").strip()
            compliance, alert = evaluate_exams(passed_exams, license_type, name)
            result = {"exam_evaluation": {"compliance": compliance, "alerts": [alert.to_dict()] if alert else []}}
            print_result(result)

        elif choice == "4":
            bc_status = input("Enter BrokerCheck status (e.g., 'active', 'inactive', 'pending'): ").strip()
            ia_status = input("Enter IAPD status (e.g., 'active', 'inactive', 'pending'): ").strip()
            individual_info = {"bcScope": bc_status, "iaScope": ia_status}
            compliance, alerts = evaluate_registration_status(individual_info)
            result = {"registration_evaluation": {"compliance": compliance, "alerts": [alert.to_dict() for alert in alerts]}}
            print_result(result)

        elif choice == "5":
            disclosures_input = input("Enter disclosures as JSON (e.g., '[{\"disclosureType\": \"Regulatory\", \"eventDate\": \"2023-01-01\", \"disclosureResolution\": \"Settled\", \"disclosureDetail\": {\"Initiated By\": \"SEC\"}}]') or press Enter for none: ").strip()
            disclosures = json.loads(disclosures_input) if disclosures_input else []
            name = input("Enter individual name (e.g., 'John Doe'): ").strip()
            compliance, summary, alerts = evaluate_disclosures(disclosures, name)
            result = {"disclosure_evaluation": {"compliance": compliance, "summary": summary, "alerts": [alert.to_dict() for alert in alerts]}}
            print_result(result)

        elif choice == "6":
            actions_input = input("Enter arbitration actions as JSON (e.g., '[{\"case_id\": \"123\", \"status\": \"pending\"}]') or press Enter for none: ").strip()
            actions = json.loads(actions_input) if actions_input else []
            name = input("Enter individual name (e.g., 'John Doe'): ").strip()
            due_diligence_input = input("Enter due diligence as JSON (e.g., '{\"sec_arbitration\": {\"records_found\": 5, \"records_filtered\": 5}}') or press Enter for none: ").strip()
            due_diligence = json.loads(due_diligence_input) if due_diligence_input else None
            compliance, explanation, alerts = evaluate_arbitration(actions, name, due_diligence)
            result = {"arbitration_evaluation": {"compliance": compliance, "explanation": explanation, "alerts": [alert.to_dict() for alert in alerts]}}
            print_result(result)

        elif choice == "7":
            actions_input = input("Enter disciplinary actions as JSON (e.g., '[{\"case_id\": \"456\"}]') or press Enter for none: ").strip()
            actions = json.loads(actions_input) if actions_input else []
            name = input("Enter individual name (e.g., 'John Doe'): ").strip()
            due_diligence_input = input("Enter due diligence as JSON (e.g., '{\"sec_disciplinary\": {\"records_found\": 3, \"records_filtered\": 3}}') or press Enter for none: ").strip()
            due_diligence = json.loads(due_diligence_input) if due_diligence_input else None
            compliance, explanation, alerts = evaluate_disciplinary(actions, name, due_diligence)
            result = {"disciplinary_evaluation": {"compliance": compliance, "explanation": explanation, "alerts": [alert.to_dict() for alert in alerts]}}
            print_result(result)

        elif choice == "8":
            actions_input = input("Enter regulatory actions as JSON (e.g., '[{\"nfa_id\": \"0569081\", \"details\": {\"action_type\": \"Regulatory\"}}]') or press Enter for none: ").strip()
            actions = json.loads(actions_input) if actions_input else []
            name = input("Enter individual name (e.g., 'Danny La'): ").strip()
            due_diligence_input = input("Enter due diligence as JSON (e.g., '{\"nfa_regulatory_actions\": {\"records_found\": 1, \"records_filtered\": 0}}') or press Enter for none: ").strip()
            due_diligence = json.loads(due_diligence_input) if due_diligence_input else None
            employee_number = input("Enter employee number (e.g., 'EN-046143') or press Enter to skip: ").strip() or None
            compliance, explanation, alerts = evaluate_regulatory(actions, name, due_diligence, employee_number)
            result = {"regulatory_evaluation": {"compliance": compliance, "explanation": explanation, "alerts": [alert.to_dict() for alert in alerts]}}
            print_result(result)

        elif choice == "9":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 9.")