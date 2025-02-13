# evaluation_library.py

import json
import re
import logging
from typing import Dict, Any, List, Set, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

import jellyfish  # Provides jaro_winkler, damerau_levenshtein_distance, nysiis

class AlertSeverity(Enum):
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"
    INFO = "INFO"  # This line ensures we can use AlertSeverity.INFO

@dataclass
class Alert:
    alert_type: str
    severity: AlertSeverity
    metadata: Dict[str, Any]
    description: str

    # NEW: Add an optional field for the alert category; default is None.
    alert_category: Optional[str] = field(default=None)

    def to_dict(self):
        return {
            "alert_type": self.alert_type,
            # If category was not assigned yet, it will remain None (or become set externally)
            "alert_category": self.alert_category,
            "severity": self.severity.value,
            "metadata": self.metadata,
            "description": self.description
        }

############################################################
# Below here, your existing logic remains mostly unchanged
############################################################

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
    """Convert a name input (string or dict) into a standardized dict with keys: 'first', 'middle', 'last'."""
    if isinstance(name_input, dict):
        return {
            "first": name_input.get("first"),
            "middle": name_input.get("middle"),
            "last": name_input.get("last"),
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
            return {
                "first": parts[0],
                "middle": " ".join(parts[1:-1]),
                "last": parts[-1],
            }

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

def get_name_variants(name: str) -> set:
    variants = {name}
    if name in nickname_dict:
        variants.update(nickname_dict[name])
    if name in reverse_nickname_dict:
        variants.update(reverse_nickname_dict[name])
    return variants

def are_nicknames(name1: str, name2: str) -> bool:
    variants1 = get_name_variants(name1)
    variants2 = get_name_variants(name2)
    return not variants1.isdisjoint(variants2)

def match_name_part(
    claim_part: Optional[str],
    corroborating_part: Optional[str],
    name_type: str
) -> float:
    if not claim_part and not corroborating_part:
        return 1.0
    if not claim_part or not corroborating_part:
        if name_type == "middle":
            return 0.5
        else:
            return 0.0

    claim_part = claim_part.strip().lower()
    corroborating_part = corroborating_part.strip().lower()

    if claim_part == corroborating_part:
        return 1.0

    if name_type == "first":
        if are_nicknames(claim_part, corroborating_part):
            return 1.0

    if name_type in ("first", "middle"):
        if len(claim_part) == 1 and claim_part[0] == corroborating_part[0]:
            return 1.0
        if len(corroborating_part) == 1 and corroborating_part[0] == claim_part[0]:
            return 1.0

    if name_type == "last":
        distance = jellyfish.damerau_levenshtein_distance(claim_part, corroborating_part)
        max_len = max(len(claim_part), len(corroborating_part))
        similarity = 1.0 - (distance / max_len) if max_len else 0.0
        return similarity if similarity >= 0.8 else 0.0
    elif name_type == "first":
        distance = jellyfish.levenshtein_distance(claim_part, corroborating_part)
        similarity = 1.0 - distance
        return similarity if similarity >= 0.85 else 0.0
    else:
        try:
            code1 = jellyfish.nysiis(claim_part)
            code2 = jellyfish.nysiis(corroborating_part)
        except Exception:
            code1 = code2 = ""
        if code1 == code2 and code1 != "":
            return 0.8
        return 0.0

def evaluate_name(
    expected_name: Any,
    fetched_name: Any,
    other_names: List[Any],
    score_threshold: float = 80.0
) -> Tuple[Dict[str, Any], Optional[Alert]]:
    claim_name = parse_name(expected_name)

    def score_single_name(claim_dict, fetched_input) -> Dict[str, Any]:
        corr_name = parse_name(fetched_input)
        
        weights = {"last": 50, "first": 40, "middle": 10}
        if not claim_dict["middle"] and not corr_name["middle"]:
            weights["middle"] = 0

        last_score = match_name_part(claim_dict["last"], corr_name["last"], "last")
        first_score = match_name_part(claim_dict["first"], corr_name["first"], "first")
        middle_score = 0.0
        if weights["middle"] != 0:
            middle_score = match_name_part(claim_dict["middle"], corr_name["middle"], "middle")

        total_score = (last_score * weights["last"]) \
                      + (first_score * weights["first"]) \
                      + (middle_score * weights["middle"])
        total_possible_weight = sum(weights.values())
        normalized_score = 0.0
        if total_possible_weight > 0:
            normalized_score = (total_score / total_possible_weight) * 100.0

        return {
            "fetched_name": corr_name,
            "score": round(normalized_score, 2),
            "first_score": round(first_score, 2),
            "middle_score": round(middle_score, 2),
            "last_score": round(last_score, 2),
        }

    all_matches = []
    main_result = score_single_name(claim_name, fetched_name)
    all_matches.append({"name_source": "main_fetched_name", **main_result})

    for idx, alt_name in enumerate(other_names):
        alt_result = score_single_name(claim_name, alt_name)
        all_matches.append({"name_source": f"other_names[{idx}]", **alt_result})

    best_match = max(all_matches, key=lambda x: x["score"])
    best_score = best_match["score"]
    name_compliance = best_score >= score_threshold

    evaluation_details = {
        "claimed_name": claim_name,
        "all_matches": all_matches,
        "best_match": {
            "name_source": best_match["name_source"],
            "fetched_name": best_match["fetched_name"],
            "score": best_score
        },
        "compliance": name_compliance
    }

    alert = None
    if not name_compliance:
        alert_description = (
            f"The highest name match score was {best_score:.2f}, "
            f"below threshold {score_threshold:.2f}."
        )
        alert = Alert(
            alert_type="Name Mismatch",
            severity=AlertSeverity.MEDIUM,
            metadata={
                "expected_name": expected_name,
                "score": best_score,
                "best_match_source": best_match["name_source"],
                "best_fetched_name": best_match["fetched_name"],
            },
            description=alert_description
        )

    return evaluation_details, alert

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
        else:
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
        else:
            return True, None

def evaluate_exams(passed_exams: Set[str], license_type: str, name: str) -> Tuple[bool, Optional[Alert]]:
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

def evaluate_disclosures(disclosures: List[Dict[str, Any]], name: str) -> Tuple[bool, Optional[str], List[Alert]]:
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
    license_type = license_type.upper()
    is_broker = 'B' in license_type
    is_ia = 'IA' in license_type
    return is_broker, is_ia

def compare_license_types(csv_license: str, bc_scope: str, ia_scope: str) -> bool:
    csv_broker, csv_ia = interpret_license_type(csv_license)
    api_broker = bc_scope.lower() == 'active'
    api_ia = ia_scope.lower() == 'active'
    return (csv_broker == api_broker) and (csv_ia == api_ia)

def check_exam_requirements(passed_exams: Set[str]) -> Dict[str, bool]:
    ia_exam_requirement = 'Series 65' in passed_exams or 'Series 66' in passed_exams
    broker_exam_requirement = 'Series 7' in passed_exams and ('Series 63' in passed_exams or 'Series 66' in passed_exams)
    return {
        'Investment Advisor': ia_exam_requirement,
        'Broker': broker_exam_requirement
    }

def generate_disclosure_alert(disclosure: Dict[str, Any]) -> Optional[Alert]:
    disclosure_type = disclosure.get('disclosureType', 'Unknown')
    event_date = disclosure.get('eventDate', 'Unknown')
    resolution = disclosure.get('disclosureResolution', 'Unknown')
    details = disclosure.get('disclosureDetail', {})
    description = ""
    severity = AlertSeverity.HIGH  # default

    if disclosure_type == 'Regulatory':
        # 1. Generate the standard regulatory description.
        description = generate_regulatory_alert_description(event_date, resolution, details)

        # 2. Check if the sanctions mention anything "civil".
        #    e.g. "Civil and Administrative Penalty(ies)/Fine(s)"
        sanctions_list = details.get('SanctionDetails', [])
        # Combine all 'Sanctions' fields into one lowercased string:
        combined_sanctions = " ".join(s.get('Sanctions', '') for s in sanctions_list).lower()

        if "civil" in combined_sanctions:
            # 3. Optionally override severity or the alert description to indicate a civil penalty
            severity = AlertSeverity.HIGH
            description += " [Detected Civil Penalty within Regulatory sanctions.]"

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
    return (f"Customer dispute on {event_date}. Resolution: {resolution}. "
            f"Allegations: {allegations}. Damage requested: {damage_requested}. "
            f"Settlement: {settlement_amount}")

def generate_criminal_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    charges_list = details.get('criminalCharges', [])
    charges = ', '.join([charge.get('Charges', '') for charge in charges_list])
    disposition = ', '.join([charge.get('Disposition', '') for charge in charges_list])
    return (f"Criminal disclosure on {event_date}. Resolution: {resolution}. "
            f"Charges: {charges}. Disposition: {disposition}")

def generate_civil_alert_description(event_date: str, resolution: str, details: Dict[str, Any]) -> str:
    allegations = details.get('Allegations', 'Not specified')
    disposition = details.get('Disposition', 'Not specified')
    return (f"Civil disclosure on {event_date}. Resolution: {resolution}. "
            f"Allegations: {allegations}. Disposition: {disposition}")

######################
# Arbitration Evaluation
######################

def evaluate_arbitration(arbitrations: List[Dict[str, Any]], name: str) -> Tuple[bool, Optional[str], List[Alert]]:
    """
    Checks a list of arbitration records for pending or adverse outcomes.
    Returns:
      (compliance_bool, explanation_string, [Alert objects])

    Example outcomes:
    - If no arbitrations found => (True, "No arbitrations found for <name>", [])
    - If some are pending or adverse => (False, "Arbitration issues found...", [Alert(...)])
    - Otherwise => (True, "<name> has arbitration history but no pending or adverse outcomes.", [])
    """

    # 1) No arbitrations => compliance = True, no alerts
    if not arbitrations:
        return True, f"No arbitrations found for {name}.", []

    # 2) Some are present; scan them for 'pending' or 'adverse' data
    pending_cases = []
    adverse_cases = []

    for arb in arbitrations:
        # If your arbitration records have different key names, adjust accordingly
        status = arb.get('status', '').lower()
        outcome = arb.get('outcome', '').lower()

        # Try both "case_number" or "Case ID" to unify your data shape
        case_number = (arb.get('case_number') 
                       or arb.get('Case ID') 
                       or 'Unknown')

        if status == 'pending':
            pending_cases.append(case_number)
        if outcome in ['award against individual', 'adverse finding']:
            adverse_cases.append(case_number)

    # 3) If any pending or adverse, we return compliance=False with an Alert
    if pending_cases or adverse_cases:
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

    # 4) Otherwise => compliance=True but you note they have some arbitration history
    return True, f"{name} has arbitration history but no pending or adverse outcomes.", []


######################
# Disciplinary Evaluation
######################

def evaluate_disciplinary(disciplinary_records: List[Dict[str, Any]], name: str) -> Tuple[bool, Optional[str], List[Alert]]:
    """
    Evaluate disciplinary records and return compliance status, explanation, and any alerts.
    
    According to specification:
    - If no disciplinary records: compliant = True, no alert.
    - If ANY disciplinary records exist: compliant = False, create alert.
    """
    if not disciplinary_records:
        return True, f"No disciplinary records found for {name}.", []

    # If we have any disciplinary records, check if they have results
    alerts = []
    has_records = False
    
    for record in disciplinary_records:
        # Skip if record has no results or empty results array
        if not record.get('result') and not record.get('results'):
            continue
            
        results = record.get('result', []) or record.get('results', [])
        if not results:
            continue
            
        has_records = True
        case_id = results[0].get('Case ID', 'Unknown') if results else 'Unknown'
        alert = Alert(
            alert_type="Disciplinary Alert",
            severity=AlertSeverity.HIGH,
            metadata={"record": record},
            description=f"Disciplinary record found: Case ID {case_id} for {name}."
        )
        alerts.append(alert)

    if not has_records:
        return True, f"No disciplinary records found for {name}.", []

    return (
        False,  # Only non-compliant if we found actual records
        f"Disciplinary records found for {name}.",
        alerts
    )

############################################################
# NEW: Category Mapping (Optional) - no scattering
############################################################

# Here's a sample helper to map alert_type -> alert_category
# You can expand or fine-tune this based on specificed taxonomy.
def determine_alert_category(alert_type: str) -> str:
    alert_type = alert_type.lower()
    if 'exam' in alert_type:
        return "EXAM"
    elif 'license' in alert_type or 'registration' in alert_type:
        return "LICENSE"
    elif 'disclosure' in alert_type:
        return "DISCLOSURE"
    elif 'disciplinary' in alert_type:
        return "DISCIPLINARY"
    elif 'arbitration' in alert_type:
        return "ARBITRATION"
    elif 'name mismatch' in alert_type:
        return "STATUS"
    else:
        return "STATUS"  # fallback or unknown
