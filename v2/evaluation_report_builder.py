from typing import Dict, Any, List
import logging
from evaluation_report_builder import EvaluationReportBuilder
from evaluation_processor import (
    evaluate_registration_status,
    evaluate_name,
    evaluate_license,
    evaluate_exams,
    evaluate_disclosures,
    evaluate_arbitration,
    evaluate_disciplinary,
    evaluate_regulatory,
    get_passed_exams,
    determine_alert_category,
)

logger = logging.getLogger('evaluation_report_director')

class EvaluationReportDirector:
    def __init__(self, builder: EvaluationReportBuilder):
        self.builder = builder

    def construct_evaluation_report(self, claim: Dict[str, Any], extracted_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Constructs a full evaluation report by performing all evaluation steps via evaluation_processor.py,
        short-circuiting if skip_reasons are present from upstream validation or processing failure.

        :param claim: A dictionary containing claim data (e.g., from a CSV row).
        :param extracted_info: A dictionary of normalized data, potentially including skip_reasons.
        :return: A complete evaluation report as an OrderedDict.
        """
        # Step 1: Set basic claim and search evaluation
        self.builder.set_claim(claim)
        search_evaluation = extracted_info.get("search_evaluation", {
            "source": "Unknown", 
            "search_strategy": "unknown", 
            "compliance": False, 
            "compliance_explanation": "No search performed."
        })
        self.builder.set_search_evaluation(search_evaluation)

        # Step 2: Check for skip reasons from upstream validation or processing failure
        skip_reasons = extracted_info.get("skip_reasons", [])
        due_diligence_alert = {
            "alert_type": "DueDiligenceNotPerformed",
            "severity": "High",
            "description": "Due diligence not performed due to record skip.",
            "alert_category": determine_alert_category("DueDiligenceNotPerformed")
        } if skip_reasons else None
        skip_explanation = f"Due diligence not performed: Record skipped due to {', '.join(skip_reasons)}"

        if skip_reasons:
            # Only search_evaluation is non-compliant for skips
            status_eval = {
                "compliance": True,
                "compliance_explanation": skip_explanation,
                "alerts": [due_diligence_alert]
            }
            name_eval = {
                "compliance": True,
                "compliance_explanation": skip_explanation,
                "evaluation_details": {},
                "alerts": [due_diligence_alert]
            }
            license_eval = {
                "compliance": True,
                "compliance_explanation": skip_explanation,
                "alerts": [due_diligence_alert]
            }
            exam_eval = {
                "compliance": True,
                "compliance_explanation": skip_explanation,
                "alerts": [due_diligence_alert]
            }
            disclosure_eval = {
                "compliance": True,
                "compliance_explanation": skip_explanation,
                "alerts": [due_diligence_alert]
            }
            disciplinary_eval = {
                "compliance": True,
                "compliance_explanation": skip_explanation,
                "actions": [],
                "alerts": [due_diligence_alert],
                "due_diligence": {}
            }
            arbitration_eval = {
                "compliance": True,
                "compliance_explanation": skip_explanation,
                "actions": [],
                "alerts": [due_diligence_alert],
                "due_diligence": {}
            }
            regulatory_eval = {
                "compliance": True,
                "compliance_explanation": skip_explanation,
                "actions": [],
                "alerts": [due_diligence_alert],
                "due_diligence": {}
            }
        else:
            # Full evaluation for non-skipped rows
            individual = extracted_info.get("individual", {})
            status_compliant, status_alerts = evaluate_registration_status(individual)
            for alert in status_alerts:
                alert.alert_category = determine_alert_category(alert.alert_type)
            status_eval = {
                "compliance": status_compliant,
                "compliance_explanation": "Registration status is valid." if status_compliant else "Registration status check failed.",
                "alerts": [alert.to_dict() for alert in status_alerts]
            }

            expected_name = f"{claim.get('first_name', '').strip()} {claim.get('last_name', '').strip()}"
            fetched_name = extracted_info.get("fetched_name", "")
            other_names = extracted_info.get("other_names", [])
            name_details, name_alert = evaluate_name(expected_name, fetched_name, other_names)
            name_eval = {
                "compliance": name_details.get("compliance", False),
                "compliance_explanation": "Name matches fetched record." if name_details.get("compliance", False) else "Name mismatch detected.",
                "evaluation_details": name_details
            }
            if name_alert:
                name_alert.alert_category = determine_alert_category(name_alert.alert_type)
                name_eval["alert"] = name_alert.to_dict()

            csv_license = claim.get("license_type", "")
            bc_scope = extracted_info.get("bc_scope", "NotInScope")
            ia_scope = extracted_info.get("ia_scope", "NotInScope")
            license_compliant, license_alert = evaluate_license(csv_license, bc_scope, ia_scope, expected_name)
            license_eval = {
                "compliance": license_compliant,
                "compliance_explanation": "License is active and compliant." if license_compliant else "License compliance failed."
            }
            if license_alert:
                license_alert.alert_category = determine_alert_category(license_alert.alert_type)
                license_eval["alert"] = license_alert.to_dict()

            exams = extracted_info.get("exams", [])
            passed_exams = get_passed_exams(exams)
            exam_compliant, exam_alert = evaluate_exams(passed_exams, csv_license, expected_name)
            exam_eval = {
                "compliance": exam_compliant,
                "compliance_explanation": "Required exams are passed." if exam_compliant else "Exam requirements not met."
            }
            if exam_alert:
                exam_alert.alert_category = determine_alert_category(exam_alert.alert_type)
                exam_eval["alert"] = exam_alert.to_dict()

            disclosures = extracted_info.get("disclosures", [])
            disclosure_compliant, disclosure_summary, disclosure_alerts = evaluate_disclosures(disclosures, expected_name)
            disclosure_eval = {
                "compliance": disclosure_compliant,
                "compliance_explanation": disclosure_summary,
                "alerts": [alert.to_dict() for alert in disclosure_alerts]
            }

            disciplinary_evaluation = extracted_info.get("disciplinary_evaluation", {})
            disciplinary_actions = disciplinary_evaluation.get("actions", [])
            disciplinary_compliant, disciplinary_explanation, disciplinary_alerts = evaluate_disciplinary(
                disciplinary_actions, expected_name, disciplinary_evaluation.get("due_diligence")
            )
            disciplinary_eval = {
                "compliance": disciplinary_compliant,
                "compliance_explanation": disciplinary_explanation,
                "actions": disciplinary_actions,
                "alerts": [alert.to_dict() for alert in disciplinary_alerts],
                "due_diligence": disciplinary_evaluation.get("due_diligence", {})
            }

            arbitration_evaluation = extracted_info.get("arbitration_evaluation", {})
            arbitration_actions = arbitration_evaluation.get("actions", [])
            arbitration_compliant, arbitration_explanation, arbitration_alerts = evaluate_arbitration(
                arbitration_actions, expected_name, arbitration_evaluation.get("due_diligence")
            )
            arbitration_eval = {
                "compliance": arbitration_compliant,
                "compliance_explanation": arbitration_explanation,
                "actions": arbitration_actions,
                "alerts": [alert.to_dict() for alert in arbitration_alerts],
                "due_diligence": arbitration_evaluation.get("due_diligence", {})
            }

            regulatory_evaluation = extracted_info.get("regulatory_evaluation", {})
            regulatory_actions = regulatory_evaluation.get("actions", [])
            regulatory_compliant, regulatory_explanation, regulatory_alerts = evaluate_regulatory(
                regulatory_actions, expected_name, regulatory_evaluation.get("due_diligence")
            )
            regulatory_eval = {
                "compliance": regulatory_compliant,
                "compliance_explanation": regulatory_explanation,
                "actions": regulatory_actions,
                "alerts": [alert.to_dict() for alert in regulatory_alerts],
                "due_diligence": regulatory_evaluation.get("due_diligence", {})
            }

        # Set all evaluations in the builder
        self.builder.set_status_evaluation(status_eval)
        self.builder.set_name_evaluation(name_eval)
        self.builder.set_license_evaluation(license_eval)
        self.builder.set_exam_evaluation(exam_eval)
        self.builder.set_disclosure_review(disclosure_eval)
        self.builder.set_disciplinary_evaluation(disciplinary_eval)
        self.builder.set_arbitration_review(arbitration_eval)
        self.builder.set_regulatory_evaluation(regulatory_eval)

        # Step 3: Final Evaluation
        overall_compliance = (
            search_evaluation.get("compliance", False) and
            status_eval.get("compliance", True) and
            name_eval.get("compliance", False) and
            license_eval.get("compliance", False) and
            exam_eval.get("compliance", False) and
            disclosure_eval.get("compliance", True) and
            disciplinary_eval.get("compliance", True) and
            arbitration_eval.get("compliance", True) and
            regulatory_eval.get("compliance", True)
        )
        all_alerts = (
            status_eval.get("alerts", []) +
            (name_eval.get("alerts", []) if "alerts" in name_eval else ([name_eval["alert"]] if name_eval.get("alert") else [])) +
            (license_eval.get("alerts", []) if "alerts" in license_eval else ([license_eval["alert"]] if license_eval.get("alert") else [])) +
            (exam_eval.get("alerts", []) if "alerts" in exam_eval else ([exam_eval["alert"]] if exam_eval.get("alert") else [])) +
            disclosure_eval.get("alerts", []) +
            disciplinary_eval.get("alerts", []) +
            arbitration_eval.get("alerts", []) +
            regulatory_eval.get("alerts", [])
        )
        overall_risk_level = "Low"
        for alert in all_alerts:
            severity = alert.get("severity", "").lower()
            if severity == "high":
                overall_risk_level = "High"
                break
            elif severity == "medium" and overall_risk_level != "High":
                overall_risk_level = "Medium"
        final_eval = {
            "overall_compliance": overall_compliance,
            "overall_risk_level": overall_risk_level,
            "recommendations": (
                "Immediate action required due to critical compliance issues."
                if not overall_compliance else "No immediate action required."
            ),
            "alerts": all_alerts
        }
        self.builder.set_final_evaluation(final_eval)

        return self.builder.build()