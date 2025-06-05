from typing import Dict, Any, List
import logging
from evaluation_report_builder import EvaluationReportBuilder
from evaluation_processor import (
    evaluate_registration_status,
    evaluate_license,
    evaluate_exams,
    evaluate_employments,  # New: Added for employment evaluation
    evaluate_disclosures,
    evaluate_arbitration,
    evaluate_disciplinary,
    evaluate_regulatory,
    get_passed_exams,
    determine_alert_category,
    AlertSeverity,
    Alert
)
from name_matcher import evaluate_name

logger = logging.getLogger('evaluation_report_director')

class EvaluationReportDirector:
    def __init__(self, builder: EvaluationReportBuilder):
        self.builder = builder

    def construct_evaluation_report(self, claim: Dict[str, Any], extracted_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Constructs a full evaluation report by performing evaluation steps via evaluation_processor.py,
        applying skip logic, search failure logic, or full evaluation as needed. Evaluations include
        registration status, name, license, exams, employment history, disclosures, disciplinary actions,
        arbitration, and regulatory actions.

        :param claim: A dictionary containing claim data (e.g., from a CSV row), including employee_number.
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

        # Extract employee_number from claim
        employee_number = claim.get("employee_number", None)
        if not employee_number:
            logger.warning("employee_number not found in claim, using 'UNKNOWN'")
            employee_number = "UNKNOWN"

        # Step 2: Check for skip reasons or search failure
        skip_reasons = search_evaluation.get("skip_reasons", [])
        fetched_name = extracted_info.get("fetched_name", "").strip()
        search_failed = not search_evaluation.get("compliance", False) or not fetched_name

        if skip_reasons or search_failed:
            # Handle skips or search failures with all sections compliant (True) since not evaluated
            explanation = (
                search_evaluation.get("compliance_explanation", "Individual not found in search.")
                if search_failed and not skip_reasons else
                f"Record skipped: {', '.join(skip_reasons)}"
            )
            alert_type = "IndividualNotFound" if search_failed and not skip_reasons else "RecordSkipped"
            severity = AlertSeverity.HIGH if search_failed else AlertSeverity.LOW

            status_eval = {
                "compliance": True,
                "compliance_explanation": explanation,
                "alerts": [Alert(
                    alert_type=alert_type,
                    severity=severity,
                    metadata={"search_evaluation": search_evaluation},
                    description=explanation,
                    alert_category=determine_alert_category(alert_type)
                ).to_dict()]
            }
            name_details, _ = evaluate_name(claim.get('first_name', '').strip() + ' ' + claim.get('last_name', '').strip(), fetched_name, extracted_info.get("other_names", []))
            # If name_details contains 'due_diligence', merge it with the rest for full detail
            if 'due_diligence' in name_details:
                evaluation_details = name_details['due_diligence'].copy()
                evaluation_details['compliance'] = name_details.get('compliance')
                evaluation_details['compliance_explanation'] = name_details.get('compliance_explanation')
                for key in ('claimed_name', 'all_matches', 'best_match'):
                    if key in name_details:
                        evaluation_details[key] = name_details[key]
            else:
                evaluation_details = name_details
            name_eval = {
                "compliance": name_details.get("compliance", False),
                "compliance_explanation": "Name matches fetched record." if name_details.get("compliance", False) else "Name mismatch detected.",
                "evaluation_details": evaluation_details,
                "alerts": []
            }
            license_eval = {
                "compliance": True,
                "compliance_explanation": explanation,
                "alerts": []
            }
            exam_eval = {
                "compliance": True,
                "compliance_explanation": explanation,
                "alerts": []
            }
            employment_eval = {
                "compliance": True,  # This is already a boolean, no need to change
                "compliance_explanation": f"No employment history evaluated: {explanation}",
                "alerts": []
            }
            disclosure_eval = {
                "compliance": True,
                "compliance_explanation": f"No disclosures evaluated: {explanation}",
                "alerts": []
            }
            disciplinary_eval = {
                "compliance": True,
                "compliance_explanation": f"No disciplinary actions evaluated: {explanation}",
                "actions": [],
                "alerts": [],
                "due_diligence": extracted_info.get("disciplinary_evaluation", {}).get("due_diligence", {})
            }
            arbitration_eval = {
                "compliance": True,
                "compliance_explanation": f"No arbitration actions evaluated: {explanation}",
                "actions": [],
                "alerts": [],
                "due_diligence": extracted_info.get("arbitration_evaluation", {}).get("due_diligence", {})
            }
            regulatory_eval = {
                "compliance": True,
                "compliance_explanation": f"No regulatory actions evaluated: {explanation}",
                "actions": [],
                "alerts": [],
                "due_diligence": extracted_info.get("regulatory_evaluation", {}).get("due_diligence", {})
            }
        else:
            # Full evaluation using evaluation_processor.py
            individual = extracted_info.get("individual", {})
            status_result, status_alerts = evaluate_registration_status(individual)
            status_eval = {
                "compliance": status_result.get("compliance", False),
                "compliance_explanation": "Registration status is valid." if status_result.get("compliance", False) else "Registration status check failed.",
                "alerts": [alert.to_dict() for alert in status_alerts]
            }

            expected_name = f"{claim.get('first_name', '').strip()} {claim.get('last_name', '').strip()}".strip()
            name_details, _ = evaluate_name(expected_name, fetched_name, extracted_info.get("other_names", []))
            if 'due_diligence' in name_details:
                evaluation_details = name_details['due_diligence'].copy()
                evaluation_details['compliance'] = name_details.get('compliance')
                evaluation_details['compliance_explanation'] = name_details.get('compliance_explanation')
                for key in ('claimed_name', 'all_matches', 'best_match'):
                    if key in name_details:
                        evaluation_details[key] = name_details[key]
            else:
                evaluation_details = name_details
            name_eval = {
                "compliance": name_details.get("compliance", False),
                "compliance_explanation": "Name matches fetched record." if name_details.get("compliance", False) else "Name mismatch detected.",
                "evaluation_details": evaluation_details,
                "alerts": []
            }

            csv_license = claim.get("license_type", "")
            bc_scope = extracted_info.get("bc_scope", "NotInScope")
            ia_scope = extracted_info.get("ia_scope", "NotInScope")
            license_result, license_alert = evaluate_license(csv_license, bc_scope, ia_scope, expected_name)
            license_eval = {
                "compliance": license_result.get("compliance", False),
                "compliance_explanation": "License is active and compliant." if license_result.get("compliance", False) else "License compliance failed.",
                "alerts": [license_alert.to_dict()] if license_alert else []
            }

            exams = extracted_info.get("exams", [])
            passed_exams = get_passed_exams(exams)
            exam_result, exam_alert = evaluate_exams(passed_exams, csv_license, expected_name)
            exam_eval = {
                "compliance": exam_result.get("compliance", False),
                "compliance_explanation": "Required exams are passed." if exam_result.get("compliance", False) else "Exam requirements not met.",
                "alerts": [exam_alert.to_dict()] if exam_alert else []
            }

            employments = extracted_info.get("employments", [])
            employment_evaluation = extracted_info.get("employment_evaluation", {})
            employment_result, employment_explanation, employment_alerts = evaluate_employments(
                employments, expected_name, csv_license, employment_evaluation.get("due_diligence")
            )
            employment_eval = {
                "compliance": employment_result.get("compliance", False),
                "compliance_explanation": employment_explanation,
                "alerts": [alert.to_dict() for alert in employment_alerts]
            }

            disclosures = extracted_info.get("disclosures", [])
            disclosure_result, disclosure_summary, disclosure_alerts = evaluate_disclosures(disclosures, expected_name)
            disclosure_eval = {
                "compliance": disclosure_result.get("compliance", False),
                "compliance_explanation": disclosure_summary,
                "alerts": [alert.to_dict() for alert in disclosure_alerts]
            }

            disciplinary_evaluation = extracted_info.get("disciplinary_evaluation", {})
            disciplinary_result, disciplinary_explanation, disciplinary_alerts = evaluate_disciplinary(
                disciplinary_evaluation.get("actions", []), expected_name, disciplinary_evaluation.get("due_diligence")
            )
            disciplinary_eval = {
                "compliance": disciplinary_result.get("compliance", False),
                "compliance_explanation": disciplinary_explanation,
                "actions": disciplinary_evaluation.get("actions", []),
                "alerts": [alert.to_dict() for alert in disciplinary_alerts],
                "due_diligence": disciplinary_evaluation.get("due_diligence", {})
            }

            arbitration_evaluation = extracted_info.get("arbitration_evaluation", {})
            arbitration_result, arbitration_explanation, arbitration_alerts = evaluate_arbitration(
                arbitration_evaluation.get("actions", []), expected_name, arbitration_evaluation.get("due_diligence")
            )
            arbitration_eval = {
                "compliance": arbitration_result.get("compliance", False),
                "compliance_explanation": arbitration_explanation,
                "actions": arbitration_evaluation.get("actions", []),
                "alerts": [alert.to_dict() for alert in arbitration_alerts],
                "due_diligence": arbitration_evaluation.get("due_diligence", {})
            }

            regulatory_evaluation = extracted_info.get("regulatory_evaluation", {})
            regulatory_result, regulatory_explanation, regulatory_alerts = evaluate_regulatory(
                regulatory_evaluation.get("actions", []),
                expected_name,
                regulatory_evaluation.get("due_diligence"),
                employee_number
            )
            regulatory_eval = {
                "compliance": regulatory_result.get("compliance", False),
                "compliance_explanation": regulatory_explanation,
                "actions": regulatory_evaluation.get("actions", []),
                "alerts": [alert.to_dict() for alert in regulatory_alerts],
                "due_diligence": regulatory_evaluation.get("due_diligence", {})
            }

        # Set evaluations
        self.builder.set_status_evaluation(status_eval)
        self.builder.set_name_evaluation(name_eval)
        self.builder.set_license_evaluation(license_eval)
        self.builder.set_exam_evaluation(exam_eval)
        self.builder.set_employment_evaluation(employment_eval)  # New: Set employment evaluation
        self.builder.set_disclosure_review(disclosure_eval)
        self.builder.set_disciplinary_evaluation(disciplinary_eval)
        self.builder.set_arbitration_review(arbitration_eval)
        self.builder.set_regulatory_evaluation(regulatory_eval)

        # Final Evaluation
        overall_compliance = (
            search_evaluation.get("compliance", False) and
            status_eval.get("compliance", True) and
            name_eval.get("compliance", True) and
            license_eval.get("compliance", True) and
            exam_eval.get("compliance", True) and
            employment_eval.get("compliance", True) and  # New: Include employment compliance
            disclosure_eval.get("compliance", True) and
            disciplinary_eval.get("compliance", True) and
            arbitration_eval.get("compliance", True) and
            regulatory_eval.get("compliance", True)
        )
        all_alerts = (
            status_eval.get("alerts", []) +
            name_eval.get("alerts", []) +
            license_eval.get("alerts", []) +
            exam_eval.get("alerts", []) +
            employment_eval.get("alerts", []) +  # New: Include employment alerts
            disclosure_eval.get("alerts", []) +
            disciplinary_eval.get("alerts", []) +
            arbitration_eval.get("alerts", []) +
            regulatory_eval.get("alerts", [])
        )
        final_alerts = [alert for alert in all_alerts if alert.get("severity", "").lower() not in ["info"]]
        
        overall_risk_level = "Low"
        for alert in final_alerts:
            severity = alert.get("severity", "").lower()
            if severity == "high":
                overall_risk_level = "High"
                break
            elif severity == "medium" and overall_risk_level != "High":
                overall_risk_level = "Medium"

        final_eval = {
            "compliance": overall_compliance,
            "compliance_explanation": (
                explanation if skip_reasons or search_failed else
                "All compliance checks completed successfully." if overall_compliance else
                "One or more compliance checks failed."
            ),
            "overall_compliance": overall_compliance,
            "overall_risk_level": overall_risk_level,
            "recommendations": (
                "Review input data or search failure reason." if skip_reasons or search_failed else
                "Immediate action required due to critical compliance issues." if not overall_compliance else
                "No immediate action required."
            ),
            "alerts": final_alerts
        }
        self.builder.set_final_evaluation(final_eval)

        return self.builder.build()