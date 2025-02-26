"""
evaluation_report_builder.py

This module defines the EvaluationReportBuilder class. Its purpose is to
incrementally assemble an evaluation report that collects various sub‐evaluations
(such as registration status, name, license, exam, disclosure, disciplinary, and arbitration).
Each setter method returns self to allow method chaining. Finally, build() returns
the fully constructed report (as an OrderedDict), which downstream code can consume.
"""

from collections import OrderedDict
from typing import Dict, Any, List
import logging

logger = logging.getLogger("evaluation_report_builder")

class EvaluationReportBuilder:
    def __init__(self, reference_id: str):
        self.report = OrderedDict()
        self.report["reference_id"] = reference_id
        # Initialize sub-sections (they can be set later)
        self.report["claim"] = {}
        self.report["search_evaluation"] = {}
        self.report["status_evaluation"] = {}
        self.report["name_evaluation"] = {}
        self.report["license_evaluation"] = {}
        self.report["exam_evaluation"] = {}
        self.report["disclosure_review"] = {}
        self.report["disciplinary_evaluation"] = {}
        self.report["arbitration_review"] = {}
        self.report["final_evaluation"] = {}

    def set_claim(self, claim: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["claim"] = claim
        return self

    def set_search_evaluation(self, search_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["search_evaluation"] = search_evaluation
        return self

    def set_status_evaluation(self, status_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["status_evaluation"] = status_evaluation
        return self

    def set_name_evaluation(self, name_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["name_evaluation"] = name_evaluation
        return self

    def set_license_evaluation(self, license_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["license_evaluation"] = license_evaluation
        return self

    def set_exam_evaluation(self, exam_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["exam_evaluation"] = exam_evaluation
        return self

    def set_disclosure_review(self, disclosure_review: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["disclosure_review"] = disclosure_review
        return self

    def set_disciplinary_evaluation(self, disciplinary_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["disciplinary_evaluation"] = disciplinary_evaluation
        return self

    def set_arbitration_review(self, arbitration_review: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["arbitration_review"] = arbitration_review
        return self

    def set_final_evaluation(self, final_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["final_evaluation"] = final_evaluation
        return self

    def build(self) -> Dict[str, Any]:
        return self.report
