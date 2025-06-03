"""
evaluation_report_builder.py

This module defines the EvaluationReportBuilder class. Its purpose is to
incrementally assemble an evaluation report that collects various sub-evaluations
(such as registration status, name, license, exam, employment history, disclosure,
disciplinary, arbitration, and regulatory). Each setter method returns self to allow
method chaining. Finally, build() returns the fully constructed report (as an OrderedDict),
which downstream code can consume.
"""

from collections import OrderedDict
from typing import Dict, Any, List, Optional
from common_types import DataSource

class EvaluationReportBuilder:
    def __init__(self, reference_id: str, default_source: Optional[str] = None):
        self.report = OrderedDict()
        self.report["reference_id"] = reference_id
        self.default_source = default_source
        
        # Initialize sub-sections (they can be set later)
        self.report["claim"] = {}
        self.report["search_evaluation"] = {}
        self.report["status_evaluation"] = {}
        self.report["name_evaluation"] = {}
        self.report["license_evaluation"] = {}
        self.report["exam_evaluation"] = {}
        self.report["employment_evaluation"] = {}  # New: Added for employment history review
        self.report["disclosure_review"] = {}
        self.report["disciplinary_evaluation"] = {}
        self.report["arbitration_review"] = {}
        self.report["regulatory_evaluation"] = {}
        self.report["final_evaluation"] = {}

    def _ensure_source_field(self, section: Dict[str, Any], section_name: str) -> Dict[str, Any]:
        """
        Ensure the section has a source field.
        If not present, try to derive it from search_evaluation or use the default source.
        
        Args:
            section: The section dictionary to check/modify
            section_name: The name of the section (for logging)
            
        Returns:
            The section with a source field added if needed
        """
        # If section already has a source field, use it
        if "source" in section:
            return section
            
        # Create a copy to avoid modifying the original
        section_copy = section.copy()
        
        # Try to get source from search_evaluation
        if "search_evaluation" in self.report and "source" in self.report["search_evaluation"]:
            search_source = self.report["search_evaluation"]["source"]
            if isinstance(search_source, list) and search_source:
                # If it's a list, use the first source
                section_copy["source"] = search_source[0]
            else:
                section_copy["source"] = search_source
        # Otherwise use the default source if provided
        elif self.default_source:
            section_copy["source"] = self.default_source
        # Last resort: use UNKNOWN
        else:
            section_copy["source"] = DataSource.UNKNOWN.value
            
        return section_copy

    def set_claim(self, claim: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["claim"] = claim  # No source needed for claim
        return self

    def set_search_evaluation(self, search_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        # Ensure search_evaluation has a source field
        if "source" not in search_evaluation:
            search_evaluation = search_evaluation.copy()
            search_evaluation["source"] = self.default_source or DataSource.UNKNOWN.value
            
        self.report["search_evaluation"] = search_evaluation
        return self

    def set_status_evaluation(self, status_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["status_evaluation"] = self._ensure_source_field(status_evaluation, "status_evaluation")
        return self

    def set_name_evaluation(self, name_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["name_evaluation"] = self._ensure_source_field(name_evaluation, "name_evaluation")
        return self

    def set_license_evaluation(self, license_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["license_evaluation"] = self._ensure_source_field(license_evaluation, "license_evaluation")
        return self

    def set_exam_evaluation(self, exam_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["exam_evaluation"] = self._ensure_source_field(exam_evaluation, "exam_evaluation")
        return self

    def set_employment_evaluation(self, employment_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        """Set the employment evaluation section of the report."""
        self.report["employment_evaluation"] = self._ensure_source_field(employment_evaluation, "employment_evaluation")
        return self

    def set_disclosure_review(self, disclosure_review: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["disclosure_review"] = self._ensure_source_field(disclosure_review, "disclosure_review")
        return self

    def set_disciplinary_evaluation(self, disciplinary_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["disciplinary_evaluation"] = self._ensure_source_field(disciplinary_evaluation, "disciplinary_evaluation")
        return self

    def set_arbitration_review(self, arbitration_review: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["arbitration_review"] = self._ensure_source_field(arbitration_review, "arbitration_review")
        return self

    def set_regulatory_evaluation(self, regulatory_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        """Set the regulatory evaluation section of the report."""
        self.report["regulatory_evaluation"] = self._ensure_source_field(regulatory_evaluation, "regulatory_evaluation")
        return self

    def set_final_evaluation(self, final_evaluation: Dict[str, Any]) -> "EvaluationReportBuilder":
        self.report["final_evaluation"] = self._ensure_source_field(final_evaluation, "final_evaluation")
        return self

    def build(self) -> Dict[str, Any]:
        return self.report