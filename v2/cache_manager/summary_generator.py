# summary_generator.py
"""
==============================================
📌 SUMMARY GENERATOR MODULE OVERVIEW
==============================================
🗂 PURPOSE
This module provides the `SummaryGenerator` class to generate compliance summaries,
taxonomy trees, risk dashboards, and data quality reports from ComplianceReportAgent JSON files.
It processes cached compliance data to produce actionable insights for regulatory analysis.

🔧 USAGE
Instantiate with a FileHandler and optional ComplianceHandler to access cached JSON files,
then call methods like `generate_compliance_summary`, `generate_taxonomy_from_latest_reports`,
`generate_risk_dashboard`, or `generate_data_quality_report` to analyze data.

📝 NOTES
- Relies on `file_handler.py` for filesystem operations and `compliance_handler.py` for report filtering.
- Outputs are either JSON strings (for summaries) or human-readable text (for taxonomy, dashboard, and quality reports).
- Designed to work with `ComplianceReportAgent_*.json` files in the cache structure.
"""

from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict

from .file_handler import FileHandler
from .compliance_handler import ComplianceHandler
import json
import logging
logger = logging.getLogger("SummaryGenerator")

class SummaryGenerator:
    """
    Generates summaries, taxonomy trees, risk dashboards, and data quality reports from compliance data.

    Attributes:
        file_handler (FileHandler): Handles filesystem operations for reading JSON files.
        compliance_handler (ComplianceHandler): Filters and retrieves latest compliance reports.
    """

    def __init__(self, file_handler: FileHandler, compliance_handler: Optional[ComplianceHandler] = None):
        """
        Initialize the SummaryGenerator with necessary handlers.

        Args:
            file_handler (FileHandler): Instance for filesystem operations.
            compliance_handler (Optional[ComplianceHandler]): Instance for compliance report operations.
                Defaults to a new ComplianceHandler using file_handler's base_path if None.
        """
        self.file_handler = file_handler
        self.compliance_handler = compliance_handler or ComplianceHandler(file_handler.base_path)

    def _extract_compliance_data(self, reports: List[Dict[str, Any]], employee_number: str) -> Tuple[List[Dict], List[Dict]]:
        """
        Extract compliance data from a list of reports for summary generation.

        Args:
            reports (List[Dict[str, Any]]): List of JSON report dictionaries.
            employee_number (str): Employee identifier to associate with the data.

        Returns:
            Tuple[List[Dict], List[Dict]]: (report_summary, subsection_summary)
                - report_summary: List of overall compliance metrics per report.
                - subsection_summary: List of detailed subsection evaluations.
        """
        report_data = []
        subsection_data = []
        for report in reports:
            emp_num = report.get('claim', {}).get('employee_number', employee_number)
            ref_id = report.get('reference_id', 'UNKNOWN')
            file_name = report.get('file_name', f"ComplianceReportAgent_{ref_id}_v1_20250307.json")
            overall_compliance = report.get('final_evaluation', {}).get('overall_compliance', False)
            report_entry = {
                'employee_number': emp_num,
                'reference_id': ref_id,
                'file_name': file_name,
                'overall_compliance': overall_compliance,
                'risk_level': report.get('final_evaluation', {}).get('risk_level', 'N/A'),
                'alert_count': len(report.get('final_evaluation', {}).get('alerts', []))
            }
            report_data.append(report_entry)
            if not overall_compliance or report.get('final_evaluation', {}).get('alerts'):
                subsections = [
                    ('search_evaluation', report.get('search_evaluation', {})),
                    ('status_evaluation', report.get('status_evaluation', {})),
                    ('name_evaluation', report.get('name_evaluation', {})),
                    ('license_evaluation', report.get('license_evaluation', {})),
                    ('exam_evaluation', report.get('exam_evaluation', {})),
                    ('disclosure_review', report.get('disclosure_review', {})),
                    ('disciplinary_evaluation', report.get('disciplinary_evaluation', {})),
                    ('arbitration_review', report.get('arbitration_review', {})),
                    ('regulatory_evaluation', report.get('regulatory_evaluation', {}))
                ]
                for section_name, section_data in subsections:
                    subsection_entry = {
                        'employee_number': emp_num,
                        'reference_id': ref_id,
                        'file_name': file_name,
                        'subsection': section_name,
                        'compliance': section_data.get('compliance', True),
                        'alert_count': len(section_data.get('alerts', [])) if section_data.get('alerts') is not None else 0,
                        'explanation': section_data.get('compliance_explanation', 'N/A')
                    }
                    subsection_data.append(subsection_entry)
        return report_data, subsection_data

    def generate_compliance_summary(self, emp_path: Path, employee_number: str, page: int = 1, page_size: int = 10) -> str:
        """
        Generate a compliance summary for a specific employee with pagination.

        Args:
            emp_path (Path): Path to the employee's cache folder.
            employee_number (str): Employee identifier.
            page (int): Page number for pagination (default: 1).
            page_size (int): Number of items per page (default: 10).

        Returns:
            str: JSON-formatted summary of compliance data.

        Example Output:
            {
              "employee_number": "EN-016314",
              "status": "success",
              "message": "Generated compliance summary for EN-016314",
              "report_summary": [...],
              "subsection_summary": [...],
              "pagination": {...}
            }
        """
        reports = [self.file_handler.read_json(f) for f in self.file_handler.list_files(emp_path, "ComplianceReportAgent_*.json") if self.file_handler.read_json(f)]
        report_summary, subsection_summary = self._extract_compliance_data(reports, employee_number)
        total_items = len(report_summary)
        total_pages = (total_items + page_size - 1) // page_size
        current_page = max(1, min(page, total_pages))
        start_idx = (current_page - 1) * page_size
        end_idx = start_idx + page_size
        result = {
            "employee_number": employee_number,
            "status": "success",
            "message": f"Generated compliance summary for {employee_number}",
            "report_summary": report_summary[start_idx:end_idx],
            "subsection_summary": [entry for entry in subsection_summary if any(report["reference_id"] == entry["reference_id"] for report in report_summary[start_idx:end_idx])],
            "pagination": {
                "total_items": total_items,
                "total_pages": total_pages,
                "current_page": current_page,
                "page_size": page_size
            }
        }
        return json.dumps(result, indent=2)

    def generate_all_compliance_summaries(self, cache_folder: Path, page: int = 1, page_size: int = 10) -> str:
        """
        Generate a compliance summary for all employees with pagination.

        Args:
            cache_folder (Path): Root cache folder containing employee subdirectories.
            page (int): Page number for pagination (default: 1).
            page_size (int): Number of items per page (default: 10).

        Returns:
            str: JSON-formatted summary of compliance data across all employees.

        Example Output:
            {
              "status": "success",
              "message": "Generated compliance summary for 10 employees (page 1 of 1)",
              "report_summary": [...],
              "subsection_summary": [...],
              "pagination": {...}
            }
        """
        if not cache_folder.exists():
            return json.dumps({"status": "warning", "message": f"Cache folder not found at {cache_folder}"}, indent=2)
        all_reports = []
        all_subsections = []
        emp_dirs = self.file_handler.list_files(cache_folder, "*")
        for emp_path in emp_dirs:
            if emp_path.is_dir():
                emp_num = emp_path.name
                reports = [self.file_handler.read_json(f) for f in self.file_handler.list_files(emp_path, "ComplianceReportAgent_*.json") if self.file_handler.read_json(f)]
                if reports:
                    report_data, subsection_data = self._extract_compliance_data(reports, emp_num)
                    all_reports.extend(report_data)
                    all_subsections.extend(subsection_data)
        total_items = len(all_reports)
        total_pages = (total_items + page_size - 1) // page_size
        current_page = max(1, min(page, total_pages))
        start_idx = (current_page - 1) * page_size
        end_idx = start_idx + page_size
        result = {
            "status": "success",
            "message": f"Generated compliance summary for {len(emp_dirs)} employees (page {current_page} of {total_pages})",
            "report_summary": all_reports[start_idx:end_idx],
            "subsection_summary": all_subsections[start_idx:end_idx],
            "pagination": {
                "total_items": total_items,
                "total_pages": total_pages,
                "current_page": current_page,
                "page_size": page_size
            }
        }
        return json.dumps(result, indent=2)

    def _build_tree(self, data: Any) -> Dict[str, Any]:
        """
        Recursively build a hierarchical tree from a JSON object for taxonomy generation.

        Args:
            data (Any): JSON data to process (dict, list, or primitive).

        Returns:
            Dict[str, Any]: Nested dictionary with "_types" (set of data types) and "children".
        """
        node = {"_types": set(), "children": {}}
        if isinstance(data, dict):
            node["_types"].add("dict")
            node["children"] = {key: self._build_tree(value) for key, value in data.items()}
        elif isinstance(data, list):
            node["_types"].add("list")
            node["children"] = [self._build_tree(item) for item in data]
        else:
            node["_types"].add(type(data).__name__)
            node["children"] = {}
        return node

    def _merge_trees(self, base_tree: Dict[str, Any], new_tree: Dict[str, Any]) -> None:
        """
        Merge a new taxonomy tree into an existing base tree in-place.

        Args:
            base_tree (Dict[str, Any]): The existing tree to merge into.
            new_tree (Dict[str, Any]): The new tree to merge from.
        """
        base_tree["_types"] |= new_tree["_types"]
        if isinstance(base_tree["children"], dict) and isinstance(new_tree["children"], dict):
            for key, value in new_tree["children"].items():
                if key not in base_tree["children"]:
                    base_tree["children"][key] = value
                else:
                    self._merge_trees(base_tree["children"][key], value)
        elif isinstance(base_tree["children"], list) and isinstance(new_tree["children"], list):
            while len(base_tree["children"]) < len(new_tree["children"]):
                base_tree["children"].append({"_types": set(), "children": {}})
            for i in range(len(new_tree["children"])):
                self._merge_trees(base_tree["children"][i], new_tree["children"][i])

    def _print_tree(self, tree: Dict[str, Any], indent: int = 0, field_name: str = "<root>") -> str:
        """
        Recursively pretty-print the taxonomy tree with indentation.

        Args:
            tree (Dict[str, Any]): The taxonomy tree to print.
            indent (int): Current indentation level (default: 0).
            field_name (str): Name of the current field (default: "<root>").

        Returns:
            str: Formatted string representation of the tree.
        """
        lines = []
        prefix = "  " * indent
        type_str = f"{{'{', '.join(tree['_types'])}'}}"
        lines.append(f"{prefix}- {field_name} (types={type_str})")
        if isinstance(tree["children"], dict):
            for key, value in tree["children"].items():
                lines.append(self._print_tree(value, indent + 1, field_name=key))
        elif isinstance(tree["children"], list):
            for i, child in enumerate(tree["children"]):
                lines.append(self._print_tree(child, indent + 1, field_name=f"[{i}]"))
        return "\n".join(lines)

    def generate_taxonomy_from_latest_reports(self) -> str:
        """
        Generate a taxonomy tree from the latest ComplianceReportAgent JSON files.

        Returns:
            str: Human-readable taxonomy tree representation.

        Example Output:
            Generated taxonomy tree from 10 latest ComplianceReportAgent JSON files
            - <root> (types={'dict'})
              - claim (types={'dict'})
                - employee_number (types={'str'})
              - final_evaluation (types={'dict'})
                - overall_compliance (types={'bool'})
        """
        latest_reports_json = self.compliance_handler.list_compliance_reports(employee_number=None, page=1, page_size=99999)
        latest_reports = json.loads(latest_reports_json)
        if latest_reports["status"] != "success":
            return "No latest compliance reports available"
        combined_tree = {"_types": set(), "children": {}}
        file_count = 0
        if "reports" in latest_reports and isinstance(latest_reports["reports"], dict):
            for emp_num, reports_list in latest_reports["reports"].items():
                for report in reports_list:
                    file_path = self.file_handler.base_path / emp_num / report["file_name"]
                    try:
                        data = self.file_handler.read_json(file_path)
                        if data:
                            json_tree = self._build_tree(data)
                            self._merge_trees(combined_tree, json_tree)
                            file_count += 1
                    except Exception as e:
                        logger.warning(f"Skipping invalid JSON file {file_path}: {str(e)}")
        header = f"Generated taxonomy tree from {file_count} latest ComplianceReportAgent JSON files\n"
        return header + self._print_tree(combined_tree)

    def generate_risk_dashboard(self) -> str:
        """
        Generate a compliance risk dashboard from the latest ComplianceReportAgent JSON files.

        Returns:
            str: Human-readable dashboard summarizing risk levels and top alerts.

        Example Output:
            Compliance Risk Dashboard (10 employees analyzed)
            - Low Risk: 1 employees (10%)
              - EN-042516: 0 alerts (severity: Unknown)
            - Medium Risk: 1 employees (10%)
              - EN-016314: 1 alert (severity: Medium)
            - High Risk: 8 employees (80%)
              - EN-042567: 8 alerts (severity: High)
            Top Alerts:
              - "Individual not found" (8 occurrences)
        """
        latest_reports_json = self.compliance_handler.list_compliance_reports(employee_number=None, page=1, page_size=99999)
        latest_reports = json.loads(latest_reports_json)
        if latest_reports["status"] != "success":
            return "No latest compliance reports available for risk analysis"
        risk_categories = {"Low": [], "Medium": [], "High": [], "Unknown": []}
        alert_counts = defaultdict(int)
        total_employees = 0
        file_count = 0
        if "reports" in latest_reports and isinstance(latest_reports["reports"], dict):
            for emp_num, reports_list in latest_reports["reports"].items():
                total_employees += 1
                if reports_list:
                    report = reports_list[0]
                    file_path = self.file_handler.base_path / emp_num / report["file_name"]
                    try:
                        data = self.file_handler.read_json(file_path)
                        if data:
                            file_count += 1
                            final_eval = data.get("final_evaluation", {})
                            risk_level = final_eval.get("risk_level", "Unknown")
                            alert_count = len(final_eval.get("alerts", []))
                            overall_compliance = final_eval.get("overall_compliance", False)
                            severity = "Unknown"
                            if alert_count > 0 and final_eval.get("alerts"):
                                severities = [alert.get("severity", "Low") for alert in final_eval["alerts"]]
                                severity_order = {"Low": 0, "Medium": 1, "High": 2}
                                severity = max(severities, key=lambda s: severity_order.get(s, 0))
                            if risk_level not in risk_categories:
                                risk_level = "Unknown"
                            if risk_level == "Unknown":
                                if overall_compliance and alert_count == 0:
                                    risk_level = "Low"
                                elif alert_count > 5 or severity == "High":
                                    risk_level = "High"
                                elif alert_count > 0:
                                    risk_level = "Medium"
                                else:
                                    risk_level = "Low"
                            risk_categories[risk_level].append(
                                f"  - {emp_num}: {alert_count} alert{'s' if alert_count != 1 else ''} (severity: {severity})"
                            )
                            for alert in final_eval.get("alerts", []):
                                alert_desc = alert.get("description", "Unnamed alert")
                                alert_counts[alert_desc] += 1
                    except Exception as e:
                        logger.warning(f"Skipping invalid JSON file {file_path}: {str(e)}")
        lines = [f"Compliance Risk Dashboard ({total_employees} employees analyzed)"]
        for category, employees in risk_categories.items():
            count = len(employees)
            percentage = (count / total_employees * 100) if total_employees > 0 else 0
            lines.append(f"- {category} Risk: {count} employees ({percentage:.0f}%)")
            lines.extend(employees)
        if alert_counts:
            lines.append("Top Alerts:")
            sorted_alerts = sorted(alert_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            for alert, count in sorted_alerts:
                lines.append(f"  - \"{alert}\" ({count} occurrence{'s' if count != 1 else ''})")
        return "\n".join(lines)

    def _check_field_value(self, data: Dict[str, Any], field_path: str) -> Tuple[bool, str]:
        """
        Check if a field exists and has a non-null, non-empty value.

        Args:
            data (Dict[str, Any]): The JSON data to check.
            field_path (str): Dot-separated path to the field (e.g., "claim.organization_name").

        Returns:
            Tuple[bool, str]: (has_value, status_message)
                - has_value: True if the field has a non-null, non-empty value.
                - status_message: "Has Value", "Missing", "Null", or "Empty" indicating the field's state.
        """
        keys = field_path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return False, "Missing"
        if current is None:
            return False, "Null"
        if isinstance(current, str) and not current.strip():
            return False, "Empty"
        return True, "Has Value"

    def generate_data_quality_report(self) -> str:
        """
        Generate a data quality report checking if specific fields have non-null, non-empty values.

        Returns:
            str: Human-readable report on field value presence for specified fields.

        Example Output:
            Data Quality Report (10 reports analyzed)
            - Field Value Presence:
              - claim.organization_name: 0% (0 with values)
                - 10 missing
                  Examples: EN-016314, EN-017901, EN-019318
              - claim.crd_number: 0% (0 with values)
                - 10 missing
                  Examples: EN-016314, EN-017901, EN-019318
              - claim.address_line1: 50% (5 with values)
                - 3 missing
                  Examples: EN-042567, EN-042571, EN-023486
                - 2 empty
                  Examples: EN-016314, EN-017901
        """
        latest_reports_json = self.compliance_handler.list_compliance_reports(employee_number=None, page=1, page_size=99999)
        latest_reports = json.loads(latest_reports_json)
        if latest_reports["status"] != "success":
            return "No latest compliance reports available for data quality analysis"

        # Define fields to check
        fields_to_check = [
            "claim.organization_name",
            "claim.crd_number",
            "claim.individual_name",
            "claim.address_line1",
            "claim.city",
            "claim.state",
            "claim.zip"
        ]

        # Initialize tracking
        field_stats = defaultdict(lambda: {"has_value": 0, "missing": 0, "null": 0, "empty": 0, "examples": []})
        total_reports = 0

        # Process each report
        if "reports" in latest_reports and isinstance(latest_reports["reports"], dict):
            for emp_num, reports_list in latest_reports["reports"].items():
                if reports_list:
                    report = reports_list[0]  # Latest report per employee
                    file_path = self.file_handler.base_path / emp_num / report["file_name"]
                    try:
                        data = self.file_handler.read_json(file_path)
                        if data:
                            total_reports += 1
                            # Check each field for values
                            for field in fields_to_check:
                                has_value, status = self._check_field_value(data, field)
                                if has_value:
                                    field_stats[field]["has_value"] += 1
                                elif status == "Missing":
                                    field_stats[field]["missing"] += 1
                                    if len(field_stats[field]["examples"]) < 3:  # Limit to 3 examples
                                        field_stats[field]["examples"].append(emp_num)
                                elif status == "Null":
                                    field_stats[field]["null"] += 1
                                    if len(field_stats[field]["examples"]) < 3:
                                        field_stats[field]["examples"].append(emp_num)
                                elif status == "Empty":
                                    field_stats[field]["empty"] += 1
                                    if len(field_stats[field]["examples"]) < 3:
                                        field_stats[field]["examples"].append(emp_num)

                    except Exception as e:
                        logger.warning(f"Skipping invalid JSON file {file_path}: {str(e)}")

        # Build the report output
        lines = [f"Data Quality Report ({total_reports} reports analyzed)"]
        lines.append("- Field Value Presence:")

        for field in sorted(fields_to_check):
            stats = field_stats[field]
            has_value_pct = (stats["has_value"] / total_reports * 100) if total_reports > 0 else 0
            lines.append(f"  - {field}: {has_value_pct:.0f}% ({stats['has_value']} with values)")
            if stats["missing"] > 0:
                lines.append(f"    - {stats['missing']} missing")
                if stats["examples"]:
                    examples = ", ".join(stats["examples"])
                    lines.append(f"      Examples: {examples}")
            if stats["null"] > 0:
                lines.append(f"    - {stats['null']} null")
                if stats["examples"] and not stats["missing"] >= total_reports:  # Avoid duplicate examples
                    examples = ", ".join(stats["examples"])
                    lines.append(f"      Examples: {examples}")
            if stats["empty"] > 0:
                lines.append(f"    - {stats['empty']} empty")
                if stats["examples"] and not (stats["missing"] + stats["null"]) >= total_reports:
                    examples = ", ".join(stats["examples"])
                    lines.append(f"      Examples: {examples}")

        return "\n".join(lines)