# ui.py
"""
==============================================
📌 COMPLIANCE API UI OVERVIEW
==============================================
🗂 PURPOSE
This script provides a Gradio-based UI for interacting with the Compliance Claim Processing API.
It allows users to submit claims, manage cache, and view compliance analytics through a web interface,
with a formatted HTML rendering of claim processing reports and an optional raw JSON view.

🔧 USAGE
Run with `python ui.py` after starting the FastAPI server (`python api.py`).
Access the UI in your browser at the provided URL (e.g., http://localhost:7860).

📝 NOTES
- Assumes the FastAPI server is running at http://localhost:8000.
- Uses `requests` for API calls and renders claim reports as HTML or pretty-printed JSON.
- Validates that at least one of crd_number, organization_crd, or organization_name is provided.
"""

import gradio as gr
import requests
import json
from typing import Dict, Any, Tuple
from logger_config import setup_logging
import logging


# Set up logging
loggers = setup_logging(debug=True)
logger = logging.getLogger('core')  # Use 'core' logger for UI-related logs
# API base URL (adjust if your FastAPI server runs on a different host/port)
API_BASE_URL = "http://localhost:8000"

def api_call(method: str, endpoint: str, data: dict = None) -> str:
    """
    Make an API call and return the response as a string.

    Args:
        method (str): HTTP method ("get", "post").
        endpoint (str): API endpoint path (e.g., "/process-claim-basic").
        data (dict, optional): JSON data for POST requests.

    Returns:
        str: Raw response text or error message.
    """
    url = f"{API_BASE_URL}{endpoint}"
    try:
        if method == "get":
            response = requests.get(url, params=data)
        elif method == "post":
            response = requests.post(url, json=data)
        else:
            return "Unsupported method"
        
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        return f"Error: {str(e)}"

def render_claim_report(report_json: str) -> Tuple[str, str]:
    """
    Render a claim processing report as formatted HTML and return both HTML and pretty-printed JSON.

    Args:
        report_json (str): JSON string of the report from the API.

    Returns:
        Tuple[str, str]: (HTML string for display, pretty-printed JSON string)
    """
    try:
        report = json.loads(report_json)
        json_output = json.dumps(report, indent=2)
    except json.JSONDecodeError:
        error_msg = f"<div style='color: red;'>Invalid report format: {report_json}</div>"
        return error_msg, report_json

    html = "<div style='font-family: Arial, sans-serif;'>"
    
    # Header
    html += f"<h2>Compliance Report (Reference ID: {report.get('reference_id', 'N/A')})</h2>"

    # Claim Section
    claim = report.get("claim", {})
    html += "<h3>Claim Details</h3><ul>"
    html += f"<li><strong>Employee Number:</strong> {claim.get('employee_number', 'N/A')}</li>"
    html += f"<li><strong>Name:</strong> {claim.get('first_name', 'N/A')} {claim.get('last_name', 'N/A')}</li>"
    html += f"<li><strong>Organization:</strong> {claim.get('organization_name', 'N/A')}</li>"
    html += f"<li><strong>CRD Number:</strong> {claim.get('crd_number', 'N/A')}</li>"
    html += f"<li><strong>Address:</strong> {claim.get('address_line1', '')} {claim.get('city', '')}, {claim.get('state', '')} {claim.get('zip', '')}</li>"
    html += "</ul>"

    # Final Evaluation Section
    final_eval = report.get("final_evaluation", {})
    html += "<h3>Final Evaluation</h3><ul>"
    html += f"<li><strong>Overall Compliance:</strong> <span style='color: {'green' if final_eval.get('overall_compliance', False) else 'red'}'>{final_eval.get('overall_compliance', 'N/A')}</span></li>"
    html += f"<li><strong>Risk Level:</strong> {final_eval.get('overall_risk_level', 'N/A')}</li>"
    html += f"<li><strong>Explanation:</strong> {final_eval.get('compliance_explanation', 'N/A')}</li>"
    html += f"<li><strong>Recommendations:</strong> {final_eval.get('recommendations', 'N/A')}</li>"
    alerts = final_eval.get("alerts", [])
    if alerts:
        html += "<li><strong>Alerts:</strong><ul>"
        for alert in alerts:
            html += f"<li>{alert.get('description', 'Unnamed alert')} (Severity: {alert.get('severity', 'N/A')})</li>"
        html += "</ul></li>"
    html += "</ul>"

    # All Evaluation Sections
    sections = [
        ("Search Evaluation", "search_evaluation"),
        ("Status Evaluation", "status_evaluation"),
        ("Name Evaluation", "name_evaluation"),
        ("License Evaluation", "license_evaluation"),
        ("Disclosure Review", "disclosure_review"),
        ("Disciplinary Evaluation", "disciplinary_evaluation"),
        ("Arbitration Review", "arbitration_review"),
        ("Regulatory Evaluation", "regulatory_evaluation")
    ]
    for title, key in sections:
        section = report.get(key, {})
        html += f"<h4>{title}</h4><ul>"
        html += f"<li><strong>Compliance:</strong> <span style='color: {'green' if section.get('compliance', False) else 'red'}'>{section.get('compliance', 'N/A')}</span></li>"
        html += f"<li><strong>Explanation:</strong> {section.get('compliance_explanation', 'N/A')}</li>"
        if key == "name_evaluation" and "evaluation_details" in section:
            details = section["evaluation_details"]
            html += f"<li><strong>Match Score:</strong> {details.get('match_score', 'N/A')} (Expected: {details.get('expected_name', 'N/A')}, Fetched: {details.get('fetched_name', 'N/A')})</li>"
        section_alerts = section.get("alerts", [])
        if section_alerts:
            html += "<li><strong>Alerts:</strong><ul>"
            for alert in section_alerts:
                html += f"<li>{alert.get('description', 'Unnamed alert')} (Severity: {alert.get('severity', 'N/A')})</li>"
            html += "</ul></li>"
        html += "</ul>"

    html += "</div>"
    return html, json_output

# Claim Processing Functions
def process_claim(mode: str, reference_id: str, employee_number: str, first_name: str, last_name: str, 
                  crd_number: str, organization_crd: str, organization_name: str, webhook_url: str) -> Tuple[str, str]:
    """
    Submit a claim to the API based on the selected mode and return rendered report and raw JSON.

    Args:
        mode (str): Processing mode ("basic", "extended", "complete").
        reference_id (str): Mandatory reference ID.
        employee_number (str): Mandatory employee number.
        first_name (str): Mandatory first name.
        last_name (str): Mandatory last name.
        crd_number (str): Optional CRD number.
        organization_crd (str): Optional organization CRD.
        organization_name (str): Optional organization name.
        webhook_url (str): Optional webhook URL.

    Returns:
        Tuple[str, str]: (Rendered HTML report or validation error, pretty-printed JSON)
    """
    # Validate mandatory fields
    if not all([reference_id, employee_number, first_name, last_name]):
        error_html = "<div style='color: red;'>Please fill in all required fields: Reference ID, Employee Number, First Name, and Last Name.</div>"
        return error_html, ""

    # Validate that at least one of crd_number, organization_crd, or organization_name is provided
    if not any([crd_number.strip(), organization_crd.strip(), organization_name.strip()]):
        error_html = "<div style='color: orange;'>Please provide at least one of: CRD Number, Organization CRD, or Organization Name.</div>"
        return error_html, ""

    # Start with mandatory fields
    data = {
        "reference_id": reference_id.strip(),
        "employee_number": employee_number.strip(),
        "first_name": first_name.strip(),
        "last_name": last_name.strip()
    }

    # Only add optional fields if they have non-empty values
    if crd_number and crd_number.strip():
        data["crd_number"] = crd_number.strip()
    if organization_crd and organization_crd.strip():
        data["organization_crd"] = organization_crd.strip()
    if organization_name and organization_name.strip():
        data["organization_name"] = organization_name.strip()
    if webhook_url and webhook_url.strip():
        data["webhook_url"] = webhook_url.strip()

    logger.debug(f"Submitting claim with data: {json.dumps(data, indent=2)}")
    endpoint = f"/process-claim-{mode}"
    raw_response = api_call("post", endpoint, data)
    return render_claim_report(raw_response)

# Cache Management Functions
def clear_cache(employee_number: str) -> str:
    if not employee_number:
        return "Error: Employee number is required"
    return api_call("post", f"/cache/clear/{employee_number}")

def clear_all_cache() -> str:
    return api_call("post", "/cache/clear-all")

def clear_agent_cache(employee_number: str, agent_name: str) -> str:
    if not employee_number or not agent_name:
        return "Error: Employee number and agent name are required"
    return api_call("post", f"/cache/clear-agent/{employee_number}/{agent_name}")

def list_cache(employee_number: str, page: int, page_size: int) -> str:
    params = {"page": page, "page_size": page_size}
    if employee_number:
        params["employee_number"] = employee_number
    return api_call("get", "/cache/list", params)

def cleanup_stale_cache() -> str:
    return api_call("post", "/cache/cleanup-stale")

# Compliance Analytics Functions
def get_compliance_summary(employee_number: str, page: int, page_size: int) -> str:
    if not employee_number:
        return "Error: Employee number is required"
    params = {"page": page, "page_size": page_size}
    return api_call("get", f"/compliance/summary/{employee_number}", params)

def get_all_compliance_summaries(page: int, page_size: int) -> str:
    params = {"page": page, "page_size": page_size}
    return api_call("get", "/compliance/all-summaries", params)

def get_taxonomy() -> str:
    return api_call("get", "/compliance/taxonomy")

def get_risk_dashboard() -> str:
    return api_call("get", "/compliance/risk-dashboard")

def get_data_quality_report() -> str:
    return api_call("get", "/compliance/data-quality")

def create_ui() -> gr.Blocks:
    """Create and return the Gradio interface."""
    with gr.Blocks(title="Compliance Claim Processing") as demo:
        gr.Markdown("# 🔍 Compliance Claim Processing")
        
        with gr.Tab("Process Claim"):
            with gr.Row():
                with gr.Column():
                    mode = gr.Radio(
                        choices=["basic", "extended", "complete"],
                        value="basic",
                        label="Processing Mode"
                    )
                    reference_id = gr.Textbox(label="Reference ID*", placeholder="e.g., REF-001")
                    employee_number = gr.Textbox(label="Employee Number*", placeholder="e.g., EMP-001")
                    first_name = gr.Textbox(label="First Name*", placeholder="e.g., John")
                    last_name = gr.Textbox(label="Last Name*", placeholder="e.g., Doe")
                    crd_number = gr.Textbox(label="CRD Number", placeholder="e.g., 123456")
                    organization_crd = gr.Textbox(label="Organization CRD", placeholder="e.g., 789012")
                    organization_name = gr.Textbox(label="Organization Name", placeholder="e.g., Acme Corp")
                    webhook_url = gr.Textbox(label="Webhook URL", placeholder="https://...")
                    submit_btn = gr.Button("Process Claim")
                
                with gr.Column():
                    result_html = gr.HTML(label="Report")
                    json_output = gr.JSON(label="Raw JSON")
            
            submit_btn.click(
                fn=process_claim,
                inputs=[mode, reference_id, employee_number, first_name, last_name, 
                       crd_number, organization_crd, organization_name, webhook_url],
                outputs=[result_html, json_output]
            )
        
        with gr.Tab("Cache Management"):
            with gr.Row():
                with gr.Column():
                    cache_emp_number = gr.Textbox(label="Employee Number")
                    agent_name = gr.Textbox(label="Agent Name")
                    page = gr.Number(value=1, label="Page")
                    page_size = gr.Number(value=10, label="Page Size")
                    
                    with gr.Row():
                        clear_cache_btn = gr.Button("Clear Cache")
                        clear_all_btn = gr.Button("Clear All Cache")
                        clear_agent_btn = gr.Button("Clear Agent Cache")
                        list_cache_btn = gr.Button("List Cache")
                        cleanup_btn = gr.Button("Cleanup Stale")
                
                with gr.Column():
                    cache_output = gr.JSON(label="Cache Operation Result")
            
            clear_cache_btn.click(fn=clear_cache, inputs=[cache_emp_number], outputs=cache_output)
            clear_all_btn.click(fn=clear_all_cache, inputs=[], outputs=cache_output)
            clear_agent_btn.click(fn=clear_agent_cache, inputs=[cache_emp_number, agent_name], outputs=cache_output)
            list_cache_btn.click(fn=list_cache, inputs=[cache_emp_number, page, page_size], outputs=cache_output)
            cleanup_btn.click(fn=cleanup_stale_cache, inputs=[], outputs=cache_output)
        
        with gr.Tab("Analytics"):
            with gr.Row():
                with gr.Column():
                    analytics_emp_number = gr.Textbox(label="Employee Number")
                    analytics_page = gr.Number(value=1, label="Page")
                    analytics_page_size = gr.Number(value=10, label="Page Size")
                    
                    with gr.Row():
                        summary_btn = gr.Button("Get Summary")
                        all_summaries_btn = gr.Button("Get All Summaries")
                        taxonomy_btn = gr.Button("Get Taxonomy")
                        risk_btn = gr.Button("Get Risk Dashboard")
                        quality_btn = gr.Button("Get Data Quality")
                
                with gr.Column():
                    analytics_output = gr.JSON(label="Analytics Result")
            
            summary_btn.click(fn=get_compliance_summary, 
                            inputs=[analytics_emp_number, analytics_page, analytics_page_size], 
                            outputs=analytics_output)
            all_summaries_btn.click(fn=get_all_compliance_summaries, 
                                  inputs=[analytics_page, analytics_page_size], 
                                  outputs=analytics_output)
            taxonomy_btn.click(fn=get_taxonomy, inputs=[], outputs=analytics_output)
            risk_btn.click(fn=get_risk_dashboard, inputs=[], outputs=analytics_output)
            quality_btn.click(fn=get_data_quality_report, inputs=[], outputs=analytics_output)
    
    return demo

if __name__ == "__main__":
    demo = create_ui()
    # Start without sharing first
    demo.launch(server_port=7860)  # Use a specific port
    # After allowing the binary, you can enable sharing
    # demo.launch(share=True, server_port=7860)