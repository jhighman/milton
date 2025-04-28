"""
==============================================
📌 COMPLIANCE CLAIM PROCESSING API OVERVIEW
==============================================
🗂 PURPOSE
This FastAPI application provides endpoints for processing individual compliance claims
and managing cached compliance data. It supports basic, extended, and complete processing modes,
along with cache management and compliance analytics features.

🔧 USAGE
Run the API with `uvicorn api:app --host 0.0.0.0 --port 8000 --log-level info`.
Use endpoints like `/process-claim-basic`, `/cache/clear`, `/compliance/risk-dashboard`, etc.

📝 NOTES
- Integrates `cache_manager` for cache operations and analytics.
- Uses `FinancialServicesFacade` for claim processing and `CacheManager` for cache management.
- Supports asynchronous webhook notifications for processed claims.
"""

import json
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, validator
import aiohttp
import asyncio
import os

from logger_config import setup_logging  # Import centralized logging config
from marshaller import Marshaller
from services import FinancialServicesFacade
from business import process_claim
from cache_manager.cache_operations import CacheManager
from cache_manager.compliance_handler import ComplianceHandler
from cache_manager.summary_generator import SummaryGenerator
from cache_manager.file_handler import FileHandler
from main_config import get_storage_config, load_config  # Import load_config
from storage_manager import StorageManager  # Import storage manager
from storage_providers.factory import StorageProviderFactory  # Import StorageProviderFactory

# Setup logging using logger_config
loggers = setup_logging(debug=True)  # Enable debug mode for detailed logs
logger = loggers["api"]  # Use 'api' logger from core group

class Settings(BaseModel):
    headless: bool = True
    debug: bool = False

# Initialize FastAPI app
app = FastAPI(
    title="Compliance Claim Processing API",
    description="API for processing individual compliance claims and managing cached compliance data with analytics",
    version="1.0.0"
)

# Global settings
settings = Settings()

# Global instances
facade = None
cache_manager = None
file_handler = None
compliance_handler = None
summary_generator = None
storage_manager = None
marshaller = None
financial_services = None

@app.on_event("startup")
async def startup_event():
    """Initialize API services on startup."""
    global facade, storage_manager, marshaller, financial_services
    
    try:
        # Load full configuration
        config = load_config()
        logger.debug(f"Full config loaded: {json.dumps(config, indent=2)}")
        
        # Get storage configuration
        storage_config = get_storage_config(config)
        logger.debug(f"Storage config retrieved: {json.dumps(storage_config, indent=2)}")
        
        # Initialize storage manager
        try:
            storage_manager = StorageManager(storage_config)
            logger.debug("Successfully created StorageManager")
        except Exception as e:
            logger.error(f"Error creating StorageManager: {str(e)}")
            raise
            
        # Initialize compliance report agent storage
        try:
            compliance_report_storage = StorageProviderFactory.create_provider(storage_config)
            logger.debug(f"Successfully initialized compliance_report_agent storage provider with base_path: {compliance_report_storage.base_path}")
            
            # Initialize compliance report agent's storage provider
            from agents.compliance_report_agent import _storage_provider
            import agents.compliance_report_agent as compliance_report_agent
            compliance_report_agent._storage_provider = compliance_report_storage
            
        except Exception as e:
            logger.error(f"Error initializing compliance report storage: {str(e)}")
            raise
            
        # Initialize Marshaller first since FinancialServicesFacade depends on it
        try:
            marshaller = Marshaller(headless=True)
            logger.debug("Successfully initialized Marshaller")
        except Exception as e:
            logger.error(f"Error initializing Marshaller: {str(e)}")
            raise
            
        # Initialize FinancialServicesFacade with storage manager
        try:
            facade = FinancialServicesFacade(
                headless=True,
                storage_manager=storage_manager
            )
            logger.debug("Successfully initialized FinancialServicesFacade")
        except Exception as e:
            logger.error(f"Error initializing FinancialServicesFacade: {str(e)}")
            raise
            
        # Initialize other services
        try:
            global cache_manager, file_handler, compliance_handler, summary_generator
            cache_manager = CacheManager()
            file_handler = FileHandler(cache_manager.cache_folder)
            compliance_handler = ComplianceHandler(file_handler.base_path)
            summary_generator = SummaryGenerator(file_handler=file_handler, compliance_handler=compliance_handler)
            logger.debug("Successfully initialized cache management services")
        except Exception as e:
            logger.error(f"Error initializing cache management services: {str(e)}")
            raise
            
        logger.info("API services successfully initialized")
        
    except Exception as e:
        logger.error(f"Critical error during startup: {str(e)}", exc_info=True)
        raise

@app.put("/settings")
async def update_settings(new_settings: Settings):
    """Update API settings and reinitialize services if needed."""
    global settings, facade
    old_headless = settings.headless
    settings = new_settings
    
    # If headless mode changed, reinitialize the facade
    if old_headless != settings.headless:
        if facade:
            facade.cleanup()  # Clean up existing WebDriver
        facade = FinancialServicesFacade(headless=settings.headless)
        logger.info(f"Reinitialized FinancialServicesFacade with headless={settings.headless}")
    
    return {"message": "Settings updated", "settings": settings.dict()}

@app.get("/settings")
async def get_settings():
    """Get current API settings."""
    return settings.dict()

# Define processing mode invariants
PROCESSING_MODES = {
    "basic": {
        "skip_disciplinary": True,
        "skip_arbitration": True,
        "skip_regulatory": True,
        "description": "Minimal processing: skips disciplinary, arbitration, and regulatory reviews"
    },
    "extended": {
        "skip_disciplinary": False,
        "skip_arbitration": False,
        "skip_regulatory": True,
        "description": "Extended processing: includes disciplinary and arbitration reviews, skips regulatory"
    },
    "complete": {
        "skip_disciplinary": False,
        "skip_arbitration": False,
        "skip_regulatory": False,
        "description": "Full processing: includes all reviews (disciplinary, arbitration, regulatory)"
    }
}

# Define the request model using Pydantic with mandatory fields
class ClaimRequest(BaseModel):
    reference_id: str
    employee_number: str
    first_name: str
    last_name: str
    individual_name: Optional[str] = None
    crd_number: Optional[str] = None
    organization_crd: Optional[str] = None
    organization_name: Optional[str] = None
    webhook_url: Optional[str] = None

    class Config:
        extra = "allow"

    @validator('crd_number', pre=True, always=True)
    def validate_crd_number(cls, v):
        """Ensure crd_number is explicitly set and not implicitly copied from organization_crd"""
        if v == "":  # Convert empty string to None
            return None
        return v

async def send_to_webhook(webhook_url: str, report: Dict[str, Any], reference_id: str):
    """Asynchronously send the report to the specified webhook URL."""
    async with aiohttp.ClientSession() as session:
        try:
            logger.info(f"Sending report to webhook URL: {webhook_url} for reference_id={reference_id}")
            async with session.post(webhook_url, json=report) as response:
                if response.status == 200:
                    logger.info(f"Successfully sent report to webhook for reference_id={reference_id}")
                else:
                    logger.error(f"Webhook delivery failed for reference_id={reference_id}: Status {response.status}, Response: {await response.text()}")
        except Exception as e:
            logger.error(f"Error sending to webhook for reference_id={reference_id}: {str(e)}", exc_info=True)

async def process_claim_helper(request: ClaimRequest, mode: str) -> Dict[str, Any]:
    """
    Helper function to process a claim with the specified mode.

    Args:
        request (ClaimRequest): The claim data to process.
        mode (str): Processing mode ("basic", "extended", "complete").

    Returns:
        Dict[str, Any]: Processed compliance report.
    """
    logger.info(f"Processing claim with mode='{mode}': {request.dict()}")

    # Extract mode settings
    mode_settings = PROCESSING_MODES[mode]
    skip_disciplinary = mode_settings["skip_disciplinary"]
    skip_arbitration = mode_settings["skip_arbitration"]
    skip_regulatory = mode_settings["skip_regulatory"]

    # Convert Pydantic model to dict for process_claim
    claim = request.dict(exclude_unset=True)
    employee_number = claim.pop("employee_number")
    webhook_url = claim.pop("webhook_url", None)

    # Set individual_name if not provided but we have first_name and last_name
    if not claim.get("individual_name") and claim.get("first_name") and claim.get("last_name"):
        claim["individual_name"] = f"{claim['first_name']} {claim['last_name']}".strip()
        logger.debug(f"Set individual_name to '{claim['individual_name']}' from first_name and last_name")

    try:
        # Process the claim
        report = process_claim(
            claim=claim,
            facade=facade,
            employee_number=employee_number,
            skip_disciplinary=skip_disciplinary,
            skip_arbitration=skip_arbitration,
            skip_regulatory=skip_regulatory
        )
        
        if report is None:
            logger.error(f"Failed to process claim for reference_id={request.reference_id}: process_claim returned None")
            raise HTTPException(status_code=500, detail="Claim processing failed unexpectedly")

        # Report is saved to cache/<employee_number>/ by process_claim
        logger.info(f"Successfully processed claim for reference_id={request.reference_id} with mode={mode}")

        # Handle webhook if provided
        if webhook_url:
            asyncio.create_task(send_to_webhook(webhook_url, report, request.reference_id))
        
        return report

    except Exception as e:
        logger.error(f"Error processing claim for reference_id={request.reference_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Existing Claim Processing Endpoints
@app.post("/process-claim-basic", response_model=Dict[str, Any])
async def process_claim_basic(request: ClaimRequest):
    """Process a claim with basic mode (skips all reviews)."""
    return await process_claim_helper(request, "basic")

@app.post("/process-claim-extended", response_model=Dict[str, Any])
async def process_claim_extended(request: ClaimRequest):
    """Process a claim with extended mode (includes disciplinary and arbitration, skips regulatory)."""
    return await process_claim_helper(request, "extended")

@app.post("/process-claim-complete", response_model=Dict[str, Any])
async def process_claim_complete(request: ClaimRequest):
    """Process a claim with complete mode (includes all reviews)."""
    return await process_claim_helper(request, "complete")

@app.get("/processing-modes")
async def get_processing_modes():
    """Return the available processing modes and their configurations."""
    return PROCESSING_MODES

# New Cache Management Endpoints
@app.post("/cache/clear/{employee_number}")
async def clear_cache(employee_number: str):
    """
    Clear all cache (except ComplianceReportAgent) for a specific employee.

    Args:
        employee_number (str): Employee identifier (e.g., "EN-016314").

    Returns:
        Dict[str, Any]: JSON response with clearance details.
    """
    result = cache_manager.clear_cache(employee_number)
    return json.loads(result)

@app.post("/cache/clear-all")
async def clear_all_cache():
    """
    Clear all cache (except ComplianceReportAgent) across all employees.

    Returns:
        Dict[str, Any]: JSON response with clearance details.
    """
    result = cache_manager.clear_all_cache()
    return json.loads(result)

@app.post("/cache/clear-agent/{employee_number}/{agent_name}")
async def clear_agent_cache(employee_number: str, agent_name: str):
    """
    Clear cache for a specific agent under an employee.

    Args:
        employee_number (str): Employee identifier (e.g., "EN-016314").
        agent_name (str): Agent name (e.g., "SEC_IAPD_Agent").

    Returns:
        Dict[str, Any]: JSON response with clearance details.
    """
    result = cache_manager.clear_agent_cache(employee_number, agent_name)
    return json.loads(result)

@app.get("/cache/list")
async def list_cache(employee_number: Optional[str] = None, page: int = 1, page_size: int = 10):
    """
    List all cached files for an employee or all employees with pagination.

    Args:
        employee_number (Optional[str]): Employee identifier or None/"ALL" for all employees.
        page (int): Page number (default: 1).
        page_size (int): Items per page (default: 10).

    Returns:
        Dict[str, Any]: JSON response with cache contents.
    """
    result = cache_manager.list_cache(employee_number or "ALL", page, page_size)
    return json.loads(result)

@app.post("/cache/cleanup-stale")
async def cleanup_stale_cache():
    """
    Delete stale cache older than 90 days (except ComplianceReportAgent).

    Returns:
        Dict[str, Any]: JSON response with cleanup details.
    """
    result = cache_manager.cleanup_stale_cache()
    return json.loads(result)

# New Compliance Analytics Endpoints
@app.get("/compliance/summary/{employee_number}")
async def get_compliance_summary(employee_number: str, page: int = 1, page_size: int = 10):
    """
    Get a compliance summary for a specific employee with pagination.

    Args:
        employee_number (str): Employee identifier (e.g., "EN-016314").
        page (int): Page number (default: 1).
        page_size (int): Items per page (default: 10).

    Returns:
        Dict[str, Any]: JSON response with summary data.
    """
    emp_path = cache_manager.cache_folder / employee_number
    result = summary_generator.generate_compliance_summary(emp_path, employee_number, page, page_size)
    return json.loads(result)

@app.get("/compliance/all-summaries")
async def get_all_compliance_summaries(page: int = 1, page_size: int = 10):
    """
    Get a compliance summary for all employees with pagination.

    Returns:
        Dict[str, Any]: JSON response with summary data.
    """
    result = summary_generator.generate_all_compliance_summaries(cache_manager.cache_folder, page, page_size)
    return json.loads(result)

@app.get("/compliance/taxonomy")
async def get_compliance_taxonomy():
    """
    Get a taxonomy tree from the latest ComplianceReportAgent JSON files.

    Returns:
        str: Human-readable taxonomy tree text.
    """
    return summary_generator.generate_taxonomy_from_latest_reports()

@app.get("/compliance/risk-dashboard")
async def get_risk_dashboard():
    """
    Get a compliance risk dashboard from the latest ComplianceReportAgent JSON files.

    Returns:
        str: Human-readable risk dashboard text.
    """
    return summary_generator.generate_risk_dashboard()

@app.get("/compliance/data-quality")
async def get_data_quality_report():
    """
    Get a data quality report checking field value presence from the latest ComplianceReportAgent JSON files.

    Returns:
        str: Human-readable data quality report text.
    """
    return summary_generator.generate_data_quality_report()

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown."""
    global facade, marshaller
    
    logger.info("Shutting down API server")
    
    try:
        if facade:
            facade.cleanup()
            logger.debug("Successfully cleaned up FinancialServicesFacade")
    except Exception as e:
        logger.error(f"Error cleaning up FinancialServicesFacade: {str(e)}")
        
    try:
        if marshaller:
            marshaller.cleanup()
            logger.debug("Successfully cleaned up Marshaller")
    except Exception as e:
        logger.error(f"Error cleaning up Marshaller: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")