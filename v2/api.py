import json
from typing import Dict, Any, Optional, Union
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, validator
from celery import Celery
from celery.result import AsyncResult
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
from main_config import get_storage_config, load_config
from storage_manager import StorageManager
from storage_providers.factory import StorageProviderFactory

# Setup logging
loggers = setup_logging(debug=True)
logger = loggers["api"]

# Initialize Celery with Redis
celery_app = Celery(
    "compliance_tasks",
    broker="redis://localhost:6379/0",  # Redis as message broker
    backend="redis://localhost:6379/0",  # Redis as result backend
)
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_track_started=True,
    task_time_limit=3600,  # 1-hour timeout for tasks
    task_concurrency=1,    # Single-threaded worker
    worker_prefetch_multiplier=1,  # Process one task at a time (FIFO)
    task_acks_late=True,   # Acknowledge tasks after completion
    task_default_queue="compliance_queue",
)

# Settings, ClaimRequest, and TaskStatusResponse models
class Settings(BaseModel):
    headless: bool = True
    debug: bool = False

class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    reference_id: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

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
        if v == "":
            return None
        return v

# Initialize FastAPI app
app = FastAPI(
    title="Compliance Claim Processing API",
    description="API for processing individual compliance claims and managing cached compliance data with analytics",
    version="1.0.0"
)

# Global instances
settings = Settings()
facade = None
cache_manager = None
file_handler = None
compliance_handler = None
summary_generator = None
storage_manager = None
marshaller = None
financial_services = None

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize API services on startup."""
    global facade, storage_manager, marshaller, financial_services, cache_manager, file_handler, compliance_handler, summary_generator
    
    try:
        # Load configuration
        config = load_config()
        logger.debug(f"Full config loaded: {json.dumps(config, indent=2)}")
        
        # Initialize storage
        storage_config = get_storage_config(config)
        logger.debug(f"Storage config retrieved: {json.dumps(storage_config, indent=2)}")
        storage_manager = StorageManager(storage_config)
        compliance_report_storage = StorageProviderFactory.create_provider(storage_config)
        logger.debug(f"Successfully initialized compliance_report_agent storage provider with base_path: {compliance_report_storage.base_path}")
        
        from agents.compliance_report_agent import _storage_provider
        import agents.compliance_report_agent as compliance_report_agent
        compliance_report_agent._storage_provider = compliance_report_storage
        
        # Initialize Marshaller and FinancialServicesFacade
        marshaller = Marshaller(headless=True)
        facade = FinancialServicesFacade(headless=True, storage_manager=storage_manager)
        
        # Initialize cache and compliance services
        cache_manager = CacheManager()
        file_handler = FileHandler(cache_manager.cache_folder)
        compliance_handler = ComplianceHandler(file_handler.base_path)
        summary_generator = SummaryGenerator(file_handler=file_handler, compliance_handler=compliance_handler)
        logger.info("API services successfully initialized")
        
    except Exception as e:
        logger.error(f"Critical error during startup: {str(e)}", exc_info=True)
        raise

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown."""
    global facade, marshaller
    logger.info("Shutting down API server")
    try:
        if facade:
            facade.cleanup()
            logger.debug("Successfully cleaned up FinancialServicesFacade")
        if marshaller:
            marshaller.cleanup()
            logger.debug("Successfully cleaned up Marshaller")
    except Exception as e:
        logger.error(f"Error cleaning up: {str(e)}")

# Helper function to get celery app for dependency injection
def get_celery_app():
    return celery_app

# Celery task for processing claims
@celery_app.task(name="process_compliance_claim", bind=True, max_retries=3, default_retry_delay=60)
def process_compliance_claim(self, request_dict: Dict[str, Any], mode: str):
    """
    Celery task to process a claim asynchronously.
    
    Args:
        request_dict (Dict[str, Any]): Claim request data.
        mode (str): Processing mode ("basic", "extended", "complete").
    
    Returns:
        Dict[str, Any]: Processed compliance report or error details.
    """
    logger.info(f"Starting Celery task for reference_id={request_dict['reference_id']} with mode={mode}")
    
    # Store reference_id in task metadata
    self.update_state(state="PENDING", meta={"reference_id": request_dict['reference_id']})
    
    try:
        # Convert dict to ClaimRequest for validation
        request = ClaimRequest(**request_dict)
        mode_settings = PROCESSING_MODES[mode]
        claim = request.dict(exclude_unset=True)
        employee_number = claim.pop("employee_number")
        webhook_url = claim.pop("webhook_url", None)

        if not claim.get("individual_name") and claim.get("first_name") and claim.get("last_name"):
            claim["individual_name"] = f"{claim['first_name']} {claim['last_name']}".strip()
            logger.debug(f"Set individual_name to '{claim['individual_name']}'")

        # Process the claim
        report = process_claim(
            claim=claim,
            facade=facade,
            employee_number=employee_number,
            skip_disciplinary=mode_settings["skip_disciplinary"],
            skip_arbitration=mode_settings["skip_arbitration"],
            skip_regulatory=mode_settings["skip_regulatory"]
        )
        
        if report is None:
            logger.error(f"Failed to process claim for reference_id={request.reference_id}: process_claim returned None")
            raise ValueError("Claim processing failed unexpectedly")

        logger.info(f"Successfully processed claim for reference_id={request.reference_id}")

        # Send to webhook if provided
        if webhook_url:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(send_to_webhook(webhook_url, report, request.reference_id))
            finally:
                loop.close()
        
        return report
    
    except Exception as e:
        logger.error(f"Error processing claim for reference_id={request_dict['reference_id']}: {str(e)}", exc_info=True)
        error_report = {
            "status": "error",
            "reference_id": request_dict["reference_id"],
            "message": f"Claim processing failed: {str(e)}"
        }
        if webhook_url:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(send_to_webhook(webhook_url, error_report, request_dict["reference_id"]))
            finally:
                loop.close()
        self.retry(exc=e, countdown=60)  # Retry after 60 seconds, up to 3 times
        return error_report

# Webhook function
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

# Helper function for synchronous claim processing
async def process_claim_helper(request: ClaimRequest, mode: str, send_webhook: bool = True) -> Dict[str, Any]:
    """
    Helper function to process a claim with the specified mode.

    Args:
        request (ClaimRequest): The claim data to process.
        mode (str): Processing mode ("basic", "extended", "complete").
        send_webhook (bool): Whether to send the result to webhook if webhook_url is provided.

    Returns:
        Dict[str, Any]: Processed compliance report.
    """
    logger.info(f"Processing claim with mode='{mode}': {request.dict()}")

    mode_settings = PROCESSING_MODES[mode]
    skip_disciplinary = mode_settings["skip_disciplinary"]
    skip_arbitration = mode_settings["skip_arbitration"]
    skip_regulatory = mode_settings["skip_regulatory"]

    claim = request.dict(exclude_unset=True)
    employee_number = claim.pop("employee_number")
    webhook_url = claim.pop("webhook_url", None)

    if not claim.get("individual_name") and claim.get("first_name") and claim.get("last_name"):
        claim["individual_name"] = f"{claim['first_name']} {claim['last_name']}".strip()
        logger.debug(f"Set individual_name to '{claim['individual_name']}' from first_name and last_name")

    try:
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

        logger.info(f"Successfully processed claim for reference_id={request.reference_id} with mode={mode}")

        if webhook_url and send_webhook:
            asyncio.create_task(send_to_webhook(webhook_url, report, request.reference_id))
        
        return report

    except Exception as e:
        logger.error(f"Error processing claim for reference_id={request.reference_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Settings endpoints
@app.put("/settings")
async def update_settings(new_settings: Settings):
    """Update API settings and reinitialize services if needed."""
    global settings, facade
    old_headless = settings.headless
    settings = new_settings
    
    if old_headless != settings.headless:
        if facade:
            facade.cleanup()
        facade = FinancialServicesFacade(headless=settings.headless, storage_manager=storage_manager)
        logger.info(f"Reinitialized FinancialServicesFacade with headless={settings.headless}")
    
    return {"message": "Settings updated", "settings": settings.dict()}

@app.get("/settings")
async def get_settings():
    """Get current API settings."""
    return settings.dict()

# Processing modes
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

# Claim processing endpoints
@app.post("/process-claim-basic", response_model=Dict[str, Any])
async def process_claim_basic(request: ClaimRequest):
    """
    Process a claim with basic mode (skips all reviews).
    If webhook_url is provided, queues the task with Celery for asynchronous processing.
    If no webhook_url, processes synchronously.
    """
    if request.webhook_url:
        logger.info(f"Queuing claim processing for reference_id={request.reference_id} with mode=basic")
        task = process_compliance_claim.delay(request.dict(), "basic")
        return {
            "status": "processing_queued",
            "reference_id": request.reference_id,
            "task_id": task.id,
            "message": "Claim processing queued; result will be sent to webhook"
        }
    else:
        logger.info(f"Synchronous processing started for reference_id={request.reference_id} with mode=basic")
        return await process_claim_helper(request, "basic")

@app.post("/process-claim-extended", response_model=Dict[str, Any])
async def process_claim_extended(request: ClaimRequest):
    """
    Process a claim with extended mode (includes disciplinary and arbitration, skips regulatory).
    If webhook_url is provided, queues the task with Celery for asynchronous processing.
    If no webhook_url, processes synchronously.
    """
    if request.webhook_url:
        logger.info(f"Queuing claim processing for reference_id={request.reference_id} with mode=extended")
        task = process_compliance_claim.delay(request.dict(), "extended")
        return {
            "status": "processing_queued",
            "reference_id": request.reference_id,
            "task_id": task.id,
            "message": "Claim processing queued; result will be sent to webhook"
        }
    else:
        logger.info(f"Synchronous processing started for reference_id={request.reference_id} with mode=extended")
        return await process_claim_helper(request, "extended")

@app.post("/process-claim-complete", response_model=Dict[str, Any])
async def process_claim_complete(request: ClaimRequest):
    """
    Process a claim with complete mode (includes all reviews).
    If webhook_url is provided, queues the task with Celery for asynchronous processing.
    If no webhook_url, processes synchronously.
    """
    if request.webhook_url:
        logger.info(f"Queuing claim processing for reference_id={request.reference_id} with mode=complete")
        task = process_compliance_claim.delay(request.dict(), "complete")
        return {
            "status": "processing_queued",
            "reference_id": request.reference_id,
            "task_id": task.id,
            "message": "Claim processing queued; result will be sent to webhook"
        }
    else:
        logger.info(f"Synchronous processing started for reference_id={request.reference_id} with mode=complete")
        return await process_claim_helper(request, "complete")

@app.get("/processing-modes")
async def get_processing_modes():
    """Return the available processing modes and their configurations."""
    return PROCESSING_MODES

@app.get("/task-status/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(task_id: str, celery_app=Depends(get_celery_app)):
    """
    Check the status of a queued or in-progress task.
    
    Args:
        task_id (str): The unique task ID returned in the response of an asynchronous claim processing request.
        
    Returns:
        TaskStatusResponse: The current status of the task, including reference_id, result or error if available.
    """
    task = AsyncResult(task_id, app=celery_app)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Retrieve task metadata from Redis
    task_info = task.info or {}
    reference_id = task_info.get("reference_id") if isinstance(task_info, dict) else None
    
    # Map Celery states to user-friendly statuses
    status_map = {
        "PENDING": "QUEUED",
        "STARTED": "PROCESSING",
        "SUCCESS": "COMPLETED",
        "FAILURE": "FAILED",
        "RETRY": "RETRYING"
    }
    status = status_map.get(task.state, task.state)
    
    # Get result or error
    result = task.result if task.state == "SUCCESS" and isinstance(task.result, dict) else None
    error = str(task.result) if task.state == "FAILURE" else None
    
    return {
        "task_id": task_id,
        "status": status,
        "reference_id": reference_id,
        "result": result,
        "error": error
    }

# Cache management endpoints
@app.post("/cache/clear/{employee_number}")
async def clear_cache(employee_number: str):
    """
    Clear all cache (except ComplianceReportAgent) for a specific employee.
    """
    result = cache_manager.clear_cache(employee_number)
    return json.loads(result)

@app.post("/cache/clear-all")
async def clear_all_cache():
    """
    Clear all cache (except ComplianceReportAgent) across all employees.
    """
    result = cache_manager.clear_all_cache()
    return json.loads(result)

@app.post("/cache/clear-agent/{employee_number}/{agent_name}")
async def clear_agent_cache(employee_number: str, agent_name: str):
    """
    Clear cache for a specific agent under an employee.
    """
    result = cache_manager.clear_agent_cache(employee_number, agent_name)
    return json.loads(result)

@app.get("/cache/list")
async def list_cache(employee_number: Optional[str] = None, page: int = 1, page_size: int = 10):
    """
    List all cached files for an employee or all employees with pagination.
    """
    result = cache_manager.list_cache(employee_number or "ALL", page, page_size)
    return json.loads(result)

@app.post("/cache/cleanup-stale")
async def cleanup_stale_cache():
    """
    Delete stale cache older than 90 days (except ComplianceReportAgent).
    """
    result = cache_manager.cleanup_stale_cache()
    return json.loads(result)

# Compliance analytics endpoints
@app.get("/compliance/summary/{employee_number}")
async def get_compliance_summary(employee_number: str, page: int = 1, page_size: int = 10):
    """
    Get a compliance summary for a specific employee with pagination.
    """
    emp_path = cache_manager.cache_folder / employee_number
    result = summary_generator.generate_compliance_summary(emp_path, employee_number, page, page_size)
    return json.loads(result)

@app.get("/compliance/all-summaries")
async def get_all_compliance_summaries(page: int = 1, page_size: int = 10):
    """
    Get a compliance summary for all employees with pagination.
    """
    result = summary_generator.generate_all_compliance_summaries(cache_manager.cache_folder, page, page_size)
    return json.loads(result)

@app.get("/compliance/taxonomy")
async def get_compliance_taxonomy():
    """
    Get a taxonomy tree from the latest ComplianceReportAgent JSON files.
    """
    return summary_generator.generate_taxonomy_from_latest_reports()

@app.get("/compliance/risk-dashboard")
async def get_risk_dashboard():
    """
    Get a compliance risk dashboard from the latest ComplianceReportAgent JSON files.
    """
    return summary_generator.generate_risk_dashboard()

@app.get("/compliance/data-quality")
async def get_data_quality_report():
    """
    Get a data quality report checking field value presence from the latest ComplianceReportAgent JSON files.
    """
    return summary_generator.generate_data_quality_report()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")