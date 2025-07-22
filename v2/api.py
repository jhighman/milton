import json
from typing import Dict, Any, Optional, Union
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, validator
from celery import Celery
from celery.result import AsyncResult
import requests
import time
import random
import os
import asyncio  # Still needed for process_claim_helper
from datetime import datetime
from enum import Enum

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
    task_concurrency=4,    # Increased from 1 to 4 for better throughput
    worker_prefetch_multiplier=1,  # Process one task at a time (FIFO)
    task_acks_late=True,   # Acknowledge tasks after completion
    task_default_queue="compliance_queue",
    
    # Broker connection retry settings for improved reliability
    broker_connection_retry=True,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=10,
    broker_connection_timeout=30,
    
    # Broker transport options with visibility timeout
    broker_transport_options={
        'visibility_timeout': 3600,  # 1 hour (matches task_time_limit)
        'queue_name_prefix': 'compliance-',
    },
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

# Webhook status tracking
class WebhookStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"

# In-memory storage for webhook statuses
# In a production environment, this could be replaced with a database
webhook_statuses = {}

@celery_app.task(
    name="send_webhook_notification",
    bind=True,
    max_retries=5,
    default_retry_delay=30,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,  # 5 minutes max delay
    retry_jitter=True
)
def send_webhook_notification(self, webhook_url: str, payload: Dict[str, Any], reference_id: str):
    """
    Celery task to send a notification to a webhook URL with retry logic.
    
    Args:
        webhook_url (str): The URL to send the webhook to
        payload (Dict[str, Any]): The data to send to the webhook
        reference_id (str): The reference ID for tracking
        
    Returns:
        Dict[str, Any]: Status information about the webhook delivery
    """
    # Update webhook status to in progress
    webhook_id = f"{reference_id}_{self.request.id}"
    webhook_statuses[webhook_id] = {
        "status": WebhookStatus.IN_PROGRESS.value,
        "reference_id": reference_id,
        "task_id": self.request.id,
        "webhook_url": webhook_url,
        "attempts": self.request.retries + 1,
        "max_attempts": 5,
        "last_attempt": datetime.utcnow().isoformat(),
        "created_at": webhook_statuses.get(webhook_id, {}).get("created_at", datetime.utcnow().isoformat())
    }
    
    try:
        logger.info(f"Sending webhook notification to {webhook_url} for reference_id={reference_id} (attempt {self.request.retries + 1})")
        
        # Use synchronous requests instead of asyncio (better for Celery workers)
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=30,  # 30 second timeout
            headers={"Content-Type": "application/json", "X-Reference-ID": reference_id}
        )
        
        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"Successfully delivered webhook for reference_id={reference_id} (status={response.status_code})")
            
            # Update webhook status to delivered
            webhook_statuses[webhook_id] = {
                **webhook_statuses[webhook_id],
                "status": WebhookStatus.DELIVERED.value,
                "response_code": response.status_code,
                "completed_at": datetime.utcnow().isoformat()
            }
            
            return {
                "success": True,
                "reference_id": reference_id,
                "status_code": response.status_code,
                "webhook_id": webhook_id
            }
        else:
            error_msg = f"Webhook delivery failed with status {response.status_code}: {response.text}"
            logger.error(f"{error_msg} for reference_id={reference_id}")
            
            # Update webhook status to retrying or failed
            if self.request.retries < 4:  # We have 5 max retries (0-4)
                status = WebhookStatus.RETRYING.value
            else:
                status = WebhookStatus.FAILED.value
                
            webhook_statuses[webhook_id] = {
                **webhook_statuses[webhook_id],
                "status": status,
                "response_code": response.status_code,
                "error": error_msg
            }
            
            # Calculate retry delay with exponential backoff and jitter
            retry_delay = min(30 * (2 ** self.request.retries), 300)  # 30s to 5min
            jitter = random.uniform(0, 0.3) * retry_delay  # Add up to 30% jitter
            retry_delay_with_jitter = retry_delay + jitter
            
            logger.info(f"Retrying webhook delivery for reference_id={reference_id} in {retry_delay_with_jitter:.2f} seconds")
            
            raise self.retry(
                exc=Exception(error_msg),
                countdown=retry_delay_with_jitter
            )
    
    except requests.RequestException as e:
        error_msg = f"Webhook request failed: {str(e)}"
        logger.error(f"{error_msg} for reference_id={reference_id}")
        
        # Update webhook status to retrying or failed
        if self.request.retries < 4:  # We have 5 max retries (0-4)
            status = WebhookStatus.RETRYING.value
        else:
            status = WebhookStatus.FAILED.value
            
        webhook_statuses[webhook_id] = {
            **webhook_statuses[webhook_id],
            "status": status,
            "error": error_msg
        }
        
        # Calculate retry delay with exponential backoff and jitter
        retry_delay = min(30 * (2 ** self.request.retries), 300)  # 30s to 5min
        jitter = random.uniform(0, 0.3) * retry_delay  # Add up to 30% jitter
        retry_delay_with_jitter = retry_delay + jitter
        
        logger.info(f"Retrying webhook delivery for reference_id={reference_id} in {retry_delay_with_jitter:.2f} seconds")
        
        raise self.retry(
            exc=e,
            countdown=retry_delay_with_jitter
        )

def initialize_services():
    """Initialize API services. Used by both FastAPI startup and Celery workers."""
    global facade, storage_manager, marshaller, financial_services, cache_manager, file_handler, compliance_handler, summary_generator
    
    # Skip initialization if already done
    if facade is not None:
        return
    
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
        logger.error(f"Critical error during initialization: {str(e)}", exc_info=True)
        raise

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize API services on startup."""
    initialize_services()

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
    
    # Ensure services are initialized for Celery worker
    initialize_services()
    
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
            logger.info(f"Queuing webhook notification for reference_id={request.reference_id}")
            send_webhook_notification.delay(webhook_url, report, request.reference_id)
        
        return report
    
    except Exception as e:
        logger.error(f"Error processing claim for reference_id={request_dict['reference_id']}: {str(e)}", exc_info=True)
        error_report = {
            "status": "error",
            "reference_id": request_dict["reference_id"],
            "message": f"Claim processing failed: {str(e)}"
        }
        if webhook_url:
            logger.info(f"Queuing webhook notification for error report, reference_id={request_dict['reference_id']}")
            send_webhook_notification.delay(webhook_url, error_report, request_dict["reference_id"])
        self.retry(exc=e, countdown=60)  # Retry after 60 seconds, up to 3 times
        return error_report

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
            logger.info(f"Queuing webhook notification for reference_id={request.reference_id}")
            send_webhook_notification.delay(webhook_url, report, request.reference_id)
        
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

# Webhook status tracking endpoints
@app.get("/webhook-status/{webhook_id}", response_model=Dict[str, Any])
async def get_webhook_status(webhook_id: str):
    """
    Get the status of a specific webhook delivery.
    
    Args:
        webhook_id (str): The webhook ID to check.
        
    Returns:
        Dict[str, Any]: The webhook status information.
    """
    if webhook_id not in webhook_statuses:
        raise HTTPException(status_code=404, detail=f"Webhook status not found for ID: {webhook_id}")
    
    return webhook_statuses[webhook_id]

@app.get("/webhook-statuses", response_model=Dict[str, Any])
async def list_webhook_statuses(
    reference_id: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    page_size: int = 10
):
    """
    List all webhook statuses with optional filtering and pagination.
    
    Args:
        reference_id (str, optional): Filter by reference ID.
        status (str, optional): Filter by status (pending, in_progress, delivered, failed, retrying).
        page (int): Page number for pagination.
        page_size (int): Number of items per page.
        
    Returns:
        Dict[str, Any]: Paginated list of webhook statuses.
    """
    # Filter statuses based on query parameters
    filtered_statuses = webhook_statuses.copy()
    
    if reference_id:
        filtered_statuses = {
            k: v for k, v in filtered_statuses.items()
            if v.get("reference_id") == reference_id
        }
    
    if status:
        filtered_statuses = {
            k: v for k, v in filtered_statuses.items()
            if v.get("status") == status
        }
    
    # Paginate results
    total_items = len(filtered_statuses)
    total_pages = (total_items + page_size - 1) // page_size if total_items > 0 else 1
    
    # Ensure page is within valid range
    if page < 1:
        page = 1
    elif page > total_pages:
        page = total_pages
    
    # Get items for current page
    start_idx = (page - 1) * page_size
    end_idx = min(start_idx + page_size, total_items)
    
    # Convert dict to list for pagination
    statuses_list = list(filtered_statuses.items())
    paginated_statuses = dict(statuses_list[start_idx:end_idx])
    
    return {
        "items": paginated_statuses,
        "total_items": total_items,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages
    }

@app.delete("/webhook-status/{webhook_id}", response_model=Dict[str, Any])
async def delete_webhook_status(webhook_id: str):
    """
    Delete a specific webhook status.
    
    Args:
        webhook_id (str): The webhook ID to delete.
        
    Returns:
        Dict[str, Any]: Confirmation of deletion.
    """
    if webhook_id not in webhook_statuses:
        raise HTTPException(status_code=404, detail=f"Webhook status not found for ID: {webhook_id}")
    
    deleted_status = webhook_statuses.pop(webhook_id)
    
    return {
        "message": f"Webhook status deleted for ID: {webhook_id}",
        "deleted_status": deleted_status
    }

@app.delete("/webhook-statuses", response_model=Dict[str, Any])
async def delete_all_webhook_statuses(
    reference_id: Optional[str] = None,
    status: Optional[str] = None
):
    """
    Delete all webhook statuses with optional filtering.
    
    Args:
        reference_id (str, optional): Filter by reference ID.
        status (str, optional): Filter by status (pending, in_progress, delivered, failed, retrying).
        
    Returns:
        Dict[str, Any]: Confirmation of deletion with count.
    """
    global webhook_statuses
    
    # If no filters, delete all statuses
    if not reference_id and not status:
        count = len(webhook_statuses)
        webhook_statuses = {}
        return {"message": f"All webhook statuses deleted ({count} total)"}
    
    # Filter statuses to delete
    to_delete = []
    
    for webhook_id, status_info in webhook_statuses.items():
        if reference_id and status_info.get("reference_id") != reference_id:
            continue
        
        if status and status_info.get("status") != status:
            continue
        
        to_delete.append(webhook_id)
    
    # Delete filtered statuses
    for webhook_id in to_delete:
        webhook_statuses.pop(webhook_id)
    
    return {
        "message": f"Webhook statuses deleted ({len(to_delete)} total)",
        "deleted_count": len(to_delete),
        "filters": {
            "reference_id": reference_id,
            "status": status
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")