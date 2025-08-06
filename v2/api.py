import json
from typing import Dict, Any, Optional, Union, List
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
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
import redis
import uuid
import traceback
from prometheus_client import Counter, Histogram, Gauge, Summary, start_http_server
import threading
import functools

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

# Initialize Redis client
redis_client = redis.Redis(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=int(os.environ.get("REDIS_PORT", 6379)),
    db=int(os.environ.get("REDIS_DB", 0)),
    decode_responses=True  # Automatically decode responses to strings
)

# Initialize Prometheus metrics
WEBHOOK_COUNTER = Counter(
    'webhook_delivery_total',
    'Total number of webhook deliveries',
    ['status', 'reference_id']
)
WEBHOOK_DELIVERY_TIME = Histogram(
    'webhook_delivery_seconds',
    'Time spent processing webhook deliveries',
    ['reference_id']
)
TASK_COUNTER = Counter(
    'task_processing_total',
    'Total number of tasks processed',
    ['status', 'mode']
)
TASK_PROCESSING_TIME = Histogram(
    'task_processing_seconds',
    'Time spent processing tasks',
    ['mode']
)
CIRCUIT_BREAKER_STATUS = Gauge(
    'circuit_breaker_status',
    'Circuit breaker status (0=closed, 1=open, 0.5=half-open)',
    ['service']
)
REDIS_KEYS_GAUGE = Gauge(
    'redis_webhook_keys',
    'Number of webhook keys in Redis',
    ['status']
)

# Start Prometheus metrics server on a separate thread
def start_metrics_server():
    start_http_server(8000)

threading.Thread(target=start_metrics_server, daemon=True).start()

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

# TTL values for different webhook statuses (in seconds)
WEBHOOK_TTL = {
    WebhookStatus.PENDING.value: 7 * 24 * 60 * 60,     # 7 days for pending
    WebhookStatus.IN_PROGRESS.value: 7 * 24 * 60 * 60, # 7 days for in progress
    WebhookStatus.DELIVERED.value: 30 * 60,            # 30 minutes for delivered
    WebhookStatus.FAILED.value: 7 * 24 * 60 * 60,      # 7 days for failed
    WebhookStatus.RETRYING.value: 7 * 24 * 60 * 60     # 7 days for retrying
}

# Redis-based webhook status storage
def get_webhook_key(webhook_id):
    """Generate Redis key for webhook status"""
    return f"webhook_status:{webhook_id}"

def save_webhook_status(webhook_id, status_data):
    """Save webhook status to Redis with appropriate TTL"""
    key = get_webhook_key(webhook_id)
    status = status_data.get("status")
    ttl = WEBHOOK_TTL.get(status, 7 * 24 * 60 * 60)  # Default to 7 days
    
    # Store as JSON string
    redis_client.set(key, json.dumps(status_data))
    redis_client.expire(key, ttl)

def get_webhook_status(webhook_id):
    """Get webhook status from Redis"""
    key = get_webhook_key(webhook_id)
    data = redis_client.get(key)
    if data:
        return json.loads(data)
    return None

def delete_webhook_status(webhook_id):
    """Delete webhook status from Redis"""
    key = get_webhook_key(webhook_id)
    status_data = get_webhook_status(webhook_id)
    if status_data:
        redis_client.delete(key)
        return status_data
    return None

def get_all_webhook_statuses(reference_id=None, status=None, page=1, page_size=10):
    """Get all webhook statuses with optional filtering and pagination"""
    # Get all webhook keys
    pattern = "webhook_status:*"
    all_keys = redis_client.keys(pattern)
    
    # Get data for all keys
    all_statuses = {}
    for key in all_keys:
        webhook_id = key.split(":", 1)[1]  # Extract ID from key
        status_data = get_webhook_status(webhook_id)
        if status_data:
            # Apply filters
            if reference_id and status_data.get("reference_id") != reference_id:
                continue
            if status and status_data.get("status") != status:
                continue
            all_statuses[webhook_id] = status_data
    
    # Paginate results
    total_items = len(all_statuses)
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
    statuses_list = list(all_statuses.items())
    paginated_statuses = dict(statuses_list[start_idx:end_idx])
    
    return {
        "items": paginated_statuses,
        "total_items": total_items,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages
    }

def delete_all_webhook_statuses(reference_id=None, status=None):
    """Delete all webhook statuses with optional filtering"""
    pattern = "webhook_status:*"
    all_keys = redis_client.keys(pattern)
    
    to_delete = []
    for key in all_keys:
        webhook_id = key.split(":", 1)[1]  # Extract ID from key
        status_data = get_webhook_status(webhook_id)
        
        if not status_data:
            continue
            
        # Apply filters
        if reference_id and status_data.get("reference_id") != reference_id:
            continue
        if status and status_data.get("status") != status:
            continue
            
        to_delete.append(webhook_id)
    
    # Delete filtered statuses
    for webhook_id in to_delete:
        delete_webhook_status(webhook_id)
    
    return len(to_delete)

# Circuit breaker implementation
class CircuitBreaker:
    def __init__(self, name, failure_threshold=5, reset_timeout=60):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.state = "closed"  # closed, open, half-open
        self.last_failure_time = 0
        self.lock = threading.RLock()
        
        # Initialize gauge with closed state
        CIRCUIT_BREAKER_STATUS.labels(service=name).set(0)
    
    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with self.lock:
                # Check if circuit is open
                if self.state == "open":
                    current_time = time.time()
                    # Check if reset timeout has elapsed
                    if current_time - self.last_failure_time > self.reset_timeout:
                        logger.info(f"Circuit {self.name} transitioning from open to half-open")
                        self.state = "half-open"
                        CIRCUIT_BREAKER_STATUS.labels(service=self.name).set(0.5)
                    else:
                        raise CircuitBreakerOpenError(f"Circuit {self.name} is open")
            
            try:
                # If circuit is closed or half-open, try the call
                result = func(*args, **kwargs)
                
                # If call succeeds and circuit was half-open, close it
                with self.lock:
                    if self.state == "half-open":
                        logger.info(f"Circuit {self.name} transitioning from half-open to closed")
                        self.state = "closed"
                        self.failures = 0
                        CIRCUIT_BREAKER_STATUS.labels(service=self.name).set(0)
                
                return result
                
            except Exception as e:
                with self.lock:
                    # Increment failure counter
                    self.failures += 1
                    self.last_failure_time = time.time()
                    
                    # If we've reached the threshold and circuit isn't already open, open it
                    if self.failures >= self.failure_threshold and self.state != "open":
                        logger.warning(f"Circuit {self.name} transitioning to open after {self.failures} failures")
                        self.state = "open"
                        CIRCUIT_BREAKER_STATUS.labels(service=self.name).set(1)
                
                # Re-raise the original exception
                raise
        
        return wrapper

class CircuitBreakerOpenError(Exception):
    """Exception raised when a circuit breaker is open"""
    pass

# Error classification
class ValidationError(Exception):
    """Exception raised for validation errors"""
    pass

class NetworkError(Exception):
    """Exception raised for network-related errors"""
    pass

class UnexpectedError(Exception):
    """Exception raised for unexpected errors"""
    pass

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
    # Generate correlation ID for tracking this webhook delivery
    correlation_id = str(uuid.uuid4())
    
    # Update webhook status to in progress
    webhook_id = f"{reference_id}_{self.request.id}"
    status_data = {
        "status": WebhookStatus.IN_PROGRESS.value,
        "reference_id": reference_id,
        "task_id": self.request.id,
        "webhook_url": webhook_url,
        "attempts": self.request.retries + 1,
        "max_attempts": 3,  # Reduced from 5 to 3 to prevent excessive retries
        "last_attempt": datetime.utcnow().isoformat(),
        "correlation_id": correlation_id
    }
    
    # Check if this is a new webhook or an update
    existing_status = get_webhook_status(webhook_id)
    if existing_status:
        status_data["created_at"] = existing_status.get("created_at")
    else:
        status_data["created_at"] = datetime.utcnow().isoformat()
    
    # Save to Redis
    save_webhook_status(webhook_id, status_data)
    
    try:
        logger.info(f"[{correlation_id}] Sending webhook notification to {webhook_url} for reference_id={reference_id} (attempt {self.request.retries + 1})")
        
        # Pre-execution validation
        if not webhook_url.startswith(('http://', 'https://')):
            error_msg = f"Invalid webhook URL format: {webhook_url}"
            logger.error(f"[{correlation_id}] {error_msg} for reference_id={reference_id}")
            
            # Update webhook status to failed
            status_data.update({
                "status": WebhookStatus.FAILED.value,
                "error": error_msg,
                "error_type": "validation_error"
            })
            save_webhook_status(webhook_id, status_data)
            
            # Store in dead letter queue
            dead_letter_key = f"dead_letter:webhook:{webhook_id}"
            redis_client.set(dead_letter_key, json.dumps({
                "webhook_id": webhook_id,
                "reference_id": reference_id,
                "webhook_url": webhook_url,
                "payload": payload,
                "error": error_msg,
                "attempts": self.request.retries + 1,
                "last_attempt": datetime.utcnow().isoformat(),
                "correlation_id": correlation_id
            }))
            redis_client.expire(dead_letter_key, 30 * 24 * 60 * 60)  # 30 days TTL
            logger.warning(f"[{correlation_id}] Moved failed webhook to dead letter queue: {dead_letter_key}")
            
            # Don't retry for validation errors
            return {
                "success": False,
                "reference_id": reference_id,
                "error": error_msg,
                "webhook_id": webhook_id,
                "correlation_id": correlation_id
            }
        
        # Use synchronous requests instead of asyncio (better for Celery workers)
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=30,  # 30 second timeout
            headers={
                "Content-Type": "application/json",
                "X-Reference-ID": reference_id,
                "X-Correlation-ID": correlation_id
            }
        )
        
        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"[{correlation_id}] Successfully delivered webhook for reference_id={reference_id} (status={response.status_code})")
            
            # Update webhook status to delivered
            status_data.update({
                "status": WebhookStatus.DELIVERED.value,
                "response_code": response.status_code,
                "completed_at": datetime.utcnow().isoformat()
            })
            save_webhook_status(webhook_id, status_data)
            
            return {
                "success": True,
                "reference_id": reference_id,
                "status_code": response.status_code,
                "webhook_id": webhook_id,
                "correlation_id": correlation_id
            }
        else:
            error_msg = f"Webhook delivery failed with status {response.status_code}: {response.text}"
            logger.error(f"[{correlation_id}] {error_msg} for reference_id={reference_id}")
            
            # Classify error based on status code
            if response.status_code >= 400 and response.status_code < 500:
                # 4xx errors are client errors - likely permanent
                if self.request.retries >= 1:  # Only retry once for client errors
                    status = WebhookStatus.FAILED.value
                    error_type = "permanent_client_error"
                else:
                    status = WebhookStatus.RETRYING.value
                    error_type = "client_error"
            else:
                # 5xx errors are server errors - likely transient
                if self.request.retries < 2:  # We have 3 max retries (0-2)
                    status = WebhookStatus.RETRYING.value
                    error_type = "transient_server_error"
                else:
                    status = WebhookStatus.FAILED.value
                    error_type = "repeated_server_error"
            
            # Update webhook status
            status_data.update({
                "status": status,
                "response_code": response.status_code,
                "error": error_msg,
                "error_type": error_type
            })
            save_webhook_status(webhook_id, status_data)
            
            # Calculate retry delay with exponential backoff and jitter
            retry_delay = min(30 * (2 ** self.request.retries), 300)  # 30s to 5min
            jitter = random.uniform(0, 0.3) * retry_delay  # Add up to 30% jitter
            retry_delay_with_jitter = retry_delay + jitter
            
            logger.info(f"[{correlation_id}] Retrying webhook delivery for reference_id={reference_id} in {retry_delay_with_jitter:.2f} seconds")
            
            # If we've reached max retries, move to dead letter queue
            if status == WebhookStatus.FAILED.value:
                # Store in dead letter queue
                dead_letter_key = f"dead_letter:webhook:{webhook_id}"
                redis_client.set(dead_letter_key, json.dumps({
                    "webhook_id": webhook_id,
                    "reference_id": reference_id,
                    "webhook_url": webhook_url,
                    "payload": payload,
                    "error": error_msg,
                    "attempts": self.request.retries + 1,
                    "last_attempt": datetime.utcnow().isoformat(),
                    "correlation_id": correlation_id
                }))
                redis_client.expire(dead_letter_key, 30 * 24 * 60 * 60)  # 30 days TTL
                logger.warning(f"[{correlation_id}] Moved failed webhook to dead letter queue: {dead_letter_key}")
            
            raise self.retry(
                exc=Exception(error_msg),
                countdown=retry_delay_with_jitter
            )
    
    except requests.RequestException as e:
        # Network errors might be transient
        error_msg = f"Webhook request failed: {str(e)}"
        logger.error(f"[{correlation_id}] {error_msg} for reference_id={reference_id}", exc_info=True)
        
        # Classify network errors
        if isinstance(e, requests.Timeout):
            error_type = "timeout"
        elif isinstance(e, requests.ConnectionError):
            error_type = "connection_error"
        else:
            error_type = "request_error"
        
        # Update webhook status
        if self.request.retries < 2:  # We have 3 max retries (0-2)
            status = WebhookStatus.RETRYING.value
        else:
            status = WebhookStatus.FAILED.value
            
        status_data.update({
            "status": status,
            "error": error_msg,
            "error_type": error_type
        })
        save_webhook_status(webhook_id, status_data)
        
        # Calculate retry delay with exponential backoff and jitter
        retry_delay = min(30 * (2 ** self.request.retries), 300)  # 30s to 5min
        jitter = random.uniform(0, 0.3) * retry_delay  # Add up to 30% jitter
        retry_delay_with_jitter = retry_delay + jitter
        
        logger.info(f"[{correlation_id}] Retrying webhook delivery for reference_id={reference_id} in {retry_delay_with_jitter:.2f} seconds")
        
        # If we've reached max retries, move to dead letter queue
        if status == WebhookStatus.FAILED.value:
            # Store in dead letter queue
            dead_letter_key = f"dead_letter:webhook:{webhook_id}"
            redis_client.set(dead_letter_key, json.dumps({
                "webhook_id": webhook_id,
                "reference_id": reference_id,
                "webhook_url": webhook_url,
                "payload": payload,
                "error": error_msg,
                "attempts": self.request.retries + 1,
                "last_attempt": datetime.utcnow().isoformat(),
                "correlation_id": correlation_id
            }))
            redis_client.expire(dead_letter_key, 30 * 24 * 60 * 60)  # 30 days TTL
            logger.warning(f"[{correlation_id}] Moved failed webhook to dead letter queue: {dead_letter_key}")
        
        raise self.retry(
            exc=e,
            countdown=retry_delay_with_jitter
        )
    except Exception as e:
        # Unexpected errors
        error_msg = f"Unexpected error during webhook delivery: {str(e)}"
        logger.error(f"[{correlation_id}] {error_msg} for reference_id={reference_id}", exc_info=True)
        
        # Update webhook status
        status_data.update({
            "status": WebhookStatus.FAILED.value,
            "error": error_msg,
            "error_type": "unexpected_error",
            "stack_trace": traceback.format_exc()
        })
        save_webhook_status(webhook_id, status_data)
        
        # Store in dead letter queue
        dead_letter_key = f"dead_letter:webhook:{webhook_id}"
        redis_client.set(dead_letter_key, json.dumps({
            "webhook_id": webhook_id,
            "reference_id": reference_id,
            "webhook_url": webhook_url,
            "payload": payload,
            "error": error_msg,
            "attempts": self.request.retries + 1,
            "last_attempt": datetime.utcnow().isoformat(),
            "correlation_id": correlation_id
        }))
        redis_client.expire(dead_letter_key, 30 * 24 * 60 * 60)  # 30 days TTL
        logger.warning(f"[{correlation_id}] Moved failed webhook to dead letter queue: {dead_letter_key}")
        
        # Re-raise the exception
        raise

def initialize_services():
    """Initialize API services. Used by both FastAPI startup and Celery workers.
    
    Returns:
        bool: True if initialization was successful, False otherwise.
    """
    global facade, storage_manager, marshaller, financial_services, cache_manager, file_handler, compliance_handler, summary_generator
    
    # Skip initialization if already done
    if facade is not None:
        logger.debug("Services already initialized, skipping initialization")
        return True
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        logger.debug(f"Full config loaded: {json.dumps(config, indent=2)}")
        
        # Initialize storage
        logger.info("Initializing storage...")
        storage_config = get_storage_config(config)
        logger.debug(f"Storage config retrieved: {json.dumps(storage_config, indent=2)}")
        storage_manager = StorageManager(storage_config)
        compliance_report_storage = StorageProviderFactory.create_provider(storage_config)
        logger.debug(f"Successfully initialized compliance_report_agent storage provider with base_path: {compliance_report_storage.base_path}")
        
        logger.info("Configuring compliance report agent...")
        try:
            from agents.compliance_report_agent import _storage_provider
            import agents.compliance_report_agent as compliance_report_agent
            compliance_report_agent._storage_provider = compliance_report_storage
            logger.debug("Successfully configured compliance report agent storage provider")
        except ImportError as ie:
            logger.error(f"Failed to import compliance_report_agent module: {str(ie)}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Failed to configure compliance report agent: {str(e)}", exc_info=True)
            return False
        
        # Initialize Marshaller and FinancialServicesFacade
        logger.info("Initializing Marshaller and FinancialServicesFacade...")
        try:
            marshaller = Marshaller(headless=True)
            # Set the facade variable
            facade = FinancialServicesFacade(headless=True, storage_manager=storage_manager)
            # Verify the facade was set
            if facade is None:
                logger.error("Failed to set global facade variable")
                return False
            logger.debug(f"Successfully initialized Marshaller and FinancialServicesFacade: {facade}")
        except Exception as e:
            logger.error(f"Failed to initialize Marshaller or FinancialServicesFacade: {str(e)}", exc_info=True)
            return False
        
        # Initialize cache and compliance services
        logger.info("Initializing cache and compliance services...")
        try:
            cache_manager = CacheManager()
            file_handler = FileHandler(cache_manager.cache_folder)
            compliance_handler = ComplianceHandler(file_handler.base_path)
            summary_generator = SummaryGenerator(file_handler=file_handler, compliance_handler=compliance_handler)
            logger.debug("Successfully initialized cache and compliance services")
        except Exception as e:
            logger.error(f"Failed to initialize cache or compliance services: {str(e)}", exc_info=True)
            return False
        
        logger.info(f"API services successfully initialized, facade: {facade}")
        return True
        
    except Exception as e:
        logger.error(f"Critical error during initialization: {str(e)}", exc_info=True)
        return False

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
    initialization_success = initialize_services()
    if not initialization_success:
        error_message = "Failed to initialize services. Check logs for details."
        logger.error(f"{error_message} for reference_id={request_dict['reference_id']}")
        error_report = {
            "status": "error",
            "reference_id": request_dict["reference_id"],
            "message": error_message
        }
        
        # Try to send webhook notification if URL is provided
        webhook_url = request_dict.get("webhook_url")
        if webhook_url:
            try:
                logger.info(f"Queuing webhook notification for error report, reference_id={request_dict['reference_id']}")
                send_webhook_notification.delay(webhook_url, error_report, request_dict["reference_id"])
            except Exception as we:
                logger.error(f"Failed to queue webhook notification: {str(we)}")
        
        return error_report
    
    # Validate that facade is properly initialized
    logger.info(f"Checking facade initialization: {facade}")
    if facade is None:
        error_message = "Service facade is not initialized. Check logs for initialization errors."
        logger.error(f"{error_message} for reference_id={request_dict['reference_id']}")
        error_report = {
            "status": "error",
            "reference_id": request_dict["reference_id"],
            "message": error_message
        }
        
        # Try to send webhook notification if URL is provided
        webhook_url = request_dict.get("webhook_url")
        if webhook_url:
            try:
                logger.info(f"Queuing webhook notification for error report, reference_id={request_dict['reference_id']}")
                send_webhook_notification.delay(webhook_url, error_report, request_dict["reference_id"])
            except Exception as we:
                logger.error(f"Failed to queue webhook notification: {str(we)}")
        
        return error_report
    
    # Create a local facade instance if needed
    local_facade = facade
    if local_facade is None:
        logger.warning(f"Global facade is None, creating a local instance for reference_id={request_dict['reference_id']}")
        try:
            local_facade = FinancialServicesFacade(headless=True, storage_manager=storage_manager)
            if local_facade is None:
                error_message = "Failed to create local facade instance"
                logger.error(f"{error_message} for reference_id={request_dict['reference_id']}")
                error_report = {
                    "status": "error",
                    "reference_id": request_dict["reference_id"],
                    "message": error_message
                }
                return error_report
        except Exception as e:
            error_message = f"Failed to create local facade instance: {str(e)}"
            logger.error(f"{error_message} for reference_id={request_dict['reference_id']}")
            error_report = {
                "status": "error",
                "reference_id": request_dict["reference_id"],
                "message": error_message
            }
            return error_report
    
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
            facade=local_facade,  # Use local_facade instead of global facade
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

    # Ensure services are initialized
    initialization_success = initialize_services()
    if not initialization_success:
        error_message = "Failed to initialize services. Check logs for details."
        logger.error(f"{error_message} for reference_id={request.reference_id}")
        raise HTTPException(status_code=500, detail=error_message)
    
    # Validate that facade is properly initialized
    if facade is None:
        error_message = "Service facade is not initialized. Check logs for initialization errors."
        logger.error(f"{error_message} for reference_id={request.reference_id}")
        raise HTTPException(status_code=500, detail=error_message)

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
            facade=local_facade,  # Use local_facade instead of global facade
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

# Health check endpoint
@app.get("/health")
async def health_check():
    """
    Health check endpoint that verifies the system's components are working correctly.
    Returns status of Redis, Celery, and other critical services.
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {}
    }
    
    # Check Redis connection
    try:
        redis_ping = redis_client.ping()
        redis_info = redis_client.info()
        webhook_keys = len(redis_client.keys("webhook_status:*"))
        dead_letter_keys = len(redis_client.keys("dead_letter:webhook:*"))
        
        health_status["components"]["redis"] = {
            "status": "up" if redis_ping else "down",
            "used_memory": redis_info.get("used_memory_human", "unknown"),
            "webhook_keys": webhook_keys,
            "dead_letter_keys": dead_letter_keys
        }
    except Exception as e:
        health_status["components"]["redis"] = {
            "status": "down",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check Celery connection
    try:
        i = celery_app.control.inspect()
        active_tasks = i.active()
        scheduled_tasks = i.scheduled()
        
        health_status["components"]["celery"] = {
            "status": "up",
            "active_tasks": len(active_tasks) if active_tasks else 0,
            "scheduled_tasks": len(scheduled_tasks) if scheduled_tasks else 0
        }
    except Exception as e:
        health_status["components"]["celery"] = {
            "status": "down",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check facade initialization
    health_status["components"]["facade"] = {
        "status": "up" if facade is not None else "down"
    }
    if facade is None:
        health_status["status"] = "degraded"
    
    # Check storage manager
    health_status["components"]["storage"] = {
        "status": "up" if storage_manager is not None else "down"
    }
    if storage_manager is None:
        health_status["status"] = "degraded"
    
    # Check circuit breakers
    circuit_breakers = {}
    for metric in CIRCUIT_BREAKER_STATUS._metrics:
        for labels, value in CIRCUIT_BREAKER_STATUS._metrics[metric].items():
            service = labels[0]
            state_value = value.get()
            state = "closed"
            if state_value == 1:
                state = "open"
            elif state_value == 0.5:
                state = "half-open"
            
            circuit_breakers[service] = {
                "state": state
            }
    
    health_status["components"]["circuit_breakers"] = circuit_breakers
    
    # If any circuit breaker is open, mark as degraded
    if any(cb["state"] == "open" for cb in circuit_breakers.values()):
        health_status["status"] = "degraded"
    
    # Return appropriate HTTP status code based on health
    return health_status

# Webhook cleanup endpoint
@app.post("/webhook-cleanup")
async def cleanup_webhooks(
    status: Optional[str] = None,
    older_than_days: Optional[int] = None,
    reference_id: Optional[str] = None
):
    """
    Cleanup webhook statuses based on criteria.
    
    Args:
        status (str, optional): Only clean webhooks with this status
        older_than_days (int, optional): Only clean webhooks older than this many days
        reference_id (str, optional): Only clean webhooks for this reference ID
        
    Returns:
        Dict[str, Any]: Count of deleted webhook statuses
    """
    pattern = "webhook_status:*"
    all_keys = redis_client.keys(pattern)
    
    to_delete = []
    for key in all_keys:
        webhook_id = key.split(":", 1)[1]  # Extract ID from key
        status_data = get_webhook_status(webhook_id)
        
        if not status_data:
            continue
            
        # Apply status filter
        if status and status_data.get("status") != status:
            continue
            
        # Apply reference_id filter
        if reference_id and status_data.get("reference_id") != reference_id:
            continue
            
        # Apply age filter
        if older_than_days:
            created_at = datetime.fromisoformat(status_data.get("created_at", datetime.utcnow().isoformat()))
            age_days = (datetime.utcnow() - created_at).days
            if age_days < older_than_days:
                continue
                
        to_delete.append(webhook_id)
    
    # Delete filtered statuses
    for webhook_id in to_delete:
        delete_webhook_status(webhook_id)
    
    return {
        "message": f"Webhook cleanup completed. Deleted {len(to_delete)} webhook statuses.",
        "deleted_count": len(to_delete),
        "filters": {
            "status": status,
            "older_than_days": older_than_days,
            "reference_id": reference_id
        }
    }

# Webhook status tracking endpoints
@app.get("/webhook-status/{webhook_id}", response_model=Dict[str, Any])
async def get_webhook_status_endpoint(webhook_id: str):
    """
    Get the status of a specific webhook delivery.
    
    Args:
        webhook_id (str): The webhook ID to check.
        
    Returns:
        Dict[str, Any]: The webhook status information.
    """
    status_data = get_webhook_status(webhook_id)
    if not status_data:
        raise HTTPException(status_code=404, detail=f"Webhook status not found for ID: {webhook_id}")
    
    return status_data

@app.get("/webhook-statuses", response_model=Dict[str, Any])
async def list_webhook_statuses_endpoint(
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
    # Use the Redis-based function to get webhook statuses
    return get_all_webhook_statuses(reference_id, status, page, page_size)

@app.delete("/webhook-status/{webhook_id}", response_model=Dict[str, Any])
async def delete_webhook_status_endpoint(webhook_id: str):
    """
    Delete a specific webhook status.
    
    Args:
        webhook_id (str): The webhook ID to delete.
        
    Returns:
        Dict[str, Any]: Confirmation of deletion.
    """
    deleted_status = delete_webhook_status(webhook_id)
    if not deleted_status:
        raise HTTPException(status_code=404, detail=f"Webhook status not found for ID: {webhook_id}")
    
    return {
        "message": f"Webhook status deleted for ID: {webhook_id}",
        "deleted_status": deleted_status
    }

@app.delete("/webhook-statuses", response_model=Dict[str, Any])
async def delete_all_webhook_statuses_endpoint(
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
    # Use the Redis-based function to delete webhook statuses
    deleted_count = delete_all_webhook_statuses(reference_id, status)
    
    return {
        "message": f"Webhook statuses deleted ({deleted_count} total)",
        "deleted_count": deleted_count,
        "filters": {
            "reference_id": reference_id,
            "status": status
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")