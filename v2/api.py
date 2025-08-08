import json
from typing import Dict, Any, Optional, Union, List, Set
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel, validator, Field
from celery import Celery
from celery.exceptions import MaxRetriesExceededError
from celery.result import AsyncResult
import requests
import time
import random
import os
import asyncio  # Still needed for process_claim_helper
from datetime import datetime, timedelta
from enum import Enum
import redis
import uuid
import socket
import traceback
import hashlib
import hmac
import re
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

# Initialize Redis clients for different purposes
# Main Redis client (for backward compatibility)
redis_client = redis.Redis(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=int(os.environ.get("REDIS_PORT", 6379)),
    db=int(os.environ.get("REDIS_DB", 0)),
    decode_responses=True  # Automatically decode responses to strings
)

# Celery Redis client (DB 1)
celery_redis_client = redis.Redis(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=int(os.environ.get("REDIS_PORT", 6379)),
    db=int(os.environ.get("CELERY_REDIS_DB", 1)),
    decode_responses=True
)

# Status tracking Redis client (DB 2)
status_redis_client = redis.Redis(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=int(os.environ.get("REDIS_PORT", 6379)),
    db=int(os.environ.get("STATUS_REDIS_DB", 2)),
    decode_responses=True
)

# Initialize Prometheus metrics
# Generate a unique worker ID for metrics
hostname = os.environ.get("HOSTNAME", socket.gethostname())
worker_id = f"{hostname}-{uuid.uuid4().hex[:6]}"

WEBHOOK_COUNTER = Counter(
    'webhook_delivery_total',
    'Total number of webhook deliveries',
    ['status', 'worker_id']
)
WEBHOOK_DELIVERY_TIME = Histogram(
    'webhook_delivery_seconds',
    'Time spent processing webhook deliveries',
    ['worker_id']
)
TASK_COUNTER = Counter(
    'task_processing_total',
    'Total number of tasks processed',
    ['status', 'mode', 'worker_id']
)
TASK_PROCESSING_TIME = Histogram(
    'task_processing_seconds',
    'Time spent processing tasks',
    ['mode', 'worker_id']
)
CIRCUIT_BREAKER_STATUS = Gauge(
    'circuit_breaker_status',
    'Circuit breaker status (0=closed, 1=open, 0.5=half-open)',
    ['service']
)
REDIS_WEBHOOK_KEYS = Gauge(
    'redis_webhook_keys',
    'Number of webhook keys in Redis',
    ['status']
)

# Start Prometheus metrics server on a separate thread if enabled
def start_metrics_server():
    if os.environ.get("ENABLE_PROMETHEUS", "false").lower() == "true":
        prometheus_port = int(os.environ.get("PROMETHEUS_PORT", 8001))
        start_http_server(prometheus_port)
        logger.info(f"Started Prometheus metrics server on port {prometheus_port}")

# Start metrics server in a separate thread if enabled
if os.environ.get("ENABLE_PROMETHEUS", "false").lower() == "true":
    threading.Thread(target=start_metrics_server, daemon=True).start()

# Initialize Celery with Redis DB 1
redis_host = os.environ.get("REDIS_HOST", "localhost")
redis_port = os.environ.get("REDIS_PORT", 6379)
redis_db = os.environ.get("CELERY_REDIS_DB", 1)

celery_app = Celery(
    "compliance_tasks",
    broker=f"redis://{redis_host}:{redis_port}/{redis_db}",  # Redis DB 1 as message broker
    backend=f"redis://{redis_host}:{redis_port}/{redis_db}",  # Redis DB 1 as result backend
)

# Define task routes
task_routes = {
    'process_compliance_claim': {'queue': 'compliance_queue'},
    'send_webhook_notification': {'queue': 'webhook_queue'},
    'dead_letter_task': {'queue': 'dead_letter_queue'},
}

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_track_started=True,
    task_time_limit=3600,  # 1-hour timeout for tasks
    worker_prefetch_multiplier=1,  # Process one task at a time (FIFO)
    task_acks_late=True,   # Acknowledge tasks after completion
    reject_on_worker_lost=True,  # Reject tasks if worker is lost
    task_default_queue="compliance_queue",
    task_routes=task_routes,
    
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

def get_reference_index_key(reference_id):
    """Generate Redis key for reference ID index"""
    return f"webhook_status:index:{reference_id}"

def get_status_index_key(status):
    """Generate Redis key for status index"""
    return f"webhook_status:index:status:{status}"

def get_dead_letter_key(webhook_id):
    """Generate Redis key for dead letter queue"""
    return f"dead_letter:webhook:{webhook_id}"

def get_dead_letter_index_key():
    """Generate Redis key for dead letter index"""
    return f"dead_letter:webhook:index"

def save_webhook_status(webhook_id, status_data):
    """Save webhook status to Redis with appropriate TTL and update indexes"""
    key = get_webhook_key(webhook_id)
    status = status_data.get("status")
    reference_id = status_data.get("reference_id")
    ttl = WEBHOOK_TTL.get(status, 7 * 24 * 60 * 60)  # Default to 7 days
    
    # Update the status data with current timestamp
    status_data["updated_at"] = datetime.utcnow().isoformat()
    
    # Store as JSON string
    status_redis_client.set(key, json.dumps(status_data))
    status_redis_client.expire(key, ttl)
    
    # Update reference ID index
    if reference_id:
        ref_index_key = get_reference_index_key(reference_id)
        status_redis_client.sadd(ref_index_key, webhook_id)
        status_redis_client.expire(ref_index_key, ttl)
    
    # Update status index
    if status:
        status_index_key = get_status_index_key(status)
        status_redis_client.sadd(status_index_key, webhook_id)
        status_redis_client.expire(status_index_key, ttl)
    
    # Update metrics
    try:
        # Count webhooks by status using index sets
        for webhook_status in WebhookStatus:
            status_index = get_status_index_key(webhook_status.value)
            count = status_redis_client.scard(status_index)
            REDIS_WEBHOOK_KEYS.labels(status=webhook_status.value).set(count)
    except Exception as e:
        logger.error(f"Error updating webhook metrics: {str(e)}")

def get_webhook_status(webhook_id):
    """Get webhook status from Redis"""
    key = get_webhook_key(webhook_id)
    data = status_redis_client.get(key)
    if data:
        return json.loads(data)
    return None

def delete_webhook_status(webhook_id):
    """Delete webhook status from Redis and remove from indexes"""
    key = get_webhook_key(webhook_id)
    status_data = get_webhook_status(webhook_id)
    
    if status_data:
        # Remove from reference ID index
        reference_id = status_data.get("reference_id")
        if reference_id:
            ref_index_key = get_reference_index_key(reference_id)
            status_redis_client.srem(ref_index_key, webhook_id)
        
        # Remove from status index
        status = status_data.get("status")
        if status:
            status_index_key = get_status_index_key(status)
            status_redis_client.srem(status_index_key, webhook_id)
        
        # Delete the status key
        status_redis_client.delete(key)
        
        # Update metrics
        try:
            # Count webhooks by status using index sets
            for webhook_status in WebhookStatus:
                status_index = get_status_index_key(webhook_status.value)
                count = status_redis_client.scard(status_index)
                REDIS_WEBHOOK_KEYS.labels(status=webhook_status.value).set(count)
        except Exception as e:
            logger.error(f"Error updating webhook metrics: {str(e)}")
            
        return status_data
    
    return None

def _iter_webhook_ids():
    """Helper function to iterate through all webhook IDs using SCAN"""
    cursor = 0
    while True:
        cursor, keys = status_redis_client.scan(cursor, match="webhook_status:*", count=1000)
        for key in keys:
            if key.startswith("webhook_status:index:"):
                continue
            # everything after first colon
            yield key.split(":", 1)[1]
        if cursor == 0:
            break

def get_all_webhook_statuses(reference_id=None, status=None, page=1, page_size=10):
    """Get all webhook statuses with optional filtering and pagination using indexes"""
    all_webhook_ids = set()
    
    # Use indexes for efficient filtering
    if reference_id and status:
        # Get intersection of reference_id and status indexes
        ref_index_key = get_reference_index_key(reference_id)
        status_index_key = get_status_index_key(status)
        all_webhook_ids = status_redis_client.sinter(ref_index_key, status_index_key)
    elif reference_id:
        # Get webhook IDs for this reference_id
        ref_index_key = get_reference_index_key(reference_id)
        all_webhook_ids = status_redis_client.smembers(ref_index_key)
    elif status:
        # Get webhook IDs for this status
        status_index_key = get_status_index_key(status)
        all_webhook_ids = status_redis_client.smembers(status_index_key)
    else:
        # No filters, use SCAN to get all webhook status keys
        all_webhook_ids = set(_iter_webhook_ids())
    
    # Get data for all webhook IDs
    all_statuses = {}
    for webhook_id in all_webhook_ids:
        status_data = get_webhook_status(webhook_id)
        if status_data:
            all_statuses[webhook_id] = status_data
    
    # Sort by updated_at in descending order
    sorted_statuses = sorted(
        all_statuses.items(),
        key=lambda x: x[1].get("updated_at", ""),
        reverse=True
    )
    
    # Paginate results
    total_items = len(sorted_statuses)
    total_pages = (total_items + page_size - 1) // page_size if total_items > 0 else 1
    
    # Ensure page is within valid range
    if page < 1:
        page = 1
    elif page > total_pages:
        page = total_pages
    
    # Get items for current page
    start_idx = (page - 1) * page_size
    end_idx = min(start_idx + page_size, total_items)
    paginated_statuses = dict(sorted_statuses[start_idx:end_idx])
    
    return {
        "items": paginated_statuses,
        "total_items": total_items,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages
    }

def delete_all_webhook_statuses(reference_id=None, status=None):
    """Delete all webhook statuses with optional filtering using indexes"""
    webhook_ids_to_delete = set()
    
    # Use indexes for efficient filtering
    if reference_id and status:
        # Get intersection of reference_id and status indexes
        ref_index_key = get_reference_index_key(reference_id)
        status_index_key = get_status_index_key(status)
        webhook_ids_to_delete = status_redis_client.sinter(ref_index_key, status_index_key)
    elif reference_id:
        # Get webhook IDs for this reference_id
        ref_index_key = get_reference_index_key(reference_id)
        webhook_ids_to_delete = status_redis_client.smembers(ref_index_key)
    elif status:
        # Get webhook IDs for this status
        status_index_key = get_status_index_key(status)
        webhook_ids_to_delete = status_redis_client.smembers(status_index_key)
    else:
        # No filters, use SCAN to get all webhook status keys
        webhook_ids_to_delete = set(_iter_webhook_ids())
    
    # Delete each webhook status
    deleted_count = 0
    for webhook_id in webhook_ids_to_delete:
        if delete_webhook_status(webhook_id):
            deleted_count += 1
    
    return deleted_count

def add_to_dead_letter_queue(webhook_id, data, ttl=30*24*60*60):
    """Add a failed webhook to the dead letter queue with 30-day TTL"""
    # Store the dead letter data
    key = get_dead_letter_key(webhook_id)
    status_redis_client.set(key, json.dumps(data))
    status_redis_client.expire(key, ttl)
    
    # Add to the index
    index_key = get_dead_letter_index_key()
    status_redis_client.sadd(index_key, webhook_id)
    status_redis_client.expire(index_key, ttl)
    
    logger.info(f"Added webhook {webhook_id} to dead letter queue with TTL of {ttl} seconds")

def get_dead_letter_queue_items(page=1, page_size=10):
    """Get items from the dead letter queue with pagination"""
    # Get all webhook IDs from the index
    index_key = get_dead_letter_index_key()
    all_webhook_ids = status_redis_client.smembers(index_key)
    
    # Get data for all webhook IDs
    all_items = {}
    for webhook_id in all_webhook_ids:
        key = get_dead_letter_key(webhook_id)
        data = status_redis_client.get(key)
        if data:
            all_items[webhook_id] = json.loads(data)
    
    # Sort by last_attempt in descending order
    sorted_items = sorted(
        all_items.items(),
        key=lambda x: x[1].get("last_attempt", ""),
        reverse=True
    )
    
    # Paginate results
    total_items = len(sorted_items)
    total_pages = (total_items + page_size - 1) // page_size if total_items > 0 else 1
    
    # Ensure page is within valid range
    if page < 1:
        page = 1
    elif page > total_pages:
        page = total_pages
    
    # Get items for current page
    start_idx = (page - 1) * page_size
    end_idx = min(start_idx + page_size, total_items)
    paginated_items = dict(sorted_items[start_idx:end_idx])
    
    return {
        "items": paginated_items,
        "total_items": total_items,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages
    }

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

# On-failure handler for webhook delivery task
def webhook_delivery_failure_handler(exc, task_id, args, kwargs, einfo):
    """Handle failures in the webhook delivery task"""
    try:
        webhook_url = args[0]
        payload = args[1]
        reference_id = args[2]
        webhook_id = f"{reference_id}_{task_id}"
        
        # Get current status
        status_data = get_webhook_status(webhook_id) or {}
        
        # Update status to failed
        status_data.update({
            "status": WebhookStatus.FAILED.value,
            "error": str(exc),
            "error_type": "max_retries_exceeded" if isinstance(exc, MaxRetriesExceededError) else "unexpected_error",
            "updated_at": datetime.utcnow().isoformat()
        })
        
        # Save updated status
        save_webhook_status(webhook_id, status_data)
        
        # Add to dead letter queue
        correlation_id = status_data.get("correlation_id", str(uuid.uuid4()))
        add_to_dead_letter_queue(webhook_id, {
            "webhook_id": webhook_id,
            "reference_id": reference_id,
            "webhook_url": webhook_url,
            "payload": payload,
            "error": str(exc),
            "error_type": status_data.get("error_type", "unexpected_error"),
            "attempts": status_data.get("attempts", 1),
            "last_attempt": datetime.utcnow().isoformat(),
            "correlation_id": correlation_id
        })
        
        # Increment failure counter
        WEBHOOK_COUNTER.labels(
            status="failed",
            worker_id=worker_id
        ).inc()
        
        logger.error(f"[{correlation_id}] Webhook delivery failed permanently for {reference_id}: {str(exc)}")
    except Exception as e:
        logger.error(f"Error in webhook failure handler: {str(e)}", exc_info=True)

@celery_app.task(
    name="send_webhook_notification",
    bind=True,
    max_retries=3,  # Max 3 attempts total (initial + 2 retries)
    default_retry_delay=30,
    retry_backoff=True,
    retry_backoff_max=300,  # 5 minutes max delay
    retry_jitter=True,
    on_failure=webhook_delivery_failure_handler
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
    # Start timing the webhook delivery
    start_time = time.time()
    
    try:
        # Generate correlation ID for tracking this webhook delivery
        correlation_id = str(uuid.uuid4())
        
        # Generate webhook_id
        webhook_id = f"{reference_id}_{self.request.id}"
        
        # Increment started counter
        WEBHOOK_COUNTER.labels(
            status="started",
            worker_id=worker_id
        ).inc()
        
        # Update webhook status to in progress
        status_data = {
            "webhook_id": webhook_id,
            "reference_id": reference_id,
            "task_id": self.request.id,
            "webhook_url": webhook_url,
            "status": WebhookStatus.IN_PROGRESS.value,
            "attempts": self.request.retries + 1,
            "max_attempts": 3,  # Max 3 attempts total
            "correlation_id": correlation_id
        }
        
        # Check if this is a new webhook or an update
        existing_status = get_webhook_status(webhook_id)
        if existing_status:
            status_data["created_at"] = existing_status.get("created_at")
        else:
            status_data["created_at"] = datetime.utcnow().isoformat()
        
        # Save to Redis DB 2
        save_webhook_status(webhook_id, status_data)
        logger.info(f"[{correlation_id}] Sending webhook notification to {webhook_url} for reference_id={reference_id} (attempt {self.request.retries + 1})")
        
        # URL validation
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
            
            # Add to dead letter queue
            add_to_dead_letter_queue(webhook_id, {
                "webhook_id": webhook_id,
                "reference_id": reference_id,
                "webhook_url": webhook_url,
                "payload": payload,
                "error": error_msg,
                "error_type": "validation_error",
                "attempts": self.request.retries + 1,
                "last_attempt": datetime.utcnow().isoformat(),
                "correlation_id": correlation_id
            })
            
            # Increment validation error counter
            WEBHOOK_COUNTER.labels(
                status="validation_error",
                worker_id=worker_id
            ).inc()
            
            # Don't retry for validation errors
            return {
                "success": False,
                "reference_id": reference_id,
                "error": error_msg,
                "webhook_id": webhook_id,
                "correlation_id": correlation_id
            }
        
        # Optional URL allowlist validation
        webhook_allowlist = os.environ.get("WEBHOOK_ALLOWLIST")
        if webhook_allowlist:
            try:
                if not re.match(webhook_allowlist, webhook_url):
                    error_msg = f"Webhook URL not in allowlist: {webhook_url}"
                    logger.error(f"[{correlation_id}] {error_msg} for reference_id={reference_id}")
                    
                    # Update webhook status to failed
                    status_data.update({
                        "status": WebhookStatus.FAILED.value,
                        "error": error_msg,
                        "error_type": "validation_error"
                    })
                    save_webhook_status(webhook_id, status_data)
                    
                    # Add to dead letter queue
                    add_to_dead_letter_queue(webhook_id, {
                        "webhook_id": webhook_id,
                        "reference_id": reference_id,
                        "webhook_url": webhook_url,
                        "payload": payload,
                        "error": error_msg,
                        "error_type": "validation_error",
                        "attempts": self.request.retries + 1,
                        "last_attempt": datetime.utcnow().isoformat(),
                        "correlation_id": correlation_id
                    })
                    
                    # Increment validation error counter
                    WEBHOOK_COUNTER.labels(
                        status="validation_error",
                        worker_id=worker_id
                    ).inc()
                    
                    # Don't retry for validation errors
                    return {
                        "success": False,
                        "reference_id": reference_id,
                        "error": error_msg,
                        "webhook_id": webhook_id,
                        "correlation_id": correlation_id
                    }
            except re.error as e:
                logger.error(f"Invalid WEBHOOK_ALLOWLIST regex: {str(e)}")
        
        # Prepare headers
        headers = {
            "Content-Type": "application/json",
            "X-Reference-ID": reference_id,
            "X-Correlation-ID": correlation_id,
            "X-Idempotency-Key": webhook_id
        }
        
        # Add HMAC signature if secret is set
        hmac_secret = os.environ.get("WEBHOOK_HMAC_SECRET")
        if hmac_secret:
            payload_bytes = json.dumps(payload).encode('utf-8')
            signature = hmac.new(
                hmac_secret.encode('utf-8'),
                payload_bytes,
                hashlib.sha256
            ).hexdigest()
            headers["X-Signature"] = f"sha256={signature}"
        
        # Use synchronous requests instead of asyncio (better for Celery workers)
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=30,  # 30 second timeout
            headers=headers
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
            
            # Increment delivered counter
            WEBHOOK_COUNTER.labels(
                status="delivered",
                worker_id=worker_id
            ).inc()
            
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
                # 4xx errors are client errors - retry once, then mark as permanent
                if self.request.retries >= 1:  # Only retry once for client errors
                    status = WebhookStatus.FAILED.value
                    error_type = "permanent_client_error"
                    
                    # Add to dead letter queue
                    add_to_dead_letter_queue(webhook_id, {
                        "webhook_id": webhook_id,
                        "reference_id": reference_id,
                        "webhook_url": webhook_url,
                        "payload": payload,
                        "error": error_msg,
                        "error_type": error_type,
                        "attempts": self.request.retries + 1,
                        "last_attempt": datetime.utcnow().isoformat(),
                        "correlation_id": correlation_id
                    })
                    
                    # Increment failed counter
                    WEBHOOK_COUNTER.labels(
                        status="failed",
                        worker_id=worker_id
                    ).inc()
                else:
                    status = WebhookStatus.RETRYING.value
                    error_type = "client_error"
                    
                    # Increment retrying counter
                    WEBHOOK_COUNTER.labels(
                        status="retrying",
                        worker_id=worker_id
                    ).inc()
            else:
                # 5xx errors are server errors - retry with backoff up to max attempts
                if self.request.retries >= 2:  # We have 3 max attempts (0-2)
                    status = WebhookStatus.FAILED.value
                    error_type = "max_retries_exceeded"
                    
                    # Add to dead letter queue
                    add_to_dead_letter_queue(webhook_id, {
                        "webhook_id": webhook_id,
                        "reference_id": reference_id,
                        "webhook_url": webhook_url,
                        "payload": payload,
                        "error": error_msg,
                        "error_type": error_type,
                        "attempts": self.request.retries + 1,
                        "last_attempt": datetime.utcnow().isoformat(),
                        "correlation_id": correlation_id
                    })
                    
                    # Increment failed counter
                    WEBHOOK_COUNTER.labels(
                        status="failed",
                        worker_id=worker_id
                    ).inc()
                else:
                    status = WebhookStatus.RETRYING.value
                    error_type = "server_error"
                    
                    # Increment retrying counter
                    WEBHOOK_COUNTER.labels(
                        status="retrying",
                        worker_id=worker_id
                    ).inc()
            
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
            
            # If we should retry, raise retry exception
            if status == WebhookStatus.RETRYING.value:
                raise self.retry(
                    exc=Exception(error_msg),
                    countdown=retry_delay_with_jitter
                )
            else:
                # Otherwise return error
                return {
                    "success": False,
                    "reference_id": reference_id,
                    "error": error_msg,
                    "webhook_id": webhook_id,
                    "correlation_id": correlation_id,
                    "status": status
                }
    
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
            error_type = "network_error"
        
        # Update webhook status
        if self.request.retries >= 2:  # We have 3 max attempts (0-2)
            status = WebhookStatus.FAILED.value
            
            # Add to dead letter queue
            add_to_dead_letter_queue(webhook_id, {
                "webhook_id": webhook_id,
                "reference_id": reference_id,
                "webhook_url": webhook_url,
                "payload": payload,
                "error": error_msg,
                "error_type": error_type,
                "attempts": self.request.retries + 1,
                "last_attempt": datetime.utcnow().isoformat(),
                "correlation_id": correlation_id
            })
            
            # Increment failed counter
            WEBHOOK_COUNTER.labels(
                status="failed",
                worker_id=worker_id
            ).inc()
        else:
            status = WebhookStatus.RETRYING.value
            
            # Increment retrying counter
            WEBHOOK_COUNTER.labels(
                status="retrying",
                worker_id=worker_id
            ).inc()
            
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
        
        # If we should retry, raise retry exception
        if status == WebhookStatus.RETRYING.value:
            raise self.retry(
                exc=e,
                countdown=retry_delay_with_jitter
            )
        else:
            # Otherwise return error
            return {
                "success": False,
                "reference_id": reference_id,
                "error": error_msg,
                "webhook_id": webhook_id,
                "correlation_id": correlation_id,
                "status": status
            }
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
        
        # Add to dead letter queue
        add_to_dead_letter_queue(webhook_id, {
            "webhook_id": webhook_id,
            "reference_id": reference_id,
            "webhook_url": webhook_url,
            "payload": payload,
            "error": error_msg,
            "error_type": "unexpected_error",
            "attempts": self.request.retries + 1,
            "last_attempt": datetime.utcnow().isoformat(),
            "correlation_id": correlation_id
        })
        
        # Increment unexpected error counter
        WEBHOOK_COUNTER.labels(
            status="unexpected_error",
            worker_id=worker_id
        ).inc()
        
        # Re-raise the exception
        raise
    finally:
        # Record delivery time regardless of outcome
        WEBHOOK_DELIVERY_TIME.labels(
            worker_id=worker_id
        ).observe(time.time() - start_time)

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
    # Start timing the task
    start_time = time.time()
    
    # Increment started counter
    TASK_COUNTER.labels(
        status="started",
        mode=mode,
        worker_id=worker_id
    ).inc()
    
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
        
        # Increment error counter
        TASK_COUNTER.labels(
            status="error",
            mode=mode,
            worker_id=worker_id
        ).inc()
        
        # Try to send webhook notification if URL is provided
        webhook_url = request_dict.get("webhook_url")
        if webhook_url:
            try:
                logger.info(f"Queuing webhook notification for error report, reference_id={request_dict['reference_id']}")
                send_webhook_notification.delay(webhook_url, error_report, request_dict["reference_id"])
            except Exception as we:
                logger.error(f"Failed to queue webhook notification: {str(we)}")
        
        return error_report
    
    # Facade check is already done in initialization_success check above, no need to check again
    
    # Define webhook_url before the try block to avoid UnboundLocalError
    webhook_url = request_dict.get("webhook_url")
    
    try:
        # Convert dict to ClaimRequest for validation
        request = ClaimRequest(**request_dict)
        mode_settings = PROCESSING_MODES[mode]
        claim = request.dict(exclude_unset=True)
        employee_number = claim.pop("employee_number")
        webhook_url = claim.pop("webhook_url", webhook_url)

        if not claim.get("individual_name") and claim.get("first_name") and claim.get("last_name"):
            claim["individual_name"] = f"{claim['first_name']} {claim['last_name']}".strip()
            logger.debug(f"Set individual_name to '{claim['individual_name']}'")

        # Process the claim
        report = process_claim(
            claim=claim,
            facade=facade,  # Use global facade
            employee_number=employee_number,
            skip_disciplinary=mode_settings["skip_disciplinary"],
            skip_arbitration=mode_settings["skip_arbitration"],
            skip_regulatory=mode_settings["skip_regulatory"]
        )
        
        if report is None:
            logger.error(f"Failed to process claim for reference_id={request.reference_id}: process_claim returned None")
            
            # Increment error counter
            TASK_COUNTER.labels(
                status="error",
                mode=mode,
                worker_id=worker_id
            ).inc()
            
            raise ValueError("Claim processing failed unexpectedly")

        logger.info(f"Successfully processed claim for reference_id={request.reference_id}")
        
        # Increment success counter
        TASK_COUNTER.labels(
            status="success",
            mode=mode,
            worker_id=worker_id
        ).inc()
        
        # Record processing time
        TASK_PROCESSING_TIME.labels(
            mode=mode,
            worker_id=worker_id
        ).observe(time.time() - start_time)

        # Send to webhook if provided
        if webhook_url:
            logger.info(f"Queuing webhook notification for reference_id={request.reference_id}")
            send_webhook_notification.delay(webhook_url, report, request.reference_id)
        
        return report
    
    except Exception as e:
        logger.error(f"Error processing claim for reference_id={request_dict['reference_id']}: {str(e)}", exc_info=True)
        
        # Increment error counter
        TASK_COUNTER.labels(
            status="error",
            mode=mode,
            worker_id=worker_id
        ).inc()
        
        error_report = {
            "status": "error",
            "reference_id": request_dict["reference_id"],
            "message": f"Claim processing failed: {str(e)}"
        }
        if webhook_url:
            logger.info(f"Queuing webhook notification for error report, reference_id={request_dict['reference_id']}")
            send_webhook_notification.delay(webhook_url, error_report, request_dict["reference_id"])
            
        # Increment retry counter
        TASK_COUNTER.labels(
            status="retrying",
            mode=mode,
            worker_id=worker_id
        ).inc()
        
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
            facade=facade,  # Use global facade
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
    Uses SCAN instead of KEYS for Redis operations.
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {}
    }
    
    # Check Redis connections (main, Celery, and status)
    try:
        # Check main Redis
        redis_ping = redis_client.ping()
        redis_info = redis_client.info()
        
        # Check Celery Redis
        celery_redis_ping = celery_redis_client.ping()
        celery_redis_info = celery_redis_client.info()
        
        # Check Status Redis
        status_redis_ping = status_redis_client.ping()
        status_redis_info = status_redis_client.info()
        
        # Get webhook counts using indexes
        webhook_counts = {}
        for status_enum in WebhookStatus:
            status_value = status_enum.value
            status_index_key = get_status_index_key(status_value)
            count = status_redis_client.scard(status_index_key)
            webhook_counts[status_value] = count
        
        # Get dead letter queue count using index
        dlq_index_key = get_dead_letter_index_key()
        dlq_count = status_redis_client.scard(dlq_index_key)
        
        # Add Redis status to health check
        health_status["components"]["redis"] = {
            "status": "up" if redis_ping and celery_redis_ping and status_redis_ping else "down",
            "main_db": {
                "status": "up" if redis_ping else "down",
                "used_memory": redis_info.get("used_memory_human", "unknown"),
                "db": redis_info.get("db0", {})
            },
            "celery_db": {
                "status": "up" if celery_redis_ping else "down",
                "used_memory": celery_redis_info.get("used_memory_human", "unknown"),
                "db": celery_redis_info.get(f"db{os.environ.get('CELERY_REDIS_DB', 1)}", {})
            },
            "status_db": {
                "status": "up" if status_redis_ping else "down",
                "used_memory": status_redis_info.get("used_memory_human", "unknown"),
                "db": status_redis_info.get(f"db{os.environ.get('STATUS_REDIS_DB', 2)}", {})
            },
            "webhook_counts": webhook_counts,
            "dead_letter_count": dlq_count
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
    webhook_ids_to_delete = set()
    
    # Use indexes for efficient filtering if possible
    if reference_id and status:
        # Get intersection of reference_id and status indexes
        ref_index_key = get_reference_index_key(reference_id)
        status_index_key = get_status_index_key(status)
        webhook_ids_to_delete = status_redis_client.sinter(ref_index_key, status_index_key)
    elif reference_id:
        # Get webhook IDs for this reference_id
        ref_index_key = get_reference_index_key(reference_id)
        webhook_ids_to_delete = status_redis_client.smembers(ref_index_key)
    elif status:
        # Get webhook IDs for this status
        status_index_key = get_status_index_key(status)
        webhook_ids_to_delete = status_redis_client.smembers(status_index_key)
    else:
        # No filters, use SCAN to get all webhook status keys
        webhook_ids_to_delete = set(_iter_webhook_ids())
    
    # Apply age filter if needed
    if older_than_days:
        filtered_ids = set()
        for webhook_id in webhook_ids_to_delete:
            status_data = get_webhook_status(webhook_id)
            if not status_data:
                continue
                
            created_at = datetime.fromisoformat(status_data.get("created_at", datetime.utcnow().isoformat()))
            age_days = (datetime.utcnow() - created_at).days
            if age_days >= older_than_days:
                filtered_ids.add(webhook_id)
        webhook_ids_to_delete = filtered_ids
    
    # Delete filtered statuses
    deleted_count = 0
    for webhook_id in webhook_ids_to_delete:
        if delete_webhook_status(webhook_id):
            deleted_count += 1
    
    return {
        "message": f"Webhook cleanup completed. Deleted {deleted_count} webhook statuses.",
        "deleted_count": deleted_count,
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

# Pydantic model for test webhook request
class TestWebhookRequest(BaseModel):
    """Pydantic model for test webhook request"""
    webhook_url: str
    payload: Dict[str, Any] = Field(default_factory=lambda: {"test": "payload"})
    
    class Config:
        schema_extra = {
            "example": {
                "webhook_url": "https://webhook.site/your-uuid",
                "payload": {
                    "test": "payload",
                    "timestamp": "2025-08-08T15:45:00Z"
                }
            }
        }

# Test endpoint for webhook DLQ testing
@app.post("/test-webhook")
async def test_webhook(request_body: TestWebhookRequest):
    """
    Test endpoint for webhook delivery testing.
    
    Args:
        request_body (TestWebhookRequest): The webhook request containing URL and payload
        
    Returns:
        Dict[str, Any]: Reference ID and task ID for tracking
    """
    # Generate a unique reference ID for testing
    reference_id = f"TEST-{uuid.uuid4().hex[:8]}"
    
    logger.info(f"Test webhook request received for URL: {request_body.webhook_url}")
    logger.info(f"Generated reference_id: {reference_id}")
    
    # Queue the webhook notification task
    task = send_webhook_notification.delay(request_body.webhook_url, request_body.payload, reference_id)
    
    return {
        "status": "webhook_queued",
        "reference_id": reference_id,
        "task_id": task.id,
        "message": "Test webhook queued for delivery"
    }

# DLQ listing endpoint
@app.get("/webhook-dlq")
async def list_dlq_webhooks(page: int = 1, page_size: int = 10):
    """
    List all webhooks in the Dead Letter Queue (DLQ).
    
    Args:
        page (int): Page number for pagination
        page_size (int): Number of items per page
        
    Returns:
        Dict[str, Any]: Paginated list of webhooks in the DLQ
    """
    try:
        # Use the existing function to get DLQ items
        dlq_items = get_dead_letter_queue_items(page, page_size)
        
        return {
            "status": "success",
            "message": f"Retrieved {len(dlq_items['items'])} DLQ items",
            "data": dlq_items
        }
    except Exception as e:
        logger.error(f"Error listing DLQ webhooks: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list DLQ webhooks: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")