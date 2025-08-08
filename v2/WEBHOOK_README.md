# Webhook Delivery System

This document provides information about the webhook delivery system implementation, including how to use it and test it.

## Overview

The webhook delivery system provides a robust, observable, and recoverable pipeline for delivering webhook notifications. It includes:

- Reliable delivery with bounded retries, backoff + jitter, and correct handling of 4xx vs 5xx/network failures
- End-to-end traceability with unique webhook_id, correlation ID, and observable status
- Isolation between Celery traffic and webhook status storage using separate Redis DBs
- Low-cardinality metrics for safe scaling
- Operational clarity with a comprehensive health endpoint
- Dead-letter queue for permanently failed deliveries

## Configuration

The webhook system can be configured using the following environment variables:

```bash
# Redis configuration
REDIS_HOST=localhost
REDIS_PORT=6379
CELERY_REDIS_DB=1
STATUS_REDIS_DB=2

# Prometheus metrics
ENABLE_PROMETHEUS=true
PROMETHEUS_PORT=8001

# Webhook security (optional)
WEBHOOK_ALLOWLIST=^https://.*\.example\.com/.*$
WEBHOOK_HMAC_SECRET=your-secret-key
```

## Starting the Workers

To start the Celery workers with the correct queues and settings:

```bash
celery -A app worker -Q compliance_queue,webhook_queue,dead_letter_queue --concurrency=4 --prefetch-multiplier=1
```

## API Endpoints

### Webhook Status Endpoints

- `GET /webhook-status/{webhook_id}` - Get the status of a specific webhook
- `GET /webhook-statuses?reference_id=&status=&page=&page_size=` - List webhook statuses with filtering and pagination
- `DELETE /webhook-status/{webhook_id}` - Delete a specific webhook status
- `DELETE /webhook-statuses?reference_id=&status=` - Bulk delete webhook statuses

### Health Check

- `GET /health` - Get the health status of the system, including Redis, Celery, and circuit breakers

### Webhook Cleanup

- `POST /webhook-cleanup?status=&older_than_days=&reference_id=` - Clean up webhook statuses based on criteria

## Testing the Webhook System

The webhook system includes test scripts to verify its functionality, especially how it handles failures.

### Prerequisites

- Python 3.6+
- Requests library (`pip install requests`)
- Running API server (default: http://localhost:8000)

### Running the Tests

1. Start the webhook receiver server in one terminal:

```bash
python webhook_receiver_server.py
```

Options:
- `--port PORT` - Port to listen on (default: 9001)
- `--failure-rate RATE` - Percentage of requests that should fail (0-100, default: 0)
- `--delay SECONDS` - Delay in seconds before responding (default: 0)

2. Run the test script in another terminal:

```bash
python test_webhook_failure.py
```

Options:
- `--api-url URL` - Base URL of the API (default: http://localhost:8000)
- `--failure [4xx|5xx|timeout|invalid_url]` - Failure scenario to test
- `--skip-receiver-check` - Skip checking if webhook receiver is running

### Testing Different Failure Scenarios

#### Testing the Dead Letter Queue (DLQ) Mechanism

```bash
# Start the webhook receiver with 100% failure rate
python webhook_receiver_server.py --failure-rate 100

# In another terminal
python test_webhook_dlq.py
```

This tests the Dead Letter Queue (DLQ) mechanism by sending a test webhook request and checking if it's moved to the DLQ after max retries.

A simpler version is also available:

```bash
python test_webhook_dlq_simple.py
```

#### 4xx Client Errors

```bash
python test_webhook_failure.py --failure 4xx
```

This tests how the system handles 4xx client errors (e.g., endpoint not found). The system should retry once, then mark as failed.

#### 5xx Server Errors

```bash
# Start the webhook receiver with 100% failure rate
python webhook_receiver_server.py --failure-rate 100

# In another terminal
python test_webhook_failure.py --failure 5xx
```

This tests how the system handles 5xx server errors. The system should retry with exponential backoff up to 3 attempts, then mark as failed.

#### Timeouts

```bash
# Start the webhook receiver with a long delay
python webhook_receiver_server.py --delay 60

# In another terminal
python test_webhook_failure.py --failure timeout
```

This tests how the system handles timeouts. The system should retry with exponential backoff up to 3 attempts, then mark as failed.

#### Invalid URLs

```bash
python test_webhook_failure.py --failure invalid_url
```

This tests how the system handles invalid URLs. The system should reject the URL during validation and not attempt delivery.

## Test Endpoint

The API includes a test endpoint specifically for testing webhook delivery:

```
POST /test-webhook?webhook_url=http://example.com/webhook
```

Parameters:
- `webhook_url` (query parameter): The URL to send the webhook to
- Request body: Any JSON payload to send to the webhook

Example:
```bash
curl -X POST "http://localhost:8000/test-webhook?webhook_url=http://localhost:9001/webhook-receiver" \
     -H "Content-Type: application/json" \
     -d '{"test_payload": {"key": "value"}}'
```

Response:
```json
{
  "status": "webhook_queued",
  "reference_id": "TEST-a1b2c3d4",
  "task_id": "e5f6g7h8-i9j0-k1l2-m3n4-o5p6q7r8s9t0",
  "message": "Test webhook queued for delivery"
}
```

## Webhook Status Lifecycle

1. **pending** - Initial state when a webhook is queued
2. **in_progress** - Webhook delivery is in progress
3. **retrying** - Webhook delivery failed but will be retried
4. **delivered** - Webhook was successfully delivered
5. **failed** - Webhook delivery failed permanently

## Dead Letter Queue

Permanently failed webhook deliveries are stored in the dead letter queue (DLQ) with a 30-day TTL. The DLQ includes:

- The original payload
- Error information
- Attempt count
- Correlation ID

## Metrics

The system exposes Prometheus metrics for monitoring:

- `webhook_delivery_total{status,worker_id}` - Counter for webhook deliveries
- `webhook_delivery_seconds{worker_id}` - Histogram for webhook delivery time
- `task_processing_total{status,mode,worker_id}` - Counter for task processing
- `task_processing_seconds{mode,worker_id}` - Histogram for task processing time
- `circuit_breaker_status{service}` - Gauge for circuit breaker status
- `redis_webhook_keys{status}` - Gauge for webhook key counts

## Troubleshooting

### Common Issues

1. **Webhook delivery fails immediately**
   - Check that the webhook URL is valid and accessible
   - Verify that the URL matches the allowlist pattern (if configured)

2. **Webhook status not found**
   - Ensure you're using the correct webhook_id format: `{reference_id}_{task_id}`
   - Check that the webhook was actually queued

3. **Metrics not available**
   - Verify that ENABLE_PROMETHEUS is set to true
   - Check that the Prometheus server is running on the configured port

### Logs

The system logs webhook delivery attempts with correlation IDs for traceability. Look for log entries with the following patterns:

- `[correlation_id] Sending webhook notification to {url} for reference_id={id}`
- `[correlation_id] Successfully delivered webhook for reference_id={id}`
- `[correlation_id] Webhook delivery failed with status {code}`
- `[correlation_id] Retrying webhook delivery for reference_id={id}`
- `[correlation_id] Moved failed webhook to dead letter queue`