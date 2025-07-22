# Compliance Claim Processing API Instructions

## Overview
This FastAPI application provides endpoints for processing individual compliance claims and managing cached compliance data. It supports:
- Basic, extended, and complete processing modes
- Cache management
- Compliance analytics features
- Configurable headless mode for browser automation

## Prerequisites
- Python 3.8+
- Chrome/Chromium browser
- ChromeDriver (matching your Chrome version)
- Redis server (for task queuing)
- Windows or Linux operating system

## Local Development Setup

### 1. Environment Setup
```bash
# Create and activate virtual environment (Windows)
python -m venv venv
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install Chrome WebDriver (ensure it matches your Chrome version)
# Add ChromeDriver to your system PATH
```

### 2. Redis Setup

#### Installing Redis on Linux
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install redis-server

# Start Redis service
sudo systemctl start redis-server
sudo systemctl enable redis-server

# Verify Redis is running
redis-cli ping
# Should return "PONG"
```

#### Installing Redis on Windows
Redis is not officially supported on Windows, but you can use:
- [Redis for Windows](https://github.com/tporadowski/redis/releases)
- [Memurai](https://www.memurai.com/) (Redis-compatible Windows service)
- WSL (Windows Subsystem for Linux) to run Redis

#### Redis Configuration
The default configuration (localhost:6379) works for development. For production, consider:
- Setting a password
- Configuring persistence
- Enabling protected mode

```bash
# Test Redis connection
redis-cli
> ping
PONG
> quit
```

### 3. Celery Worker Setup
The API uses Celery with Redis for task queuing with specific configuration to ensure single-threaded, FIFO processing:

```bash
# Start Celery worker
celery -A api.celery_app worker --loglevel=info --concurrency=1 --prefetch-multiplier=1
```

Key Celery configuration parameters:
- `task_concurrency=1`: Ensures single-threaded processing
- `worker_prefetch_multiplier=1`: Processes one task at a time (FIFO order)
- `task_acks_late=True`: Acknowledges tasks after completion

This configuration prevents resource contention by ensuring tasks are processed sequentially.

### 4. Running the API Locally
```bash
# Basic run command
python -m uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at:
- API Endpoints: http://localhost:8000
- Interactive API Documentation: http://localhost:8000/docs

### 3. Logging Configuration
The API uses a structured logging system with the following groups:
- `services`: Core services (services, normalizer, marshaller, etc.)
- `agents`: Various agent modules (FINRA, SEC, NFA)
- `evaluation`: Evaluation processing modules
- `core`: Main application components

Logs are written to:
- Console output
- `logs/app.log` (rotated, max 10MB per file, 5 backups)

To enable debug logging:
```python
loggers = setup_logging(debug=True)
```

To reconfigure logging for specific groups:
```python
reconfigure_logging(loggers, {'services'}, {'services': 'DEBUG'})
```

### 4. Headless Mode Configuration
The API runs in headless mode by default (True), which is recommended for production use. For debugging purposes, you can disable headless mode through the settings API endpoint.

#### Checking Current Settings
```bash
curl http://localhost:8000/settings
```
Example response:
```json
{
    "headless": true,
    "debug": false
}
```

#### Disabling Headless Mode for Debugging
```bash
curl -X PUT http://localhost:8000/settings ^
  -H "Content-Type: application/json" ^
  -d "{\"headless\": false, \"debug\": true}"
```

Note: Disabling headless mode will:
- Show browser windows during automation
- Slow down processing
- Consume more resources
- May interfere with other processes

It should only be used during development/debugging when you need to visually inspect the browser automation process.

#### Restoring Normal Operation
```bash
curl -X PUT http://localhost:8000/settings ^
  -H "Content-Type: application/json" ^
  -d "{\"headless\": true, \"debug\": false}"
```

The API will automatically reinitialize the browser automation services when headless mode is changed, ensuring a clean state for subsequent operations.

## API Usage Examples

### 1. Process a Basic Claim
```bash
curl -X POST http://localhost:8000/process-claim-basic ^
  -H "Content-Type: application/json" ^
  -d "{
    \"reference_id\": \"REF123\",
    \"employee_number\": \"EMP456\",
    \"first_name\": \"John\",
    \"last_name\": \"Doe\",
    \"organization_name\": \"ACME Corp\"
  }"
```

### 2. Process an Extended Claim with Webhook (Asynchronous)
```bash
curl -X POST http://localhost:8000/process-claim-extended ^
  -H "Content-Type: application/json" ^
  -d "{
    \"reference_id\": \"REF124\",
    \"employee_number\": \"EMP457\",
    \"first_name\": \"Jane\",
    \"last_name\": \"Smith\",
    \"organization_name\": \"XYZ Inc\",
    \"webhook_url\": \"http://your-webhook.com/endpoint\"
  }"
```

When a webhook URL is provided, the API processes the claim asynchronously and returns an immediate response:
```json
{
  "status": "processing_started",
  "reference_id": "REF124",
  "message": "Claim processing started; result will be sent to webhook"
}
```

The complete result will be sent to the specified webhook URL once processing is complete.

### 3. Cache Management
```bash
# Clear cache for specific employee
curl -X POST http://localhost:8000/cache/clear/EMP456

# List cache contents
curl http://localhost:8000/cache/list?employee_number=EMP456&page=1&page_size=10

# Clean up stale cache
curl -X POST http://localhost:8000/cache/cleanup-stale
```

### 4. Compliance Analytics
```bash
# Get compliance summary for employee
curl http://localhost:8000/compliance/summary/EMP456

# Get risk dashboard
curl http://localhost:8000/compliance/risk-dashboard

# Get data quality report
curl http://localhost:8000/compliance/data-quality
```

## Troubleshooting

### Common Issues

1. ChromeDriver Version Mismatch
```
WARNING - The chromedriver version detected in PATH might not be compatible with the detected chrome version
```
Solution: Download the matching ChromeDriver version from https://chromedriver.chromium.org/downloads

2. WebDriver Initialization Failures
- Ensure Chrome is installed
- Verify ChromeDriver is in PATH
- Check Windows Defender or antivirus isn't blocking ChromeDriver

3. Logging Issues
- Check write permissions for the `logs` directory
- Verify log rotation is working (`logs/app.log`)
- Use `flush_logs()` if logs aren't appearing immediately

### Getting Help
1. Check the logs:
   - Console output
   - `logs/app.log`
2. Enable debug logging:
   ```python
   loggers = setup_logging(debug=True)
   ```
3. Review specific logger groups:
   ```python
   reconfigure_logging(loggers, {'services'}, {'services': 'DEBUG'})
   ```

## Processing Modes and Asynchronous Processing

### Processing Modes

#### Basic Mode
- Skips disciplinary reviews
- Skips arbitration reviews
- Skips regulatory reviews
- Fastest processing time

#### Extended Mode
- Includes disciplinary reviews
- Includes arbitration reviews
- Skips regulatory reviews
- Moderate processing time

#### Complete Mode
- Includes all reviews
- Most comprehensive results
- Longest processing time

### Synchronous vs. Asynchronous Processing

The API supports both synchronous and asynchronous processing:

#### Synchronous Processing
- Used when no webhook URL is provided
- The API processes the claim and returns the complete result in the response
- The client must wait for the entire processing to complete
- Suitable for quick operations or when immediate results are needed

#### Asynchronous Processing
- Used when a webhook URL is provided in the request
- The API returns an immediate acknowledgment response
- Processing continues in the background
- Results are sent to the specified webhook URL when complete
- Suitable for long-running operations or when integrating with other systems

## API Endpoints

### Claim Processing
- `POST /process-claim-basic`: Basic processing mode
- `POST /process-claim-extended`: Extended processing mode
- `POST /process-claim-complete`: Complete processing mode

### Task Status
- `GET /task-status/{task_id}`: Check the status of a queued or in-progress task
  - **Parameters**:
    - `task_id` (path): The unique task ID returned in the response of an asynchronous claim processing request.
  - **Response**:
    ```json
    {
      "task_id": "string",
      "status": "QUEUED | PROCESSING | COMPLETED | FAILED | RETRYING",
      "reference_id": "string",
      "result": {object} | null,
      "error": "string" | null
    }
    ```
  - **Example**:
    ```bash
    curl http://localhost:8000/task-status/123e4567-e89b-12d3-a456-426655440000
    ```
    Response:
    ```json
    {
      "task_id": "123e4567-e89b-12d3-a456-426655440000",
      "status": "QUEUED",
      "reference_id": "REF123",
      "result": null,
      "error": null
    }
    ```
  - **Notes**:
    - Use the `task_id` returned from a `/process-claim-*` request with a `webhook_url`.
    - Statuses: `QUEUED` (task in queue), `PROCESSING` (task started), `COMPLETED` (task succeeded), `FAILED` (task failed), `RETRYING` (task retrying after failure).
    - In production, consider implementing rate-limiting to avoid overloading the Redis backend.

#### Validating the Task Status Endpoint in Production

To validate the endpoint in your production environment:

1. Deploy the updated api.py to your production environment:
```bash
python -m uvicorn api:app --host 0.0.0.0 --port 8000
```

2. Start the Celery worker:
```bash
celery -A api.celery_app worker --loglevel=info --concurrency=1 --prefetch-multiplier=1
```

3. Submit multiple async requests to simulate a high-volume scenario:
```bash
for i in {1..5}; do
  curl -X POST http://production-host:8000/process-claim-basic \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer <your-token>" \
    -d "{\"reference_id\": \"REF${i}\", \"employee_number\": \"EN-01631${i}\", \"first_name\": \"John\", \"last_name\": \"Doe\", \"crd_number\": \"123456${i}\", \"webhook_url\": \"https://webhook.site/test-${i}\"}"
done
```

Expected responses:
```json
{"status": "processing_queued", "reference_id": "REF1", "task_id": "uuid1"}
{"status": "processing_queued", "reference_id": "REF2", "task_id": "uuid2"}
...
```

4. Query task status for each task_id:
```bash
curl -H "Authorization: Bearer <your-token>" http://production-host:8000/task-status/<task_id>
```

Expected response (e.g., for a queued task):
```json
{
  "task_id": "uuid1",
  "status": "QUEUED",
  "reference_id": "REF1",
  "result": null,
  "error": null
}
```

5. Monitor task progression through different states:
   - Initially tasks will show as `QUEUED`
   - As they start processing, they'll change to `PROCESSING`
   - Finally, they'll show as `COMPLETED` or `FAILED`
   - If errors occur and retries are triggered, they'll show as `RETRYING`

### Cache Management
- `POST /cache/clear/{employee_number}`: Clear employee cache
- `POST /cache/clear-all`: Clear all cache
- `GET /cache/list`: List cached files

### Compliance Analytics
- `GET /compliance/summary/{employee_number}`: Get employee compliance summary
- `GET /compliance/risk-dashboard`: View risk dashboard
- `GET /compliance/data-quality`: Check data quality

## Testing Webhooks Locally

To test webhook functionality locally, you need a way to receive webhook callbacks. Here are several approaches:

### 1. Using ngrok for Local Webhook Testing

[ngrok](https://ngrok.com/) creates a public URL for your local server, allowing external services to send webhooks to your local machine:

```bash
# Install ngrok
# Run your API server
python -m uvicorn api:app --host 0.0.0.0 --port 8000 --reload

# In another terminal, start ngrok to expose your local server
ngrok http 8000
```

Use the ngrok URL in your webhook_url parameter:
```bash
curl -X POST http://localhost:8000/process-claim-basic ^
  -H "Content-Type: application/json" ^
  -d "{
    \"reference_id\": \"REF123\",
    \"employee_number\": \"EMP456\",
    \"first_name\": \"John\",
    \"last_name\": \"Doe\",
    \"organization_name\": \"ACME Corp\",
    \"webhook_url\": \"https://your-ngrok-url.ngrok.io/webhook-receiver\"
  }"
```

### 2. Using a Simple Local Webhook Receiver

Create a simple webhook receiver using FastAPI in a separate file (e.g., `webhook_receiver.py`):

```python
from fastapi import FastAPI, Request
import uvicorn
import json

app = FastAPI()

@app.post("/webhook-receiver")
async def webhook_receiver(request: Request):
    data = await request.json()
    print("Received webhook data:")
    print(json.dumps(data, indent=2))
    return {"status": "received"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
```

Run this receiver on a different port:
```bash
python webhook_receiver.py
```

Then use `http://localhost:8001/webhook-receiver` as your webhook URL:
```bash
curl -X POST http://localhost:8000/process-claim-basic ^
  -H "Content-Type: application/json" ^
  -d "{
    \"reference_id\": \"REF123\",
    \"employee_number\": \"EMP456\",
    \"first_name\": \"John\",
    \"last_name\": \"Doe\",
    \"organization_name\": \"ACME Corp\",
    \"webhook_url\": \"http://localhost:8001/webhook-receiver\"
  }"
```

### 3. Using Webhook Testing Services

Online services like [Webhook.site](https://webhook.site/) provide temporary URLs for testing webhooks:

1. Visit [Webhook.site](https://webhook.site/)
2. Copy your unique URL
3. Use it as the webhook_url in your API requests
4. View incoming webhook data in real-time on the website

## Webhook Delivery System Improvements

The API includes an improved webhook delivery system with enhanced reliability, monitoring, and observability features.

### Key Improvements

1. **Enhanced Celery Configuration**
   - Increased task concurrency from 1 to 4 for better throughput
   - Added broker connection retry settings for improved reliability
   - Set broker transport options with visibility timeout
   ```bash
   # Start Celery worker with improved configuration
   celery -A api.celery_app worker --loglevel=info --concurrency=4
   ```

2. **Dedicated Webhook Delivery Task**
   - Separate Celery task for webhook delivery
   - Exponential backoff with jitter for retries (30s to 5min)
   - Synchronous HTTP requests for better Celery worker compatibility
   - Detailed logging for webhook delivery attempts

3. **Webhook Status Tracking**
   - In-memory storage for webhook delivery statuses
   - Status tracking throughout the webhook delivery process
   - API endpoints for webhook status management

### Webhook Status Endpoints

- `GET /webhook-status/{webhook_id}`: Get status of a specific webhook delivery
  ```bash
  curl http://localhost:8000/webhook-status/ref123_task456
  ```
  Response:
  ```json
  {
    "status": "delivered",
    "reference_id": "ref123",
    "task_id": "task456",
    "webhook_url": "https://webhook.site/test-webhook",
    "attempts": 1,
    "max_attempts": 5,
    "last_attempt": "2025-07-22T17:30:00.000Z",
    "created_at": "2025-07-22T17:29:55.000Z",
    "completed_at": "2025-07-22T17:30:01.000Z",
    "response_code": 200
  }
  ```

- `GET /webhook-statuses`: List all webhook statuses with pagination and filtering
  ```bash
  # List all webhook statuses
  curl http://localhost:8000/webhook-statuses
  
  # Filter by reference_id
  curl http://localhost:8000/webhook-statuses?reference_id=ref123
  
  # Filter by status
  curl http://localhost:8000/webhook-statuses?status=failed
  
  # Pagination
  curl http://localhost:8000/webhook-statuses?page=2&page_size=10
  ```

- `DELETE /webhook-status/{webhook_id}`: Delete a specific webhook status
  ```bash
  curl -X DELETE http://localhost:8000/webhook-status/ref123_task456
  ```

- `DELETE /webhook-statuses`: Delete all webhook statuses with optional filtering
  ```bash
  # Delete all webhook statuses
  curl -X DELETE http://localhost:8000/webhook-statuses
  
  # Delete all failed webhook statuses
  curl -X DELETE http://localhost:8000/webhook-statuses?status=failed
  
  # Delete all webhook statuses for a specific reference_id
  curl -X DELETE http://localhost:8000/webhook-statuses?reference_id=ref123
  ```

### Production Considerations

For production deployment, consider the following additional improvements:

1. **Persistent Storage for Webhook Statuses**
   - Replace the in-memory dictionary with a persistent storage solution (Redis, database)
   - Example implementation:
   ```python
   # Using Redis for webhook status storage
   def store_webhook_status(webhook_id, status_data):
       redis_client.hset("webhook_statuses", webhook_id, json.dumps(status_data))
   
   def get_webhook_status(webhook_id):
       data = redis_client.hget("webhook_statuses", webhook_id)
       return json.loads(data) if data else None
   ```

2. **Monitoring and Alerting**
   - Set up Prometheus metrics for webhook delivery success/failure rates
   - Configure alerts for high failure rates or increased latency
   - Monitor Celery queue length to detect backlog issues
   - Example metrics to track:
     - Webhook delivery success rate
     - Webhook delivery latency
     - Retry counts
     - Queue backlog

3. **Rate Limiting and Throttling**
   - Implement per-endpoint rate limiting to prevent overwhelming webhook receivers
   - Consider adding backpressure mechanisms for high-volume webhook destinations

4. **Security Enhancements**
   - Validate webhook URLs against an allowlist
   - Add HMAC signatures to webhook payloads for verification
   - Consider implementing webhook authentication
   - Example signature implementation:
   ```python
   def sign_webhook_payload(payload, secret):
       signature = hmac.new(
           secret.encode(),
           json.dumps(payload).encode(),
           hashlib.sha256
       ).hexdigest()
       return signature
   ```

5. **Logging and Troubleshooting**
   - Ensure comprehensive logging with correlation IDs
   - Log full request/response details for failed webhooks
   - Implement distributed tracing (e.g., OpenTelemetry)
   - Troubleshooting commands:
   ```bash
   # Check Celery queue status
   celery -A api.celery_app inspect active_queues
   
   # View active tasks
   celery -A api.celery_app inspect active
   
   # Check webhook status via API
   curl http://localhost:8000/webhook-status/{webhook_id}
   
   # List all failed webhooks
   curl http://localhost:8000/webhook-statuses?status=failed
   ```

6. **Automatic Cleanup**
   - Implement a scheduled task to clean up old webhook statuses
   - Example cleanup task:
   ```python
   @celery_app.task(name="cleanup_old_webhook_statuses")
   def cleanup_old_webhook_statuses():
       """Remove webhook statuses older than 30 days."""
       cutoff_date = datetime.now() - timedelta(days=30)
       # If using Redis:
       for webhook_id, status_json in redis_client.hscan_iter("webhook_statuses"):
           status = json.loads(status_json)
           created_at = datetime.fromisoformat(status.get("created_at"))
           if created_at < cutoff_date:
               redis_client.hdel("webhook_statuses", webhook_id)
   ```

7. **Circuit Breaker Pattern**
   - Implement circuit breakers for webhook endpoints with high failure rates
   - Temporarily disable delivery attempts to consistently failing endpoints
   - Example implementation using a library like `pybreaker`:
   ```python
   from pybreaker import CircuitBreaker
   
   # Create circuit breakers for each webhook endpoint
   webhook_breakers = {}
   
   def get_circuit_breaker(webhook_url):
       domain = urlparse(webhook_url).netloc
       if domain not in webhook_breakers:
           webhook_breakers[domain] = CircuitBreaker(
               fail_max=5,
               reset_timeout=60,
               exclude=[requests.exceptions.Timeout]
           )
       return webhook_breakers[domain]
   
   # Use in webhook delivery
   breaker = get_circuit_breaker(webhook_url)
   response = breaker.call(lambda: requests.post(webhook_url, json=payload))
   ```

8. **Webhook Replay Capability**
   - Add functionality to manually replay failed webhooks
   - Store the original payload for replay purposes
   - Example API endpoint:
   ```python
   @app.post("/webhook-status/{webhook_id}/replay")
   async def replay_webhook(webhook_id: str):
       """Replay a failed webhook delivery."""
       if webhook_id not in webhook_statuses:
           raise HTTPException(status_code=404, detail="Webhook not found")
           
       status = webhook_statuses[webhook_id]
       if status["status"] != WebhookStatus.FAILED.value:
           raise HTTPException(status_code=400, detail="Only failed webhooks can be replayed")
           
       # Queue a new webhook delivery task
       send_webhook_notification.delay(
           status["webhook_url"],
           status.get("payload", {}),
           status["reference_id"]
       )
       
       return {"message": f"Webhook {webhook_id} queued for replay"}
   ```

### Testing Webhook Delivery

The project includes a dedicated test file for webhook delivery functionality:

```bash
# Run webhook delivery tests
python test_webhook_delivery.py
```

This test verifies:
- Webhook task is called when a webhook URL is provided
- Webhook task is not called when no webhook URL is provided
- Successful webhook delivery updates the status correctly
- Failed webhook delivery triggers a retry with exponential backoff
- Webhook status tracking endpoints work correctly

## Running Automated Tests

The project includes automated tests for the API functionality:

```bash
# Run asynchronous API tests
pytest test_api_async.py -v

# Run concurrency behavior tests
pytest test_api_concurrency.py -v
```

The tests include validation of:
- Synchronous and asynchronous processing
- Concurrency behavior (FIFO processing)
- Error handling and retry logic
- Webhook integration
- Task status endpoint functionality

These tests verify:
- Synchronous processing works as expected
- Asynchronous processing returns immediate responses
- Background tasks are properly scheduled
- Webhook delivery functions correctly
- Error handling works in both modes
- Concurrency behavior ensures FIFO, single-threaded processing
- API remains responsive under load

### Concurrency Testing

The `test_api_concurrency.py` test specifically validates that:

1. For asynchronous requests (with webhook_url):
   - The API responds immediately with "processing_queued" status
   - Tasks are processed in FIFO order
   - Tasks are processed sequentially with no overlap (single-threaded execution)
   - Each task starts only after the previous one completes

2. For synchronous requests (without webhook_url):
   - The API processes the request immediately
   - The full report is returned directly in the response
   - No tasks are queued

This test ensures that the Celery configuration (task_concurrency=1, worker_prefetch_multiplier=1) successfully prevents resource contention crashes while maintaining API responsiveness.

## Support
For additional support or questions, contact the development team or refer to the internal documentation.