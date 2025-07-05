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

## Running Automated Tests

The project includes automated tests for the API functionality:

```bash
# Run asynchronous API tests
pytest test_api_async.py -v

# Run concurrency behavior tests
pytest test_api_concurrency.py -v
```

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