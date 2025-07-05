# Compliance Claim Processing API

This API processes compliance claims in three modes (basic, extended, complete) using a single-threaded, FIFO task queue to prevent crashes from resource-intensive claim processing.

## Features

- Asynchronous processing: Requests with webhook_url are queued, returning "processing_queued", with results sent to the webhook
- Synchronous processing: Requests without webhook_url are processed immediately
- Single-threaded, FIFO task processing using Celery with Redis
- Robust error handling and retry logic

## Installation

1. Clone the repository
2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
```
3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

The API uses Redis for task queuing:
- Broker: redis://localhost:6379/0
- Backend: redis://localhost:6379/0

Celery is configured with:
- task_concurrency=1
- worker_prefetch_multiplier=1
- task_acks_late=True

This ensures single-threaded, FIFO processing of tasks.

## Running the API

1. Start Redis:
```bash
redis-server
```

2. Start Celery worker:
```bash
celery -A api.celery_app worker --loglevel=info
```

3. Start the FastAPI server:
```bash
uvicorn api:app --reload
```

## API Endpoints

- POST /process-claim-basic - Process a claim with basic mode
- POST /process-claim-extended - Process a claim with extended mode
- POST /process-claim-complete - Process a claim with complete mode
- GET /task-status/{task_id} - Check the status of a queued or in-progress task

### Task Status Endpoint

The task status endpoint allows you to check the status of asynchronous tasks:

```bash
GET /task-status/{task_id}
```

Parameters:
- `task_id` (path): The unique task ID returned in the response of an asynchronous claim processing request.

Response:
```json
{
  "task_id": "string",
  "status": "QUEUED | PROCESSING | COMPLETED | FAILED | RETRYING",
  "reference_id": "string",
  "result": {object} | null,
  "error": "string" | null
}
```

Status values:
- `QUEUED`: Task is in queue waiting to be processed
- `PROCESSING`: Task has started processing
- `COMPLETED`: Task has completed successfully
- `FAILED`: Task has failed
- `RETRYING`: Task is being retried after a failure

Example:
```bash
curl http://localhost:8000/task-status/123e4567-e89b-12d3-a456-426655440000
```

## Running Automated Tests

The project includes automated tests for API functionality and concurrency behavior, implemented using Python's `unittest` framework.

### Testing Dependencies
Install the required testing dependencies in your virtual environment:
```bash
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install pytest pytest-asyncio fastapi httpx celery
```

### Running Tests
To run the concurrency tests:
```bash
python -m unittest test_api_concurrency.py
```

The tests verify:
1. Concurrency behavior (FIFO processing)
2. Synchronous processing
3. Error handling and retry logic
4. Webhook failure handling

The tests use in-memory Celery workers and mock Redis to isolate concurrency concerns without requiring real Redis or external services.