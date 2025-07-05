# API Integration Test

This integration test verifies the end-to-end functionality of the API, focusing on the asynchronous processing of compliance claims with webhooks.

## Overview

The test:
1. Sends three successive compliance claim requests with webhook URLs
2. Polls the task status endpoint until each task completes
3. Logs detailed metrics about polling attempts and processing time
4. Verifies successful completion of all tasks

## Test Data

The test uses three predefined test cases:

1. **James Betzig** (CRD: 2457078)
2. **Adrian Larson** (CRD: 3098721)
3. **Mark Copeland** (CRD: 1844301)

Each request includes a unique webhook URL to receive the results.

## Prerequisites

Before running the test:

1. Ensure the API server is running on http://localhost:8000 (or update the `API_BASE_URL` in the test file)
2. Make sure Redis is running for Celery task processing
3. Ensure Celery workers are running with:
   ```
   celery -A api.celery_app worker --loglevel=info
   ```

## Running the Test

You can run the integration test in two ways:

### Option 1: Using the runner script

```bash
./run_integration_test.py
```

This script will run only the integration test and provide a summary of the results.

### Option 2: Using unittest directly

```bash
python -m unittest test_api_integration.py
```

## Test Output

The test logs detailed information about:

- Request submission time
- Number of polling attempts for each task
- Total processing time for each task
- Success/failure status

Example output:

```
==================================================
INTEGRATION TEST RESULTS SUMMARY
==================================================
Reference ID: Integration-Test-1
  Request Time: 0.15 seconds
  Polling Attempts: 12
  Total Processing Time: 24.35 seconds
  Success: True
--------------------------------------------------
Reference ID: Integration-Test-2
  Request Time: 0.12 seconds
  Polling Attempts: 14
  Total Processing Time: 28.76 seconds
  Success: True
--------------------------------------------------
Reference ID: Integration-Test-3
  Request Time: 0.14 seconds
  Polling Attempts: 13
  Total Processing Time: 26.42 seconds
  Success: True
--------------------------------------------------
```

## Troubleshooting

If the test fails:

1. **API Connection Issues**: Verify the API server is running and accessible
2. **Webhook Issues**: The test uses webhook.site URLs; ensure internet connectivity
3. **Task Processing Issues**: Check Celery worker logs for errors
4. **Timeout Issues**: If tasks take too long, increase the `max_attempts` parameter in the `poll_task_status` method

## Modifying the Test

To modify the test data or behavior:

1. Edit the `test_requests` list in the `setUp` method to change request data
2. Adjust polling parameters in the `poll_task_status` method
3. Update the `API_BASE_URL` if your API is running on a different host/port