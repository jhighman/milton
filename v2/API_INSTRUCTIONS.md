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
- Windows operating system

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

### 2. Running the API Locally
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

### 2. Process an Extended Claim with Webhook
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

## Processing Modes

### Basic Mode
- Skips disciplinary reviews
- Skips arbitration reviews
- Skips regulatory reviews
- Fastest processing time

### Extended Mode
- Includes disciplinary reviews
- Includes arbitration reviews
- Skips regulatory reviews
- Moderate processing time

### Complete Mode
- Includes all reviews
- Most comprehensive results
- Longest processing time

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

## Support
For additional support or questions, contact the development team or refer to the internal documentation. 