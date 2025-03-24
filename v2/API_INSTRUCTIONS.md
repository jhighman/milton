# Compliance Claim Processing API Instructions

## Overview
This FastAPI application provides endpoints for processing individual compliance claims and managing cached compliance data. It supports basic, extended, and complete processing modes, along with cache management and compliance analytics features.

## Prerequisites
- Python 3.8+
- Chrome/Chromium browser
- ChromeDriver (compatible with your Chrome version)
- Virtual environment (recommended)

## Local Development Setup

### 1. Environment Setup
```bash
# Create and activate virtual environment
python -m venv myvenv
source myvenv/bin/activate  # On Windows: myvenv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Running the API Locally
```bash
# Basic run command
python -m uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at:
- API Endpoints: http://localhost:8000
- Swagger Documentation: http://localhost:8000/docs
- ReDoc Documentation: http://localhost:8000/redoc

### 3. Headless Mode Configuration
The API runs in headless mode by default. You can modify this setting in two ways:

1. At startup through environment variables:
```bash
export HEADLESS_MODE=false
python -m uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

2. At runtime through the API:
```bash
# Check current settings
curl http://localhost:8000/settings

# Update settings
curl -X PUT http://localhost:8000/settings \
  -H "Content-Type: application/json" \
  -d '{"headless": false, "debug": true}'
```

## Production Deployment

### 1. Security Considerations
Before deploying to production:
- Set up proper authentication/authorization
- Configure CORS settings
- Use HTTPS
- Set up rate limiting
- Configure proper logging
- Use environment variables for sensitive data

### 2. Production Server Setup
For production, use a production-grade ASGI server like Gunicorn with Uvicorn workers:

```bash
# Install Gunicorn
pip install gunicorn

# Run with Gunicorn (adjust worker count based on CPU cores)
gunicorn api:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

### 3. Environment Variables
Set these environment variables in production:
```bash
export PRODUCTION=true
export HEADLESS_MODE=true
export DEBUG=false
export LOG_LEVEL=info
export ALLOWED_HOSTS=your-domain.com
export MAX_WORKERS=4
```

### 4. Docker Deployment
If using Docker:

```dockerfile
FROM python:3.8-slim

# Install Chrome and ChromeDriver
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV HEADLESS_MODE=true
ENV CHROME_BIN=/usr/bin/chromium

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application code
COPY . .

# Run the application
CMD ["gunicorn", "api:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]
```

Build and run:
```bash
docker build -t compliance-api .
docker run -p 8000:8000 compliance-api
```

### 5. Health Checks
Monitor these endpoints for system health:
- `/health`: Basic health check
- `/metrics`: Application metrics (if implemented)

### 6. Logging
Configure proper logging in production:
```python
# Example logging configuration
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'app.log',
            'maxBytes': 1024 * 1024 * 5,  # 5 MB
            'backupCount': 5,
        },
    },
    'root': {
        'handlers': ['file'],
        'level': 'INFO',
    },
}
```

### 7. Monitoring
Consider implementing:
- Prometheus metrics
- Error tracking (e.g., Sentry)
- Performance monitoring
- Resource usage alerts

## Troubleshooting

### Common Issues

1. ChromeDriver Version Mismatch
```bash
# Check Chrome version
google-chrome --version

# Download matching ChromeDriver version
# Update ChromeDriver path in configuration
```

2. WebDriver Initialization Failures
- Ensure Chrome/Chromium is installed
- Verify ChromeDriver is in PATH
- Check file permissions

3. Memory Issues
- Monitor memory usage
- Adjust worker count
- Implement proper cleanup

### Getting Help
- Check the logs: `tail -f app.log`
- Review Swagger documentation
- Contact support team

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