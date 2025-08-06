# System Stability Enhancements: DevOps Troubleshooting Guide

## Overview

This document provides a comprehensive guide for DevOps teams managing the compliance reporting system. It explains the stability enhancements implemented to address system crashes and handle high request loads, focusing on the integration between Redis, Celery, and the new stability features.

## Architecture Components

### Redis Integration

Redis serves as a critical component in our enhanced architecture:

1. **Webhook Status Persistence**
   - All webhook statuses are now stored in Redis instead of in-memory
   - Keys follow the pattern: `webhook_status:{webhook_id}`
   - TTL-based automatic cleanup:
     - Successful webhooks: 30 minutes
     - Failed/pending/retrying webhooks: 7 days

2. **Dead Letter Queue**
   - Failed webhook deliveries are stored in Redis
   - Keys follow the pattern: `dead_letter:webhook:{webhook_id}`
   - 30-day TTL for all dead letter entries
   - Contains full context including payload, error details, and correlation ID

### Celery Configuration

Celery handles asynchronous task processing with these stability enhancements:

1. **Task Configuration**
   - `task_concurrency=4`: Processes up to 4 tasks simultaneously
   - `worker_prefetch_multiplier=1`: Ensures FIFO processing
   - `task_acks_late=True`: Acknowledges tasks only after successful completion
   - `task_time_limit=3600`: 1-hour timeout for long-running tasks

2. **Retry Mechanism**
   - Intelligent retry logic based on error type
   - Maximum 3 retries (reduced from 5)
   - Exponential backoff with jitter to prevent thundering herd problems
   - Different retry strategies for 4xx vs 5xx errors

### Circuit Breaker Pattern

Protects the system from cascading failures:

1. **States**
   - Closed: Normal operation, all requests pass through
   - Open: Service considered down, requests fail fast
   - Half-open: Testing if service has recovered

2. **Configuration**
   - Default failure threshold: 5 consecutive failures
   - Default reset timeout: 60 seconds
   - Automatic state transitions based on success/failure

### Monitoring

1. **Prometheus Metrics**
   - Webhook delivery counters and timers
   - Task processing metrics
   - Circuit breaker status gauges
   - Redis key counts

2. **Health Check Endpoint**
   - `/health` endpoint provides system component status
   - Checks Redis, Celery, facade, storage, and circuit breakers

## Troubleshooting Guide

### Redis Issues

#### Symptoms: Webhook statuses not persisting or retrieving incorrectly

1. **Check Redis Connection**
   ```bash
   redis-cli ping
   ```
   Should return "PONG". If not, Redis may be down.

2. **Check Redis Memory Usage**
   ```bash
   redis-cli info memory
   ```
   If `used_memory_human` is close to the configured limit, Redis may be evicting keys.

3. **Inspect Webhook Keys**
   ```bash
   redis-cli keys "webhook_status:*"
   ```
   Lists all webhook status keys.

4. **Check TTL on Keys**
   ```bash
   redis-cli ttl webhook_status:{reference_id}
   ```
   Verifies TTL is set correctly.

5. **Examine Dead Letter Queue**
   ```bash
   redis-cli keys "dead_letter:webhook:*"
   ```
   Lists all dead letter entries.

#### Resolution Steps

1. If Redis is down, restart the Redis service:
   ```bash
   sudo systemctl restart redis
   ```

2. If memory usage is high, consider increasing Redis memory or cleaning up old keys:
   ```bash
   # Manual cleanup of old webhook statuses
   curl -X POST "http://localhost:8000/webhook-cleanup?older_than_days=30"
   ```

3. If TTLs are not set correctly, check the `WEBHOOK_TTL` configuration in `api.py`.

### Celery Issues

#### Symptoms: Tasks not processing or getting stuck

1. **Check Celery Workers**
   ```bash
   celery -A api.celery_app status
   ```
   Verifies workers are running.

2. **Inspect Active Tasks**
   ```bash
   celery -A api.celery_app inspect active
   ```
   Shows currently running tasks.

3. **Check Scheduled Tasks**
   ```bash
   celery -A api.celery_app inspect scheduled
   ```
   Shows tasks scheduled for retry.

4. **Examine Task Queue**
   ```bash
   redis-cli -n 0 llen compliance-compliance_queue
   ```
   Shows number of tasks in the queue.

#### Resolution Steps

1. If workers are not running, start them:
   ```bash
   celery -A api.celery_app worker --loglevel=info
   ```

2. If tasks are stuck, purge the queue and restart workers:
   ```bash
   celery -A api.celery_app purge
   sudo systemctl restart celery
   ```

3. For long-running tasks, check the task logs:
   ```bash
   tail -f celery.log
   ```

### Circuit Breaker Issues

#### Symptoms: Too many "Circuit is open" errors

1. **Check Circuit Breaker Status**
   ```bash
   curl http://localhost:8000/health
   ```
   Look for circuit breakers in the "open" state.

2. **Monitor Circuit Breaker Metrics**
   ```bash
   curl http://localhost:8000/metrics | grep circuit_breaker
   ```
   Shows current state of all circuit breakers.

#### Resolution Steps

1. If a circuit is stuck open, you can manually reset it by restarting the API service:
   ```bash
   sudo systemctl restart compliance-api
   ```

2. Investigate the underlying service that caused the circuit to open:
   - Check external service logs
   - Verify network connectivity
   - Test the service directly

3. Adjust circuit breaker parameters if needed:
   - Increase failure threshold for flaky but necessary services
   - Decrease reset timeout for faster recovery

### Webhook Delivery Issues

#### Symptoms: Webhooks failing to deliver

1. **Check Webhook Statuses**
   ```bash
   curl http://localhost:8000/webhook-statuses
   ```
   Lists all webhook statuses with pagination.

2. **Examine Failed Webhooks**
   ```bash
   curl "http://localhost:8000/webhook-statuses?status=failed"
   ```
   Shows only failed webhooks.

3. **Check Dead Letter Queue**
   ```bash
   redis-cli keys "dead_letter:webhook:*" | wc -l
   ```
   Counts entries in the dead letter queue.

#### Resolution Steps

1. For validation errors (invalid URLs), fix the webhook URL in the client application.

2. For connection errors, verify the webhook endpoint is accessible:
   ```bash
   curl -v {webhook_url}
   ```

3. For server errors (5xx), check with the webhook provider.

4. To retry a failed webhook manually, you would need to resubmit the original request.

## Load Handling

The system is designed to handle high loads through several mechanisms:

1. **Asynchronous Processing**
   - All webhook deliveries are processed asynchronously via Celery
   - API endpoints return immediately, queuing the actual work

2. **Concurrency Control**
   - Celery is configured to process 4 tasks concurrently
   - Redis connections are pooled for efficient resource usage

3. **Backpressure Mechanisms**
   - Circuit breakers prevent overwhelming failing services
   - Exponential backoff with jitter spreads out retries

4. **Resource Management**
   - TTL-based cleanup prevents Redis memory exhaustion
   - Task timeouts prevent worker starvation

## Monitoring Recommendations

1. **Set Up Prometheus + Grafana**
   - Scrape metrics from port 8000
   - Create dashboards for:
     - Webhook delivery success/failure rates
     - Task processing times
     - Circuit breaker states
     - Redis memory usage

2. **Configure Alerts**
   - Critical:
     - Redis connection failures
     - Circuit breaker open state
     - Health check failures
     - Dead letter queue growth
   - Warning:
     - High task retry rates
     - Webhook delivery failures
     - Circuit breaker half-open state

3. **Log Aggregation**
   - Collect logs from API, Celery, and Redis
   - Search for correlation IDs to trace requests
   - Set up alerts for error spikes

## Maintenance Tasks

1. **Regular Cleanup**
   - Run webhook cleanup periodically:
     ```bash
     # Clean up delivered webhooks older than 1 day
     curl -X POST "http://localhost:8000/webhook-cleanup?status=delivered&older_than_days=1"
     ```

2. **Redis Maintenance**
   - Monitor Redis memory usage
   - Consider Redis persistence options (RDB/AOF)
   - Set up Redis replication for high availability

3. **Celery Worker Management**
   - Monitor worker health
   - Restart workers periodically to prevent memory leaks
   - Scale workers based on queue length

## Emergency Procedures

### System Overload

If the system is overloaded (high CPU, memory usage, or queue length):

1. Temporarily increase Celery workers:
   ```bash
   celery -A api.celery_app worker --concurrency=8 --loglevel=info
   ```

2. Throttle incoming requests at the load balancer level.

3. Clean up Redis to free memory:
   ```bash
   curl -X POST "http://localhost:8000/webhook-cleanup?status=delivered"
   ```

### Complete System Failure

In case of complete system failure:

1. Restart core services:
   ```bash
   sudo systemctl restart redis
   sudo systemctl restart compliance-api
   sudo systemctl restart celery
   ```

2. Check system health:
   ```bash
   curl http://localhost:8000/health
   ```

3. Verify data integrity:
   ```bash
   redis-cli keys "webhook_status:*" | wc -l
   ```

4. Process dead letter queue items if needed.

## Conclusion

The stability enhancements implemented in the compliance reporting system provide robust mechanisms for handling high loads, recovering from failures, and maintaining data consistency. By following this troubleshooting guide, DevOps teams can effectively monitor, maintain, and troubleshoot the system to ensure reliable operation.