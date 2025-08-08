#!/usr/bin/env python3
"""
Test script for webhook DLQ mechanism.

This script tests the webhook DLQ mechanism by sending a test webhook request
and checking if it's moved to the DLQ after max retries.
"""

import requests
import json
import time
import redis
import sys
import logging
import argparse
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_webhook_dlq(api_url="http://localhost:8000", redis_host="localhost", redis_port=6379, redis_db=2):
    """
    Test the webhook DLQ mechanism.
    
    Args:
        api_url: Base URL of the API
        redis_host: Redis host
        redis_port: Redis port
        redis_db: Redis DB for webhook status tracking
    
    Returns:
        bool: True if test passed, False otherwise
    """
    # API endpoint for testing webhooks
    url = f"{api_url}/test-webhook"
    
    # Test payload
    test_payload = {
        "test_payload": {
            "key": "value"
        }
    }
    
    # Webhook URL (should return 500 error)
    webhook_url = "http://localhost:9001/webhook-receiver"
    
    # Headers
    headers = {
        "Content-Type": "application/json"
    }
    
    # Send the request
    logger.info(f"Sending test webhook request to {url}")
    logger.info(f"Test payload: {json.dumps(test_payload, indent=2)}")
    
    try:
        response = requests.post(
            url,
            params={"webhook_url": webhook_url},
            json=test_payload,
            headers=headers
        )
        
        # Log the response
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response body: {response.text}")
        
        # Extract reference_id and task_id from response
        try:
            response_data = response.json()
            reference_id = response_data.get("reference_id")
            task_id = response_data.get("task_id")
            
            if reference_id and task_id:
                logger.info(f"Reference ID: {reference_id}")
                logger.info(f"Task ID: {task_id}")
                
                # Wait for webhook to be processed and retried (up to 60 seconds)
                logger.info("Waiting for webhook to be processed and retried (timeout: 60 seconds)...")
                
                # Connect to Redis
                redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
                
                # Wait in smaller increments and check status
                webhook_id = f"{reference_id}_{task_id}"
                for i in range(12):  # 12 x 5 seconds = 60 seconds
                    time.sleep(5)
                    
                    # Check webhook status
                    status_key = f"webhook_status:{webhook_id}"
                    status_data_raw = redis_client.get(status_key)
                    
                    if status_data_raw:
                        status_data = json.loads(status_data_raw)
                        status = status_data.get("status")
                        attempts = status_data.get("attempts", 0)
                        logger.info(f"Webhook status: {status}, Attempts: {attempts}")
                        
                        if status == "failed":
                            logger.info("Webhook has failed. Checking DLQ...")
                            break
                        elif attempts >= 3:
                            logger.info(f"Webhook has reached max retries ({attempts}). Checking DLQ...")
                            break
                    else:
                        logger.info(f"No webhook status found for {webhook_id}")
                
                # Check if webhook is in DLQ
                dlq_key = f"dead_letter:webhook:{webhook_id}"
                dlq_data_raw = redis_client.get(dlq_key)
                
                if dlq_data_raw:
                    dlq_data = json.loads(dlq_data_raw)
                    logger.info(f"Webhook found in DLQ: {json.dumps(dlq_data, indent=2)}")
                    logger.info("DLQ mechanism is working correctly!")
                    
                    # Verify DLQ data structure
                    required_fields = ["webhook_id", "reference_id", "webhook_url", "payload", 
                                      "error", "error_type", "attempts", "last_attempt", "correlation_id"]
                    
                    missing_fields = [field for field in required_fields if field not in dlq_data]
                    if missing_fields:
                        logger.warning(f"DLQ entry is missing required fields: {missing_fields}")
                        return False
                    
                    # Check if webhook is in DLQ index
                    dlq_index = "dead_letter:webhook:index"
                    dlq_index_data = redis_client.smembers(dlq_index)
                    
                    if webhook_id in dlq_index_data:
                        logger.info(f"Webhook ID {webhook_id} found in DLQ index")
                    else:
                        logger.warning(f"Webhook ID {webhook_id} not found in DLQ index")
                        return False
                    
                    return True
                else:
                    logger.warning(f"Webhook not found in DLQ: {dlq_key}")
                    
                    # Check if webhook is in DLQ index
                    dlq_index = "dead_letter:webhook:index"
                    dlq_index_data = redis_client.smembers(dlq_index)
                    
                    if dlq_index_data:
                        # Convert to list for logging
                        dlq_index_list = list(dlq_index_data)
                        logger.info(f"DLQ index contains: {dlq_index_list}")
                        if webhook_id in dlq_index_list:
                            logger.info(f"Webhook ID {webhook_id} found in DLQ index but not in DLQ")
                    else:
                        logger.warning("DLQ index is empty")
                    
                    return False
            else:
                logger.warning("Could not extract task_id from response")
                return False
        except Exception as e:
            logger.error(f"Error checking webhook status: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"Error sending request: {str(e)}")
        return False

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Test webhook DLQ mechanism')
    parser.add_argument('--api-url', default='http://localhost:8000',
                        help='Base URL of the API (default: http://localhost:8000)')
    parser.add_argument('--redis-host', default='localhost',
                        help='Redis host (default: localhost)')
    parser.add_argument('--redis-port', type=int, default=6379,
                        help='Redis port (default: 6379)')
    parser.add_argument('--redis-db', type=int, default=2,
                        help='Redis DB for webhook status tracking (default: 2)')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    logger.info("Starting webhook DLQ test...")
    logger.info(f"API URL: {args.api_url}")
    logger.info(f"Redis: {args.redis_host}:{args.redis_port} DB {args.redis_db}")
    
    # Make sure the webhook receiver is configured to return 500 errors
    logger.info("Ensuring webhook receiver is configured to return 500 errors...")
    try:
        requests.get("http://localhost:9001/status")
        logger.info("Webhook receiver is running. Make sure it's configured with --failure-rate 100")
    except requests.exceptions.ConnectionError:
        logger.error("Webhook receiver is not running. Please start it with:")
        logger.error("python webhook_receiver_server.py --failure-rate 100")
        sys.exit(1)
    
    result = test_webhook_dlq(
        api_url=args.api_url,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db
    )
    
    if result:
        logger.info("✅ TEST PASSED: Webhook was moved to DLQ after max retries")
        sys.exit(0)
    else:
        logger.error("❌ TEST FAILED: Webhook was not moved to DLQ after max retries")
        sys.exit(1)