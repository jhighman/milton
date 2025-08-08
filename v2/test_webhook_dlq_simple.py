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
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_webhook_dlq():
    """Test the webhook DLQ mechanism."""
    # API endpoint for testing webhooks
    url = "http://localhost:8000/test-webhook"
    
    # Test payload
    test_payload = {
        "test_payload": {
            "key": "value"
        }
    }
    
    # Webhook URL (should return 500 error)
    webhook_url = "http://localhost:9001/webhook-receiver"
    
    # Send the request
    logger.info(f"Sending test webhook request to {url}")
    response = requests.post(
        url,
        params={"webhook_url": webhook_url},
        json=test_payload
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
            redis_client = redis.Redis(host="localhost", port=6379, db=2, decode_responses=True)
            
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
            logger.warning("Could not extract reference_id or task_id from response")
            return False
    except Exception as e:
        logger.error(f"Error checking webhook status: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Starting webhook DLQ test...")
    result = test_webhook_dlq()
    
    if result:
        logger.info("Test passed: Webhook was moved to DLQ after max retries")
        sys.exit(0)
    else:
        logger.error("Test failed: Webhook was not moved to DLQ after max retries")
        sys.exit(1)