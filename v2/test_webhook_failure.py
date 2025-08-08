#!/usr/bin/env python3
"""
Test script for webhook reliability implementation.

This script tests the webhook functionality by sending requests to the API
with and without a webhook URL. It requires the webhook_receiver_server.py
to be running in a separate terminal.

Usage:
    1. Start the webhook receiver server in a separate terminal:
       python webhook_receiver_server.py [--failure-rate RATE] [--delay SECONDS]
    
    2. Run this test script:
       python test_webhook_failure.py
"""

import requests
import json
import logging
import sys
import time
import argparse
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_webhook_delivery(api_url="http://localhost:8000", failure_scenario=None):
    """
    Test the webhook functionality with the new reliability implementation.
    
    Args:
        api_url: Base URL of the API
        failure_scenario: Type of failure to test (None, '4xx', '5xx', 'timeout', 'invalid_url')
    """
    # API endpoint
    url = f"{api_url}/process-claim-basic"
    
    # Base payload
    payload = {
        "reference_id": f"TEST-{int(time.time())}",
        "employee_number": "EN-TEST",
        "first_name": "Test",
        "last_name": "User",
        "crd_number": "12345",
        "organization_crd": "67890",
        "organization_name": "Test Organization"
    }
    
    # Configure webhook URL based on failure scenario
    if failure_scenario == '4xx':
        # 4xx error - client error
        payload["webhook_url"] = "http://localhost:9001/nonexistent-endpoint"
    elif failure_scenario == '5xx':
        # 5xx error - server error (using failure rate on webhook receiver)
        payload["webhook_url"] = "http://localhost:9001/webhook-receiver"
        # Note: Set --failure-rate 100 on webhook_receiver_server.py
    elif failure_scenario == 'timeout':
        # Timeout error (using delay on webhook receiver)
        payload["webhook_url"] = "http://localhost:9001/webhook-receiver"
        # Note: Set --delay 60 on webhook_receiver_server.py
    elif failure_scenario == 'invalid_url':
        # Invalid URL format
        payload["webhook_url"] = "invalid-url"
    else:
        # Normal case - should succeed
        payload["webhook_url"] = "http://localhost:9001/webhook-receiver"
    
    # Headers
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        # Send the request
        logger.info(f"Sending request to {url} with payload: {json.dumps(payload, indent=2)}")
        response = requests.post(url, json=payload, headers=headers)
        
        # Log the response
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response body: {response.text}")
        
        # Extract task_id from response for status checking
        try:
            response_data = response.json()
            task_id = response_data.get("task_id")
            reference_id = response_data.get("reference_id")
            
            if task_id and reference_id:
                logger.info(f"Task ID: {task_id}")
                logger.info(f"Reference ID: {reference_id}")
                
                # Check webhook status
                time.sleep(2)  # Wait a bit for the task to start
                monitor_webhook_status(api_url, reference_id, task_id, failure_scenario)
            else:
                logger.warning("No task_id or reference_id in response")
        except Exception as e:
            logger.error(f"Error parsing response: {str(e)}")
        
        return response
    except Exception as e:
        logger.error(f"Error sending request: {str(e)}")
        return None

def test_without_webhook(api_url="http://localhost:8000"):
    """Test the API without webhook callback."""
    # API endpoint
    url = f"{api_url}/process-claim-basic"
    
    # Payload without webhook_url
    payload = {
        "reference_id": f"TEST-NOWH-{int(time.time())}",
        "employee_number": "EN-TEST",
        "first_name": "Test",
        "last_name": "User",
        "crd_number": "12345",
        "organization_crd": "67890",
        "organization_name": "Test Organization"
        # No webhook_url means synchronous processing
    }
    
    # Headers
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        # Send the request
        logger.info(f"Testing without webhook - Sending request to {url}")
        response = requests.post(url, json=payload, headers=headers)
        
        # Log the response
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response body: {response.text[:500]}...")  # Log first 500 chars
        
        return response
    except Exception as e:
        logger.error(f"Error sending request: {str(e)}")
        return None

def monitor_webhook_status(api_url, reference_id, task_id, failure_scenario, timeout=60):
    """
    Monitor the status of a webhook delivery.
    
    Args:
        api_url: Base URL of the API
        reference_id: Reference ID of the webhook
        task_id: Task ID of the webhook
        failure_scenario: Type of failure being tested
        timeout: Maximum time to wait for webhook completion (seconds)
    """
    webhook_id = f"{reference_id}_{task_id}"
    url = f"{api_url}/webhook-status/{webhook_id}"
    
    logger.info(f"Monitoring webhook status at {url}")
    logger.info(f"Failure scenario: {failure_scenario}")
    logger.info(f"Timeout: {timeout} seconds")
    
    # Wait for webhook to be processed
    start_time = time.time()
    last_status = None
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url)
            
            if response.status_code == 200:
                status_data = response.json()
                current_status = status_data.get("status")
                
                # Only log if status has changed
                if current_status != last_status:
                    logger.info(f"Webhook status: {json.dumps(status_data, indent=2)}")
                    last_status = current_status
                
                # Check if webhook has completed
                if current_status in ["delivered", "failed"]:
                    logger.info(f"Webhook delivery completed with status: {current_status}")
                    
                    # Validate the outcome based on the failure scenario
                    validate_outcome(status_data, failure_scenario)
                    
                    return status_data
            else:
                logger.warning(f"Failed to get webhook status: {response.status_code} - {response.text}")
            
            # Wait before checking again
            time.sleep(5)
        except Exception as e:
            logger.error(f"Error checking webhook status: {str(e)}")
            time.sleep(5)
    
    logger.warning(f"Timed out waiting for webhook completion after {timeout} seconds")
    
    # Final status check
    try:
        response = requests.get(url)
        if response.status_code == 200:
            final_status = response.json()
            logger.info(f"Final webhook status: {json.dumps(final_status, indent=2)}")
            validate_outcome(final_status, failure_scenario)
            return final_status
    except Exception as e:
        logger.error(f"Error checking final webhook status: {str(e)}")
    
    return None

def validate_outcome(status_data, failure_scenario):
    """
    Validate that the webhook outcome matches the expected result for the failure scenario.
    
    Args:
        status_data: Webhook status data
        failure_scenario: Type of failure being tested
    """
    status = status_data.get("status")
    error_type = status_data.get("error_type")
    
    if failure_scenario == '4xx':
        if status == "failed" and error_type in ["permanent_client_error", "client_error"]:
            logger.info("✅ PASS: 4xx error correctly handled as client error")
        else:
            logger.warning(f"❌ FAIL: 4xx error not handled correctly. Status: {status}, Error type: {error_type}")
    
    elif failure_scenario == '5xx':
        if status == "failed" and error_type in ["max_retries_exceeded", "server_error"]:
            logger.info("✅ PASS: 5xx error correctly handled with retries and eventual failure")
        else:
            logger.warning(f"❌ FAIL: 5xx error not handled correctly. Status: {status}, Error type: {error_type}")
    
    elif failure_scenario == 'timeout':
        if status == "failed" and error_type in ["timeout", "network_error"]:
            logger.info("✅ PASS: Timeout correctly handled as network error")
        else:
            logger.warning(f"❌ FAIL: Timeout not handled correctly. Status: {status}, Error type: {error_type}")
    
    elif failure_scenario == 'invalid_url':
        if status == "failed" and error_type == "validation_error":
            logger.info("✅ PASS: Invalid URL correctly rejected during validation")
        else:
            logger.warning(f"❌ FAIL: Invalid URL not rejected correctly. Status: {status}, Error type: {error_type}")
    
    elif failure_scenario is None:
        if status == "delivered":
            logger.info("✅ PASS: Webhook delivered successfully")
        else:
            logger.warning(f"❌ FAIL: Webhook delivery failed unexpectedly. Status: {status}, Error type: {error_type}")

def check_webhook_receiver_running():
    """Check if the webhook receiver server is running."""
    try:
        response = requests.get("http://localhost:9001/status", timeout=1)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Test webhook reliability implementation')
    parser.add_argument('--api-url', default='http://localhost:8000',
                        help='Base URL of the API (default: http://localhost:8000)')
    parser.add_argument('--failure', choices=['4xx', '5xx', 'timeout', 'invalid_url'],
                        help='Failure scenario to test')
    parser.add_argument('--skip-receiver-check', action='store_true',
                        help='Skip checking if webhook receiver is running')
    return parser.parse_args()

def main():
    """Main function to run the tests."""
    args = parse_args()
    
    # Check if webhook receiver is running
    if not args.skip_receiver_check and not check_webhook_receiver_running():
        logger.warning("Webhook receiver server is not running")
        logger.info("Please start the webhook receiver server in a separate terminal:")
        logger.info("python webhook_receiver_server.py")
        
        # Ask the user if they want to continue
        response = input("Do you want to continue anyway? (y/n): ")
        if response.lower() != 'y':
            logger.info("Exiting test script")
            return
    
    try:
        # First test: without webhook
        logger.info("=== TESTING WITHOUT WEBHOOK ===")
        response_no_webhook = test_without_webhook(args.api_url)
        
        if response_no_webhook and response_no_webhook.status_code == 200:
            logger.info("✅ PASS: Request without webhook was successful")
        else:
            logger.error("❌ FAIL: Request without webhook failed")
            return
        
        # Wait a bit before the next test
        time.sleep(5)
        
        # Second test: with webhook
        logger.info("\n=== TESTING WITH WEBHOOK ===")
        if args.failure:
            logger.info(f"Testing failure scenario: {args.failure}")
        else:
            logger.info("Testing normal webhook delivery (no failure)")
        
        response = test_webhook_delivery(args.api_url, args.failure)
        
        if response and response.status_code == 200:
            logger.info("✅ PASS: API request with webhook was successful")
        else:
            logger.error("❌ FAIL: API request with webhook failed")
    
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")

if __name__ == "__main__":
    main()