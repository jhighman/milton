import unittest
import requests
import time
import json
import logging
import uuid
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("api_integration_test")

# API configuration
API_BASE_URL = "http://localhost:8000"  # Update with your actual API URL
WEBHOOK_BASE_URL = "https://webhook.site/"  # Using webhook.site for testing

class APIIntegrationTest(unittest.TestCase):
    """Integration test for the API to verify end-to-end processing with webhooks."""
    
    def setUp(self):
        """Set up test data and webhook URLs."""
        # Generate unique webhook IDs for each test
        self.webhook_ids = [str(uuid.uuid4()) for _ in range(3)]
        
        # Test data for the three requests
        self.test_requests = [
            {
                "reference_id": "Integration-Test-1",
                "first_name": "James",
                "last_name": "Betzig",
                "employee_number": "IT-EN-042361",
                "crdNumber": "2457078",
                "requestType": "ENROLL",
                "packageName": "BROKERCHECK",
                "crd_number": "2457078",
                "individual_name": "James Betzig",
                "organization_crd": None,
                "webhook_url": f"{WEBHOOK_BASE_URL}{self.webhook_ids[0]}"
            },
            {
                "reference_id": "Integration-Test-2",
                "first_name": "Adrian",
                "last_name": "Larson",
                "employee_number": "IT-EN-044069",
                "crdNumber": "3098721",
                "requestType": "ENROLL",
                "packageName": "BROKERCHECK",
                "crd_number": "3098721",
                "individual_name": "Adrian Larson",
                "organization_crd": None,
                "webhook_url": f"{WEBHOOK_BASE_URL}{self.webhook_ids[1]}"
            },
            {
                "reference_id": "Integration-Test-3",
                "first_name": "Mark",
                "last_name": "Copeland",
                "employee_number": "IT-EN-046309",
                "zip": "92705",
                "crdNumber": "1844301",
                "requestType": "ENROLL",
                "packageName": "BROKERCHECK",
                "crd_number": "1844301",
                "individual_name": "Mark Copeland",
                "organization_crd": None,
                "webhook_url": f"{WEBHOOK_BASE_URL}{self.webhook_ids[2]}"
            }
        ]
        
        # Ensure API is running
        try:
            response = requests.get(f"{API_BASE_URL}/settings")
            if response.status_code != 200:
                logger.error(f"API is not responding correctly. Status code: {response.status_code}")
                self.skipTest("API is not available")
        except requests.RequestException as e:
            logger.error(f"Failed to connect to API: {e}")
            self.skipTest("API is not available")
    
    def poll_task_status(self, task_id, reference_id, max_attempts=30, poll_interval=2):
        """
        Poll the task status endpoint until the task is complete or max attempts reached.
        
        Args:
            task_id (str): The task ID to poll
            reference_id (str): The reference ID for logging
            max_attempts (int): Maximum number of polling attempts
            poll_interval (int): Time in seconds between polling attempts
            
        Returns:
            tuple: (success, result, attempts, elapsed_time)
        """
        start_time = time.time()
        attempts = 0
        
        for attempt in range(1, max_attempts + 1):
            attempts = attempt
            try:
                response = requests.get(f"{API_BASE_URL}/task-status/{task_id}")
                if response.status_code != 200:
                    logger.warning(f"Failed to get task status for {reference_id}. Status code: {response.status_code}")
                    time.sleep(poll_interval)
                    continue
                
                status_data = response.json()
                status = status_data.get("status")
                
                logger.info(f"Poll attempt {attempt} for {reference_id}: Status = {status}")
                
                if status == "COMPLETED":
                    elapsed_time = time.time() - start_time
                    logger.info(f"Task {reference_id} completed after {attempts} attempts ({elapsed_time:.2f} seconds)")
                    return True, status_data.get("result"), attempts, elapsed_time
                
                if status == "FAILED":
                    elapsed_time = time.time() - start_time
                    logger.error(f"Task {reference_id} failed after {attempts} attempts ({elapsed_time:.2f} seconds)")
                    return False, status_data.get("error"), attempts, elapsed_time
                
                # Still processing, wait and try again
                time.sleep(poll_interval)
                
            except requests.RequestException as e:
                logger.error(f"Error polling task status for {reference_id}: {e}")
                time.sleep(poll_interval)
        
        # Max attempts reached
        elapsed_time = time.time() - start_time
        logger.warning(f"Max polling attempts ({max_attempts}) reached for {reference_id} after {elapsed_time:.2f} seconds")
        return False, "Timeout waiting for task completion", attempts, elapsed_time
    
    def test_sequential_requests_with_polling(self):
        """Test sending three requests in succession and polling for completion."""
        results = []
        
        for i, request_data in enumerate(self.test_requests, 1):
            reference_id = request_data["reference_id"]
            logger.info(f"Sending request {i}: {reference_id}")
            
            # Send the request
            start_time = time.time()
            response = requests.post(f"{API_BASE_URL}/process-claim-basic", json=request_data)
            request_time = time.time() - start_time
            
            self.assertEqual(response.status_code, 200, f"Request {i} failed with status code {response.status_code}")
            
            response_data = response.json()
            logger.info(f"Request {i} response: {json.dumps(response_data, indent=2)}")
            
            # Verify the response contains expected fields
            self.assertIn("task_id", response_data, f"Response for request {i} missing task_id")
            self.assertEqual(response_data["reference_id"], reference_id, f"Reference ID mismatch for request {i}")
            self.assertEqual(response_data["status"], "processing_queued", f"Unexpected status for request {i}")
            
            # Poll for task completion
            task_id = response_data["task_id"]
            success, result, attempts, elapsed_time = self.poll_task_status(task_id, reference_id)
            
            # Store results for reporting
            results.append({
                "reference_id": reference_id,
                "request_time": request_time,
                "polling_attempts": attempts,
                "total_time": elapsed_time,
                "success": success
            })
            
            # Verify task completed successfully
            self.assertTrue(success, f"Task {reference_id} did not complete successfully")
            
            # Optional: add a small delay between requests to ensure they're processed in order
            if i < len(self.test_requests):
                time.sleep(1)
        
        # Print summary of results
        logger.info("\n" + "="*50)
        logger.info("INTEGRATION TEST RESULTS SUMMARY")
        logger.info("="*50)
        for result in results:
            logger.info(f"Reference ID: {result['reference_id']}")
            logger.info(f"  Request Time: {result['request_time']:.2f} seconds")
            logger.info(f"  Polling Attempts: {result['polling_attempts']}")
            logger.info(f"  Total Processing Time: {result['total_time']:.2f} seconds")
            logger.info(f"  Success: {result['success']}")
            logger.info("-"*50)
        
        # Verify all tasks completed successfully
        all_successful = all(result["success"] for result in results)
        self.assertTrue(all_successful, "Not all tasks completed successfully")

if __name__ == "__main__":
    unittest.main()