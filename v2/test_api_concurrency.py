import unittest
import threading
import queue
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
import time
from datetime import datetime
from api import app, PROCESSING_MODES

# Mock task result class
class MockTaskResult:
    def __init__(self, task_id):
        self.id = task_id

class TestConcurrency(unittest.TestCase):
    def setUp(self):
        # Initialize FastAPI test client
        self.client = TestClient(app)
        
        # Mock Celery task
        self.celery_task_patch = patch("api.process_compliance_claim.delay")
        self.mock_celery_task = self.celery_task_patch.start()
        
        # Mock process_claim
        self.process_claim_patch = patch("api.process_claim")
        self.mock_process_claim = self.process_claim_patch.start()
        
        # Mock send_to_webhook
        self.webhook_patch = patch("api.send_to_webhook")
        self.mock_webhook = self.webhook_patch.start()
        
        # Task processing queue and results
        self.task_queue = queue.Queue()
        self.processed_tasks = []
        self.webhook_calls = []
        self.task_timestamps = []
        
        # Set up the mock delay method to simulate task queuing
        def mock_delay_side_effect(request_dict, mode):
            reference_id = request_dict["reference_id"]
            task_id = f"task-{reference_id}"
            
            # Add task to queue for processing
            self.task_queue.put((reference_id, request_dict, mode))
            
            # Return a mock task result
            task_result = MockTaskResult(task_id)
            return task_result
            
        self.mock_celery_task.side_effect = mock_delay_side_effect
        
        # Set up the mock process_claim to simulate task processing
        def mock_process_claim_side_effect(claim, facade, employee_number, skip_disciplinary, skip_arbitration, skip_regulatory):
            # Record timestamp of task execution
            timestamp = datetime.now().timestamp()
            self.task_timestamps.append(timestamp)
            
            # Record the processed task
            self.processed_tasks.append(claim["reference_id"])
            
            return {"status": "success", "reference_id": claim["reference_id"], "data": "mocked_report"}
            
        self.mock_process_claim.side_effect = mock_process_claim_side_effect
        
        # Set up the mock webhook to record webhook calls
        async def mock_send_to_webhook(webhook_url, report, reference_id):
            self.webhook_calls.append((webhook_url, report["reference_id"]))
            
        self.mock_webhook.side_effect = mock_send_to_webhook

    def tearDown(self):
        # Stop patches
        self.celery_task_patch.stop()
        self.process_claim_patch.stop()
        self.webhook_patch.stop()
        
    def process_tasks(self):
        """Process all tasks in the queue sequentially"""
        while not self.task_queue.empty():
            # Get the next task from the queue
            reference_id, request_dict, mode = self.task_queue.get()
            
            # Simulate 2-second processing time
            time.sleep(2)
            
            # Process the task
            employee_number = request_dict.get("employee_number")
            webhook_url = request_dict.get("webhook_url")
            
            # Call the mocked process_claim
            claim = request_dict.copy()
            if "webhook_url" in claim:
                del claim["webhook_url"]
            if "employee_number" in claim:
                del claim["employee_number"]
                
            mode_settings = PROCESSING_MODES[mode]
            
            result = self.mock_process_claim(
                claim=claim,
                facade=None,
                employee_number=employee_number,
                skip_disciplinary=mode_settings["skip_disciplinary"],
                skip_arbitration=mode_settings["skip_arbitration"],
                skip_regulatory=mode_settings["skip_regulatory"]
            )

    def test_concurrency_behavior(self):
        """Test the complete concurrency behavior of the API for asynchronous requests"""
        # Submit 3 claim processing requests with webhook_url
        requests = [
            {
                "reference_id": f"REF{i}",
                "employee_number": f"EN-01631{i}",
                "first_name": "John",
                "last_name": "Doe",
                "crd_number": f"123456{i}",
                "webhook_url": f"https://webhook.site/test-{i}"
            } for i in range(1, 4)
        ]

        # 1. Test API Responsiveness
        # Send requests and measure response time
        responses = []
        response_times = []
        
        for req in requests:
            start_time = time.time()
            response = self.client.post("/process-claim-basic", json=req)
            end_time = time.time()
            
            responses.append(response)
            response_times.append(end_time - start_time)

        # Verify immediate responses (all should be quick)
        for i, response in enumerate(responses, 1):
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["status"], "processing_queued")
            self.assertEqual(data["reference_id"], f"REF{i}")
            self.assertIn("task_id", data)
            
        # All responses should be fast (under 0.5 seconds)
        for i, response_time in enumerate(response_times, 1):
            self.assertLess(
                response_time, 0.5,
                f"Response {i} took {response_time:.2f}s, which is too slow for an async request"
            )
            
        # Verify that tasks were queued in the correct order
        self.assertEqual(self.task_queue.qsize(), 3, "Expected 3 tasks in the queue")
        
        # 2. Test FIFO Processing
        # Process all tasks in the queue
        self.process_tasks()
        
        # Verify tasks were processed in FIFO order
        self.assertEqual(
            self.processed_tasks,
            ["REF1", "REF2", "REF3"],
            "Tasks should be processed in FIFO order"
        )
        
        # 3. Test Sequential Processing (no overlap)
        # Verify timestamps show sequential processing
        self.assertEqual(len(self.task_timestamps), 3, "Expected 3 task timestamps")
        for i in range(1, len(self.task_timestamps)):
            time_diff = self.task_timestamps[i] - self.task_timestamps[i-1]
            self.assertGreaterEqual(
                time_diff, 2.0,
                f"Task {i+1} started only {time_diff:.2f}s after task {i}, violating sequential processing"
            )
            
    def test_synchronous_processing(self):
        """Test that synchronous requests (no webhook_url) are processed immediately"""
        # Create a request without webhook_url
        request = {
            "reference_id": "REF_SYNC",
            "employee_number": "EN-016314",
            "first_name": "John",
            "last_name": "Doe",
            "crd_number": "1234567"
        }
        
        # Set up the mock process_claim to return a result
        self.mock_process_claim.return_value = {
            "status": "success",
            "reference_id": "REF_SYNC",
            "data": "mocked_report"
        }
        
        # Send the request and measure response time
        start_time = time.time()
        response = self.client.post("/process-claim-basic", json=request)
        end_time = time.time()
        response_time = end_time - start_time
        
        # Verify the response
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "success")
        self.assertEqual(data["reference_id"], "REF_SYNC")
        
        # Verify that process_claim was called directly (not queued)
        self.mock_process_claim.assert_called_once()
        
        # Verify that no task was queued
        self.assertEqual(self.task_queue.qsize(), 0, "No tasks should be queued for synchronous requests")
        
        # Verify that no webhook calls were made
        self.assertEqual(len(self.webhook_calls), 0, "No webhook calls should be made for synchronous requests")

if __name__ == "__main__":
    unittest.main()