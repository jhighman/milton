"""
Unit tests for asynchronous claim processing functionality.

These tests verify that:
1. Synchronous processing works as before
2. Asynchronous processing returns immediate responses
3. Background tasks are properly scheduled
4. The process_claim_async function correctly handles processing and webhook delivery
5. Error handling works in both synchronous and asynchronous modes

Run with:
    pytest test_api_async.py -v
"""

import pytest
from fastapi.testclient import TestClient
from fastapi import BackgroundTasks
from unittest.mock import AsyncMock, patch, MagicMock

from api import app, process_claim_helper, process_claim_async

client = TestClient(app)

# Test synchronous processing (no webhook URL)
def test_process_claim_basic_sync():
    """Test that synchronous processing works as before."""
    with patch("api.process_claim_helper", return_value={"status": "success"}) as mock_helper:
        response = client.post(
            "/process-claim-basic",
            json={
                "reference_id": "TEST-001",
                "employee_number": "EN-12345",
                "first_name": "John",
                "last_name": "Doe",
                "crd_number": "123456",
                "organization_name": "Test Firm"
            }
        )
        assert response.status_code == 200
        assert response.json() == {"status": "success"}
        mock_helper.assert_called_once()
        # Verify it was called with send_webhook=True (default)
        args, kwargs = mock_helper.call_args
        assert kwargs.get("send_webhook", True) is True

# Test asynchronous processing (with webhook URL)
def test_process_claim_basic_async():
    """Test that asynchronous processing returns immediate responses."""
    with patch("api.BackgroundTasks.add_task") as mock_add_task:
        response = client.post(
            "/process-claim-basic",
            json={
                "reference_id": "TEST-002",
                "employee_number": "EN-12345",
                "first_name": "Jane",
                "last_name": "Doe",
                "crd_number": "654321",
                "organization_name": "Test Firm",
                "webhook_url": "http://example.com/webhook"
            }
        )
        assert response.status_code == 200
        assert response.json() == {
            "status": "processing_started",
            "reference_id": "TEST-002",
            "message": "Claim processing started; result will be sent to webhook"
        }
        mock_add_task.assert_called_once()
        # Verify process_claim_async was added to background tasks
        args, kwargs = mock_add_task.call_args
        assert args[0] == process_claim_async

# Test extended mode with async processing
def test_process_claim_extended_async():
    """Test that extended mode also supports asynchronous processing."""
    with patch("api.BackgroundTasks.add_task") as mock_add_task:
        response = client.post(
            "/process-claim-extended",
            json={
                "reference_id": "TEST-003",
                "employee_number": "EN-12345",
                "first_name": "Jane",
                "last_name": "Doe",
                "crd_number": "654321",
                "organization_name": "Test Firm",
                "webhook_url": "http://example.com/webhook"
            }
        )
        assert response.status_code == 200
        assert response.json() == {
            "status": "processing_started",
            "reference_id": "TEST-003",
            "message": "Claim processing started; result will be sent to webhook"
        }
        mock_add_task.assert_called_once()
        # Verify it was called with mode="extended"
        args, kwargs = mock_add_task.call_args
        assert kwargs.get("mode") == "extended"

# Test complete mode with async processing
def test_process_claim_complete_async():
    """Test that complete mode also supports asynchronous processing."""
    with patch("api.BackgroundTasks.add_task") as mock_add_task:
        response = client.post(
            "/process-claim-complete",
            json={
                "reference_id": "TEST-004",
                "employee_number": "EN-12345",
                "first_name": "Jane",
                "last_name": "Doe",
                "crd_number": "654321",
                "organization_name": "Test Firm",
                "webhook_url": "http://example.com/webhook"
            }
        )
        assert response.status_code == 200
        assert response.json() == {
            "status": "processing_started",
            "reference_id": "TEST-004",
            "message": "Claim processing started; result will be sent to webhook"
        }
        mock_add_task.assert_called_once()
        # Verify it was called with mode="complete"
        args, kwargs = mock_add_task.call_args
        assert kwargs.get("mode") == "complete"

# Test process_claim_async function
@pytest.mark.asyncio
async def test_process_claim_async():
    """Test that process_claim_async correctly processes claims and sends to webhook."""
    # Mock the request object
    mock_request = MagicMock()
    mock_request.reference_id = "TEST-005"
    mock_request.webhook_url = "http://example.com/webhook"
    
    # Mock the helper function
    mock_helper = AsyncMock(return_value={"status": "success", "reference_id": "TEST-005"})
    
    # Mock the send_to_webhook function
    mock_send = AsyncMock()
    
    with patch("api.process_claim_helper", mock_helper), \
         patch("api.send_to_webhook", mock_send):
        await process_claim_async(mock_request, "basic")
        
        # Verify helper was called with send_webhook=False
        mock_helper.assert_called_once_with(mock_request, "basic", send_webhook=False)
        
        # Verify webhook was called with the result
        mock_send.assert_called_once_with(
            "http://example.com/webhook", 
            {"status": "success", "reference_id": "TEST-005"}, 
            "TEST-005"
        )

# Test process_claim_async error handling
@pytest.mark.asyncio
async def test_process_claim_async_error():
    """Test that process_claim_async correctly handles errors and sends them to webhook."""
    # Mock the request object
    mock_request = MagicMock()
    mock_request.reference_id = "TEST-006"
    mock_request.webhook_url = "http://example.com/webhook"
    
    # Mock the helper function to raise an exception
    mock_helper = AsyncMock(side_effect=Exception("Test error"))
    
    # Mock the send_to_webhook function
    mock_send = AsyncMock()
    
    with patch("api.process_claim_helper", mock_helper), \
         patch("api.send_to_webhook", mock_send):
        await process_claim_async(mock_request, "basic")
        
        # Verify helper was called
        mock_helper.assert_called_once()
        
        # Verify error was sent to webhook
        mock_send.assert_called_once()
        args, kwargs = mock_send.call_args
        assert args[0] == "http://example.com/webhook"
        assert args[2] == "TEST-006"
        assert "error" in args[1]["status"]
        assert "Test error" in args[1]["message"]

if __name__ == "__main__":
    pytest.main(["-v"])