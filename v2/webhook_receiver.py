"""
Simple webhook receiver for testing asynchronous claim processing.

This script starts a FastAPI server that listens for webhook callbacks
from the claim processing API. It logs all received webhooks to the console
and keeps track of them in memory for verification.

Usage:
    python webhook_receiver.py

The server will start on http://localhost:8080/webhook
"""

from fastapi import FastAPI, Request
import uvicorn
import json
from datetime import datetime
from typing import List, Dict, Any

app = FastAPI(title="Webhook Receiver")

# Store received webhooks in memory for verification
received_webhooks: List[Dict[str, Any]] = []

@app.post("/webhook")
async def webhook_handler(request: Request):
    """Handle incoming webhook callbacks and log them."""
    data = await request.json()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Store the webhook data
    received_webhooks.append(data)
    
    # Log the webhook
    print(f"\n[{timestamp}] Webhook #{len(received_webhooks)} received:")
    print(json.dumps(data, indent=2))
    
    return {"status": "received", "timestamp": timestamp}

@app.get("/webhooks")
async def list_webhooks():
    """Return all received webhooks."""
    return {"count": len(received_webhooks), "webhooks": received_webhooks}

@app.delete("/webhooks")
async def clear_webhooks():
    """Clear all received webhooks."""
    received_webhooks.clear()
    return {"status": "cleared", "count": 0}

if __name__ == "__main__":
    print("Starting webhook receiver on http://localhost:8080/webhook")
    print("Use http://localhost:8080/webhooks to view all received webhooks")
    print("Use http://localhost:8080/webhooks with DELETE method to clear webhooks")
    uvicorn.run(app, host="0.0.0.0", port=8080)