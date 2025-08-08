#!/usr/bin/env python3
"""
Webhook Receiver Server for testing webhook delivery.

This server listens for webhook notifications and can be configured to simulate
different failure scenarios to test the webhook delivery system's reliability.

Usage:
    python webhook_receiver_server.py [--port PORT] [--failure-rate RATE] [--delay SECONDS]

Options:
    --port PORT         Port to listen on (default: 9001)
    --failure-rate RATE Percentage of requests that should fail (0-100, default: 0)
    --delay SECONDS     Delay in seconds before responding (default: 0)
"""

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
import threading

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='webhook_receiver.log',
    filemode='a'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

logger = logging.getLogger(__name__)

# Global settings
settings = {
    'failure_rate': 0,  # Percentage of requests that should fail (0-100)
    'delay': 0,         # Delay in seconds before responding
    'received_count': 0,
    'success_count': 0,
    'failure_count': 0,
    'start_time': datetime.now()
}

class WebhookHandler(BaseHTTPRequestHandler):
    """HTTP request handler for webhook receiver."""
    
    def _set_response(self, status_code=200, content_type='application/json'):
        """Set the response headers."""
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        self.end_headers()
    
    def do_GET(self):
        """Handle GET requests."""
        if self.path == '/status':
            # Return server status
            self._set_response()
            status = {
                'status': 'running',
                'uptime': str(datetime.now() - settings['start_time']),
                'received_count': settings['received_count'],
                'success_count': settings['success_count'],
                'failure_count': settings['failure_count'],
                'failure_rate': settings['failure_rate'],
                'delay': settings['delay']
            }
            self.wfile.write(json.dumps(status).encode('utf-8'))
        else:
            # Return 404 for other paths
            self._set_response(404)
            self.wfile.write(json.dumps({'error': 'Not found'}).encode('utf-8'))
    
    def do_POST(self):
        """Handle POST requests."""
        if self.path == '/webhook-receiver':
            # Process webhook
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            
            # Log headers
            headers = dict(self.headers)
            logger.info(f"Received webhook with headers: {json.dumps(headers)}")
            
            # Parse and log the data
            try:
                webhook_data = json.loads(post_data.decode('utf-8'))
                logger.info(f"Received webhook data: {json.dumps(webhook_data)[:500]}...")
                
                # Save to file
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                reference_id = webhook_data.get('reference_id', 'unknown')
                filename = f"webhook_data_{reference_id}_{timestamp}.json"
                
                with open(filename, 'w') as f:
                    json.dump({
                        'headers': headers,
                        'data': webhook_data,
                        'received_at': datetime.now().isoformat()
                    }, f, indent=2)
                
                logger.info(f"Saved webhook data to {filename}")
                
                # Update stats
                settings['received_count'] += 1
                
                # Apply configured delay
                if settings['delay'] > 0:
                    logger.info(f"Delaying response for {settings['delay']} seconds")
                    time.sleep(settings['delay'])
                
                # Determine if this request should fail based on failure rate
                should_fail = random.random() * 100 < settings['failure_rate']
                
                if should_fail:
                    # Simulate a server error
                    settings['failure_count'] += 1
                    logger.info(f"Simulating failure (failure rate: {settings['failure_rate']}%)")
                    self._set_response(500)
                    self.wfile.write(json.dumps({
                        'error': 'Simulated server error',
                        'reference_id': reference_id
                    }).encode('utf-8'))
                else:
                    # Return success
                    settings['success_count'] += 1
                    self._set_response()
                    self.wfile.write(json.dumps({
                        'success': True,
                        'message': 'Webhook received successfully',
                        'reference_id': reference_id
                    }).encode('utf-8'))
                
            except json.JSONDecodeError:
                logger.error(f"Failed to parse webhook data: {post_data.decode('utf-8')}")
                self._set_response(400)
                self.wfile.write(json.dumps({
                    'error': 'Invalid JSON'
                }).encode('utf-8'))
        else:
            # Return 404 for other paths
            self._set_response(404)
            self.wfile.write(json.dumps({'error': 'Not found'}).encode('utf-8'))

def run_server(port=9001):
    """Run the webhook receiver server."""
    server_address = ('', port)
    httpd = HTTPServer(server_address, WebhookHandler)
    logger.info(f'Starting webhook receiver server on port {port}')
    logger.info(f'Failure rate: {settings["failure_rate"]}%')
    logger.info(f'Response delay: {settings["delay"]} seconds')
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info('Stopping server...')
        httpd.server_close()
        logger.info('Server stopped')

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Webhook Receiver Server')
    parser.add_argument('--port', type=int, default=9001,
                        help='Port to listen on (default: 9001)')
    parser.add_argument('--failure-rate', type=float, default=0,
                        help='Percentage of requests that should fail (0-100, default: 0)')
    parser.add_argument('--delay', type=float, default=0,
                        help='Delay in seconds before responding (default: 0)')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    
    # Update settings
    settings['failure_rate'] = args.failure_rate
    settings['delay'] = args.delay
    
    # Run server
    run_server(args.port)