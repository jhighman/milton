#!/usr/bin/env python3
"""
Script to run the API integration test.
This script runs the integration test that sends three requests in succession
and polls for their completion, logging the results.
"""

import unittest
import sys
import logging
from test_api_integration import APIIntegrationTest

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("integration_test_runner")
    
    logger.info("Starting API Integration Test")
    logger.info("This test will send three requests with webhooks and poll for completion")
    
    # Create a test suite with our integration test
    suite = unittest.TestSuite()
    suite.addTest(APIIntegrationTest("test_sequential_requests_with_polling"))
    
    # Run the test suite
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Report results
    if result.wasSuccessful():
        logger.info("Integration test completed successfully!")
        sys.exit(0)
    else:
        logger.error("Integration test failed!")
        sys.exit(1)