#!/usr/bin/env python3
"""
Test script to reproduce the 'NoneType' object has no attribute issue
seen in production with the facade object.
"""

import json
import logging
import sys
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s: %(levelname)s/%(processName)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("test_facade_issue")
logger.setLevel(logging.DEBUG)
print("Logger initialized")

# Import the necessary modules
try:
    from services import FinancialServicesFacade
    from business import process_claim
    from main_config import load_config, get_storage_config
    from storage_manager import StorageManager
    from storage_providers.factory import StorageProviderFactory
except ImportError as e:
    logger.error(f"Failed to import required modules: {e}")
    sys.exit(1)

# Global variables (similar to api.py)
facade = None
storage_manager = None

def initialize_services():
    """Initialize services similar to api.py's initialize_services function."""
    global facade, storage_manager
    
    try:
        logger.info("Initializing services...")
        
        # Load configuration
        config = load_config()
        logger.debug(f"Full config loaded: {json.dumps(config, indent=2)}")
        
        # Initialize storage
        storage_config = get_storage_config(config)
        logger.debug(f"Storage config retrieved: {json.dumps(storage_config, indent=2)}")
        storage_manager = StorageManager(storage_config)
        compliance_report_storage = StorageProviderFactory.create_provider(storage_config)
        logger.debug(f"Successfully initialized compliance_report_agent storage provider with base_path: {compliance_report_storage.base_path}")
        
        # Initialize FinancialServicesFacade
        facade = FinancialServicesFacade(headless=True, storage_manager=storage_manager)
        logger.info("Services successfully initialized")
        
    except Exception as e:
        logger.error(f"Critical error during initialization: {str(e)}", exc_info=True)
        # Note: In production, this exception might be caught and not re-raised,
        # which would leave facade as None
        raise

def test_with_none_facade():
    """Test process_claim with a None facade to reproduce the error."""
    logger.info("Testing process_claim with None facade")
    
    # Create a test claim similar to the one in the error log
    claim = {
        "reference_id": "TEST_REFERENCE_ID",
        "first_name": "Jason",
        "last_name": "Chandler",
        "individual_name": "Jason Chandler",
        "crd_number": "2382465",
        "organization_crd": None,
        "organization_name": "dev-fmr"
    }
    
    employee_number = "EN-TEST"
    
    try:
        # Intentionally use None as facade to reproduce the error
        result = process_claim(
            claim=claim,
            facade=None,  # This should cause the same error as in production
            employee_number=employee_number,
            skip_disciplinary=False,
            skip_arbitration=False,
            skip_regulatory=True
        )
        logger.info(f"Result: {result}")
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        # This should show the same error as in production

def test_with_initialized_facade():
    """Test process_claim with a properly initialized facade."""
    logger.info("Testing process_claim with initialized facade")
    
    # Initialize services first
    initialize_services()
    
    # Create a test claim similar to the one in the error log
    claim = {
        "reference_id": "TEST_REFERENCE_ID",
        "first_name": "Jason",
        "last_name": "Chandler",
        "individual_name": "Jason Chandler",
        "crd_number": "2382465",
        "organization_crd": None,
        "organization_name": "dev-fmr"
    }
    
    employee_number = "EN-TEST"
    
    try:
        # Use the properly initialized facade
        result = process_claim(
            claim=claim,
            facade=facade,
            employee_number=employee_number,
            skip_disciplinary=False,
            skip_arbitration=False,
            skip_regulatory=True
        )
        logger.info(f"Result: {result}")
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)

def test_initialize_services_failure():
    """Test what happens when initialize_services fails but doesn't raise an exception."""
    global facade
    
    logger.info("Testing initialize_services failure scenario")
    
    # Simulate a failure in initialize_services that doesn't raise an exception
    try:
        # This will intentionally fail but not raise an exception
        facade = None  # Reset facade to None
        
        # Now try to use the facade that should be None
        claim = {
            "reference_id": "TEST_REFERENCE_ID",
            "first_name": "Jason",
            "last_name": "Chandler",
            "individual_name": "Jason Chandler",
            "crd_number": "2382465",
            "organization_crd": None,
            "organization_name": "dev-fmr"
        }
        
        employee_number = "EN-TEST"
        
        result = process_claim(
            claim=claim,
            facade=facade,  # This is None
            employee_number=employee_number,
            skip_disciplinary=False,
            skip_arbitration=False,
            skip_regulatory=True
        )
        logger.info(f"Result: {result}")
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    print("Starting test script")
    logger.info("Starting test script")
    
    # Test with None facade to reproduce the error
    print("Running test_with_none_facade()")
    test_with_none_facade()
    
    # Test with a properly initialized facade
    print("Running test_with_initialized_facade()")
    test_with_initialized_facade()
    
    # Test what happens when initialize_services fails but doesn't raise an exception
    print("Running test_initialize_services_failure()")
    test_initialize_services_failure()
    
    print("Test script completed")
    logger.info("Test script completed")