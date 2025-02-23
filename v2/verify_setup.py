import os
import sys
import json
import logging
import subprocess
from pathlib import Path

# Add parent directory to path if not already there
if str(Path(__file__).parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent))

from agents.sec_iapd_agent import search_individual
from agents.exceptions import RateLimitExceeded

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_directories():
    """Verify required directories exist"""
    required_dirs = ['drop', 'output', 'archive', 'cache', 'index']
    missing_dirs = []
    
    for directory in required_dirs:
        if not os.path.exists(directory):
            missing_dirs.append(directory)
    
    if missing_dirs:
        logger.error(f"Missing directories: {', '.join(missing_dirs)}")
        logger.info("Run 'python setup.py' to create required directories")
        return False
    
    logger.info("All required directories present")
    return True

def verify_config():
    """Verify config.json exists and has required fields"""
    if not os.path.exists('config.json'):
        logger.error("config.json not found")
        logger.info("Run 'python setup.py' to create config file")
        return False
    
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        
        required_fields = [
            "evaluate_name",
            "evaluate_license",
            "evaluate_exams",
            "evaluate_disclosures"
        ]
        
        missing_fields = [field for field in required_fields if field not in config]
        
        if missing_fields:
            logger.error(f"Missing config fields: {', '.join(missing_fields)}")
            return False
            
        logger.info("Config file valid")
        return True
    
    except json.JSONDecodeError:
        logger.error("Invalid JSON in config.json")
        return False

def verify_api_access():
    """Verify API access by making a test request"""
    try:
        # Test SEC IAPD API
        result = search_individual("1438859", "VERIFY_SETUP", logger)
        if result and result["hits"]["total"] > 0:
            logger.info("SEC IAPD API access verified")
            return True
        else:
            logger.error("SEC IAPD API test failed")
            return False
    except RateLimitExceeded:
        logger.warning("SEC IAPD API rate limit reached")
        return True  # Consider rate limit a "successful" test
    except Exception as e:
        logger.error(f"API test failed: {str(e)}")
        return False

def verify_bdd_tests():
    """Verify BDD tests can run and pass"""
    logger.info("Verifying BDD tests...")
    
    try:
        # Run just the BDD feature tests
        result = subprocess.run([
            "pytest",
            "tests/steps/test_due_diligence_steps.py",
            "-v",
            "--gherkin-terminal-reporter"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("BDD tests passed successfully")
            return True
        else:
            logger.error("BDD tests failed")
            logger.error(f"Test output:\n{result.stdout}\n{result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Error running BDD tests: {str(e)}")
        return False

def main():
    logger.info("Starting setup verification...")
    
    checks = [
        ("Directory structure", verify_directories),
        ("Configuration file", verify_config),
        ("API access", verify_api_access),
        ("BDD tests", verify_bdd_tests)  # Added BDD test verification
    ]
    
    all_passed = True
    for name, check in checks:
        logger.info(f"\nVerifying {name}...")
        if not check():
            all_passed = False
    
    if all_passed:
        logger.info("\nAll checks passed! Setup is complete.")
        return 0
    else:
        logger.error("\nSome checks failed. See above for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 