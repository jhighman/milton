import os
import sys
import json
import logging
import subprocess
import argparse
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

def verify_unit_tests(with_coverage=False, test_file=None):
    """Verify unit tests can run and pass"""
    logger.info("Verifying unit tests...")
    
    try:
        # Build pytest command
        cmd = ["pytest", "-v", "--sugar", "--instafail", "-p", "no:warnings"]
        
        if with_coverage:
            cmd.extend(["--cov=agents", "--cov-report=term-missing"])
        
        if test_file:
            cmd.append(f"tests/{test_file}")
        else:
            cmd.extend(["tests", "--ignore=tests/steps"])
        
        # Run the tests
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("Unit tests passed successfully")
            return True
        else:
            logger.error("Unit tests failed")
            logger.error(f"Test output:\n{result.stdout}\n{result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Error running unit tests: {str(e)}")
        return False

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Verify Milton V2 setup")
    parser.add_argument("--quick", action="store_true", help="Run quick verification (dirs and config only)")
    parser.add_argument("--directories-only", action="store_true", help="Only verify directory structure")
    parser.add_argument("--config-only", action="store_true", help="Only verify configuration")
    parser.add_argument("--api-only", action="store_true", help="Only verify API access")
    parser.add_argument("--tests-only", action="store_true", help="Only run unit tests")
    parser.add_argument("--with-coverage", action="store_true", help="Run tests with coverage report")
    parser.add_argument("--test-file", help="Run specific test file")
    return parser.parse_args()

def main():
    args = parse_args()
    logger.info("Starting setup verification...")
    
    # Handle selective verification
    if args.directories_only:
        return 0 if verify_directories() else 1
    elif args.config_only:
        return 0 if verify_config() else 1
    elif args.api_only:
        return 0 if verify_api_access() else 1
    elif args.tests_only:
        return 0 if verify_unit_tests(args.with_coverage, args.test_file) else 1
    
    # Quick verification (dirs and config only)
    if args.quick:
        checks = [
            ("Directory structure", verify_directories),
            ("Configuration file", verify_config)
        ]
    else:
        # Full verification
        checks = [
            ("Directory structure", verify_directories),
            ("Configuration file", verify_config),
            ("API access", verify_api_access),
            ("Unit tests", lambda: verify_unit_tests(args.with_coverage, args.test_file))
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