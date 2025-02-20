import os
import json
import sys
import pytest
from pathlib import Path
from contextlib import contextmanager
from agents.finra_disciplinary_agent import get_driver, search_individual
from agents.nfa_basic_agent import search_nfa_profile, create_driver as nfa_create_driver
from agents.finra_arbitration_agent import search_individual as finra_search_individual, create_driver as finra_create_driver
from agents.sec_arbitration_agent import process_name
from agents.api_client import get_organization_crd, get_individual_basic_info

def green_check(message):
    """Print message with green check mark"""
    print(f"✅ {message}")

def red_x(message):
    """Print message with red X"""
    print(f"❌ {message}")

def warning(message):
    """Print warning message"""
    print(f"⚠️  {message}")

def check_directories():
    """Check if all required directories exist"""
    required_dirs = ['drop', 'output', 'archive', 'cache', 'index']
    missing_dirs = []
    
    for dir_path in required_dirs:
        if not os.path.exists(dir_path):
            missing_dirs.append(dir_path)
    
    return missing_dirs

def check_config():
    """Check if config.json exists and has required fields"""
    config_path = 'config.json'
    required_fields = [
        'evaluate_name',
        'evaluate_license',
        'evaluate_exams',
        'evaluate_disclosures'
    ]
    
    if not os.path.exists(config_path):
        return False, f"Missing config file: {config_path}"
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            
        missing_fields = [field for field in required_fields if field not in config]
        if missing_fields:
            return False, f"Config file missing fields: {', '.join(missing_fields)}"
            
    except json.JSONDecodeError:
        return False, f"Invalid JSON in config file: {config_path}"
        
    return True, "Config file valid"

def check_index_file():
    """Check if organizationsCrd.jsonl exists and is valid"""
    index_path = 'index/organizationsCrd.jsonl'
    
    if not os.path.exists(index_path):
        return False, f"Missing index file: {index_path}\nPlease copy organizationsCrd.jsonl to the index directory before proceeding."
        
    try:
        with open(index_path, 'r') as f:
            # Check first line can be parsed as JSON
            first_line = f.readline().strip()
            json.loads(first_line)
    except json.JSONDecodeError:
        return False, f"Invalid JSONL format in: {index_path}"
    except Exception as e:
        return False, f"Error reading index file: {str(e)}"
        
    return True, "Index file valid"

def check_venv():
    """Check if running in a virtual environment"""
    return hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)

def check_selenium_setup():
    """Check if Selenium and Chrome are working properly"""
    try:
        with get_driver(headless=True) as driver:
            # Try a simple search that should return no results
            result = search_individual(driver, "zballs", "maginszi")
            if result.get("result") == "No Results Found":
                return True, "Selenium and Chrome are working properly"
            else:
                return False, "Selenium test returned unexpected results"
    except Exception as e:
        return False, f"Selenium/Chrome setup error: {str(e)}"

def check_sec_agent():
    """Check if SEC agent is working properly"""
    try:
        # Run the tests from test_sec_arbitration_agent.py
        test_result = pytest.main([
            "-v",
            "tests/test_sec_arbitration_agent.py",
            "--no-header",  # Cleaner output
            "--tb=no"  # Hide traceback
        ])
        
        if test_result == 0:  # pytest.ExitCode.OK
            return True, "SEC agent is working properly"
        else:
            return False, "SEC agent tests failed"
            
    except Exception as e:
        return False, f"SEC agent setup error: {str(e)}"

def check_finra_arbitration_agent():
    """Check if FINRA arbitration agent is working properly"""
    try:
        # Run the tests from test_finra_arbitration_agent.py
        test_result = pytest.main([
            "-v",
            "tests/test_finra_arbitration_agent.py",
            "--no-header",  # Cleaner output
            "--tb=no"  # Hide traceback
        ])
        
        if test_result == 0:  # pytest.ExitCode.OK
            return True, "FINRA arbitration agent is working properly"
        else:
            return False, "FINRA arbitration agent tests failed"
            
    except Exception as e:
        return False, f"FINRA arbitration agent setup error: {str(e)}"

def check_nfa_basic_agent():
    """Check if NFA BASIC agent is working properly"""
    try:
        # Run the tests from test_nfa_basic_agent.py
        test_result = pytest.main([
            "-v",
            "tests/test_nfa_basic_agent.py",
            "--no-header",
            "--tb=no"
        ])
        
        if test_result == 0:
            return True, "NFA BASIC agent is working properly"
        else:
            return False, "NFA BASIC agent tests failed"
            
    except Exception as e:
        return False, f"NFA BASIC agent setup error: {str(e)}"

def main():
    print("\nVerifying FINRA Data Processing System setup...")
    print("--------------------------------------------\n")
    success = True
    
    # Ensure we're in the v2 directory
    if not os.path.basename(os.getcwd()) == 'v2':
        os.chdir('v2')
        green_check("Changed working directory to v2/")
    
    # Check directories
    missing_dirs = check_directories()
    if missing_dirs:
        red_x("Missing directories:")
        for dir_path in missing_dirs:
            print(f"   - {dir_path}")
        success = False
    else:
        green_check("All required directories exist")
    
    # Check config
    config_valid, config_msg = check_config()
    if not config_valid:
        red_x(f"Config check failed: {config_msg}")
        success = False
    else:
        green_check("Config file valid")
    
    # Check Selenium setup
    selenium_valid, selenium_msg = check_selenium_setup()
    if not selenium_valid:
        red_x(f"Selenium check failed: {selenium_msg}")
        success = False
    else:
        green_check("Selenium setup valid")
    
    # Add SEC agent check
    sec_valid, sec_msg = check_sec_agent()
    if not sec_valid:
        red_x(f"SEC agent check failed: {sec_msg}")
        success = False
    else:
        green_check("SEC agent setup valid")
    
    # Add FINRA arbitration check after SEC check
    finra_valid, finra_msg = check_finra_arbitration_agent()
    if not finra_valid:
        red_x(f"FINRA arbitration agent check failed: {finra_msg}")
        success = False
    else:
        green_check("FINRA arbitration agent setup valid")
    
    # Add NFA BASIC check after FINRA check
    nfa_valid, nfa_msg = check_nfa_basic_agent()
    if not nfa_valid:
        red_x(f"NFA BASIC agent check failed: {nfa_msg}")
        success = False
    else:
        green_check("NFA BASIC agent setup valid")
    
    # Check index file
    index_valid, index_msg = check_index_file()
    if not index_valid:
        red_x(f"Index file check failed: {index_msg}")
        success = False
    else:
        green_check("Index file valid")
    
    # Check virtual environment
    if not check_venv():
        warning("Not running in a virtual environment")
    else:
        green_check("Running in virtual environment")
    
    print("\nVerification Results:")
    print("-------------------")
    if success:
        green_check("Setup verification completed successfully!")
    else:
        red_x("Setup verification failed! Please check the errors above and refer to instructions.txt")
        sys.exit(1)

if __name__ == "__main__":
    main() 