import sys
import subprocess
from typing import List
import os
from datetime import datetime

def ensure_reports_dir():
    """Ensure the reports directory exists"""
    if not os.path.exists('reports'):
        os.makedirs('reports')

def get_report_path(test_type: str) -> str:
    """Generate a unique report path based on test type and timestamp"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"reports/test_report_{test_type}_{timestamp}.html"

def run_tests(test_level: int = 1) -> bool:
    """
    Run tests based on test level
    1 = unit tests only (default)
    2 = integration tests only
    3 = all tests
    """
    ensure_reports_dir()
    test_commands: List[str] = []
    test_type = ""
    
    if test_level == 1:
        print("\nRunning unit tests...")
        test_type = "unit"
        test_commands = [
            "pytest",
            "-v",
            "--color=yes",
            "-m", "not integration",
            "--html=" + get_report_path(test_type),
            "--self-contained-html"
        ]
    elif test_level == 2:
        print("\nRunning integration tests...")
        test_type = "integration"
        test_commands = [
            "pytest",
            "-v",
            "--color=yes",
            "-m", "integration",
            "--html=" + get_report_path(test_type),
            "--self-contained-html"
        ]
    elif test_level == 3:
        print("\nRunning all tests...")
        test_type = "all"
        test_commands = [
            "pytest",
            "-v",
            "--color=yes",
            "--html=" + get_report_path(test_type),
            "--self-contained-html"
        ]
    else:
        print(f"Invalid test level: {test_level}")
        return False

    try:
        # Run tests without capturing output so we can see progress in real-time
        result = subprocess.run(test_commands, capture_output=False, text=True)
        success = result.returncode == 0
        
        if success:
            print(f"\n✨ Test report generated: {get_report_path(test_type)}")
        else:
            print(f"\n❌ Tests failed. Check the report: {get_report_path(test_type)}")
            
        return success
    except subprocess.CalledProcessError:
        return False
    except Exception as e:
        print(f"Error running tests: {str(e)}")
        return False

if __name__ == "__main__":
    # Get test level from command line argument, default to 1
    test_level = 1
    if len(sys.argv) > 1:
        try:
            test_level = int(sys.argv[1])
        except ValueError:
            print("Invalid test level. Using default (1)")
    
    if run_tests(test_level):
        sys.exit(0)
    else:
        sys.exit(1) 