import sys
import subprocess
from typing import List

def run_tests(test_level: int = 1) -> bool:
    """
    Run tests based on test level
    1 = unit tests only (default)
    2 = integration tests only
    3 = all tests
    """
    test_commands: List[str] = []
    
    if test_level == 1:
        print("\nRunning unit tests...")
        test_commands = ["pytest", "tests/", "-v", "-m", "not integration"]
    elif test_level == 2:
        print("\nRunning integration tests...")
        test_commands = ["pytest", "tests/", "-v", "-m", "integration"]
    elif test_level == 3:
        print("\nRunning all tests...")
        test_commands = ["pytest", "tests/", "-v"]
    else:
        print(f"Invalid test level: {test_level}")
        return False

    try:
        result = subprocess.run(test_commands, check=True)
        return result.returncode == 0
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