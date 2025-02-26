#!/usr/bin/env python3
import subprocess
import sys
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

def clear_screen():
    """Clear the terminal screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def run_command(command, error_message, ignore_errors=False, show_output=False):
    """Run a command and handle any errors"""
    try:
        result = subprocess.run(
            command,
            check=not ignore_errors,
            capture_output=not show_output,  # Don't capture output if show_output is True
            text=True
        )
        if result.returncode != 0 and not ignore_errors:
            logger.error(f"{error_message}")
            if not show_output:  # Only show captured output if we're not already showing it in real-time
                if result.stdout:
                    logger.error(f"Output:\n{result.stdout}")
                if result.stderr:
                    logger.error(f"Errors:\n{result.stderr}")
            return False
        return True
    except Exception as e:
        if not ignore_errors:
            logger.error(f"{error_message}: {e}")
        return False

def display_menu():
    """Display the verification menu"""
    clear_screen()
    print("\n🔍 Milton V2 Verification Menu 🔍")
    print("\nVerification Options:")
    print("1. Verify Directory Structure")
    print("2. Verify Configuration")
    print("3. Test API Access")
    print("\nTest Options:")
    print("4. Run Unit Tests")
    print("5. Run Integration Tests")
    print("6. Run All Tests")
    print("7. Run Tests with Coverage")
    print("8. Run Specific Test File")
    print("\n0. Exit")
    return input("\nEnter your choice (0-8): ")

def ensure_reports_dir():
    """Ensure the reports directory exists"""
    if not os.path.exists('reports'):
        os.makedirs('reports')

def get_report_path(test_type: str) -> str:
    """Generate a unique report path based on test type and timestamp"""
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"reports/test_report_{test_type}_{timestamp}.html"

def get_test_files():
    """Get list of test files from the tests directory"""
    test_files = []
    try:
        for file in os.listdir('tests'):
            if file.startswith('test_') and file.endswith('.py'):
                test_files.append(file)
        return sorted(test_files)
    except Exception as e:
        logger.error(f"Error reading test files: {str(e)}")
        return []

def select_test_file():
    """Display menu of available test files and get user selection"""
    test_files = get_test_files()
    
    if not test_files:
        print("\n❌ No test files found in tests directory")
        return None
    
    print("\n📝 Available Test Files:")
    for idx, file in enumerate(test_files, 1):
        print(f"{idx}. {file}")
    
    while True:
        try:
            choice = input("\nEnter the number of the test file to run (or 0 to cancel): ")
            if choice == "0":
                return None
            
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(test_files):
                return test_files[choice_idx]
            else:
                print("Invalid selection. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

def run_specific_verification(choice):
    """Run the selected verification option"""
    clear_screen()
    ensure_reports_dir()
    
    if choice == "1":
        print("\n📁 Verifying Directory Structure...")
        run_command(
            [sys.executable, "verify_setup.py", "--directories-only"],
            "Directory verification failed",
            ignore_errors=True
        )
    elif choice == "2":
        print("\n⚙️ Verifying Configuration...")
        run_command(
            [sys.executable, "verify_setup.py", "--config-only"],
            "Configuration verification failed",
            ignore_errors=True
        )
    elif choice == "3":
        print("\n🌐 Testing API Access...")
        run_command(
            [sys.executable, "verify_setup.py", "--api-only"],
            "API verification failed",
            ignore_errors=True
        )
    elif choice == "4":
        print("\n🧪 Running Unit Tests...")
        run_command(
            [sys.executable, "run_tests.py", "1"],
            "Unit tests failed",
            ignore_errors=True,
            show_output=True
        )
    elif choice == "5":
        print("\n🔄 Running Integration Tests...")
        run_command(
            [sys.executable, "run_tests.py", "2"],
            "Integration tests failed",
            ignore_errors=True,
            show_output=True
        )
    elif choice == "6":
        print("\n🔬 Running All Tests...")
        run_command(
            [sys.executable, "run_tests.py", "3"],
            "All tests failed",
            ignore_errors=True,
            show_output=True
        )
    elif choice == "7":
        print("\n📊 Running Tests with Coverage...")
        report_path = get_report_path("coverage")
        run_command(
            [
                "pytest",
                "--cov=agents",
                "--cov-report=term-missing",
                "--cov-report=html:reports/coverage",
                "-v",
                "--color=yes",
                "--html=" + report_path,
                "--self-contained-html"
            ],
            "Coverage tests failed",
            ignore_errors=True,
            show_output=True
        )
        print(f"\n✨ Test report generated: {report_path}")
        print("📊 Coverage report generated: reports/coverage/index.html")
    elif choice == "8":
        test_file = select_test_file()
        if test_file:
            print(f"\n🔬 Running {test_file}...")
            report_path = get_report_path(f"file_{os.path.splitext(test_file)[0]}")
            run_command(
                [
                    "pytest",
                    f"tests/{test_file}",
                    "-v",
                    "--color=yes",
                    "--html=" + report_path,
                    "--self-contained-html"
                ],
                f"Test {test_file} failed",
                ignore_errors=True,
                show_output=True
            )
            print(f"\n✨ Test report generated: {report_path}")
    
    input("\nPress Enter to continue...")

def interactive_menu():
    """Run the interactive verification menu"""
    while True:
        choice = display_menu()
        if choice == "0":
            print("\nExiting verification menu. Project is ready for development!")
            break
        elif choice in ["1", "2", "3", "4", "5", "6", "7", "8"]:
            run_specific_verification(choice)
        else:
            print("\nInvalid choice. Press Enter to try again...")
            input()

def main():
    logger.info("Starting project setup...")
    
    # Step 1: Install the package in development mode
    logger.info("\n1. Installing package...")
    if not run_command(
        [sys.executable, "-m", "pip", "install", "-e", "."],
        "Failed to install package"
    ):
        return 1

    # Step 2: Basic setup verification
    logger.info("\n2. Running initial setup verification...")
    run_command(
        [sys.executable, "verify_setup.py", "--quick"],
        "Initial setup verification completed with some issues",
        ignore_errors=True
    )
    
    logger.info("\n✨ Basic setup completed! ✨")
    
    # Step 3: Interactive verification menu
    print("\nWould you like to run additional verifications? (y/n)")
    if input().lower().startswith('y'):
        interactive_menu()
    else:
        logger.info("\nSkipping additional verifications. Project is ready for development!")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 