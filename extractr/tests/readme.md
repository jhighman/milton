# Test Suite Documentation

Welcome to the test suite for the **Evaluation Framework** application. This directory contains unit tests for the various modules of the application, ensuring code reliability and facilitating future refactoring.

## Overview

The test suite is organized to cover each module individually:

- `test_api_client.py`: Tests the `ApiClient` class responsible for API interactions and caching.
- `test_csv_processor.py`: Tests the `CsvProcessor` class that handles CSV file processing and evaluations.
- `test_checkpoint_manager.py`: Tests the `CheckpointManager` class for checkpoint saving and loading.
- `test_config_loader.py`: Tests the configuration loading functionality.
- `test_signal_handler.py`: Tests the signal handling mechanisms.
- `test_logger_setup.py`: Tests the logging configuration setup.

## Prerequisites

- **Python Version**: Ensure you are using Python 3.6 or higher.
- **Dependencies**: The tests use the `unittest` and `unittest.mock` modules, which are part of the Python Standard Library. No external packages are required unless your application code has additional dependencies (e.g., `requests`).

Running Individual Test Modules
To run a specific test module, use the following command:

python -m unittest tests.test_module_name

Replace test_module_name with the name of the test module you want to run. For example, to run tests for api_client.py:

python -m unittest tests.test_api_client

This command will discover and run all tests in the tests directory that match the pattern test*.py.

Running Individual Test Modules
To run a specific test module, use the following command:

python -m unittest tests.test_module_name
Replace test_module_name with the name of the test module you want to run. For example, to run tests for api_client.py:

python -m unittest tests.test_api_client
Test Coverage

While the provided tests cover key functionalities of each module, you may consider using a coverage tool to measure the extent of your test suite. One popular tool is coverage.py.

Installing coverage.py
pip install coverage
Running Tests with Coverage

coverage run -m unittest discover -s tests
Generating a Coverage Report

coverage report -m
This will display a report showing the percentage of code covered by the tests for each module.

Writing Additional Tests

When adding new features or modules to the application, it's important to include corresponding tests. Here are some guidelines:

Test Structure: Each test module should correspond to a module in your application.
Naming Conventions: Test methods should start with test_ to be discoverable by unittest.
Mocking External Dependencies: Use unittest.mock to mock external dependencies like API calls or file I/O operations.
Assertions: Use assertions to check that the code behaves as expected.
Mocking and Patching

The tests make extensive use of mocking to simulate external dependencies and control the testing environment.

Mocking API Calls: External API calls are mocked to return predefined responses, ensuring tests are not dependent on external services.
Mocking File I/O: File operations are mocked where necessary to avoid reading from or writing to the actual filesystem during tests.
Patching: The patch() function from unittest.mock is used to temporarily replace methods or objects during a test.
Handling Test Data

Some tests may require sample data files or configurations.

Test Data Files: Place any test-specific files within the tests directory or a subdirectory like tests/test_data/.
Cleaning Up: Ensure that any files or directories created during tests are cleaned up in the tearDown() method to prevent interference with other tests.
Test Output

Verbose Mode: To see detailed information during test execution, you can run tests in verbose mode:

python -m unittest discover -s tests -v
Logs: If your application or tests produce logs, you can configure the logging level in logger_setup.py to control the verbosity during testing.

