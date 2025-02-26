# V2 Agent


## Installation

To set up the project, simply run:

```bash
python setup_project.py
```

This will:
1. Install the package in development mode
2. Create necessary directories (drop, output, archive, cache, index)
3. Generate a default config.json file
4. Install all required dependencies
5. Run initial setup verification

After the basic setup, you'll be presented with an interactive verification menu that allows you to:
- Verify specific components individually
- Run different types of tests
- Check API connectivity
- Generate test coverage reports

### Interactive Verification Menu

The menu provides the following options:

Verification Options:
1. Verify Directory Structure
2. Verify Configuration
3. Test API Access

Test Options:
4. Run Unit Tests
5. Run Integration Tests
6. Run All Tests
7. Run Tests with Coverage
8. Run Specific Test File

You can access this menu anytime by running:
```bash
python setup_project.py
```
And selecting 'y' when asked about additional verifications.

### Test Levels

The project supports three levels of testing:

1. Unit Tests (Level 1)
   - Fast, isolated tests
   - No external dependencies
   - Run with: `python run_tests.py 1`

2. Integration Tests (Level 2)
   - Tests with external dependencies
   - API integrations
   - Run with: `python run_tests.py 2`

3. All Tests (Level 3)
   - Runs both unit and integration tests
   - Complete test coverage
   - Run with: `python run_tests.py 3`

You can also run tests directly using pytest:
```bash
# Run unit tests only
pytest tests/ -v -m "not integration"

# Run integration tests only
pytest tests/ -v -m "integration"

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=agents --cov-report=term-missing -v
```

Note: During development, some unit tests may fail - this is expected and won't prevent you from setting up the project for development.

### Running Tests

To run the tests separately:

```bash
# Run all tests
pytest tests/

# Run tests with progress visualization
pytest tests/ --sugar -v

# Run specific test file
pytest tests/test_specific_file.py
```

The setup script will guide you through the process and show progress for each step.

### Test Reports

After running tests, HTML reports are automatically generated in the `reports` directory:

- Test Reports: `reports/test_report_*.html`
  - Includes test results, durations, and failure details
  - Generated for all test runs
  - Timestamp-based naming for historical tracking

- Coverage Reports: `reports/coverage/index.html`
  - Detailed code coverage information
  - Line-by-line coverage analysis
  - Generated when running tests with coverage

Example report paths:
```bash
# Unit test report
reports/test_report_unit_20240225_123456.html

# Integration test report
reports/test_report_integration_20240225_123456.html

# Coverage report
reports/coverage/index.html
```

The reports are self-contained HTML files that can be opened in any web browser.

## Project Structure

The package automatically creates the following directories:
- `drop/`: Directory for incoming documents
- `output/`: Directory for processed results
- `archive/`: Directory for processed documents
- `cache/`: Directory for temporary files
- `index/`: Directory for indexing data

## Configuration

A `config.json` file will be automatically created with default settings:
```json
{
    "evaluate_name": true,
    "evaluate_license": true,
    "evaluate_exams": true,
    "evaluate_disclosures": true
}
```

You can modify these settings according to your needs.

## Requirements

- Python 3.8 or higher
- Required packages (automatically installed):
  - requests >= 2.25.0
  - pytest >= 6.0.0
  - pytest-mock >= 3.6.0 

## Setup Verification

After installation, you can verify your setup using the verification script:

```bash
# Basic verification
python verify_setup.py

# For more detailed logging, use Python's verbose mode
python -v verify_setup.py
```

The verification script includes enhanced test visualization with:
- Progress bars and colored output (pytest-sugar)
- Instant failure reporting (pytest-instafail)
- Suppressed warnings for cleaner output

The verification script checks:
1. Directory structure (drop, output, archive, cache, index)
2. Configuration file validity
3. API access and connectivity
4. Unit test execution

Each check will output detailed status information. If any check fails, the script will provide specific error messages and remediation steps.

Example output:
```
INFO:root:Starting setup verification...
INFO:root:Verifying Directory structure...
INFO:root:All required directories present
INFO:root:Verifying Configuration file...
INFO:root:Config file valid
INFO:root:Verifying API access...
INFO:root:SEC IAPD API access verified
INFO:root:Verifying Unit tests...
Running tests with pytest-sugar ✨ 
 tests/test_example.py ✓✓✓✓✓                                              100% 
INFO:root:Unit tests passed successfully
INFO:root:All checks passed! Setup is complete.
``` 