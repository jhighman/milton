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

## Main Menu Options

The V2 Agent application provides an intuitive menu interface to control program execution. Here's a detailed breakdown of each option:

1. **Run batch processing**: Processes all CSV files in the drop folder, generates compliance reports, and moves processed files to the archive folder. Use this for full compliance checks on multiple files.

2. **Toggle Reviews**:
   - Toggle disciplinary review
   - Toggle arbitration review
   - Toggle regulatory review
   These options enable/disable specific compliance checks. Skipping checks improves performance but may miss certain compliance issues.

3. **Save settings**: Persists your current configuration (review toggles, logging preferences) to `config.json`. Useful for maintaining settings across sessions, especially in headless mode.

4. **Manage logging groups**: Controls logging verbosity for specific system components. Enables detailed debugging for targeted areas while keeping other logs concise.

5. **Flush logs**: Clears existing log files. Use when logs become unwieldy or you need a clean slate for troubleshooting.

6. **Set trace mode**: Enables comprehensive logging across all components. Ideal for deep debugging or complex issue investigation.

7. **Set production mode**: Minimizes logging to essential information. Optimizes performance for production environments.

8. **Exit**: Terminates the program.

## Logging System

### Logging Groups Overview

The V2 Agent organizes logging into functional groups that mirror its architecture:

- **services**: Data normalization, marshaling, and business logic
- **agents**: Agent-specific operations (data retrieval, processing)
- **evaluation**: Compliance decision logic and rule application
- **core**: High-level operations (batch processing, system events)

### Troubleshooting Scenarios

#### Scenario 1: Debugging Agent Issues
When an agent misbehaves (incorrect data retrieval/processing):

```bash
# Example logging output
[DEBUG] agents: Fetching data from endpoint X
[DEBUG] agents: Parsing response: {key: value}
[DEBUG] agents: Processing rule Y failed due to null value
```

**Approach**:
1. Enable agents group at DEBUG level
2. Disable or set other groups to ERROR
3. Use menu option 6 (Manage logging groups)
4. Save configuration for future use

#### Scenario 2: Evaluation Logic Issues
For investigating compliance results or alert triggers:

```bash
# Example logging output
[DEBUG] evaluation: Applying rule 'threshold > 10' to value 8
[DEBUG] evaluation: Rule failed, marking as non-compliant
[WARNING] services: Data normalization skipped due to missing field
```

**Approach**:
1. Set evaluation group to DEBUG
2. Set other groups to WARNING
3. Focus on rule execution and decision paths

#### Scenario 3: Data Flow Tracing
When tracking data movement between components:

```bash
# Example logging output
[DEBUG] agents: Retrieved data: {temp: 15}
[INFO] services: Normalized data: {temp: 15}
[DEBUG] evaluation: Rule 'temp < 20' passed
```

**Approach**:
1. Enable agents and evaluation at DEBUG
2. Set services to INFO
3. Disable core logging
4. Monitor data transformations and handoffs

#### Scenario 4: Performance Analysis
For identifying system bottlenecks:

```bash
# Example logging output
[INFO] agents: API call took 2.3s
[INFO] evaluation: Processed 100 records in 1.8s
```

**Approach**:
1. Start with trace mode (option 8)
2. Refine by setting specific groups to DEBUG
3. Monitor timing and resource usage patterns

### Logging Best Practices

1. **Start Clean**: Use the flush logs option before troubleshooting
2. **Progressive Detail**: Begin with trace mode, then narrow focus
3. **Targeted Debugging**: Use DEBUG level selectively to avoid log pollution
4. **Save Configurations**: Preserve useful logging setups for recurring issues

### Logging Configuration

You can configure logging through:
- Interactive menu options
- Command line arguments
- Configuration file settings

Example configuration in `config.json`:
```json
{
    "enabled_logging_groups": ["core", "agents"],
    "logging_levels": {
        "core": "INFO",
        "agents": "DEBUG",
        "services": "WARNING",
        "evaluation": "WARNING"
    }
}
``` 