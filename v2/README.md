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