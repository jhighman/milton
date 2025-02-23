import pytest
import os
import json
import logging
from unittest.mock import Mock, patch
import csv
import signal
from datetime import datetime
from collections import OrderedDict
import sys
from pathlib import Path

# Add project root to path if not already there
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Add parent dir to path to find main.py
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from main import (
    main, process_csv, process_row, load_config, generate_reference_id,
    setup_folders, load_checkpoint, save_checkpoint, get_csv_files,
    archive_file, resolve_headers, signal_handler, INPUT_FOLDER, OUTPUT_FOLDER,
    ARCHIVE_FOLDER, CHECKPOINT_FILE, canonical_fields
)

# Setup logging for tests
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('test_main')

@pytest.fixture
def mock_facade():
    """Mock FinancialServicesFacade."""
    facade = Mock()
    return facade

@pytest.fixture
def temp_dirs(tmp_path):
    """Create temporary directories for testing."""
    input_dir = tmp_path / "drop"
    output_dir = tmp_path / "output"
    archive_dir = tmp_path / "archive"
    input_dir.mkdir()
    output_dir.mkdir()
    archive_dir.mkdir()
    return input_dir, output_dir, archive_dir

@pytest.fixture
def sample_csv(temp_dirs):
    """Create a sample CSV in the temp input folder."""
    input_dir, _, _ = temp_dirs
    csv_path = input_dir / "claims.csv"
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['CRD', 'first_name', 'last_name', 'orgCRD'])
        writer.writerow(['11111', 'John', 'Doe', '99999'])
        writer.writerow(['22222', 'Jane', 'Smith', '88888'])
    return str(csv_path)

@pytest.fixture(autouse=True)
def reset_globals():
    """Reset global variables before each test."""
    global current_csv, current_line
    current_csv = None
    current_line = 0

def test_resolve_headers():
    """Test header mapping."""
    fieldnames = ['CRD Number', 'First Name', 'Last Name', 'orgCRD']
    resolved = resolve_headers(fieldnames)
    assert resolved == {
        'CRD Number': 'crd_number',
        'First Name': 'first_name',
        'Last Name': 'last_name',
        'orgCRD': 'organization_crd_number'
    }

def test_generate_reference_id():
    """Test reference ID generation."""
    ref_id = generate_reference_id()
    assert ref_id.startswith('DEF-')
    assert len(ref_id) == 16

def test_load_config(temp_dirs):
    """Test config loading with defaults."""
    _, output_dir, _ = temp_dirs
    config = load_config(str(output_dir / "nonexistent.json"))
    assert config == {
    "evaluate_name": True,
    "evaluate_license": True,
    "evaluate_exams": True,
    "evaluate_disclosures": True
}

def test_process_row_success(mock_facade, temp_dirs, caplog):
    """Test processing a row with successful search."""
    caplog.set_level(logging.DEBUG)
    _, output_dir, _ = temp_dirs
    row = {'CRD': '11111', 'first_name': 'John', 'last_name': 'Doe', 'orgCRD': '99999'}
    resolved_headers = {'CRD': 'crd_number', 'first_name': 'first_name', 'last_name': 'last_name', 'orgCRD': 'organization_crd_number'}
    config = load_config()

    with patch('main.OUTPUT_FOLDER', str(output_dir)), \
         patch('main.process_claim', return_value={
             "search_evaluation": {
                 "search_strategy": "search_with_both_crds",
                 "compliance": True,
                 "search_outcome": "SEC_IAPD hit",
                 "compliance_explanation": "Record found via SEC_IAPD."
             }
         }):
        process_row(row, resolved_headers, mock_facade, config)
        report_path = output_dir / "11111.json"
        assert report_path.exists(), f"Report not found at {report_path}"
        with open(report_path, 'r') as f:
            report = json.load(f)
            assert report['claim']['individual_name'] == 'John Doe'
            assert report['search_evaluation']['compliance'] is True

def test_process_row_fail(mock_facade, temp_dirs, caplog):
    """Test processing a row with failed search."""
    caplog.set_level(logging.DEBUG)
    _, output_dir, _ = temp_dirs
    row = {'CRD': '22222', 'first_name': 'Jane', 'last_name': 'Smith', 'orgCRD': '88888'}
    resolved_headers = {'CRD': 'crd_number', 'first_name': 'first_name', 'last_name': 'last_name', 'orgCRD': 'organization_crd_number'}
    config = load_config()

    with patch('main.OUTPUT_FOLDER', str(output_dir)), \
         patch('main.process_claim', return_value={
             "search_evaluation": {
                 "search_strategy": "search_with_both_crds",
                 "compliance": False,
                 "search_outcome": "No records found",
                 "compliance_explanation": "No records found"
             }
         }):
        process_row(row, resolved_headers, mock_facade, config)
        report_path = output_dir / "22222.json"
        assert report_path.exists(), f"Report not found at {report_path}"
        with open(report_path, 'r') as f:
            report = json.load(f)
            assert report['claim']['individual_name'] == 'Jane Smith'
            assert report['search_evaluation']['compliance'] is False

def test_process_csv_full(sample_csv, mock_facade, temp_dirs, caplog):
    """Test full CSV processing."""
    caplog.set_level(logging.DEBUG)
    input_dir, output_dir, _ = temp_dirs
    config = load_config()
    
    with patch('main.INPUT_FOLDER', str(input_dir)), \
         patch('main.OUTPUT_FOLDER', str(output_dir)), \
         patch('main.CHECKPOINT_FILE', str(output_dir / "checkpoint.json")), \
         patch('main.process_claim', return_value={
             "search_evaluation": {
                 "search_strategy": "search_with_both_crds",
                 "compliance": True,
                 "search_outcome": "SEC_IAPD hit",
                 "compliance_explanation": "Record found via SEC_IAPD."
             }
         }):
        process_csv(sample_csv, 0, mock_facade, config, 0.01)
        checkpoint_path = output_dir / "checkpoint.json"
        assert checkpoint_path.exists(), f"Checkpoint not found at {checkpoint_path}"
        checkpoint = load_checkpoint()
        assert checkpoint["csv_file"] == "claims.csv"
        assert checkpoint["line"] == 3
        json_files = [f for f in output_dir.glob('*.json') if f.name != "checkpoint.json"]
        assert len(json_files) == 2, f"Expected 2 JSON reports, got {len(json_files)}"

def test_process_csv_resume(sample_csv, mock_facade, temp_dirs, caplog):
    """Test resuming from checkpoint."""
    caplog.set_level(logging.DEBUG)
    input_dir, output_dir, _ = temp_dirs
    config = load_config()
    
    with patch('main.INPUT_FOLDER', str(input_dir)), \
         patch('main.OUTPUT_FOLDER', str(output_dir)), \
         patch('main.CHECKPOINT_FILE', str(output_dir / "checkpoint.json")):
        save_checkpoint("claims.csv", 2)  # Skip first row
        with patch('main.process_claim', return_value={
            "search_evaluation": {
                "search_strategy": "search_with_both_crds",
                "compliance": True,
                "search_outcome": "SEC_IAPD hit",
                "compliance_explanation": "Record found via SEC_IAPD."
            }
        }):
            process_csv(sample_csv, 2, mock_facade, config, 0.01)
    checkpoint = load_checkpoint()
            assert checkpoint["line"] == 3
            json_files = [f for f in output_dir.glob('*.json') if f.name != "checkpoint.json"]
            assert len(json_files) == 1, f"Expected 1 JSON report, got {len(json_files)}"

def test_archive_file(sample_csv, temp_dirs):
    """Test archiving a CSV."""
    input_dir, output_dir, archive_dir = temp_dirs
    with patch('main.ARCHIVE_FOLDER', str(archive_dir)):
        archive_file(sample_csv)
        date_str = datetime.now().strftime("%m-%d-%Y")
        archived_path = archive_dir / date_str / "claims.csv"
        assert archived_path.exists(), f"Archived file not found at {archived_path}"

def test_signal_handler(temp_dirs, monkeypatch):
    """Test signal handling."""
    input_dir, output_dir, _ = temp_dirs
    
    with patch('main.OUTPUT_FOLDER', str(output_dir)), \
         patch('main.CHECKPOINT_FILE', str(output_dir / "checkpoint.json")), \
         patch('main.current_csv', "test.csv"), \
         patch('main.current_line', 42), \
         patch('builtins.exit') as mock_exit:
        signal_handler(signal.SIGINT, None)
        checkpoint = load_checkpoint()
        assert checkpoint == {"csv_file": "test.csv", "line": 42}
        mock_exit.assert_called_once_with(0)

def test_main_full_run(sample_csv, temp_dirs, monkeypatch, caplog):
    """Test full main execution."""
    caplog.set_level(logging.INFO)
    input_dir, output_dir, archive_dir = temp_dirs
    
    mock_args = Mock(diagnostic=True, wait_time=0.01)
    monkeypatch.setattr('argparse.ArgumentParser.parse_args', lambda self: mock_args)
    
    with patch('main.INPUT_FOLDER', str(input_dir)), \
         patch('main.OUTPUT_FOLDER', str(output_dir)), \
         patch('main.ARCHIVE_FOLDER', str(archive_dir)), \
         patch('main.CHECKPOINT_FILE', str(output_dir / "checkpoint.json")), \
         patch('main.FinancialServicesFacade', return_value=Mock()), \
         patch('main.process_claim', return_value={
             "search_evaluation": {
                 "search_strategy": "search_with_both_crds",
                 "compliance": True,
                 "search_outcome": "SEC_IAPD hit",
                 "compliance_explanation": "Record found via SEC_IAPD."
             }
         }), \
         patch('main.archive_file'):  # Prevent file move
    main()
        assert "Processed 1 files" in caplog.text, "Expected 'Processed 1 files' in logs"
        assert "2 records" in caplog.text, "Expected '2 records' in logs"
        json_files = [f for f in output_dir.glob('*.json') if f.name != "checkpoint.json"]
        assert len(json_files) == 2, f"Expected 2 JSON reports, got {len(json_files)}"
        assert not os.path.exists(CHECKPOINT_FILE), "Checkpoint file should be removed"