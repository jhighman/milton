import pytest
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
import json
import shutil
from datetime import datetime, timedelta

# Add parent directory to path (assuming tests are in a subdirectory like 'tests/')
sys.path.append(str(Path(__file__).parent.parent))

from marshaller import (
    create_driver, check_cache_or_fetch, fetch_agent_data, log_request, 
    load_cached_data, save_cached_data, write_manifest, read_manifest, 
    is_cache_valid, build_cache_path, CACHE_FOLDER, CACHE_TTL_DAYS, DATE_FORMAT
)

# Set up test logging
logger = logging.getLogger("test_marshaller")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s'))
logger.addHandler(handler)

# Test fixtures
@pytest.fixture(autouse=True)
def setup_cache_folder(monkeypatch, tmp_path):
    """Setup temporary cache folder for all tests."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr('marshaller.CACHE_FOLDER', cache_dir)
    return cache_dir

@pytest.fixture
def temp_cache(setup_cache_folder):
    """Create a temporary cache directory."""
    return setup_cache_folder

@pytest.fixture
def sample_params():
    """Sample parameters for testing."""
    return {"first_name": "John", "last_name": "Doe"}


@pytest.fixture
def mock_driver():
    """Mock WebDriver for testing."""
    with patch('selenium.webdriver.Chrome') as mock:
        driver = MagicMock()
        mock.return_value = driver
        yield driver


def test_is_cache_valid():
    """Test cache validity check."""
    valid_date = (datetime.now() - timedelta(days=CACHE_TTL_DAYS - 1)).strftime(DATE_FORMAT)
    invalid_date = (datetime.now() - timedelta(days=CACHE_TTL_DAYS + 1)).strftime(DATE_FORMAT)
    assert is_cache_valid(valid_date) == True
    assert is_cache_valid(invalid_date) == False

def test_build_cache_path(temp_cache):
    """Test cache path construction."""
    path = build_cache_path("EMP001", "NFA_Basic_Agent", "search_individual")
    expected = temp_cache / "EMP001" / "NFA_Basic_Agent" / "search_individual"
    assert path == expected

def test_log_request(temp_cache):
    """Test logging a request to file."""
    employee_number = "EMP001"
    # Create the employee directory
    (temp_cache / employee_number).mkdir(parents=True, exist_ok=True)
    log_request(employee_number, "NFA_Basic_Agent", "search_individual", "Cached")
    log_file = temp_cache / employee_number / "request_log.txt"
    assert log_file.exists()
    with log_file.open("r") as f:
        log_entry = f.read()
        assert "NFA_Basic_Agent/search_individual - Cached" in log_entry

def test_save_and_load_cached_data(temp_cache):
    """Test saving and loading cached data."""
    cache_path = temp_cache / "EMP001" / "NFA_Basic_Agent" / "search_individual"
    data = {"result": "Test Data"}
    file_name = "NFA_Basic_Agent_EMP001_search_individual_20230101.json"
    save_cached_data(cache_path, file_name, data)
    loaded_data = load_cached_data(cache_path)
    assert loaded_data == data

def test_write_and_read_manifest(temp_cache):
    """Test manifest file operations."""
    cache_path = temp_cache / "EMP001" / "NFA_Basic_Agent" / "search_individual"
    cache_path.mkdir(parents=True, exist_ok=True)
    timestamp = "2023-01-01 12:00:00"
    write_manifest(cache_path, timestamp)
    cached_date = read_manifest(cache_path)
    assert cached_date == "20230101"

# Mocked Integration Tests

@patch("marshaller.fetch_agent_data")
def test_check_cache_or_fetch_hit(mock_fetch, temp_cache, sample_params, mock_driver):
    """Test cache hit scenario."""
    employee_number = "EMP001"
    agent_name = "NFA_Basic_Agent"
    service = "search_individual"
    cache_path = build_cache_path(employee_number, agent_name, service)
    cache_path.mkdir(parents=True, exist_ok=True)
    
    # Create a valid cached file
    data = [{"result": "Cached Result"}]
    file_name = f"NFA_Basic_Agent_{employee_number}_search_individual_{datetime.now().strftime(DATE_FORMAT)}.json"
    save_cached_data(cache_path, file_name, data[0])
    write_manifest(cache_path, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    with patch("marshaller.logger") as mock_logger:
        result = check_cache_or_fetch(agent_name, service, employee_number, sample_params, mock_driver)
        assert result == data
        mock_logger.info.assert_called_with(f"Cache hit for {agent_name}/{service}/{employee_number}")
        mock_fetch.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])