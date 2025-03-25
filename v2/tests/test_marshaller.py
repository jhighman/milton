"""
Test suite for the marshaller module.
"""

import unittest
import os
import json
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from marshaller import (
    Marshaller,
    build_cache_path,
    read_manifest,
    write_manifest,
    load_cached_data,
    save_cached_data,
    save_multiple_results,
    log_request,
    is_cache_valid
)

class TestMarshaller(unittest.TestCase):
    """Test cases for the marshaller module."""
    
    def setUp(self):
        """Set up test environment."""
        self.employee_number = "TEST123"
        self.agent_name = "TestAgent"
        self.service_name = "test_service"
        self.test_date = "20240101"
        self.test_data = {"key": "value"}
        
        # Mock storage provider
        self.mock_storage = MagicMock()
        self.mock_storage.file_exists.return_value = True
        self.mock_storage.read_file.return_value = json.dumps(self.test_data).encode()
        self.mock_storage.write_file.return_value = True
        self.mock_storage.delete_file.return_value = True
        self.mock_storage.get_file_modified_time.return_value = datetime.now()
        self.mock_storage.list_files.return_value = ["test.json"]
        
        # Patch storage provider
        self.storage_patcher = patch('marshaller.storage_provider', self.mock_storage)
        self.storage_patcher.start()
        
    def tearDown(self):
        """Clean up test environment."""
        self.storage_patcher.stop()
        
    def test_build_cache_path(self):
        """Test building cache path."""
        path = build_cache_path(self.employee_number, self.agent_name, self.service_name)
        expected = f"cache/{self.employee_number}/{self.agent_name}/{self.service_name}"
        self.assertEqual(str(path), expected)
        
    def test_read_manifest(self):
        """Test reading manifest."""
        manifest_content = f"Cached on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        self.mock_storage.read_file.return_value = manifest_content.encode()
        
        result = read_manifest(build_cache_path(self.employee_number, self.agent_name, self.service_name))
        self.assertIsNotNone(result)
        
    def test_write_manifest(self):
        """Test writing manifest."""
        cache_path = build_cache_path(self.employee_number, self.agent_name, self.service_name)
        write_manifest(cache_path, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.mock_storage.write_file.assert_called_once()
        
    def test_load_cached_data(self):
        """Test loading cached data."""
        cache_path = build_cache_path(self.employee_number, self.agent_name, self.service_name)
        result = load_cached_data(cache_path)
        self.assertEqual(result, self.test_data)
        
    def test_save_cached_data(self):
        """Test saving cached data."""
        cache_path = build_cache_path(self.employee_number, self.agent_name, self.service_name)
        file_name = f"{self.agent_name}_{self.employee_number}_{self.service_name}_{self.test_date}.json"
        save_cached_data(cache_path, file_name, self.test_data)
        self.mock_storage.write_file.assert_called_once()
        
    def test_save_multiple_results(self):
        """Test saving multiple results."""
        cache_path = build_cache_path(self.employee_number, self.agent_name, self.service_name)
        results = [{"key1": "value1"}, {"key2": "value2"}]
        save_multiple_results(cache_path, self.agent_name, self.employee_number, self.service_name, self.test_date, results)
        self.assertEqual(self.mock_storage.write_file.call_count, 2)
        
    def test_log_request(self):
        """Test logging request."""
        log_request(self.employee_number, self.agent_name, self.service_name, "Cached")
        self.mock_storage.write_file.assert_called_once()
        
    def test_is_cache_valid(self):
        """Test cache validity check."""
        valid_date = (datetime.now() - timedelta(days=89)).strftime("%Y%m%d")
        invalid_date = (datetime.now() - timedelta(days=91)).strftime("%Y%m%d")
        self.assertTrue(is_cache_valid(valid_date))
        self.assertFalse(is_cache_valid(invalid_date))
        
    def test_marshaller_class(self):
        """Test Marshaller class."""
        marshaller = Marshaller()
        
        # Test initialization
        self.assertTrue(marshaller.headless)
        self.assertIsNone(marshaller.driver)
        
        # Test cleanup
        marshaller.cleanup()
        self.assertIsNone(marshaller.driver)

if __name__ == '__main__':
    unittest.main()