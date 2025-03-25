"""
Tests for the StorageManager class.
"""

import os
import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch

from storage_manager import StorageManager
from storage_providers.base import StorageProvider
from storage_providers.local_provider import LocalStorageProvider
from storage_providers.s3_provider import S3StorageProvider

@pytest.fixture
def temp_config(tmp_path):
    """Create a temporary configuration file."""
    config = {
        "storage": {
            "mode": "local",
            "local": {
                "input_folder": "drop",
                "output_folder": "output",
                "archive_folder": "archive",
                "cache_folder": "cache"
            },
            "s3": {
                "aws_region": "us-east-1",
                "input_bucket": "test-input",
                "input_prefix": "input/",
                "output_bucket": "test-output",
                "output_prefix": "output/",
                "archive_bucket": "test-archive",
                "archive_prefix": "archive/",
                "cache_bucket": "test-cache",
                "cache_prefix": "cache/"
            }
        }
    }
    config_path = tmp_path / "config.json"
    with open(config_path, 'w') as f:
        json.dump(config, f)
    return str(config_path)

@pytest.fixture
def mock_provider():
    """Create a mock storage provider."""
    provider = Mock(spec=StorageProvider)
    provider.read_file.return_value = b"test content"
    provider.write_file.return_value = True
    provider.list_files.return_value = ["file1.txt", "file2.txt"]
    provider.delete_file.return_value = True
    provider.move_file.return_value = True
    provider.file_exists.return_value = True
    provider.create_directory.return_value = True
    provider.get_file_size.return_value = 100
    provider.get_file_modified_time.return_value = 1234567890.0
    return provider

def test_init_with_config(temp_config):
    """Test initialization with a configuration file."""
    storage = StorageManager(temp_config)
    assert storage.config["storage"]["mode"] == "local"
    assert isinstance(storage.provider, LocalStorageProvider)

def test_init_without_config():
    """Test initialization without a configuration file."""
    storage = StorageManager("nonexistent.json")
    assert storage.config["storage"]["mode"] == "local"  # Default mode
    assert isinstance(storage.provider, LocalStorageProvider)

def test_create_local_provider(temp_config):
    """Test creation of local storage provider."""
    storage = StorageManager(temp_config)
    assert isinstance(storage.provider, LocalStorageProvider)
    assert str(storage.provider.input_folder) == "drop"
    assert str(storage.provider.output_folder) == "output"

def test_create_s3_provider(temp_config):
    """Test creation of S3 storage provider."""
    with open(temp_config, 'r') as f:
        config = json.load(f)
    config["storage"]["mode"] = "s3"
    with open(temp_config, 'w') as f:
        json.dump(config, f)
    
    storage = StorageManager(temp_config)
    assert isinstance(storage.provider, S3StorageProvider)
    assert storage.provider.aws_region == "us-east-1"
    assert storage.provider.input_bucket == "test-input"

def test_invalid_storage_mode(temp_config):
    """Test handling of invalid storage mode."""
    with open(temp_config, 'r') as f:
        config = json.load(f)
    config["storage"]["mode"] = "invalid"
    with open(temp_config, 'w') as f:
        json.dump(config, f)
    
    with pytest.raises(ValueError, match="Unsupported storage mode"):
        StorageManager(temp_config)

def test_file_operations(mock_provider):
    """Test file operations through the storage manager."""
    with patch('storage_manager.StorageManager._create_provider', return_value=mock_provider):
        storage = StorageManager()
        
        # Test read_file
        content = storage.read_file("test.txt")
        assert content == b"test content"
        mock_provider.read_file.assert_called_once_with("test.txt")
        
        # Test write_file
        success = storage.write_file("test.txt", "content")
        assert success is True
        mock_provider.write_file.assert_called_once_with("test.txt", "content")
        
        # Test list_files
        files = storage.list_files("directory")
        assert files == ["file1.txt", "file2.txt"]
        mock_provider.list_files.assert_called_once_with("directory", None)
        
        # Test delete_file
        success = storage.delete_file("test.txt")
        assert success is True
        mock_provider.delete_file.assert_called_once_with("test.txt")
        
        # Test move_file
        success = storage.move_file("source.txt", "dest.txt")
        assert success is True
        mock_provider.move_file.assert_called_once_with("source.txt", "dest.txt")
        
        # Test file_exists
        exists = storage.file_exists("test.txt")
        assert exists is True
        mock_provider.file_exists.assert_called_once_with("test.txt")
        
        # Test create_directory
        success = storage.create_directory("new_dir")
        assert success is True
        mock_provider.create_directory.assert_called_once_with("new_dir")
        
        # Test get_file_size
        size = storage.get_file_size("test.txt")
        assert size == 100
        mock_provider.get_file_size.assert_called_once_with("test.txt")
        
        # Test get_file_modified_time
        mtime = storage.get_file_modified_time("test.txt")
        assert mtime == 1234567890.0
        mock_provider.get_file_modified_time.assert_called_once_with("test.txt")

def test_update_config(mock_provider):
    """Test configuration updates."""
    with patch('storage_manager.StorageManager._create_provider', return_value=S3StorageProvider(
        aws_region="us-west-2",
        input_bucket="new-input",
        input_prefix="new-input/",
        output_bucket="new-output",
        output_prefix="new-output/",
        archive_bucket="new-archive",
        archive_prefix="new-archive/",
        cache_bucket="new-cache",
        cache_prefix="new-cache/"
    )):
        storage = StorageManager()
        
        # Update config to S3 mode
        new_config = {
            "storage": {
                "mode": "s3",
                "s3": {
                    "aws_region": "us-west-2",
                    "input_bucket": "new-input",
                    "input_prefix": "new-input/",
                    "output_bucket": "new-output",
                    "output_prefix": "new-output/",
                    "archive_bucket": "new-archive",
                    "archive_prefix": "new-archive/",
                    "cache_bucket": "new-cache",
                    "cache_prefix": "new-cache/"
                }
            }
        }
        
        storage.update_config(new_config)
        assert storage.config == new_config
        assert isinstance(storage.provider, S3StorageProvider)
        assert storage.provider.aws_region == "us-west-2"
        assert storage.provider.input_bucket == "new-input"

def test_save_config(temp_config):
    """Test saving configuration to file."""
    storage = StorageManager(temp_config)
    
    # Modify config
    storage.config["storage"]["mode"] = "s3"
    
    # Save config
    success = storage.save_config(temp_config)
    assert success is True
    
    # Verify saved config
    with open(temp_config, 'r') as f:
        saved_config = json.load(f)
    assert saved_config["storage"]["mode"] == "s3"

def test_save_config_error():
    """Test handling of config save errors."""
    storage = StorageManager()
    
    # Try to save to a directory that doesn't exist
    success = storage.save_config("/nonexistent/dir/config.json")
    assert success is False 