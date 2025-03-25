"""
Tests for the S3StorageProvider class.
"""

import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from datetime import datetime

from storage_providers.s3_provider import S3StorageProvider

@pytest.fixture
def provider():
    """Create an S3StorageProvider instance with test configuration."""
    return S3StorageProvider(
        aws_region="us-east-1",
        input_bucket="test-input",
        input_prefix="input/",
        output_bucket="test-output",
        output_prefix="output/",
        archive_bucket="test-archive",
        archive_prefix="archive/",
        cache_bucket="test-cache",
        cache_prefix="cache/"
    )

@pytest.fixture
def mock_s3():
    """Create a mock S3 client."""
    with patch('boto3.client') as mock_client:
        s3 = Mock()
        mock_client.return_value = s3
        yield s3

def test_init(provider):
    """Test initialization of S3StorageProvider."""
    assert provider.aws_region == "us-east-1"
    assert provider.input_bucket == "test-input"
    assert provider.input_prefix == "input/"
    assert provider.output_bucket == "test-output"
    assert provider.output_prefix == "output/"
    assert provider.archive_bucket == "test-archive"
    assert provider.archive_prefix == "archive/"
    assert provider.cache_bucket == "test-cache"
    assert provider.cache_prefix == "cache/"

def test_read_file(provider, mock_s3):
    """Test reading a file from S3."""
    test_content = b"Hello, World!"
    mock_s3.get_object.return_value = {
        'Body': Mock(read=lambda: test_content),
        'LastModified': datetime.now()
    }
    
    content = provider.read_file("test.txt")
    assert content == test_content
    mock_s3.get_object.assert_called_once_with(
        Bucket="test-input",
        Key="input/test.txt"
    )

def test_read_file_not_found(provider, mock_s3):
    """Test reading a non-existent file from S3."""
    mock_s3.get_object.side_effect = ClientError(
        {'Error': {'Code': 'NoSuchKey', 'Message': 'Not found'}},
        'GetObject'
    )
    
    with pytest.raises(FileNotFoundError):
        provider.read_file("nonexistent.txt")

def test_write_file(provider, mock_s3):
    """Test writing a file to S3."""
    test_content = "Hello, World!"
    
    # Write string content
    success = provider.write_file("test.txt", test_content)
    assert success is True
    mock_s3.put_object.assert_called_once_with(
        Bucket="test-output",
        Key="output/test.txt",
        Body=test_content.encode()
    )
    
    # Write bytes content
    mock_s3.reset_mock()
    success = provider.write_file("test.txt", test_content.encode())
    assert success is True
    mock_s3.put_object.assert_called_once_with(
        Bucket="test-output",
        Key="output/test.txt",
        Body=test_content.encode()
    )

def test_write_file_binary(provider, mock_s3):
    """Test writing binary content to S3."""
    test_content = b"\x00\x01\x02\x03"
    
    success = provider.write_file("test.bin", test_content)
    assert success is True
    mock_s3.put_object.assert_called_once_with(
        Bucket="test-output",
        Key="output/test.bin",
        Body=test_content
    )

def test_list_files(provider, mock_s3):
    """Test listing files in an S3 directory."""
    mock_s3.list_objects_v2.return_value = {
        'Contents': [
            {'Key': 'input/file1.txt'},
            {'Key': 'input/file2.txt'},
            {'Key': 'input/subdir/file3.txt'}
        ]
    }
    
    files = provider.list_files("input")
    assert len(files) == 2
    assert "file1.txt" in files
    assert "file2.txt" in files
    
    mock_s3.list_objects_v2.assert_called_once_with(
        Bucket="test-input",
        Prefix="input/"
    )

def test_list_files_with_pattern(provider, mock_s3):
    """Test listing files with a pattern in S3."""
    mock_s3.list_objects_v2.return_value = {
        'Contents': [
            {'Key': 'input/file1.txt'},
            {'Key': 'input/file2.txt'},
            {'Key': 'input/subdir/file3.txt'}
        ]
    }
    
    files = provider.list_files("input", "file1.*")
    assert len(files) == 1
    assert files[0] == "file1.txt"

def test_list_files_not_found(provider, mock_s3):
    """Test listing files in a non-existent S3 directory."""
    mock_s3.list_objects_v2.return_value = {}
    
    files = provider.list_files("nonexistent")
    assert len(files) == 0

def test_delete_file(provider, mock_s3):
    """Test deleting a file from S3."""
    success = provider.delete_file("test.txt")
    assert success is True
    mock_s3.delete_object.assert_called_once_with(
        Bucket="test-input",
        Key="input/test.txt"
    )

def test_delete_file_not_found(provider, mock_s3):
    """Test deleting a non-existent file from S3."""
    mock_s3.delete_object.side_effect = ClientError(
        {'Error': {'Code': 'NoSuchKey', 'Message': 'Not found'}},
        'DeleteObject'
    )
    
    success = provider.delete_file("nonexistent.txt")
    assert success is False

def test_move_file(provider, mock_s3):
    """Test moving a file within S3."""
    mock_s3.copy_object.return_value = {}
    mock_s3.delete_object.return_value = {}
    
    success = provider.move_file("source.txt", "dest.txt")
    assert success is True
    
    mock_s3.copy_object.assert_called_once_with(
        Bucket="test-input",
        CopySource={"Bucket": "test-input", "Key": "input/source.txt"},
        Key="output/dest.txt"
    )
    mock_s3.delete_object.assert_called_once_with(
        Bucket="test-input",
        Key="input/source.txt"
    )

def test_move_file_not_found(provider, mock_s3):
    """Test moving a non-existent file in S3."""
    mock_s3.copy_object.side_effect = ClientError(
        {'Error': {'Code': 'NoSuchKey', 'Message': 'Not found'}},
        'CopyObject'
    )
    
    success = provider.move_file("nonexistent.txt", "dest.txt")
    assert success is False

def test_file_exists(provider, mock_s3):
    """Test checking if a file exists in S3."""
    mock_s3.head_object.return_value = {}
    
    assert provider.file_exists("test.txt") is True
    mock_s3.head_object.assert_called_once_with(
        Bucket="test-input",
        Key="input/test.txt"
    )

def test_file_exists_not_found(provider, mock_s3):
    """Test checking if a non-existent file exists in S3."""
    mock_s3.head_object.side_effect = ClientError(
        {'Error': {'Code': '404', 'Message': 'Not found'}},
        'HeadObject'
    )
    
    assert provider.file_exists("nonexistent.txt") is False

def test_create_directory(provider, mock_s3):
    """Test creating a directory in S3."""
    success = provider.create_directory("new_dir")
    assert success is True
    mock_s3.put_object.assert_called_once_with(
        Bucket="test-input",
        Key="input/new_dir/"
    )

def test_get_file_size(provider, mock_s3):
    """Test getting file size from S3."""
    mock_s3.head_object.return_value = {'ContentLength': 100}
    
    size = provider.get_file_size("test.txt")
    assert size == 100
    mock_s3.head_object.assert_called_once_with(
        Bucket="test-input",
        Key="input/test.txt"
    )

def test_get_file_size_not_found(provider, mock_s3):
    """Test getting size of non-existent file from S3."""
    mock_s3.head_object.side_effect = ClientError(
        {'Error': {'Code': '404', 'Message': 'Not found'}},
        'HeadObject'
    )
    
    with pytest.raises(FileNotFoundError):
        provider.get_file_size("nonexistent.txt")

def test_get_file_modified_time(provider, mock_s3):
    """Test getting file modification time from S3."""
    test_time = datetime.now()
    mock_s3.head_object.return_value = {'LastModified': test_time}
    
    mtime = provider.get_file_modified_time("test.txt")
    assert isinstance(mtime, float)
    assert mtime > 0
    mock_s3.head_object.assert_called_once_with(
        Bucket="test-input",
        Key="input/test.txt"
    )

def test_get_file_modified_time_not_found(provider, mock_s3):
    """Test getting modification time of non-existent file from S3."""
    mock_s3.head_object.side_effect = ClientError(
        {'Error': {'Code': '404', 'Message': 'Not found'}},
        'HeadObject'
    )
    
    with pytest.raises(FileNotFoundError):
        provider.get_file_modified_time("nonexistent.txt") 