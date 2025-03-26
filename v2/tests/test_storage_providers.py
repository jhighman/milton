"""
Tests for storage provider implementations.
"""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from datetime import datetime

from storage_providers.base_provider import BaseStorageProvider as StorageProvider
from storage_providers.local_provider import LocalStorageProvider
from storage_providers.s3_provider import S3StorageProvider
from storage_providers.factory import StorageProviderFactory

class TestLocalStorageProvider(unittest.TestCase):
    """Test cases for LocalStorageProvider."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.provider = LocalStorageProvider(base_path=self.temp_dir)
    
    def tearDown(self):
        """Clean up test environment."""
        # Remove all files in temp directory
        for root, dirs, files in os.walk(self.temp_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.temp_dir)
    
    def test_write_and_read_file(self):
        """Test writing and reading a file."""
        content = b"test content"
        path = "test.txt"
        
        # Write file
        self.assertTrue(self.provider.write_file(path, content))
        
        # Read file
        read_content = self.provider.read_file(path)
        self.assertEqual(read_content, content)
    
    def test_write_and_read_string(self):
        """Test writing and reading a string."""
        content = "test content"
        path = "test.txt"
        
        # Write file
        self.assertTrue(self.provider.write_file(path, content))
        
        # Read file
        read_content = self.provider.read_file(path)
        self.assertEqual(read_content.decode('utf-8'), content)
    
    def test_list_files(self):
        """Test listing files in a directory."""
        # Create test files
        files = ["test1.txt", "test2.txt", "subdir/test3.txt"]
        for file in files:
            self.provider.write_file(file, "content")
        
        # List all files
        listed_files = self.provider.list_files("")
        self.assertEqual(set(listed_files), set(files))
        
        # List files with pattern
        listed_files = self.provider.list_files("", "test*.txt")
        self.assertEqual(set(listed_files), {"test1.txt", "test2.txt"})
    
    def test_delete_file(self):
        """Test deleting a file."""
        path = "test.txt"
        self.provider.write_file(path, "content")
        
        # Delete file
        self.assertTrue(self.provider.delete_file(path))
        
        # Verify file is deleted
        self.assertFalse(self.provider.file_exists(path))
    
    def test_move_file(self):
        """Test moving a file."""
        source = "source.txt"
        destination = "dest.txt"
        content = "test content"
        
        self.provider.write_file(source, content)
        
        # Move file
        self.assertTrue(self.provider.move_file(source, destination))
        
        # Verify source is deleted and destination exists
        self.assertFalse(self.provider.file_exists(source))
        self.assertTrue(self.provider.file_exists(destination))
        
        # Verify content is preserved
        read_content = self.provider.read_file(destination)
        self.assertEqual(read_content.decode('utf-8'), content)
    
    def test_create_directory(self):
        """Test creating a directory."""
        path = "testdir"
        
        # Create directory
        self.assertTrue(self.provider.create_directory(path))
        
        # Verify directory exists
        self.assertTrue(os.path.isdir(os.path.join(self.temp_dir, path)))
    
    def test_get_file_size(self):
        """Test getting file size."""
        content = "test content"
        path = "test.txt"
        
        self.provider.write_file(path, content)
        
        size = self.provider.get_file_size(path)
        self.assertEqual(size, len(content.encode('utf-8')))
    
    def test_get_file_modified_time(self):
        """Test getting file modified time."""
        path = "test.txt"
        self.provider.write_file(path, "content")
        
        mtime = self.provider.get_file_modified_time(path)
        self.assertIsInstance(mtime, float)
        self.assertGreater(mtime, 0)

class TestS3StorageProvider(unittest.TestCase):
    """Test cases for S3StorageProvider."""
    
    def setUp(self):
        """Set up test environment."""
        self.bucket = "test-bucket"
        self.region = "us-east-1"
        self.prefix = "test/"
        self.provider = S3StorageProvider(
            aws_region=self.region,
            input_bucket=self.bucket,
            input_prefix=self.prefix
        )
        
        # Create a mock S3 client
        self.mock_client = MagicMock()
        self.provider.client = self.mock_client
    
    def test_write_file(self):
        """Test writing a file to S3."""
        path = "test.txt"
        content = "test content"
        
        # Mock successful response
        self.mock_client.put_object.return_value = {}
        
        # Write file
        self.assertTrue(self.provider.write_file(path, content))
        
        # Verify S3 client was called correctly
        # The S3 provider adds 'output/' prefix for write operations
        self.mock_client.put_object.assert_called_once_with(
            Bucket=self.bucket,
            Key=f"output/{path}",
            Body=content.encode('utf-8')
        )
    
    def test_read_file(self):
        """Test reading a file from S3."""
        path = "test.txt"
        content = b"test content"
        
        # Mock successful response
        mock_body = MagicMock()
        mock_body.read.return_value = content
        self.mock_client.get_object.return_value = {'Body': mock_body}
        
        # Read file
        read_content = self.provider.read_file(path)
        
        # Verify content and S3 client was called correctly
        self.assertEqual(read_content, content)
        self.mock_client.get_object.assert_called_once_with(
            Bucket=self.bucket,
            Key=f"{self.prefix}{path}"
        )
    
    def test_list_files(self):
        """Test listing files in S3."""
        # The S3 provider uses list_objects_v2 directly, not paginator
        self.mock_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': f"{self.prefix}test1.txt"},
                {'Key': f"{self.prefix}test2.txt"},
                {'Key': f"{self.prefix}subdir/test3.txt"}
            ]
        }
        
        # List files
        files = self.provider.list_files("")
        
        # Verify files and S3 client was called correctly
        self.assertEqual(len(files), 3)
        self.assertTrue("test1.txt" in files)
        self.assertTrue("test2.txt" in files)
        self.assertTrue("subdir/test3.txt" in files)
        self.mock_client.list_objects_v2.assert_called_once()
    
    def test_delete_file(self):
        """Test deleting a file from S3."""
        path = "test.txt"
        
        # Mock successful response
        self.mock_client.delete_object.return_value = {}
        
        # Delete file
        self.assertTrue(self.provider.delete_file(path))
        
        # Verify S3 client was called correctly
        self.mock_client.delete_object.assert_called_once_with(
            Bucket=self.bucket,
            Key=f"{self.prefix}{path}"
        )
    
    def test_move_file(self):
        """Test moving a file in S3."""
        source = "source.txt"
        destination = "dest.txt"
        
        # Mock successful responses
        self.mock_client.copy_object.return_value = {}
        self.mock_client.delete_object.return_value = {}
        
        # Move file
        self.assertTrue(self.provider.move_file(source, destination))
        
        # Verify S3 client was called correctly
        # The S3 provider adds 'output/' prefix for the destination in move operations
        self.mock_client.copy_object.assert_called_once_with(
            CopySource={'Bucket': self.bucket, 'Key': f"{self.prefix}{source}"},
            Bucket=self.bucket,
            Key=f"output/{destination}"
        )
        self.mock_client.delete_object.assert_called_once_with(
            Bucket=self.bucket,
            Key=f"{self.prefix}{source}"
        )
    
    def test_file_exists(self):
        """Test checking if a file exists in S3."""
        path = "test.txt"
        
        # Mock successful response
        self.mock_client.head_object.return_value = {}
        
        # Check file exists
        self.assertTrue(self.provider.file_exists(path))
        
        # Verify S3 client was called correctly
        self.mock_client.head_object.assert_called_once_with(
            Bucket=self.bucket,
            Key=f"{self.prefix}{path}"
        )
    
    def test_create_directory(self):
        """Test creating a directory in S3."""
        path = "testdir"
        
        # Mock successful response
        self.mock_client.put_object.return_value = {}
        
        # Create directory
        self.assertTrue(self.provider.create_directory(path))
        
        # Verify S3 client was called correctly
        # The S3 provider adds 'input/' prefix for directory creation
        self.mock_client.put_object.assert_called_once_with(
            Bucket=self.bucket,
            Key=f"input/{path}/"
        )
    
    def test_get_file_size(self):
        """Test getting file size from S3."""
        path = "test.txt"
        size = 123
        
        # Mock successful response
        self.mock_client.head_object.return_value = {'ContentLength': size}
        
        # Get file size
        file_size = self.provider.get_file_size(path)
        
        # Verify size and S3 client was called correctly
        self.assertEqual(file_size, size)
        self.mock_client.head_object.assert_called_once_with(
            Bucket=self.bucket,
            Key=f"{self.prefix}{path}"
        )
    
    def test_get_file_modified_time(self):
        """Test getting file modified time from S3."""
        path = "test.txt"
        mtime = 1234567890.0
        
        # Mock successful response
        self.mock_client.head_object.return_value = {
            'LastModified': datetime.fromtimestamp(mtime)
        }
        
        # Get file modified time
        file_mtime = self.provider.get_file_modified_time(path)
        
        # Verify time and S3 client was called correctly
        self.assertEqual(file_mtime, mtime)
        self.mock_client.head_object.assert_called_once_with(
            Bucket=self.bucket,
            Key=f"{self.prefix}{path}"
        )

class TestStorageProviderFactory(unittest.TestCase):
    """Test cases for StorageProviderFactory."""
    
    def test_create_local_provider(self):
        """Test creating a local storage provider."""
        config = {
            'type': 'local',
            'base_path': '/tmp/test',
            'input_folder': 'input',
            'output_folder': 'output',
            'archive_folder': 'archive',
            'cache_folder': 'cache'
        }
        
        provider = StorageProviderFactory.create_provider(config)
        self.assertIsInstance(provider, LocalStorageProvider)
        # On macOS, /tmp is a symlink to /private/tmp, so we need to check if the path ends with '/tmp/test'
        self.assertTrue(str(provider.base_path).endswith('/tmp/test'))
    
    def test_create_s3_provider(self):
        """Test creating an S3 storage provider."""
        config = {
            'type': 's3',
            'aws_region': 'us-east-1',
            's3': {
                'input_bucket': 'test-bucket',
                'input_prefix': 'test/'
            }
        }
        
        provider = StorageProviderFactory.create_provider(config)
        self.assertIsInstance(provider, S3StorageProvider)
        self.assertEqual(provider.input_bucket, 'test-bucket')
        self.assertEqual(provider.input_prefix, 'test/')
    
    def test_create_provider_with_invalid_type(self):
        """Test creating a provider with invalid type."""
        config = {
            'type': 'invalid',
            'base_path': '/tmp/test'
        }
        
        with self.assertRaises(ValueError):
            StorageProviderFactory.create_provider(config)
    
    def test_create_local_provider_with_missing_config(self):
        """Test creating a local provider with missing configuration."""
        config = {
            'type': 'local'
        }
        
        with self.assertRaises(ValueError):
            StorageProviderFactory.create_provider(config)
    
    def test_create_s3_provider_with_missing_config(self):
        """Test creating an S3 provider with missing configuration."""
        config = {
            'type': 's3'
        }
        
        with self.assertRaises(ValueError):
            StorageProviderFactory.create_provider(config)

if __name__ == '__main__':
    unittest.main() 