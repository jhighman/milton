"""
S3 storage provider implementation.

This module implements the StorageProvider interface for AWS S3 operations.
"""

import boto3
from botocore.exceptions import ClientError
from typing import List, Optional, Union, BinaryIO
import logging

from .base import StorageProvider

logger = logging.getLogger(__name__)

class S3StorageProvider(StorageProvider):
    """Implementation of StorageProvider for AWS S3 operations."""
    
    def __init__(self, aws_region: str, bucket_name: str, base_prefix: str = ""):
        """
        Initialize the S3 storage provider.
        
        Args:
            aws_region: AWS region name.
            bucket_name: S3 bucket name.
            base_prefix: Base prefix for all operations (optional).
        """
        self.bucket_name = bucket_name
        self.base_prefix = base_prefix.rstrip('/') + '/' if base_prefix else ""
        self.s3_client = boto3.client('s3', region_name=aws_region)
        logger.info(f"Initialized S3StorageProvider with bucket: {bucket_name}, prefix: {base_prefix}")
    
    def _get_s3_key(self, path: str) -> str:
        """
        Get the full S3 key by joining with the base prefix.
        
        Args:
            path: The relative path.
            
        Returns:
            The full S3 key.
        """
        return self.base_prefix + path.lstrip('/')
    
    def read_file(self, path: str) -> bytes:
        """Read a file and return its contents as bytes."""
        key = self._get_s3_key(path)
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except ClientError as e:
            logger.error(f"Error reading file {key}: {e}")
            raise IOError(f"Error reading file {key}: {e}")
    
    def write_file(self, path: str, content: Union[str, bytes, BinaryIO]) -> bool:
        """Write content to a file."""
        key = self._get_s3_key(path)
        try:
            # Handle different content types
            if isinstance(content, str):
                content = content.encode('utf-8')
            elif isinstance(content, BinaryIO):
                content = content.read()
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=content
            )
            return True
        except ClientError as e:
            logger.error(f"Error writing file {key}: {e}")
            return False
    
    def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
        """List files in a directory, optionally filtered by pattern."""
        prefix = self._get_s3_key(directory)
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            files = []
            
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('/'):  # Skip directory markers
                            continue
                        # Convert to relative path
                        rel_path = key[len(self.base_prefix):]
                        files.append(rel_path)
            
            return files
        except ClientError as e:
            logger.error(f"Error listing directory {prefix}: {e}")
            raise IOError(f"Error listing directory {prefix}: {e}")
    
    def delete_file(self, path: str) -> bool:
        """Delete a file."""
        key = self._get_s3_key(path)
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            logger.error(f"Error deleting file {key}: {e}")
            return False
    
    def move_file(self, source: str, destination: str) -> bool:
        """Move a file from source to destination."""
        source_key = self._get_s3_key(source)
        dest_key = self._get_s3_key(destination)
        try:
            # Copy the object
            self.s3_client.copy_object(
                CopySource={'Bucket': self.bucket_name, 'Key': source_key},
                Bucket=self.bucket_name,
                Key=dest_key
            )
            # Delete the original
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=source_key)
            return True
        except ClientError as e:
            logger.error(f"Error moving file from {source_key} to {dest_key}: {e}")
            return False
    
    def file_exists(self, path: str) -> bool:
        """Check if a file exists."""
        key = self._get_s3_key(path)
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            logger.error(f"Error checking file existence {key}: {e}")
            return False
    
    def create_directory(self, path: str) -> bool:
        """Create a directory (represented by a zero-length object with trailing slash)."""
        key = self._get_s3_key(path).rstrip('/') + '/'
        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            logger.error(f"Error creating directory {key}: {e}")
            return False
    
    def get_file_size(self, path: str) -> int:
        """Get the size of a file in bytes."""
        key = self._get_s3_key(path)
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return response['ContentLength']
        except ClientError as e:
            logger.error(f"Error getting file size for {key}: {e}")
            raise IOError(f"Error getting file size for {key}: {e}")
    
    def get_file_modified_time(self, path: str) -> float:
        """Get the last modified time of a file."""
        key = self._get_s3_key(path)
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return response['LastModified'].timestamp()
        except ClientError as e:
            logger.error(f"Error getting file modified time for {key}: {e}")
            raise IOError(f"Error getting file modified time for {key}: {e}") 