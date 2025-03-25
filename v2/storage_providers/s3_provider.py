"""
S3 storage provider implementation using boto3 for AWS S3 operations.
"""

import os
import boto3
from typing import List, Optional, Union, BinaryIO
from pathlib import Path
from botocore.exceptions import ClientError
from datetime import datetime

from .base import StorageProvider

class S3StorageProvider(StorageProvider):
    """Implementation of StorageProvider for AWS S3 operations."""
    
    def __init__(self, aws_region: str, input_bucket: str, input_prefix: str,
                 output_bucket: str, output_prefix: str, archive_bucket: str,
                 archive_prefix: str, cache_bucket: str, cache_prefix: str):
        """
        Initialize the S3 storage provider.
        
        Args:
            aws_region (str): AWS region (e.g., 'us-east-1')
            input_bucket (str): S3 bucket for input files
            input_prefix (str): Prefix for input files in the bucket
            output_bucket (str): S3 bucket for output files
            output_prefix (str): Prefix for output files in the bucket
            archive_bucket (str): S3 bucket for archived files
            archive_prefix (str): Prefix for archived files in the bucket
            cache_bucket (str): S3 bucket for cached files
            cache_prefix (str): Prefix for cached files in the bucket
        """
        super().__init__()
        self.aws_region = aws_region
        self.input_bucket = input_bucket
        self.input_prefix = input_prefix.rstrip('/')
        self.output_bucket = output_bucket
        self.output_prefix = output_prefix.rstrip('/')
        self.archive_bucket = archive_bucket
        self.archive_prefix = archive_prefix.rstrip('/')
        self.cache_bucket = cache_bucket
        self.cache_prefix = cache_prefix.rstrip('/')
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3', region_name=aws_region)
    
    def _get_s3_key(self, path: str, prefix: str) -> str:
        """Convert local path to S3 key."""
        return f"{prefix}/{path.lstrip('/')}"
    
    def _get_local_path(self, s3_key: str, prefix: str) -> str:
        """Convert S3 key to local path."""
        return s3_key[len(prefix):].lstrip('/')
    
    def read_file(self, path: str) -> bytes:
        """Read a file from S3 and return its contents as bytes."""
        try:
            # Determine which bucket and prefix to use based on the path
            if path.startswith(self.input_prefix):
                bucket = self.input_bucket
                prefix = self.input_prefix
            elif path.startswith(self.output_prefix):
                bucket = self.output_bucket
                prefix = self.output_prefix
            elif path.startswith(self.archive_prefix):
                bucket = self.archive_bucket
                prefix = self.archive_prefix
            elif path.startswith(self.cache_prefix):
                bucket = self.cache_bucket
                prefix = self.cache_prefix
            else:
                raise ValueError(f"Path {path} does not match any configured prefix")
            
            s3_key = self._get_s3_key(path, prefix)
            response = self.s3_client.get_object(Bucket=bucket, Key=s3_key)
            return response['Body'].read()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                self.logger.error(f"File not found in S3: {path}")
                raise FileNotFoundError(f"File not found: {path}")
            self.logger.error(f"Error reading file from S3 {path}: {str(e)}")
            raise IOError(f"Error reading file: {str(e)}")
    
    def write_file(self, path: str, content: Union[str, bytes, BinaryIO]) -> bool:
        """Write content to a file in S3."""
        try:
            # Determine which bucket and prefix to use based on the path
            if path.startswith(self.input_prefix):
                bucket = self.input_bucket
                prefix = self.input_prefix
            elif path.startswith(self.output_prefix):
                bucket = self.output_bucket
                prefix = self.output_prefix
            elif path.startswith(self.archive_prefix):
                bucket = self.archive_bucket
                prefix = self.archive_prefix
            elif path.startswith(self.cache_prefix):
                bucket = self.cache_bucket
                prefix = self.cache_prefix
            else:
                raise ValueError(f"Path {path} does not match any configured prefix")
            
            # Handle different content types
            if isinstance(content, str):
                content = content.encode('utf-8')
            elif isinstance(content, BinaryIO):
                content = content.read()
            
            s3_key = self._get_s3_key(path, prefix)
            self.s3_client.put_object(Bucket=bucket, Key=s3_key, Body=content)
            return True
        except ClientError as e:
            self.logger.error(f"Error writing file to S3 {path}: {str(e)}")
            return False
    
    def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
        """List files in an S3 directory, optionally filtered by pattern."""
        try:
            # Determine which bucket and prefix to use based on the directory
            if directory.startswith(self.input_prefix):
                bucket = self.input_bucket
                prefix = self.input_prefix
            elif directory.startswith(self.output_prefix):
                bucket = self.output_bucket
                prefix = self.output_prefix
            elif directory.startswith(self.archive_prefix):
                bucket = self.archive_bucket
                prefix = self.archive_prefix
            elif directory.startswith(self.cache_prefix):
                bucket = self.cache_bucket
                prefix = self.cache_prefix
            else:
                raise ValueError(f"Directory {directory} does not match any configured prefix")
            
            s3_prefix = self._get_s3_key(directory, prefix)
            if not s3_prefix.endswith('/'):
                s3_prefix += '/'
            
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)
            files = []
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    local_path = self._get_local_path(obj['Key'], prefix)
                    if pattern:
                        if Path(local_path).match(pattern):
                            files.append(local_path)
                    else:
                        files.append(local_path)
            
            return files
        except ClientError as e:
            self.logger.error(f"Error listing directory in S3 {directory}: {str(e)}")
            raise IOError(f"Error listing directory: {str(e)}")
    
    def delete_file(self, path: str) -> bool:
        """Delete a file from S3."""
        try:
            # Determine which bucket and prefix to use based on the path
            if path.startswith(self.input_prefix):
                bucket = self.input_bucket
                prefix = self.input_prefix
            elif path.startswith(self.output_prefix):
                bucket = self.output_bucket
                prefix = self.output_prefix
            elif path.startswith(self.archive_prefix):
                bucket = self.archive_bucket
                prefix = self.archive_prefix
            elif path.startswith(self.cache_prefix):
                bucket = self.cache_bucket
                prefix = self.cache_prefix
            else:
                raise ValueError(f"Path {path} does not match any configured prefix")
            
            s3_key = self._get_s3_key(path, prefix)
            self.s3_client.delete_object(Bucket=bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return False
            self.logger.error(f"Error deleting file from S3 {path}: {str(e)}")
            return False
    
    def move_file(self, source: str, destination: str) -> bool:
        """Move a file within or between S3 buckets."""
        try:
            # Determine source bucket and prefix
            if source.startswith(self.input_prefix):
                source_bucket = self.input_bucket
                source_prefix = self.input_prefix
            elif source.startswith(self.output_prefix):
                source_bucket = self.output_bucket
                source_prefix = self.output_prefix
            elif source.startswith(self.archive_prefix):
                source_bucket = self.archive_bucket
                source_prefix = self.archive_prefix
            elif source.startswith(self.cache_prefix):
                source_bucket = self.cache_bucket
                source_prefix = self.cache_prefix
            else:
                raise ValueError(f"Source path {source} does not match any configured prefix")
            
            # Determine destination bucket and prefix
            if destination.startswith(self.input_prefix):
                dest_bucket = self.input_bucket
                dest_prefix = self.input_prefix
            elif destination.startswith(self.output_prefix):
                dest_bucket = self.output_bucket
                dest_prefix = self.output_prefix
            elif destination.startswith(self.archive_prefix):
                dest_bucket = self.archive_bucket
                dest_prefix = self.archive_prefix
            elif destination.startswith(self.cache_prefix):
                dest_bucket = self.cache_bucket
                dest_prefix = self.cache_prefix
            else:
                raise ValueError(f"Destination path {destination} does not match any configured prefix")
            
            source_key = self._get_s3_key(source, source_prefix)
            dest_key = self._get_s3_key(destination, dest_prefix)
            
            # Copy the object
            self.s3_client.copy_object(
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                Bucket=dest_bucket,
                Key=dest_key
            )
            
            # Delete the original
            self.s3_client.delete_object(Bucket=source_bucket, Key=source_key)
            return True
        except ClientError as e:
            self.logger.error(f"Error moving file in S3 from {source} to {destination}: {str(e)}")
            return False
    
    def file_exists(self, path: str) -> bool:
        """Check if a file exists in S3."""
        try:
            # Determine which bucket and prefix to use based on the path
            if path.startswith(self.input_prefix):
                bucket = self.input_bucket
                prefix = self.input_prefix
            elif path.startswith(self.output_prefix):
                bucket = self.output_bucket
                prefix = self.output_prefix
            elif path.startswith(self.archive_prefix):
                bucket = self.archive_bucket
                prefix = self.archive_prefix
            elif path.startswith(self.cache_prefix):
                bucket = self.cache_bucket
                prefix = self.cache_prefix
            else:
                return False
            
            s3_key = self._get_s3_key(path, prefix)
            self.s3_client.head_object(Bucket=bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            self.logger.error(f"Error checking file existence in S3 {path}: {str(e)}")
            return False
    
    def create_directory(self, path: str) -> bool:
        """Create a directory in S3 (creates an empty object with trailing slash)."""
        try:
            # Determine which bucket and prefix to use based on the path
            if path.startswith(self.input_prefix):
                bucket = self.input_bucket
                prefix = self.input_prefix
            elif path.startswith(self.output_prefix):
                bucket = self.output_bucket
                prefix = self.output_prefix
            elif path.startswith(self.archive_prefix):
                bucket = self.archive_bucket
                prefix = self.archive_prefix
            elif path.startswith(self.cache_prefix):
                bucket = self.cache_bucket
                prefix = self.cache_prefix
            else:
                raise ValueError(f"Path {path} does not match any configured prefix")
            
            s3_key = self._get_s3_key(path, prefix)
            if not s3_key.endswith('/'):
                s3_key += '/'
            
            self.s3_client.put_object(Bucket=bucket, Key=s3_key, Body='')
            return True
        except ClientError as e:
            self.logger.error(f"Error creating directory in S3 {path}: {str(e)}")
            return False
    
    def get_file_size(self, path: str) -> int:
        """Get the size of a file in S3."""
        try:
            # Determine which bucket and prefix to use based on the path
            if path.startswith(self.input_prefix):
                bucket = self.input_bucket
                prefix = self.input_prefix
            elif path.startswith(self.output_prefix):
                bucket = self.output_bucket
                prefix = self.output_prefix
            elif path.startswith(self.archive_prefix):
                bucket = self.archive_bucket
                prefix = self.archive_prefix
            elif path.startswith(self.cache_prefix):
                bucket = self.cache_bucket
                prefix = self.cache_prefix
            else:
                raise ValueError(f"Path {path} does not match any configured prefix")
            
            s3_key = self._get_s3_key(path, prefix)
            response = self.s3_client.head_object(Bucket=bucket, Key=s3_key)
            return response['ContentLength']
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.logger.error(f"File not found in S3: {path}")
                raise FileNotFoundError(f"File not found: {path}")
            self.logger.error(f"Error getting file size from S3 {path}: {str(e)}")
            raise IOError(f"Error getting file size: {str(e)}")
    
    def get_file_modified_time(self, path: str) -> float:
        """Get the last modified time of a file in S3."""
        try:
            # Determine which bucket and prefix to use based on the path
            if path.startswith(self.input_prefix):
                bucket = self.input_bucket
                prefix = self.input_prefix
            elif path.startswith(self.output_prefix):
                bucket = self.output_bucket
                prefix = self.output_prefix
            elif path.startswith(self.archive_prefix):
                bucket = self.archive_bucket
                prefix = self.archive_prefix
            elif path.startswith(self.cache_prefix):
                bucket = self.cache_bucket
                prefix = self.cache_prefix
            else:
                raise ValueError(f"Path {path} does not match any configured prefix")
            
            s3_key = self._get_s3_key(path, prefix)
            response = self.s3_client.head_object(Bucket=bucket, Key=s3_key)
            return response['LastModified'].timestamp()
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.logger.error(f"File not found in S3: {path}")
                raise FileNotFoundError(f"File not found: {path}")
            self.logger.error(f"Error getting modified time from S3 {path}: {str(e)}")
            raise IOError(f"Error getting modified time: {str(e)}") 