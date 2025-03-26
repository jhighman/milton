"""
Pytest fixtures for integration tests.
"""

import pytest
import os
import tempfile
from unittest.mock import Mock, patch
from datetime import datetime
from botocore.exceptions import ClientError

@pytest.fixture
def mock_s3_integration():
    """Create a mock S3 client for integration tests."""
    with patch('boto3.client') as mock_client:
        s3 = Mock()
        
        # Store files in memory for integration tests
        s3._files = {}
        
        # Mock get_object to return the actual content that was put
        def mock_get_object(Bucket, Key):
            # For integration tests, we want to return the actual content
            # that was written with put_object
            if Key in s3._files:
                content = s3._files[Key]
                return {
                    'Body': Mock(read=lambda: content),
                    'LastModified': datetime.now()
                }
            else:
                error_response = {'Error': {'Code': 'NoSuchKey', 'Message': 'Not found'}}
                raise ClientError(error_response, 'GetObject')
        
        # Mock put_object to store the content
        def mock_put_object(Bucket, Key, Body=None):
            s3._files[Key] = Body
            return {}
        
        # Mock list_objects_v2 to return the actual files
        def mock_list_objects_v2(Bucket, Prefix):
            contents = []
            for key in s3._files:
                if key.startswith(Prefix):
                    contents.append({'Key': key})
            
            if contents:
                return {'Contents': contents}
            return {}
        
        # Mock head_object to check if file exists and get metadata
        def mock_head_object(Bucket, Key):
            if Key in s3._files:
                # Make sure we return the actual content length
                content = s3._files[Key]
                return {
                    'ContentLength': len(content),
                    'LastModified': datetime.now()
                }
            else:
                error_response = {'Error': {'Code': '404', 'Message': 'Not found'}}
                raise ClientError(error_response, 'HeadObject')
        
        # Mock delete_object
        def mock_delete_object(Bucket, Key):
            if Key in s3._files:
                del s3._files[Key]
            return {}
        
        # Mock copy_object
        def mock_copy_object(Bucket, Key, CopySource):
            source_bucket = CopySource['Bucket']
            source_key = CopySource['Key']
            
            if source_key in s3._files:
                s3._files[Key] = s3._files[source_key]
                return {}
            else:
                error_response = {'Error': {'Code': 'NoSuchKey', 'Message': 'Not found'}}
                raise ClientError(error_response, 'CopyObject')
        
        # Configure the mock
        s3.get_object.side_effect = mock_get_object
        s3.put_object.side_effect = mock_put_object
        s3.list_objects_v2.side_effect = mock_list_objects_v2
        s3.head_object.side_effect = mock_head_object
        s3.delete_object.side_effect = mock_delete_object
        s3.copy_object.side_effect = mock_copy_object
        
        mock_client.return_value = s3
        yield s3