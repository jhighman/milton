# Storage Providers

A unified interface for file storage operations, supporting both local filesystem and AWS S3 storage.

## Features

- Abstract base class defining a common interface for storage operations
- Local filesystem implementation using standard Python file operations
- AWS S3 implementation using boto3
- Factory class for creating storage provider instances based on configuration
- Comprehensive test coverage for all implementations

## Installation

1. Install the package dependencies:
```bash
pip install -r requirements.txt
```

2. For AWS S3 support, configure your AWS credentials using one of the following methods:
   - Environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`)
   - AWS credentials file (`~/.aws/credentials`)
   - IAM role (when running on AWS)

## Usage

### Basic Usage

```python
from storage_providers import StorageProviderFactory

# Create a local storage provider
local_config = {
    'type': 'local',
    'base_path': '/path/to/base/directory'
}
local_provider = StorageProviderFactory.create_provider(local_config)

# Create an S3 storage provider
s3_config = {
    'type': 's3',
    'aws_region': 'us-east-1',
    'bucket_name': 'my-bucket',
    'base_prefix': 'my/prefix/'
}
s3_provider = StorageProviderFactory.create_provider(s3_config)

# Use the provider
content = b"Hello, World!"
provider.write_file('test.txt', content)
read_content = provider.read_file('test.txt')
```

### Available Operations

All storage providers implement the following operations:

- `read_file(path: str) -> bytes`: Read a file and return its contents
- `write_file(path: str, content: Union[str, bytes, BinaryIO]) -> bool`: Write content to a file
- `list_files(directory: str, pattern: Optional[str] = None) -> List[str]`: List files in a directory
- `delete_file(path: str) -> bool`: Delete a file
- `move_file(source: str, destination: str) -> bool`: Move a file
- `file_exists(path: str) -> bool`: Check if a file exists
- `create_directory(path: str) -> bool`: Create a directory
- `get_file_size(path: str) -> int`: Get the size of a file
- `get_file_modified_time(path: str) -> float`: Get the last modified time of a file

## Testing

Run the tests using:
```bash
python -m unittest tests/test_storage_providers.py
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 