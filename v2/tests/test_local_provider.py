"""
Tests for the LocalStorageProvider implementation.
"""

import os
import pytest
import tempfile
from pathlib import Path
from storage_providers.local_provider import LocalStorageProvider

@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir

@pytest.fixture
def provider(temp_dir):
    """Create a LocalStorageProvider instance with test directories."""
    # Create test directories
    input_dir = os.path.join(temp_dir, "input")
    output_dir = os.path.join(temp_dir, "output")
    archive_dir = os.path.join(temp_dir, "archive")
    cache_dir = os.path.join(temp_dir, "cache")
    
    os.makedirs(input_dir)
    os.makedirs(output_dir)
    os.makedirs(archive_dir)
    os.makedirs(cache_dir)
    
    return LocalStorageProvider(
        base_path=temp_dir,
        input_folder="input",
        output_folder="output",
        archive_folder="archive",
        cache_folder="cache"
    )

def test_provider_initialization(provider, temp_dir):
    """Test that the provider is initialized with correct paths."""
    assert isinstance(provider.base_path, Path)
    # Compare resolved paths to handle symlinks
    assert provider.base_path.resolve() == Path(temp_dir).resolve()
    assert provider.input_folder == "input"
    assert provider.output_folder == "output"
    assert provider.archive_folder == "archive"
    assert provider.cache_folder == "cache"

def test_write_and_read_file(provider):
    """Test writing and reading a file."""
    test_content = b"Hello, World!"
    test_path = "test.txt"
    
    # Write file
    assert provider.write_file(test_path, test_content)
    
    # Read file
    content = provider.read_file(test_path)
    assert content == test_content

def test_write_and_read_file_with_string(provider):
    """Test writing and reading a file with string content."""
    test_content = "Hello, World!"
    test_path = "test.txt"
    
    # Write file
    assert provider.write_file(test_path, test_content)
    
    # Read file
    content = provider.read_file(test_path)
    assert content.decode() == test_content

def test_list_files(provider):
    """Test listing files in a directory."""
    # Create test files
    test_files = ["file1.txt", "file2.txt", "subdir/file3.txt"]
    for file in test_files:
        provider.write_file(file, b"test content")
    
    # List files
    files = provider.list_files("")
    assert len(files) >= len(test_files)
    assert all(file in files for file in test_files)

def test_list_files_with_pattern(provider):
    """Test listing files with a pattern."""
    # Create test files
    test_files = ["file1.txt", "file2.txt", "file3.csv"]
    for file in test_files:
        provider.write_file(file, b"test content")
    
    # List files with pattern
    txt_files = provider.list_files("", pattern="*.txt")
    assert len(txt_files) == 2
    assert all(file.endswith(".txt") for file in txt_files)

def test_delete_file(provider):
    """Test deleting a file."""
    test_path = "test.txt"
    provider.write_file(test_path, b"test content")
    
    # Delete file
    assert provider.delete_file(test_path)
    
    # Verify file is deleted
    assert not provider.file_exists(test_path)

def test_move_file(provider):
    """Test moving a file."""
    source = "source.txt"
    destination = "destination.txt"
    test_content = b"test content"
    
    # Create source file
    provider.write_file(source, test_content)
    
    # Move file
    assert provider.move_file(source, destination)
    
    # Verify file was moved
    assert not provider.file_exists(source)
    assert provider.file_exists(destination)
    assert provider.read_file(destination) == test_content

def test_file_exists(provider):
    """Test checking if a file exists."""
    test_path = "test.txt"
    
    # File should not exist initially
    assert not provider.file_exists(test_path)
    
    # Create file
    provider.write_file(test_path, b"test content")
    
    # File should exist now
    assert provider.file_exists(test_path)

def test_create_directory(provider):
    """Test creating a directory."""
    test_dir = "test_dir"
    
    # Create directory
    assert provider.create_directory(test_dir)
    
    # Verify directory exists
    # The create_directory method creates the directory directly under base_path
    full_path = provider.base_path / test_dir
    assert full_path.is_dir()

def test_get_file_size(provider):
    """Test getting file size."""
    test_path = "test.txt"
    test_content = b"test content"
    
    # Create file
    provider.write_file(test_path, test_content)
    
    # Get file size
    size = provider.get_file_size(test_path)
    assert size == len(test_content)

def test_get_file_modified_time(provider):
    """Test getting file modification time."""
    test_path = "test.txt"
    test_content = b"test content"
    
    # Create file
    provider.write_file(test_path, test_content)
    
    # Get modification time
    mtime = provider.get_file_modified_time(test_path)
    assert isinstance(mtime, float)
    assert mtime > 0

def test_error_handling(provider):
    """Test error handling for various operations."""
    # Test reading non-existent file
    with pytest.raises(FileNotFoundError):
        provider.read_file("nonexistent.txt")
    
    # Test deleting non-existent file
    assert not provider.delete_file("nonexistent.txt")
    
    # Test moving non-existent file
    assert not provider.move_file("nonexistent.txt", "destination.txt")
    
    # Test getting size of non-existent file
    with pytest.raises(FileNotFoundError):
        provider.get_file_size("nonexistent.txt")
    
    # Test getting modification time of non-existent file
    with pytest.raises(FileNotFoundError):
        provider.get_file_modified_time("nonexistent.txt")

def test_path_normalization(provider):
    """Test path normalization."""
    # Test with different path separators
    test_path = "test\\dir/file.txt"
    normalized_path = provider._normalize_path(test_path)
    assert normalized_path == "test/dir/file.txt"
    
    # Test with absolute path
    abs_path = os.path.abspath("test.txt")
    normalized_abs_path = provider._normalize_path(abs_path)
    assert normalized_abs_path == "test.txt" 