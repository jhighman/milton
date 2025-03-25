"""
Tests for the LocalStorageProvider class.
"""

import os
import pytest
from pathlib import Path
from datetime import datetime

from storage_providers.local_provider import LocalStorageProvider

@pytest.fixture
def temp_dirs(tmp_path):
    """Create temporary directories for testing."""
    input_dir = tmp_path / "drop"
    output_dir = tmp_path / "output"
    archive_dir = tmp_path / "archive"
    cache_dir = tmp_path / "cache"
    
    for dir_path in [input_dir, output_dir, archive_dir, cache_dir]:
        dir_path.mkdir()
    
    return {
        "input": str(input_dir),
        "output": str(output_dir),
        "archive": str(archive_dir),
        "cache": str(cache_dir)
    }

@pytest.fixture
def provider(temp_dirs):
    """Create a LocalStorageProvider instance."""
    return LocalStorageProvider(
        input_folder=temp_dirs["input"],
        output_folder=temp_dirs["output"],
        archive_folder=temp_dirs["archive"],
        cache_folder=temp_dirs["cache"]
    )

def test_init_creates_directories(tmp_path):
    """Test that initialization creates required directories."""
    input_dir = tmp_path / "drop"
    output_dir = tmp_path / "output"
    archive_dir = tmp_path / "archive"
    cache_dir = tmp_path / "cache"
    
    provider = LocalStorageProvider(
        input_folder=str(input_dir),
        output_folder=str(output_dir),
        archive_folder=str(archive_dir),
        cache_folder=str(cache_dir)
    )
    
    assert input_dir.exists()
    assert output_dir.exists()
    assert archive_dir.exists()
    assert cache_dir.exists()

def test_read_file(provider, temp_dirs):
    """Test reading a file."""
    # Create a test file
    test_file = Path(temp_dirs["input"]) / "test.txt"
    test_content = "Hello, World!"
    test_file.write_text(test_content)
    
    # Read the file
    content = provider.read_file(str(test_file))
    assert content == test_content.encode()

def test_read_file_not_found(provider):
    """Test reading a non-existent file."""
    with pytest.raises(FileNotFoundError):
        provider.read_file("nonexistent.txt")

def test_write_file(provider, temp_dirs):
    """Test writing a file."""
    test_file = Path(temp_dirs["output"]) / "test.txt"
    test_content = "Hello, World!"
    
    # Write string content
    success = provider.write_file(str(test_file), test_content)
    assert success is True
    assert test_file.read_text() == test_content
    
    # Write bytes content
    test_file.unlink()
    success = provider.write_file(str(test_file), test_content.encode())
    assert success is True
    assert test_file.read_text() == test_content

def test_write_file_binary(provider, temp_dirs):
    """Test writing binary content."""
    test_file = Path(temp_dirs["output"]) / "test.bin"
    test_content = b"\x00\x01\x02\x03"
    
    success = provider.write_file(str(test_file), test_content)
    assert success is True
    assert test_file.read_bytes() == test_content

def test_list_files(provider, temp_dirs):
    """Test listing files in a directory."""
    # Create test files
    test_dir = Path(temp_dirs["input"])
    (test_dir / "file1.txt").write_text("content1")
    (test_dir / "file2.txt").write_text("content2")
    (test_dir / "subdir").mkdir()
    (test_dir / "subdir" / "file3.txt").write_text("content3")
    
    # List all files
    files = provider.list_files(str(test_dir))
    assert len(files) == 2
    assert "file1.txt" in files
    assert "file2.txt" in files
    
    # List files with pattern
    files = provider.list_files(str(test_dir), "file1.*")
    assert len(files) == 1
    assert files[0] == "file1.txt"

def test_list_files_not_found(provider):
    """Test listing files in a non-existent directory."""
    with pytest.raises(FileNotFoundError):
        provider.list_files("nonexistent")

def test_delete_file(provider, temp_dirs):
    """Test deleting a file."""
    test_file = Path(temp_dirs["input"]) / "test.txt"
    test_file.write_text("content")
    
    success = provider.delete_file(str(test_file))
    assert success is True
    assert not test_file.exists()

def test_delete_file_not_found(provider):
    """Test deleting a non-existent file."""
    success = provider.delete_file("nonexistent.txt")
    assert success is False

def test_move_file(provider, temp_dirs):
    """Test moving a file."""
    source = Path(temp_dirs["input"]) / "source.txt"
    destination = Path(temp_dirs["output"]) / "dest.txt"
    source.write_text("content")
    
    success = provider.move_file(str(source), str(destination))
    assert success is True
    assert not source.exists()
    assert destination.exists()
    assert destination.read_text() == "content"

def test_move_file_not_found(provider, temp_dirs):
    """Test moving a non-existent file."""
    destination = Path(temp_dirs["output"]) / "dest.txt"
    success = provider.move_file("nonexistent.txt", str(destination))
    assert success is False

def test_file_exists(provider, temp_dirs):
    """Test checking if a file exists."""
    test_file = Path(temp_dirs["input"]) / "test.txt"
    test_file.write_text("content")
    
    assert provider.file_exists(str(test_file)) is True
    assert provider.file_exists("nonexistent.txt") is False

def test_create_directory(provider, temp_dirs):
    """Test creating a directory."""
    new_dir = Path(temp_dirs["input"]) / "new_dir"
    
    success = provider.create_directory(str(new_dir))
    assert success is True
    assert new_dir.exists()
    assert new_dir.is_dir()

def test_create_directory_exists(provider, temp_dirs):
    """Test creating a directory that already exists."""
    existing_dir = Path(temp_dirs["input"])
    
    success = provider.create_directory(str(existing_dir))
    assert success is True
    assert existing_dir.exists()
    assert existing_dir.is_dir()

def test_get_file_size(provider, temp_dirs):
    """Test getting file size."""
    test_file = Path(temp_dirs["input"]) / "test.txt"
    test_content = "Hello, World!"
    test_file.write_text(test_content)
    
    size = provider.get_file_size(str(test_file))
    assert size == len(test_content)

def test_get_file_size_not_found(provider):
    """Test getting size of non-existent file."""
    with pytest.raises(FileNotFoundError):
        provider.get_file_size("nonexistent.txt")

def test_get_file_modified_time(provider, temp_dirs):
    """Test getting file modification time."""
    test_file = Path(temp_dirs["input"]) / "test.txt"
    test_file.write_text("content")
    
    mtime = provider.get_file_modified_time(str(test_file))
    assert isinstance(mtime, float)
    assert mtime > 0

def test_get_file_modified_time_not_found(provider):
    """Test getting modification time of non-existent file."""
    with pytest.raises(FileNotFoundError):
        provider.get_file_modified_time("nonexistent.txt") 