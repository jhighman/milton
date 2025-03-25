"""
Tests for the base StorageProvider interface.
"""

import pytest
from abc import ABC
from storage_providers.base import StorageProvider

def test_storage_provider_is_abstract():
    """Test that StorageProvider is an abstract base class."""
    assert issubclass(StorageProvider, ABC)

def test_storage_provider_abstract_methods():
    """Test that all required methods are abstract."""
    # Create a concrete class that doesn't implement any methods
    class ConcreteProvider(StorageProvider):
        pass
    
    # Try to instantiate the concrete class
    with pytest.raises(TypeError):
        ConcreteProvider()
    
    # Create a concrete class that implements all methods
    class CompleteProvider(StorageProvider):
        def read_file(self, path: str) -> bytes:
            return b""
        
        def write_file(self, path: str, content: Any) -> bool:
            return True
        
        def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
            return []
        
        def delete_file(self, path: str) -> bool:
            return True
        
        def move_file(self, source: str, destination: str) -> bool:
            return True
        
        def file_exists(self, path: str) -> bool:
            return True
        
        def create_directory(self, path: str) -> bool:
            return True
        
        def get_file_size(self, path: str) -> int:
            return 0
        
        def get_file_modified_time(self, path: str) -> float:
            return 0.0
    
    # This should work without raising TypeError
    provider = CompleteProvider()
    assert isinstance(provider, StorageProvider)

def test_storage_provider_logger():
    """Test that StorageProvider initializes a logger."""
    class TestProvider(StorageProvider):
        def read_file(self, path: str) -> bytes:
            return b""
        
        def write_file(self, path: str, content: Any) -> bool:
            return True
        
        def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
            return []
        
        def delete_file(self, path: str) -> bool:
            return True
        
        def move_file(self, source: str, destination: str) -> bool:
            return True
        
        def file_exists(self, path: str) -> bool:
            return True
        
        def create_directory(self, path: str) -> bool:
            return True
        
        def get_file_size(self, path: str) -> int:
            return 0
        
        def get_file_modified_time(self, path: str) -> float:
            return 0.0
    
    provider = TestProvider()
    assert provider.logger is not None
    assert provider.logger.name == "TestProvider" 