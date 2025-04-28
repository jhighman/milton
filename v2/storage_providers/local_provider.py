"""
Local filesystem storage provider implementation.
"""
import os
import shutil
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Union
from .base_provider import BaseStorageProvider

logger = logging.getLogger(__name__)

class LocalStorageProvider(BaseStorageProvider):
    """Storage provider that uses local filesystem."""
    
    def __init__(self):
        """Initialize local storage provider."""
        super().__init__()
        self.base_path: Optional[Path] = None
        self.input_path: Optional[Path] = None
        self.output_path: Optional[Path] = None
        self.archive_path: Optional[Path] = None
        self.cache_path: Optional[Path] = None
        
    def initialize(self, config: Dict[str, Any]):
        """Initialize with configuration dictionary.
        
        Args:
            config: Configuration dictionary containing paths
                Required keys:
                - base_path: Base directory for all operations
                Optional keys:
                - input_path: Directory for input files (default: base_path/input)
                - output_path: Directory for output files (default: base_path/output)
                - archive_path: Directory for archived files (default: base_path/archive)
                - cache_path: Directory for cached files (default: base_path/cache)
        """
        if not isinstance(config, dict):
            raise ValueError("Configuration must be a dictionary")
            
        if 'base_path' not in config:
            raise ValueError("base_path is required in configuration")
            
        # Set up base path
        self.base_path = Path(config['base_path'])
        
        # Set up other paths with defaults
        self.input_path = Path(config.get('input_path', self.base_path / 'input'))
        self.output_path = Path(config.get('output_path', self.base_path / 'output'))
        self.archive_path = Path(config.get('archive_path', self.base_path / 'archive'))
        self.cache_path = Path(config.get('cache_path', self.base_path / 'cache'))
        
        # Create all directories
        for path in [self.base_path, self.input_path, self.output_path, self.archive_path, self.cache_path]:
            try:
                path.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Created directory: {path}")
            except Exception as e:
                logger.error(f"Error creating directory {path}: {str(e)}")
                raise
                
        logger.info(f"Initialized LocalStorageProvider with paths:")
        logger.info(f"  base_path: {self.base_path}")
        logger.info(f"  input_path: {self.input_path}")
        logger.info(f"  output_path: {self.output_path}")
        logger.info(f"  archive_path: {self.archive_path}")
        logger.info(f"  cache_path: {self.cache_path}")
        
    def _ensure_initialized(self):
        """Ensure provider is initialized before use."""
        if not self.base_path:
            raise RuntimeError("LocalStorageProvider not initialized - call initialize() first")
            
    def save_file(self, file_path: str, content: Any) -> bool:
        """Save content to a file."""
        self._ensure_initialized()
        try:
            full_path = self._get_full_path(file_path)
            os.makedirs(full_path.parent, exist_ok=True)
            
            if isinstance(content, (dict, list)):
                full_path.write_text(json.dumps(content, indent=2))
            else:
                full_path.write_text(str(content))
                    
            logger.debug(f"Successfully saved file: {full_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving file {file_path}: {str(e)}")
            return False
            
    def read_file(self, file_path: str) -> Optional[Any]:
        """Read content from a file."""
        self._ensure_initialized()
        try:
            full_path = self._get_full_path(file_path)
            if not full_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
                
            content = full_path.read_text()
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                return content
                    
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise
            
    def delete_file(self, file_path: str) -> bool:
        """Delete a file."""
        self._ensure_initialized()
        try:
            full_path = self._get_full_path(file_path)
            if full_path.exists():
                full_path.unlink()
                logger.debug(f"Successfully deleted file: {full_path}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error deleting file {file_path}: {str(e)}")
            return False
            
    def list_files(self, directory: str = "") -> List[str]:
        """List files in directory."""
        self._ensure_initialized()
        try:
            full_path = self._get_full_path(directory)
            if not full_path.exists():
                return []
                
            files = []
            for path in full_path.rglob('*'):
                if path.is_file():
                    rel_path = str(path.relative_to(self.base_path))
                    files.append(rel_path)
            return files
            
        except Exception as e:
            logger.error(f"Error listing files in {directory}: {str(e)}")
            return []
    
    def _normalize_path(self, path: str) -> str:
        """Normalize a path by converting backslashes to forward slashes and making it relative.

        Args:
            path: Path to normalize

        Returns:
            Normalized path
        """
        # Convert to Path object
        path_obj = Path(path)
        
        # Make absolute path relative to base_path
        if path_obj.is_absolute():
            try:
                path_obj = path_obj.relative_to(self.base_path)
            except ValueError:
                # If paths are on different drives or not relative, just use the filename
                path_obj = Path(path_obj.name)
        
        # Convert to string with forward slashes
        normalized = str(path_obj).replace('\\', '/')
        
        # Remove leading slashes
        normalized = normalized.lstrip('/')
        
        return normalized

    def _get_full_path(self, path: str) -> Path:
        """Get the full filesystem path for a given path.

        Args:
            path: Path relative to base directory

        Returns:
            Full filesystem path as Path object
        """
        self._ensure_initialized()
        normalized_path = self._normalize_path(path)
        # Convert base_path to Path if it's a string
        if isinstance(self.base_path, str):
            self.base_path = Path(self.base_path)
        return self.base_path / normalized_path
    
    def write_file(self, path: str, content: Any) -> bool:
        """Write content to a file."""
        return self.save_file(path, content)
    
    def move_file(self, source: str, dest: str) -> bool:
        """Move a file from source to destination."""
        self._ensure_initialized()
        try:
            source_path = self._get_full_path(source)
            dest_path = self._get_full_path(dest)
            
            # Create destination directory if it doesn't exist
            os.makedirs(dest_path.parent, exist_ok=True)
            
            # Move the file
            shutil.move(str(source_path), str(dest_path))
            return True
            
        except Exception as e:
            logger.error(f"Error moving file from {source} to {dest}: {str(e)}")
            return False
    
    def file_exists(self, path: str) -> bool:
        """Check if a file exists."""
        self._ensure_initialized()
        try:
            full_path = self._get_full_path(path)
            return full_path.exists()
        except Exception as e:
            logger.error(f"Error checking if file exists {path}: {str(e)}")
            return False
    
    def create_directory(self, path: str) -> bool:
        """Create a directory."""
        self._ensure_initialized()
        try:
            full_path = self._get_full_path(path)
            full_path.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"Error creating directory {path}: {str(e)}")
            return False
    
    def get_file_size(self, path: str) -> int:
        """Get file size in bytes."""
        self._ensure_initialized()
        try:
            full_path = self._get_full_path(path)
            return full_path.stat().st_size
        except FileNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error getting file size {path}: {str(e)}")
            raise
    
    def get_file_modified_time(self, path: str) -> float:
        """Get file last modified time as Unix timestamp."""
        self._ensure_initialized()
        try:
            full_path = self._get_full_path(path)
            return full_path.stat().st_mtime
        except FileNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error getting file modified time {path}: {str(e)}")
            raise 