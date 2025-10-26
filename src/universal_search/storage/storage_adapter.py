"""
Storage abstraction layer for parsed content.

This module provides a storage abstraction that allows switching between
local filesystem and cloud storage providers without changing the application code.
"""

import os
import json
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pathlib import Path


class StorageAdapter(ABC):
    """Abstract base class for storage adapters."""
    
    @abstractmethod
    def save(self, path: str, content: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Save content to storage.
        
        Args:
            path: Storage path for the content
            content: Text content to save
            metadata: Optional metadata to store alongside content
            
        Returns:
            True if successful
            
        Raises:
            OSError: If file system operations fail
            IOError: If file write operations fail
            RuntimeError: For other unexpected errors
        """
        pass
    
    @abstractmethod
    def load(self, path: str) -> Optional[str]:
        """
        Load content from storage.
        
        Args:
            path: Storage path of the content
            
        Returns:
            Content string if found, None otherwise
        """
        pass
    
    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        Check if content exists at path.
        
        Args:
            path: Storage path to check
            
        Returns:
            True if exists, False otherwise
        """
        pass
    
    @abstractmethod
    def delete(self, path: str) -> bool:
        """
        Delete content from storage.
        
        Args:
            path: Storage path to delete
            
        Returns:
            True if successful, False otherwise
        """
        pass


class LocalStorageAdapter(StorageAdapter):
    """Local filesystem storage adapter."""
    
    def __init__(self, storage_root: str = None):
        """
        Initialize local storage adapter.
        
        Args:
            storage_root: Root directory for storage. Defaults to environment variable
                         STORAGE_ROOT or './storage' if not set.
        """
        self.storage_root = Path(storage_root or os.getenv('STORAGE_ROOT', './storage'))
        self.storage_root.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        (self.storage_root / 'parsed').mkdir(exist_ok=True)
        (self.storage_root / 'metadata').mkdir(exist_ok=True)
    
    def _get_full_path(self, path: str) -> Path:
        """Get full filesystem path for a storage path."""
        return self.storage_root / path
    
    def _get_metadata_path(self, path: str) -> Path:
        """Get metadata file path for a storage path."""
        return self.storage_root / 'metadata' / f"{Path(path).stem}.json"
    
    def save(self, path: str, content: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Save content to local filesystem.
        
        Args:
            path: Storage path for the content
            content: Text content to save
            metadata: Optional metadata to store alongside content
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            OSError: If file system operations fail
            IOError: If file write operations fail
        """
        try:
            full_path = self._get_full_path(path)
            
            # Ensure parent directory exists
            full_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write content
            with open(full_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            # Write metadata if provided
            if metadata:
                metadata_path = self._get_metadata_path(path)
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            return True
            
        except OSError as e:
            error_msg = f"Failed to save content to {path}: {str(e)}"
            print(error_msg)
            raise OSError(error_msg) from e
        except IOError as e:
            error_msg = f"Failed to write content to {path}: {str(e)}"
            print(error_msg)
            raise IOError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error saving to {path}: {str(e)}"
            print(error_msg)
            raise RuntimeError(error_msg) from e
    
    def load(self, path: str) -> Optional[str]:
        """
        Load content from local filesystem.
        
        Args:
            path: Storage path of the content
            
        Returns:
            Content string if found, None otherwise
        """
        try:
            full_path = self._get_full_path(path)
            with open(full_path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            return None
        except Exception as e:
            print(f"Error loading from local storage at {path}: {str(e)}")
            return None
    
    def exists(self, path: str) -> bool:
        """
        Check if content exists in local filesystem.
        
        Args:
            path: Storage path to check
            
        Returns:
            True if exists, False otherwise
        """
        full_path = self._get_full_path(path)
        return full_path.exists()
    
    def delete(self, path: str) -> bool:
        """
        Delete content from local filesystem.
        
        Args:
            path: Storage path to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            full_path = self._get_full_path(path)
            
            if full_path.exists():
                full_path.unlink()
            
            # Also delete metadata if it exists
            metadata_path = self._get_metadata_path(path)
            if metadata_path.exists():
                metadata_path.unlink()
            
            return True
            
        except Exception as e:
            print(f"Error deleting from local storage at {path}: {str(e)}")
            return False


class S3StorageAdapter(StorageAdapter):
    """S3-compatible storage adapter (placeholder for future implementation)."""
    
    def __init__(self, bucket_name: str, region: str = 'us-east-1'):
        """
        Initialize S3 storage adapter.
        
        Args:
            bucket_name: S3 bucket name
            region: AWS region
        """
        self.bucket_name = bucket_name
        self.region = region
        # TODO: Implement S3 client initialization
    
    def save(self, path: str, content: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Placeholder for S3 save implementation."""
        raise NotImplementedError("S3StorageAdapter not yet implemented")
    
    def load(self, path: str) -> Optional[str]:
        """Placeholder for S3 load implementation."""
        raise NotImplementedError("S3StorageAdapter not yet implemented")
    
    def exists(self, path: str) -> bool:
        """Placeholder for S3 exists implementation."""
        raise NotImplementedError("S3StorageAdapter not yet implemented")
    
    def delete(self, path: str) -> bool:
        """Placeholder for S3 delete implementation."""
        raise NotImplementedError("S3StorageAdapter not yet implemented")


class StorageFactory:
    """Factory class for creating storage adapters based on configuration."""
    
    @staticmethod
    def create_adapter(config: Dict[str, Any]) -> StorageAdapter:
        """
        Create a storage adapter based on configuration.
        
        Args:
            config: Storage configuration dictionary containing:
                - storage_type: 'local' or 's3'
                - storage_root: Root directory for local storage (optional)
                - s3_bucket_name: S3 bucket name (required for S3)
                - aws_region: AWS region (optional, defaults to 'us-east-1')
        
        Returns:
            Configured storage adapter instance
            
        Raises:
            ValueError: If storage_type is unsupported or required config is missing
        """
        storage_type = config.get('storage_type', 'local').lower()
        
        if storage_type == 'local':
            storage_root = config.get('storage_root', './storage')
            return LocalStorageAdapter(storage_root)
        elif storage_type == 's3':
            bucket_name = config.get('s3_bucket_name')
            if not bucket_name:
                raise ValueError("s3_bucket_name is required for S3 storage")
            region = config.get('aws_region', 'us-east-1')
            return S3StorageAdapter(bucket_name, region)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")


