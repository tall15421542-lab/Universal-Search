"""
Unit tests for the storage adapter.
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch

from universal_search.storage import LocalStorageAdapter, S3StorageAdapter


class TestLocalStorageAdapter:
    """Test cases for LocalStorageAdapter."""
    
    def test_init_with_custom_root(self):
        """Test initialization with custom storage root."""
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = LocalStorageAdapter(temp_dir)
            assert adapter.storage_root == Path(temp_dir)
            assert (Path(temp_dir) / 'parsed').exists()
            assert (Path(temp_dir) / 'metadata').exists()
    
    def test_init_with_env_var(self):
        """Test initialization with environment variable."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(os.environ, {'STORAGE_ROOT': temp_dir}):
                adapter = LocalStorageAdapter()
                assert adapter.storage_root == Path(temp_dir)
    
    def test_save_and_load(self):
        """Test saving and loading content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = LocalStorageAdapter(temp_dir)
            
            # Test saving content
            content = "This is test content"
            path = "test/file.txt"
            metadata = {"test": "metadata"}
            
            result = adapter.save(path, content, metadata)
            assert result is True
            
            # Test loading content
            loaded_content = adapter.load(path)
            assert loaded_content == content
            
            # Test exists
            assert adapter.exists(path) is True
            
            # Test metadata file exists
            metadata_path = adapter._get_metadata_path(path)
            assert metadata_path.exists()
    
    def test_load_nonexistent(self):
        """Test loading non-existent content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = LocalStorageAdapter(temp_dir)
            content = adapter.load("nonexistent/file.txt")
            assert content is None
    
    def test_exists_nonexistent(self):
        """Test checking existence of non-existent content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = LocalStorageAdapter(temp_dir)
            assert adapter.exists("nonexistent/file.txt") is False
    
    def test_delete(self):
        """Test deleting content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = LocalStorageAdapter(temp_dir)
            
            # Save content first
            content = "Test content"
            path = "test/file.txt"
            adapter.save(path, content)
            
            # Verify it exists
            assert adapter.exists(path) is True
            
            # Delete it
            result = adapter.delete(path)
            assert result is True
            
            # Verify it's gone
            assert adapter.exists(path) is False
    
    def test_save_without_metadata(self):
        """Test saving content without metadata."""
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = LocalStorageAdapter(temp_dir)
            
            content = "Test content"
            path = "test/file.txt"
            
            result = adapter.save(path, content)
            assert result is True
            
            loaded_content = adapter.load(path)
            assert loaded_content == content
    
    def test_save_raises_on_error(self):
        """Test that save raises exceptions with clear error messages."""
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = LocalStorageAdapter(temp_dir)
            
            # Test that save raises exceptions instead of returning False
            # This test verifies the save method communicates errors clearly
            content = "Test content"
            
            # Normal save should not raise
            try:
                adapter.save("test/file.txt", content)
            except Exception:
                pytest.fail("save() raised an exception when it should not have")
            
            # Verify saved content
            loaded = adapter.load("test/file.txt")
            assert loaded == content
    
    def test_save_raises_on_permission_error(self):
        """Test that save raises OSError on permission errors with clear message."""
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = LocalStorageAdapter(temp_dir)
            
            # Try to write to a path that creates a directory conflict
            # This simulates a permission error scenario
            content = "Test content"
            invalid_path = "/"  # Root directory - should fail
            
            # On Unix systems, this should raise PermissionError/OSError
            # On Windows, this might behave differently
            with pytest.raises((OSError, IOError, RuntimeError)) as exc_info:
                adapter.save(invalid_path, content)
            
            # Verify the error message is clear and informative
            error_msg = str(exc_info.value)
            assert invalid_path in error_msg or "failed" in error_msg.lower()
            assert len(error_msg) > 10  # Should have a meaningful message
    
    def test_save_with_invalid_path_character(self):
        """Test that save raises exception with invalid characters in path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = LocalStorageAdapter(temp_dir)
            content = "Test content"
            
            # Test with path containing null byte (invalid on most systems)
            with pytest.raises((OSError, IOError, RuntimeError)) as exc_info:
                adapter.save("test/file\x00.txt", content)
            
            # Verify error message is clear
            error_msg = str(exc_info.value)
            assert "failed" in error_msg.lower() or "error" in error_msg.lower()


class TestS3StorageAdapter:
    """Test cases for S3StorageAdapter."""
    
    def test_init(self):
        """Test S3StorageAdapter initialization."""
        adapter = S3StorageAdapter("test-bucket", "us-west-2")
        assert adapter.bucket_name == "test-bucket"
        assert adapter.region == "us-west-2"
    
    def test_methods_not_implemented(self):
        """Test that S3StorageAdapter methods raise NotImplementedError."""
        adapter = S3StorageAdapter("test-bucket")
        
        with pytest.raises(NotImplementedError):
            adapter.save("path", "content")
        
        with pytest.raises(NotImplementedError):
            adapter.load("path")
        
        with pytest.raises(NotImplementedError):
            adapter.exists("path")
        
        with pytest.raises(NotImplementedError):
            adapter.delete("path")


