"""
Test suite for Google Drive file listing functionality.

This module contains comprehensive tests for the Google Drive client
that handles OAuth authentication and file listing operations.
"""

import pytest
import json
import os
from unittest.mock import Mock, patch, mock_open
from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError


class TestDriveClient:
    """Test class for Google Drive client functionality."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.test_credentials_file = "credentials.json"
        self.test_credentials = {
            "web": {
                "client_id": "test-client-id",
                "project_id": "test-project",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_secret": "test-client-secret",
                "redirect_uris": ["http://localhost:8080/"]
            }
        }
        
        self.test_token_data = {
            "token": "test-access-token",
            "refresh_token": "test-refresh-token",
            "token_uri": "https://oauth2.googleapis.com/token",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "scopes": ["https://www.googleapis.com/auth/drive.readonly"],
            "expiry": "2024-12-31T23:59:59Z"
        }
        
        self.mock_file_list_response = {
            "files": [
                {
                    "id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
                    "name": "Test Document",
                    "mimeType": "application/vnd.google-apps.document",
                    "createdTime": "2024-01-01T00:00:00.000Z",
                    "modifiedTime": "2024-01-01T00:00:00.000Z"
                },
                {
                    "id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms2",
                    "name": "Test Spreadsheet",
                    "mimeType": "application/vnd.google-apps.spreadsheet",
                    "createdTime": "2024-01-02T00:00:00.000Z",
                    "modifiedTime": "2024-01-02T00:00:00.000Z"
                }
            ]
        }

    def test_load_credentials_from_file_success(self):
        """Test successful loading of credentials from credentials.json file."""
        with patch("builtins.open", mock_open(read_data=json.dumps(self.test_credentials))):
            with patch("json.load") as mock_json_load:
                mock_json_load.return_value = self.test_credentials
                
                # Import here to avoid import issues during test discovery
                from drive_client import DriveClient
                
                client = DriveClient()
                credentials = client._load_credentials()
                
                assert credentials == self.test_credentials
                mock_json_load.assert_called_once()

    def test_load_credentials_file_not_found(self):
        """Test handling when credentials.json file is not found."""
        with patch("builtins.open", side_effect=FileNotFoundError("File not found")):
            from drive_client import DriveClient
            
            client = DriveClient()
            
            with pytest.raises(FileNotFoundError, match="credentials.json file not found"):
                client._load_credentials()

    def test_load_credentials_invalid_json(self):
        """Test handling of invalid JSON in credentials.json file."""
        with patch("builtins.open", mock_open(read_data="invalid json")):
            with patch("json.load", side_effect=json.JSONDecodeError("Invalid JSON", "", 0)):
                from drive_client import DriveClient
                
                client = DriveClient()
                
                with pytest.raises(json.JSONDecodeError):
                    client._load_credentials()

    def test_validate_credentials_structure_valid(self):
        """Test validation of valid credentials structure."""
        from drive_client import DriveClient
        
        client = DriveClient()
        
        # Should not raise any exception
        client._validate_credentials_structure(self.test_credentials)

    def test_validate_credentials_structure_missing_web(self):
        """Test validation fails when 'web' key is missing."""
        invalid_credentials = {"invalid": "structure"}
        
        from drive_client import DriveClient
        
        client = DriveClient()
        
        with pytest.raises(ValueError, match="Invalid credentials structure"):
            client._validate_credentials_structure(invalid_credentials)

    def test_validate_credentials_structure_missing_required_fields(self):
        """Test validation fails when required fields are missing."""
        incomplete_credentials = {
            "web": {
                "client_id": "test-id",
                # Missing other required fields
            }
        }
        
        from drive_client import DriveClient
        
        client = DriveClient()
        
        with pytest.raises(ValueError, match="Missing required credential fields"):
            client._validate_credentials_structure(incomplete_credentials)

    @patch('os.path.exists')
    @patch('google_auth_oauthlib.flow.InstalledAppFlow.from_client_config')
    def test_authenticate_success(self, mock_from_client_config, mock_exists):
        """Test successful OAuth authentication flow."""
        # Mock the flow and credentials
        mock_flow = Mock()
        mock_credentials = Mock()
        mock_credentials.to_json.return_value = '{"token": "test"}'
        mock_flow.run_local_server.return_value = mock_credentials
        mock_from_client_config.return_value = mock_flow
        mock_exists.return_value = False  # No existing token
        
        from drive_client import DriveClient
        
        client = DriveClient()
        
        credentials = client.authenticate(self.test_credentials)
        
        assert credentials == mock_credentials
        mock_from_client_config.assert_called_once()
        mock_flow.run_local_server.assert_called_once_with(port=8080)

    @patch('google_auth_oauthlib.flow.InstalledAppFlow.from_client_config')
    def test_authenticate_invalid_credentials(self, mock_from_client_config):
        """Test authentication failure with invalid credentials."""
        mock_from_client_config.side_effect = Exception("Invalid credentials")
        
        from drive_client import DriveClient
        
        client = DriveClient()
        
        with pytest.raises(Exception, match="Authentication failed"):
            client.authenticate(self.test_credentials)

    @patch('googleapiclient.discovery.build')
    def test_list_files_success(self, mock_build):
        """Test successful file listing from Google Drive."""
        # Mock the service and files list
        mock_service = Mock()
        mock_files = Mock()
        mock_list = Mock()
        
        mock_list.execute.return_value = self.mock_file_list_response
        mock_files.list.return_value = mock_list
        mock_service.files.return_value = mock_files
        mock_build.return_value = mock_service
        
        from drive_client import DriveClient
        
        client = DriveClient()
        files, next_page_token = client.list_files(mock_service)
        
        assert len(files) == 2
        assert files[0]["name"] == "Test Document"
        assert files[1]["name"] == "Test Spreadsheet"
        assert next_page_token is None  # No next page token in mock response
        mock_files.list.assert_called_once_with(
            pageSize=100,
            fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime)"
        )

    @patch('googleapiclient.discovery.build')
    def test_list_files_api_error(self, mock_build):
        """Test handling of Google Drive API errors."""
        mock_service = Mock()
        mock_files = Mock()
        mock_list = Mock()
        
        # Simulate API error
        mock_list.execute.side_effect = HttpError(
            resp=Mock(status=403),
            content=b'{"error": {"message": "Forbidden"}}'
        )
        mock_files.list.return_value = mock_list
        mock_service.files.return_value = mock_files
        mock_build.return_value = mock_service
        
        from drive_client import DriveClient
        
        client = DriveClient()
        
        with pytest.raises(HttpError):
            client.list_files(mock_service)

    @patch('googleapiclient.discovery.build')
    def test_list_files_empty_response(self, mock_build):
        """Test handling of empty file list response."""
        mock_service = Mock()
        mock_files = Mock()
        mock_list = Mock()
        
        mock_list.execute.return_value = {"files": []}
        mock_files.list.return_value = mock_list
        mock_service.files.return_value = mock_files
        mock_build.return_value = mock_service
        
        from drive_client import DriveClient
        
        client = DriveClient()
        files, next_page_token = client.list_files(mock_service)
        
        assert len(files) == 0
        assert next_page_token is None

    @patch('googleapiclient.discovery.build')
    def test_list_files_pagination(self, mock_build):
        """Test file listing with pagination functionality."""
        mock_service = Mock()
        mock_files = Mock()
        mock_list = Mock()
        
        # Test 1: First page with nextPageToken
        first_page_response = {
            "files": [{"id": "1", "name": "File 1"}],
            "nextPageToken": "next-token"
        }
        
        # Test 2: Second page with specific page_token parameter
        second_page_response = {
            "files": [{"id": "2", "name": "File 2"}],
            "nextPageToken": None
        }
        
        mock_list.execute.side_effect = [first_page_response, second_page_response]
        mock_files.list.return_value = mock_list
        mock_service.files.return_value = mock_files
        mock_build.return_value = mock_service
        
        from drive_client import DriveClient
        
        client = DriveClient()
        
        # Test first page (no page_token)
        files, next_page_token = client.list_files(mock_service)
        assert len(files) == 1
        assert files[0]["name"] == "File 1"
        assert next_page_token == "next-token"
        
        # Test second page (with page_token)
        files, next_page_token = client.list_files(mock_service, page_size=50, page_token="next-token")
        assert len(files) == 1
        assert files[0]["name"] == "File 2"
        assert next_page_token is None
        
        # Verify API calls
        assert mock_files.list.call_count == 2
        mock_files.list.assert_any_call(
            pageSize=100,
            fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime)"
        )
        mock_files.list.assert_any_call(
            pageSize=50,
            pageToken="next-token",
            fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime)"
        )

    @patch('googleapiclient.discovery.build')
    def test_list_files_api_error(self, mock_build):
        """Test successful creation of Google Drive service."""
        with patch('drive_client.build') as mock_build:
            mock_service = Mock()
            mock_build.return_value = mock_service
            
            # Create a mock credentials object
            mock_credentials = Mock()
            
            from drive_client import DriveClient
            
            client = DriveClient()
            service = client.get_drive_service(mock_credentials)
            
            assert service == mock_service
            mock_build.assert_called_once_with(
                'drive', 'v3', credentials=mock_credentials
            )

    def test_get_drive_service_build_error(self):
        """Test handling of service build errors."""
        with patch('drive_client.build', side_effect=Exception("Build error")):
            from drive_client import DriveClient
            
            # Create a mock credentials object
            mock_credentials = Mock()
            
            client = DriveClient()
            
            with pytest.raises(Exception, match="Failed to create Drive service"):
                client.get_drive_service(mock_credentials)

    def test_main_function_success(self):
        """Test successful execution of main function."""
        with patch('drive_client.DriveClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            
            mock_credentials = {"web": {"client_id": "test"}}
            mock_service = Mock()
            mock_files = [{"name": "test.txt"}]
            
            mock_client._load_credentials.return_value = mock_credentials
            mock_client.authenticate.return_value = Mock()  # Mock credentials object
            mock_client.get_drive_service.return_value = mock_service
            # Mock list_files to return (files, next_page_token) tuple
            mock_client.list_files.return_value = (mock_files, None)
            
            from drive_client import main
            
            # Should not raise any exception
            main()

    def test_main_function_authentication_failure(self):
        """Test main function handling authentication failure."""
        with patch('drive_client.DriveClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            
            # Make the client.main() method raise FileNotFoundError
            mock_client.main.side_effect = FileNotFoundError("File not found")
            
            from drive_client import main
            
            with pytest.raises(FileNotFoundError):
                main()

    def test_scope_validation(self):
        """Test that the correct OAuth scope is used."""
        from drive_client import DriveClient
        
        client = DriveClient()
        
        # Verify the scope constant is correct
        assert client.SCOPE == "https://www.googleapis.com/auth/drive.readonly"

    def test_redirect_uri_validation(self):
        """Test that the correct redirect URI is used."""
        from drive_client import DriveClient
        
        client = DriveClient()
        
        # Verify the redirect URI constant is correct
        assert client.REDIRECT_URI == "http://localhost:8080/"


if __name__ == "__main__":
    pytest.main([__file__])
