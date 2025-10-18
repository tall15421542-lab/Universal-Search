"""
Google Drive Client for listing files using OAuth2 authentication.

This module provides a client for authenticating with Google Drive API
and listing files from the user's Google Drive.
"""

from asyncio import create_eager_task_factory
import json
import os
from typing import Dict, List, Any, Optional
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class DriveClient:
    """Client for Google Drive API operations."""
    
    # OAuth2 configuration constants
    SCOPE = "https://www.googleapis.com/auth/drive.readonly"
    REDIRECT_URI = "http://localhost:8080/"
    CREDENTIALS_FILE = "credentials.json"
    
    def __init__(self):
        """Initialize the Drive client."""
        self.credentials = None
        self.service = None
    
    def _load_credentials(self) -> Dict[str, Any]:
        """
        Load OAuth2 credentials from credentials.json file.
        
        Returns:
            Dict containing OAuth2 credentials configuration.
            
        Raises:
            FileNotFoundError: If credentials.json file is not found.
            json.JSONDecodeError: If credentials.json contains invalid JSON.
        """
        try:
            with open(self.CREDENTIALS_FILE, 'r') as file:
                credentials = json.load(file)
                return credentials
        except FileNotFoundError:
            raise FileNotFoundError(f"{self.CREDENTIALS_FILE} file not found")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON in {self.CREDENTIALS_FILE}: {e}", e.doc, e.pos)
    
    def _validate_credentials_structure(self, credentials: Dict[str, Any]) -> None:
        """
        Validate that credentials have the required structure.
        
        Args:
            credentials: Credentials dictionary to validate.
            
        Raises:
            ValueError: If credentials structure is invalid.
        """
        if not isinstance(credentials, dict) or 'web' not in credentials:
            raise ValueError("Invalid credentials structure")
        
        web_config = credentials['web']
        required_fields = [
            'client_id', 'project_id', 'auth_uri', 'token_uri',
            'auth_provider_x509_cert_url', 'client_secret', 'redirect_uris'
        ]
        
        missing_fields = [field for field in required_fields if field not in web_config]
        if missing_fields:
            raise ValueError(f"Missing required credential fields: {', '.join(missing_fields)}")
    
    def authenticate(self, credentials_config: Dict[str, Any]) -> Credentials:
        """
        Perform OAuth2 authentication flow.
        
        Args:
            credentials_config: OAuth2 credentials configuration.
            
        Returns:
            Authenticated credentials object.
            
        Raises:
            Exception: If authentication fails.
        """
        try:
            # Check if token.json exists
            if os.path.exists('token.json'):
                # Load credentials from token.json
                try:
                    self.credentials = Credentials.from_authorized_user_file('token.json', [self.SCOPE])
                    
                    # Check if credentials are valid
                    if self.credentials.valid:
                        return self.credentials
                    
                    # If invalid, try to refresh
                    if self.credentials.expired and self.credentials.refresh_token:
                        try:
                            self.credentials.refresh(Request())
                            # If refresh successful, save and return
                            with open('token.json', 'w') as token:
                                token.write(self.credentials.to_json())
                            return self.credentials
                        except Exception:
                            # Refresh failed, will fall back to installed credentials
                            pass
                    
                except (ValueError, json.JSONDecodeError):
                    # Invalid token file, will fall back to installed credentials
                    pass
            
            # Fall back to installed credentials and write to token.json
            # Convert web credentials to installed format for InstalledAppFlow
            self._validate_credentials_structure(credentials_config)
            
            # Convert web credentials to installed app format
            installed_credentials = {
                "installed": credentials_config['web']
            }
            
            # Create OAuth2 flow using InstalledAppFlow
            flow = InstalledAppFlow.from_client_config(
                installed_credentials, [self.SCOPE]
            )
            self.credentials = flow.run_local_server(port=8080)
            
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(self.credentials.to_json())
            
            return self.credentials
            
        except Exception as e:
            raise Exception(f"Authentication failed: {str(e)}")
    
    def get_drive_service(self, credentials: Credentials):
        """
        Create Google Drive API service.
        
        Args:
            credentials: Authenticated credentials object.
            
        Returns:
            Google Drive API service object.
            
        Raises:
            Exception: If service creation fails.
        """
        try:
            service = build('drive', 'v3', credentials=credentials)
            return service
        except Exception as e:
            raise Exception(f"Failed to create Drive service: {str(e)}")
    
    def list_files(self, service, page_size: int = 100, page_token: Optional[str] = None) -> tuple[List[Dict[str, Any]], Optional[str]]:
        """
        List files from Google Drive with pagination support.
        
        Args:
            service: Google Drive API service object.
            page_size: Number of files to fetch per page.
            page_token: Token for the next page of results. If None, starts from the beginning.
            
        Returns:
            Tuple containing:
            - List of file dictionaries containing file metadata for the current page
            - Next page token (None if this is the last page)
            
        Raises:
            HttpError: If Google Drive API returns an error.
        """
        try:
            # Prepare query parameters
            query_params = {
                'pageSize': page_size,
                'fields': 'nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, size, webViewLink, webContentLink, parents, owners)'
            }
            
            if page_token:
                query_params['pageToken'] = page_token
            
            # Execute API call
            results = service.files().list(**query_params).execute()
            files = results.get('files', [])
            next_page_token = results.get('nextPageToken')
            
            return files, next_page_token
                    
        except HttpError as e:
            # Re-raise HttpError to be handled by caller
            raise e
    
    def main(self) -> None:
        """
        Main function to authenticate and list files from Google Drive.
        
        This function orchestrates the complete flow:
        1. Load credentials from credentials.json
        2. Authenticate with Google Drive API
        3. Create Drive service
        4. List files
        5. Display results
        """
        try:
            # Load credentials
            print("Loading credentials...")
            credentials_config = self._load_credentials()
            
            # Authenticate (method now handles file reading internally)
            print("Authenticating with Google Drive...")
            credentials = self.authenticate(credentials_config)
            
            # Create Drive service
            print("Creating Drive service...")
            service = self.get_drive_service(credentials)
            
            # List files with pagination
            print("Fetching files from Google Drive...")
            all_files = []
            page_token = None
            
            while True:
                files, next_page_token = self.list_files(service, page_size=100, page_token=page_token)
                all_files.extend(files)
                
                if not next_page_token:
                    break
                    
                page_token = next_page_token
            
            # Display results
            print(f"\nFound {len(all_files)} files:")
            print("-" * 50)
            
            if all_files:
                for i, file in enumerate(all_files, 1):
                    print(f"{i}. {file.get('name', 'Unknown')}")
                    print(f"   ID: {file.get('id', 'Unknown')}")
                    print(f"   Type: {file.get('mimeType', 'Unknown')}")
                    print(f"   Created: {file.get('createdTime', 'Unknown')}")
                    print(f"   Modified: {file.get('modifiedTime', 'Unknown')}")
                    print()
            else:
                print("No files found in Google Drive.")
                
        except FileNotFoundError as e:
            print(f"Error: {e}")
            print("Please ensure credentials.json exists and contains valid OAuth2 credentials.")
            raise  # Re-raise the exception for test compatibility
        except Exception as e:
            print(f"Error: {e}")
            print("Please check your credentials and try again.")
            raise  # Re-raise the exception for test compatibility


def main():
    """Entry point for the application."""
    client = DriveClient()
    client.main()


if __name__ == "__main__":
    main()
