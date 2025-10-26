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
        Perform OAuth2 authentication flow and initialize the Drive service.
        
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
                        self.service = self.get_drive_service(self.credentials)
                        return self.credentials
                    
                    # If invalid, try to refresh
                    if self.credentials.expired and self.credentials.refresh_token:
                        try:
                            self.credentials.refresh(Request())
                            # If refresh successful, save and return
                            with open('token.json', 'w') as token:
                                token.write(self.credentials.to_json())
                            self.service = self.get_drive_service(self.credentials)
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
            
            # Initialize the Drive service
            self.service = self.get_drive_service(self.credentials)
            
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
    
    def list_files(self, page_size: int = 100, page_token: Optional[str] = None) -> tuple[List[Dict[str, Any]], Optional[str]]:
        """
        List files from Google Drive with pagination support.
        
        Args:
            page_size: Number of files to fetch per page.
            page_token: Token for the next page of results. If None, starts from the beginning.
            
        Returns:
            Tuple containing:
            - List of file dictionaries containing file metadata for the current page
            - Next page token (None if this is the last page)
            
        Raises:
            HttpError: If Google Drive API returns an error.
        """
        if not self.service:
            raise Exception("Drive service not initialized. Call authenticate() first.")
            
        try:
            # Prepare query parameters
            query_params = {
                'pageSize': page_size,
                'fields': 'nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, size, webViewLink, webContentLink, parents, owners)'
            }
            
            if page_token:
                query_params['pageToken'] = page_token
            
            # Execute API call
            results = self.service.files().list(**query_params).execute()
            files = results.get('files', [])
            next_page_token = results.get('nextPageToken')
            
            return files, next_page_token
                    
        except HttpError as e:
            # Re-raise HttpError to be handled by caller
            raise e
    
    
    def is_pdf_file(self, mime_type: str) -> bool:
        """
        Check if the given MIME type represents a PDF file.
        
        This method knows how Google Drive represents PDF files,
        including both standard PDFs and Google Docs exported as PDFs.
        
        Args:
            mime_type: MIME type string from Google Drive API
            
        Returns:
            True if it's a PDF file, False otherwise
        """
        # Standard PDF MIME type
        if mime_type == "application/pdf":
            return True
        
        # Google Docs that can be exported as PDF
        pdf_exportable_types = [
            "application/vnd.google-apps.document",  # Google Docs
        ]
        
        if mime_type in pdf_exportable_types:
            return True
        
        return False
    
    def get_file_bytes(self, file_id: str) -> bytes:
        """
        Get the raw byte data of a file from Google Drive by its ID.
        
        Args:
            file_id: The ID of the file to get bytes from.
            
        Returns:
            The raw byte data of the file.
            
        Raises:
            HttpError: If Google Drive API returns an error.
            FileNotFoundError: If the file ID doesn't exist.
            Exception: If getting file bytes fails for other reasons.
        """
        if not self.service:
            raise Exception("Drive service not initialized. Call authenticate() first.")
            
        try:
            # First, get file metadata to determine file type
            file_metadata = self.service.files().get(fileId=file_id).execute()
            mime_type = file_metadata.get('mimeType', '')
            
            # Determine if this is a Google Workspace file that needs export
            is_google_doc = mime_type.startswith('application/vnd.google-apps.')
            
            if is_google_doc:
                # For Google Docs, Sheets, Slides, etc., we need to export
                # Determine export format based on mime type
                export_formats = {
                    'application/vnd.google-apps.document': 'application/pdf',
                    'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    'application/vnd.google-apps.presentation': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
                    'application/vnd.google-apps.drawing': 'image/png'
                }
                
                export_mime_type = export_formats.get(mime_type, 'application/pdf')
                
                # Export the file and get bytes
                request = self.service.files().export_media(fileId=file_id, mimeType=export_mime_type)
                file_bytes = request.execute()
            else:
                # For regular files, get bytes directly
                request = self.service.files().get_media(fileId=file_id)
                file_bytes = request.execute()
            
            return file_bytes
            
        except HttpError as e:
            if e.resp.status == 404:
                raise FileNotFoundError(f"File with ID '{file_id}' not found")
            else:
                raise e
        except Exception as e:
            raise Exception(f"Failed to get file bytes: {str(e)}")
    
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
            
            # List files with pagination
            print("Fetching files from Google Drive...")
            all_files = []
            page_token = None
            
            while True:
                files, next_page_token = self.list_files(page_size=100, page_token=page_token)
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
                
                # Demonstrate download functionality with the first file
                if all_files:
                    print("\n" + "=" * 50)
                    print("DOWNLOAD DEMONSTRATION")
                    print("=" * 50)
                    first_file = all_files[0]
                    file_id = first_file.get('id')
                    file_name = first_file.get('name', 'Unknown')
                    
                    if file_id:
                        print(f"Downloading first file: '{file_name}' (ID: {file_id})")
                        try:
                            downloaded_path = self.download_file_by_id(file_id)
                            print(f"Successfully downloaded file to: {downloaded_path}")
                        except Exception as download_error:
                            print(f"Download failed: {download_error}")
                    else:
                        print("Cannot download file: No file ID available")
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


def download_file_example(file_id: str, output_path: Optional[str] = None) -> None:
    """
    Example function demonstrating how to download a specific file by ID.
    
    Args:
        file_id: The Google Drive file ID to download.
        output_path: Optional path where the file should be saved.
    """
    client = DriveClient()
    
    try:
        # Load credentials
        credentials_config = client._load_credentials()
        
        # Authenticate (this now initializes the service automatically)
        credentials = client.authenticate(credentials_config)
        
        # Download the file
        downloaded_path = client.download_file_by_id(file_id, output_path)
        print(f"File downloaded successfully to: {downloaded_path}")
        
    except Exception as e:
        print(f"Error downloading file: {e}")


def get_file_bytes_example(file_id: str) -> bytes:
    """
    Example function demonstrating how to get raw byte data from a Google Drive file.
    
    Args:
        file_id: The Google Drive file ID to get bytes from.
        
    Returns:
        The raw byte data of the file.
    """
    client = DriveClient()
    
    try:
        # Load credentials
        credentials_config = client._load_credentials()
        
        # Authenticate (this now initializes the service automatically)
        credentials = client.authenticate(credentials_config)
        
        # Get file bytes
        file_bytes = client.get_file_bytes(file_id)
        print(f"Successfully retrieved {len(file_bytes)} bytes from file")
        
        return file_bytes
        
    except Exception as e:
        print(f"Error getting file bytes: {e}")
        raise


def main():
    """Entry point for the application."""
    client = DriveClient()
    client.main()


if __name__ == "__main__":
    main()
