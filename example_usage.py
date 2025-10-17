#!/usr/bin/env python3
"""
Example usage of the Google Drive client.

This script demonstrates how to use the DriveClient to list files
from Google Drive using OAuth2 authentication.
"""

from drive_client import DriveClient


def example_usage():
    """Example of how to use the Google Drive client."""
    print("Google Drive File Listing Example")
    print("=" * 40)
    
    # Create a Drive client instance
    client = DriveClient()
    
    try:
        # Load credentials from tokens.json
        print("1. Loading credentials...")
        credentials_config = client._load_credentials()
        print("   ✅ Credentials loaded successfully")
        
        # Authenticate with Google Drive
        print("2. Authenticating with Google Drive...")
        print("   📱 Please complete the OAuth2 flow in your browser")
        credentials = client.authenticate(credentials_config)
        print("   ✅ Authentication successful")
        
        # Create Google Drive service
        print("3. Creating Drive service...")
        service = client.get_drive_service(credentials)
        print("   ✅ Drive service created")
        
        # List files
        print("4. Fetching files from Google Drive...")
        files = client.list_files(service)
        print(f"   ✅ Found {len(files)} files")
        
        # Display results
        print("\n📁 Files in your Google Drive:")
        print("-" * 50)
        
        if files:
            for i, file in enumerate(files[:10], 1):  # Show first 10 files
                print(f"{i:2d}. {file.get('name', 'Unknown')}")
                print(f"    📄 Type: {file.get('mimeType', 'Unknown')}")
                print(f"    📅 Modified: {file.get('modifiedTime', 'Unknown')}")
                print()
            
            if len(files) > 10:
                print(f"... and {len(files) - 10} more files")
        else:
            print("No files found in Google Drive.")
            
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print("Please ensure credentials.json exists and contains valid OAuth2 credentials.")
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Please check your credentials and try again.")


if __name__ == "__main__":
    example_usage()