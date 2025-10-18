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
        
        # Example 1: List all files using helper method
        print("4a. Fetching all files using helper method...")
        all_files = client.list_all_files(service)
        print(f"   ✅ Found {len(all_files)} files")
        
        # Example 2: Manual pagination control
        print("\n4b. Manual pagination example (first 2 pages):")
        page_token = None
        page_count = 0
        total_files = 0
        
        while page_count < 2:  # Limit to 2 pages for example
            page_count += 1
            files, next_page_token = client.list_files(service, page_size=5, page_token=page_token)
            total_files += len(files)
            
            print(f"   📄 Page {page_count}: {len(files)} files")
            for file in files:
                print(f"      - {file.get('name', 'Unknown')}")
            
            if not next_page_token:
                print("   ✅ No more pages available")
                break
                
            page_token = next_page_token
        
        print(f"   ✅ Total files from manual pagination: {total_files}")
        
        # Display results from helper method
        print("\n📁 Sample files from your Google Drive:")
        print("-" * 50)
        
        if all_files:
            for i, file in enumerate(all_files[:10], 1):  # Show first 10 files
                print(f"{i:2d}. {file.get('name', 'Unknown')}")
                print(f"    📄 Type: {file.get('mimeType', 'Unknown')}")
                print(f"    📅 Modified: {file.get('modifiedTime', 'Unknown')}")
                print()
            
            if len(all_files) > 10:
                print(f"... and {len(all_files) - 10} more files")
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