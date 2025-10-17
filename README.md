# Universal Search - Google Drive Integration

This project provides a Python client for listing files from Google Drive using OAuth2 authentication.

## Features

- OAuth2 authentication with Google Drive API
- File listing with pagination support
- Comprehensive error handling
- Test-driven development approach

## Requirements

- Python 3.7+
- Google Cloud Project with Drive API enabled
- OAuth2 credentials configured

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure OAuth2 credentials in `credentials.json`:
```json
{
  "web": {
    "client_id": "your-client-id",
    "project_id": "your-project-id",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_secret": "your-client-secret",
    "redirect_uris": ["http://localhost:8080/"]
  }
}
```

## Testing

The project follows a test-driven development approach. Tests are written first to define the expected behavior.

### Running Tests

1. Run all tests:
```bash
python run_tests.py
```

2. Or run tests directly with pytest:
```bash
pytest test_drive_client.py -v
```

### Test Coverage

The test suite covers:

- **Authentication Tests**: OAuth2 flow, credential validation, token handling
- **File Listing Tests**: Basic listing, pagination, empty responses
- **Error Handling Tests**: API errors, network issues, invalid credentials
- **Configuration Tests**: Credential structure validation, scope verification

### Test Structure

- `TestDriveClient`: Main test class containing all test methods
- Mock objects used for Google API calls to ensure isolated testing
- Comprehensive error scenario coverage
- Edge case testing (empty responses, pagination, etc.)

## Implementation Status

✅ **Complete**: Both the test suite and implementation are complete and all tests pass!

The implementation includes:
- `DriveClient` class with all required methods:
  - `_load_credentials()`: Load OAuth2 credentials from credentials.json
  - `_validate_credentials_structure()`: Validate credential format
  - `authenticate()`: Perform OAuth2 authentication flow
  - `get_drive_service()`: Create Google Drive API service
  - `list_files()`: List files with pagination support
  - `main()`: Main execution function

## Usage

### Basic Usage

```python
from drive_client import DriveClient

# Create client and run the complete flow
client = DriveClient()
client.main()
```

### Advanced Usage

```python
from drive_client import DriveClient

client = DriveClient()

# Load credentials
credentials_config = client._load_credentials()

# Authenticate
credentials = client.authenticate(credentials_config)

# Create service
service = client.get_drive_service(credentials)

# List files
files = client.list_files(service)
print(f"Found {len(files)} files")
```

### Example Script

Run the example script to see the client in action:

```bash
python3 example_usage.py
```

## Configuration

- **OAuth Scope**: `https://www.googleapis.com/auth/drive.readonly`
- **Redirect URI**: `http://localhost:8080/`
- **Credentials File**: `credentials.json`

## Features Implemented

- ✅ OAuth2 authentication with Google Drive API
- ✅ Credential validation and error handling
- ✅ File listing with automatic pagination
- ✅ Comprehensive error handling
- ✅ Complete test coverage (18 tests, all passing)
- ✅ Type hints and documentation
- ✅ Example usage script
