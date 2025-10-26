"""
Configuration settings for Schema Registry and Avro serialization.
"""

import os
from typing import Dict, Any

# Schema Registry Configuration
# Default to localhost for local development, use schema-registry:8081 for Docker networking
SCHEMA_REGISTRY_CONFIG = {
    'url': os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081'),
}

# Schema Configuration
SCHEMA_CONFIG = {
    'drive_file_schema_name': 'DriveFile',
    'parsed_file_schema_name': 'ParsedFile',
    'file_chunk_schema_name': 'FileChunk',
    'schema_namespace': 'com.universalsearch.drive',
}

# Avro Serializer Configuration
# Note: Cannot enable both 'use.latest.version' and 'auto.register.schemas' at the same time
# - auto.register.schemas: True means schemas will be auto-registered if not present
# - use.latest.version: False means we use the schema ID embedded in the message
AVRO_SERIALIZER_CONFIG = {
    'auto.register.schemas': True,
    'normalize.schemas': True,
}

def get_schema_registry_config() -> Dict[str, Any]:
    """
    Get the Schema Registry client configuration.
    
    Returns:
        Dict containing the Schema Registry configuration (only 'url' key).
    """
    return {'url': SCHEMA_REGISTRY_CONFIG['url']}

def get_avro_serializer_config() -> Dict[str, Any]:
    """
    Get the Avro serializer configuration.
    
    Returns:
        Dict containing the Avro serializer configuration.
    """
    return AVRO_SERIALIZER_CONFIG.copy()

def get_drive_file_schema_name() -> str:
    """
    Get the Avro schema name for drive files.
    
    Returns:
        String containing the schema name.
    """
    return SCHEMA_CONFIG['drive_file_schema_name']

def get_schema_namespace() -> str:
    """
    Get the Avro schema namespace.
    
    Returns:
        String containing the schema namespace.
    """
    return SCHEMA_CONFIG['schema_namespace']

def get_parsed_schema_name() -> str:
    """
    Get the Avro schema name for parsed files.
    
    Returns:
        String containing the schema name.
    """
    return SCHEMA_CONFIG['parsed_file_schema_name']

def get_chunk_schema_name() -> str:
    """
    Get the Avro schema name for file chunks.
    
    Returns:
        String containing the schema name.
    """
    return SCHEMA_CONFIG['file_chunk_schema_name']

