"""
Configuration settings for Kafka producer and schema registry.
"""

import os
from typing import Dict, Any

# Kafka Configuration
# Default to localhost for local development, use kafka:9092 for Docker networking
KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'client.id': 'drive-client-producer',
    'acks': 'all',  # Wait for all replicas to acknowledge
    'retries': 3,
    'retry.backoff.ms': 1000,
    'batch.size': 16384,
    'linger.ms': 100,
    'compression.type': 'snappy',
    'enable.idempotence': True,
    'request.timeout.ms': 30000,
    'delivery.timeout.ms': 120000,
}

# Schema Registry Configuration
# Default to localhost for local development, use schema-registry:8081 for Docker networking
SCHEMA_REGISTRY_CONFIG = {
    'url': os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081'),
}

# Topic Configuration
TOPIC_CONFIG = {
    'drive_files_topic': os.getenv('DRIVE_FILES_TOPIC', 'drive-files'),
    'schema_name': 'DriveFile',
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

def get_producer_config() -> Dict[str, Any]:
    """
    Get the Kafka producer configuration.
    
    Note: Schema Registry settings should NOT be included in the producer config.
    They are used separately when initializing the SchemaRegistryClient and AvroSerializer.
    
    Returns:
        Dict containing the Kafka producer configuration.
    """
    return KAFKA_CONFIG.copy()

def get_topic_name() -> str:
    """
    Get the Kafka topic name for drive files.
    
    Returns:
        String containing the topic name.
    """
    return TOPIC_CONFIG['drive_files_topic']

def get_schema_name() -> str:
    """
    Get the Avro schema name.
    
    Returns:
        String containing the schema name.
    """
    return TOPIC_CONFIG['schema_name']

def get_schema_namespace() -> str:
    """
    Get the Avro schema namespace.
    
    Returns:
        String containing the schema namespace.
    """
    return TOPIC_CONFIG['schema_namespace']

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
