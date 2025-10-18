"""
Configuration settings for Kafka producer and schema registry.
"""

import os
from typing import Dict, Any

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'client.id': 'drive-client-producer',
    'acks': 'all',  # Wait for all replicas to acknowledge
    'retries': 3,
    'retry.backoff.ms': 1000,
    'batch.size': 16384,
    'linger.ms': 10,
    'buffer.memory': 33554432,
    'compression.type': 'snappy',
    'max.in.flight.requests.per.connection': 1,
    'enable.idempotence': True,
    'request.timeout.ms': 30000,
    'delivery.timeout.ms': 120000,
}

# Schema Registry Configuration
SCHEMA_REGISTRY_CONFIG = {
    'url': os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081'),
    'auto.register.schemas': True,
    'use.latest.version': True,
    'normalize.schemas': True,
}

# Topic Configuration
TOPIC_CONFIG = {
    'drive_files_topic': os.getenv('DRIVE_FILES_TOPIC', 'drive-files'),
    'schema_name': 'DriveFile',
    'schema_namespace': 'com.universalsearch.drive',
}

# Avro Serializer Configuration
AVRO_SERIALIZER_CONFIG = {
    'auto.register.schemas': True,
    'use.latest.version': True,
    'normalize.schemas': True,
}

def get_producer_config() -> Dict[str, Any]:
    """
    Get the complete producer configuration combining Kafka and Schema Registry settings.
    
    Returns:
        Dict containing the complete producer configuration.
    """
    config = KAFKA_CONFIG.copy()
    config.update({
        'schema.registry.url': SCHEMA_REGISTRY_CONFIG['url'],
        'auto.register.schemas': SCHEMA_REGISTRY_CONFIG['auto.register.schemas'],
        'use.latest.version': SCHEMA_REGISTRY_CONFIG['use.latest.version'],
        'normalize.schemas': SCHEMA_REGISTRY_CONFIG['normalize.schemas'],
    })
    return config

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
