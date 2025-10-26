"""
Configuration settings for Kafka producer and consumer.
"""

import os
from typing import Dict, Any

# Common Kafka Configuration (shared between producer and consumer)
KAFKA_COMMON_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'request.timeout.ms': 30000,
}

# Producer-specific Configuration
KAFKA_PRODUCER_CONFIG = {
    'acks': 'all',  # Wait for all replicas to acknowledge
    'retries': 3,
    'retry.backoff.ms': 1000,
    'batch.size': 16384,
    'linger.ms': 100,
    'compression.type': 'snappy',
    'enable.idempotence': True,
    'delivery.timeout.ms': 120000,
}

# Consumer-specific Configuration
KAFKA_CONSUMER_CONFIG = {
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 1000,
}

# Topic Configuration
TOPIC_CONFIG = {
    'drive_files_topic': os.getenv('DRIVE_FILES_TOPIC', 'drive-files'),
    'parsed_files_topic': os.getenv('PARSED_FILES_TOPIC', 'drive-files-parsed'),
    'chunks_topic': os.getenv('CHUNKS_TOPIC', 'drive-files-chunks'),
}

def get_producer_config(client_id: str) -> Dict[str, Any]:
    """
    Get the Kafka producer configuration.
    
    Args:
        client_id: Client ID to use for the Kafka producer.
    
    Returns:
        Dict containing the Kafka producer configuration.
    """
    config = KAFKA_COMMON_CONFIG.copy()
    producer_config = KAFKA_PRODUCER_CONFIG.copy()
    
    # Set client.id from parameter
    producer_config['client.id'] = client_id
    
    config.update(producer_config)
    return config

def get_consumer_config(client_id: str, group_id: str) -> Dict[str, Any]:
    """
    Get the Kafka consumer configuration.
    
    Args:
        client_id: Client ID to use for the Kafka consumer.
        group_id: Consumer group ID to use for the Kafka consumer.
    
    Returns:
        Dict containing the consumer configuration.
    """
    config = KAFKA_COMMON_CONFIG.copy()
    consumer_config = KAFKA_CONSUMER_CONFIG.copy()
    
    # Set client.id and group.id from parameters
    consumer_config['client.id'] = client_id
    consumer_config['group.id'] = group_id
    
    config.update(consumer_config)
    return config

def get_drive_files_topic() -> str:
    """
    Get the Kafka topic name for drive files.
    
    Returns:
        String containing the topic name.
    """
    return TOPIC_CONFIG['drive_files_topic']

def get_parsed_files_topic() -> str:
    """
    Get the Kafka topic name for parsed files.
    
    Returns:
        String containing the topic name.
    """
    return TOPIC_CONFIG['parsed_files_topic']

def get_chunks_topic() -> str:
    """
    Get the Kafka topic name for file chunks.
    
    Returns:
        String containing the topic name.
    """
    return TOPIC_CONFIG['chunks_topic']