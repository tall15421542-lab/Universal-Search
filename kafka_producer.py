"""
Kafka Producer for sending Google Drive files using Avro serialization.

This module provides a producer for sending Google Drive file metadata
to Kafka topics using Avro serialization and Confluent Schema Registry.
"""

import json
import time
from typing import Dict, List, Any, Optional
from confluent_kafka import Producer, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from kafka_config import (
    get_producer_config, 
    get_topic_name, 
    get_schema_name, 
    get_schema_namespace,
    get_schema_registry_config,
    get_avro_serializer_config
)


class DriveFileKafkaProducer:
    """Kafka producer for Google Drive file metadata using Avro serialization."""
    
    def __init__(self):
        """
        Initialize the Kafka producer with Avro serialization.
        
        Raises:
            Exception: If producer initialization fails.
        """
        self.producer = None
        self.schema_registry_client = None
        self.avro_serializer = None
        self.topic_name = get_topic_name()
        
        self._initialize_schema_registry()
        self._initialize_producer()
    
    
    def _initialize_schema_registry(self) -> None:
        """
        Initialize the Schema Registry client and Avro serializer.
        
        Raises:
            Exception: If schema registry initialization fails.
        """
        try:
            # Initialize Schema Registry client using config from kafka_config
            schema_registry_config = get_schema_registry_config()
            self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
            
            # Load the Avro schema from file
            schema_path = "schemas/drive_file.avsc"
            with open(schema_path, 'r') as f:
                schema_str = f.read()
            
            # Create Avro serializer with explicit schema
            avro_config = get_avro_serializer_config()
            self.avro_serializer = AvroSerializer(
                self.schema_registry_client,
                schema_str,
                conf=avro_config
            )
        except Exception as e:
            raise Exception(f"Failed to initialize schema registry: {str(e)}")
    
    def _initialize_producer(self) -> None:
        """
        Initialize the Kafka producer.
        
        Raises:
            Exception: If producer initialization fails.
        """
        try:
            config = get_producer_config()
            self.producer = Producer(config)
        except Exception as e:
            raise Exception(f"Failed to initialize Kafka producer: {str(e)}")
    
    def _delivery_callback(self, err, msg) -> None:
        """
        Callback function for message delivery confirmation.
        
        Args:
            err: Error object if delivery failed.
            msg: Message object if delivery succeeded.
        """
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            key = msg.key()
            key_info = f" (key: {key.decode('utf-8')})" if key else ""
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}{key_info}")
    
    def send_file(self, file_data: Dict[str, Any]) -> bool:
        """
        Send a single Google Drive file to Kafka.
        
        Args:
            file_data: File data from Google Drive API.
            
        Returns:
            True if message was queued successfully, False otherwise.
        """
        try:
            # Ensure required fields have defaults
            file_data.setdefault('id', '')
            file_data.setdefault('parents', [])
            
            # Create a proper data structure that matches the Avro schema
            # The Avro serializer expects specific field names and types
            avro_data = {
                'id': file_data.get('id', ''),
                'name': file_data.get('name', ''),
                'mimeType': file_data.get('mimeType'),
                'createdTime': file_data.get('createdTime'),
                'modifiedTime': file_data.get('modifiedTime'),
                'size': int(file_data.get('size', 0)) if file_data.get('size') else None,
                'webViewLink': file_data.get('webViewLink'),
                'webContentLink': file_data.get('webContentLink'),
                'parents': file_data.get('parents', []),
                'owners': []  # Default empty array for owners
            }
            
            print(avro_data)
            # Check if serializer is properly initialized
            if self.avro_serializer is None:
                raise Exception("Avro serializer is not initialized")
            
            # Serialize the data
            serialized_data = self.avro_serializer(
                avro_data,
                SerializationContext(self.topic_name, MessageField.VALUE)
            )
            
            # Use file ID as the message key for proper partitioning
            file_id = file_data.get('id', '')
            if not file_id:
                print(f"Warning: No file ID found for file {file_data.get('name', 'Unknown')}")
                file_id = f"unknown_{int(time.time() * 1000)}"  # Fallback key
            
            # Produce the message with file ID as key
            self.producer.produce(
                topic=self.topic_name,
                key=file_id.encode('utf-8'),  # Kafka keys are bytes
                value=serialized_data,
                callback=self._delivery_callback
            )
            
            print(f"Queued file: {file_data.get('name', 'Unknown')} (ID: {file_id})")
            return True
            
        except Exception as e:
            print(f"Failed to send file {file_data.get('name', 'Unknown')}: {str(e)}")
            return False
    
    def send_files(self, files_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Send multiple Google Drive files to Kafka.
        
        Args:
            files_data: List of file data from Google Drive API.
            
        Returns:
            Dictionary with success and failure counts.
        """
        success_count = 0
        failure_count = 0
        
        print(f"Sending {len(files_data)} files to Kafka topic '{self.topic_name}'...")
        
        for file_data in files_data:
            if self.send_file(file_data):
                success_count += 1
            else:
                failure_count += 1
        
        # Wait for all messages to be delivered
        self.producer.flush()
        
        result = {
            'success': success_count,
            'failure': failure_count,
            'total': len(files_data)
        }
        
        print(f"Delivery completed: {success_count} successful, {failure_count} failed")
        return result
    
    def close(self) -> None:
        """Close the producer and free resources."""
        if self.producer:
            self.producer.flush()
            print("Kafka producer closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
