#!/usr/bin/env python3
"""
Demo script to simulate the Flink parser/chunker pipeline.
This demonstrates the data flow without requiring PyFlink installation.
"""

import json
import time
import logging
import traceback
import uuid
import sys
from datetime import datetime
from typing import Dict, Any, List
from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

from universal_search.config.kafka_config import (
    get_producer_config, get_consumer_config,
    get_drive_files_topic, get_parsed_files_topic, get_chunks_topic
)
from universal_search.config.schema_registry_config import (
    get_schema_registry_config, 
    get_avro_serializer_config
)
from universal_search.config.storage_config import get_storage_config
from universal_search.parsers.pdf_parser import PDFParser
from universal_search.chunkers.text_chunker import TextChunker
from universal_search.storage import StorageFactory
from universal_search.clients.drive_client import DriveClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PipelineDemo:
    """Demo class to simulate the Flink parser/chunker pipeline."""
    
    def __init__(self):
        self.pdf_parser = PDFParser()
        self.text_chunker = TextChunker(window_size=1000, overlap=200)
        
        # Initialize storage adapter using factory
        storage_config = get_storage_config()
        self.storage_adapter = StorageFactory.create_adapter(storage_config)
        
        # Setup Kafka producer
        self.producer = Producer(get_producer_config('demo-pipeline-producer'))
        
        # Setup Kafka consumer with unique group ID to start from the beginning
        group_id = f'demo-consumer-{uuid.uuid4().hex[:8]}'
        consumer_config = get_consumer_config('demo-pipeline-consumer', group_id)
        consumer_config['auto.offset.reset'] = 'earliest'  # Start from the beginning to process all messages
        self.consumer = Consumer(consumer_config)
        
        # Setup schema registry
        schema_registry_config = get_schema_registry_config()
        self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
        
        # Setup serializers/deserializers using Schema Registry
        self._setup_serializers()
        
        # Setup Google Drive client
        self.drive_client = self._setup_google_drive_client()
        
        # State for tracking processed files
        self.processed_files = {}
    
    def _setup_serializers(self):
        """Setup Avro serializers/deserializers using Schema Registry."""
        try:
            # Load schema files
            parsed_file_schema = self._load_schema_file('parsed_file.avsc')
            file_chunk_schema = self._load_schema_file('file_chunk.avsc')
            
            # Single deserializer for all topics - schema determined by SerializationContext
            self.deserializer = AvroDeserializer(
                self.schema_registry_client,
                schema_str=None,  # Schema Registry will fetch the schema based on context
                from_dict=lambda obj, ctx: obj  # Simple pass-through for now
            )
            
            # Custom to_dict function to handle None values properly
            def to_dict_custom(obj, ctx):
                """Convert Python dict to Avro-compatible dict, handling None values."""
                if isinstance(obj, dict):
                    # Return dict as-is, since Python None values work fine with Avro union types
                    return obj
                return obj
            
            # Create separate serializers for each topic with their specific schemas
            self.parsed_file_serializer = AvroSerializer(
                self.schema_registry_client,
                schema_str=parsed_file_schema,  # ParsedFile schema
                to_dict=to_dict_custom
            )
            
            self.file_chunk_serializer = AvroSerializer(
                self.schema_registry_client,
                schema_str=file_chunk_schema,  # FileChunk schema
                to_dict=to_dict_custom
            )
            
            logger.info("Separate serializers setup successfully using Schema Registry")
        except Exception as e:
            logger.error(f"Failed to setup serializers: {e}")
            raise
    
    def _load_schema_file(self, schema_filename: str) -> str:
        """Load Avro schema from file."""
        import os
        
        # Get the directory of this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        schema_path = os.path.join(script_dir, 'schemas', schema_filename)
        
        try:
            with open(schema_path, 'r') as f:
                schema_content = f.read()
            logger.info(f"Loaded schema from {schema_path}")
            return schema_content
        except FileNotFoundError:
            logger.error(f"Schema file not found: {schema_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading schema file {schema_path}: {e}")
            raise
    
    def _setup_google_drive_client(self):
        """Setup Google Drive client."""
        logger.info("Setting up Google Drive client...")
        
        try:
            # Create Drive client
            drive_client = DriveClient()
            
            # Load credentials
            credentials_config = drive_client._load_credentials()
            
            # Authenticate (this initializes the service automatically)
            drive_client.authenticate(credentials_config)
            
            logger.info("Google Drive client setup successful")
            return drive_client
            
        except Exception as e:
            logger.error(f"Failed to setup Google Drive client: {e}")
            logger.error("Cannot proceed without Google Drive authentication. Exiting.")
            sys.exit(1)
    
    def run_parser_demo(self, max_files: int = 10):
        """Simulate the parser job."""
        logger.info(f"Starting parser demo - processing up to {max_files} files")
        
        self.consumer.subscribe([get_drive_files_topic()])
        logger.info(f"Subscribed to topic: {get_drive_files_topic()}")
        
        files_processed = 0
        messages_checked = 0
        
        try:
            no_message_count = 0
            max_no_message_count = 30  # Exit after 30 seconds of no messages
            
            while files_processed < max_files:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    no_message_count += 1
                    if no_message_count >= max_no_message_count:
                        logger.info(f"No messages received for {max_no_message_count} seconds. Exiting parser demo.")
                        break
                    continue
                
                # Reset counter when we get a message
                no_message_count = 0
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info("Reached end of partition")
                        break
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                try:
                    # Deserialize the message
                    drive_file = self.deserializer(
                        msg.value(), 
                        SerializationContext(get_drive_files_topic(), MessageField.VALUE)
                    )
                    
                    messages_checked += 1
                    logger.info(f"Processing file: {drive_file['name']} (ID: {drive_file['id']}) - Message {messages_checked}")
                    
                    # Check if it's a PDF file (using DriveClient to know how Drive represents PDFs)
                    if not self.drive_client.is_pdf_file(drive_file.get('mimeType')):
                        logger.info(f"Skipping non-PDF file: {drive_file['name']}")
                        continue
                    
                    # Check if we've already processed this file
                    file_id = drive_file['id']
                    modified_time = drive_file.get('modifiedTime')
                    
                    if file_id in self.processed_files:
                        last_processed = self.processed_files[file_id]
                        if modified_time and modified_time <= last_processed:
                            logger.info(f"Skipping already processed file: {drive_file['name']}")
                            continue
                    
                    # Download PDF from Google Drive and parse it
                    try:
                        # Get the PDF file bytes from Google Drive using DriveClient
                        pdf_bytes = self.drive_client.get_file_bytes(file_id)
                        
                        logger.info(f"Retrieved PDF bytes: {drive_file['name']} ({len(pdf_bytes)} bytes)")
                        
                        # Parse the PDF using the new interface
                        extracted_text, parsing_status = self.pdf_parser.parse_pdf_from_bytes(pdf_bytes)
                        parsing_timestamp = datetime.utcnow().isoformat() + "Z"
                        
                        if parsing_status == "success" and extracted_text:
                            logger.info(f"Successfully parsed PDF: {drive_file['name']} ({len(extracted_text)} chars)")
                        elif parsing_status == "failed":
                            logger.warning(f"Failed to parse PDF: {drive_file['name']}")
                            continue
                        elif parsing_status == "empty":
                            logger.warning(f"PDF contains no text: {drive_file['name']}")
                            continue
                    except Exception as e:
                        logger.error(f"Failed to download/parse PDF {drive_file['name']}: {e}")
                        continue
                    
                    # Store the parsed content
                    storage_path = f"parsed/{drive_file['id']}.txt"
                    try:
                        self.storage_adapter.save(
                            storage_path, 
                            extracted_text,
                            metadata={'file_id': drive_file['id'], 'file_name': drive_file['name'], 'mime_type': drive_file.get('mimeType')}
                        )
                    except Exception as e:
                        logger.error(f"Failed to save content for file {drive_file['name']}: {e}")
                        continue
                    
                    # Create parsed file message
                    # Note: textLength must be a long (int in Python), not None
                    parsed_file = {
                        'id': file_id,
                        'name': drive_file['name'],
                        'mimeType': drive_file.get('mimeType'),  # Can be None
                        'modifiedTime': modified_time,  # Can be None
                        'storagePath': storage_path,
                        'textLength': len(extracted_text),  # int/long value
                        'parseTimestamp': parsing_timestamp,
                        'parseStatus': parsing_status,
                        'errorMessage': None  # Can be None
                    }
                    
                    # Send to parsed files topic
                    try:
                        value = self.parsed_file_serializer(parsed_file, SerializationContext(get_parsed_files_topic(), MessageField.VALUE));
                    except Exception as e:
                        logger.error(f"Failed to serialize parsed file: {e}")
                        logger.error(traceback.format_exc())
                        continue
                    
                    self.producer.produce(
                        topic=get_parsed_files_topic(),
                        value=value,
                        key=file_id.encode('utf-8')
                    )
                    
                    # Update state
                    self.processed_files[file_id] = modified_time
                    files_processed += 1
                    
                    logger.info(f"Successfully processed file {files_processed}/{max_files}: {drive_file['name']}")
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    logger.error(traceback.format_exc())
                    continue
        
        finally:
            # Don't close consumer here as it's reused in chunker demo
            self.producer.flush()
        
        logger.info(f"Parser demo completed - processed {files_processed} files")
    
    def run_chunker_demo(self, max_files: int = 10):
        """Simulate the chunker job."""
        logger.info(f"Starting chunker demo - processing up to {max_files} files")
        
        # Subscribe to topic
        self.consumer.subscribe([get_parsed_files_topic()])
        logger.info(f"Subscribed to topic: {get_parsed_files_topic()}")
        
        files_processed = 0
        
        try:
            no_message_count = 0
            max_no_message_count = 10  # Exit after 10 seconds of no messages
            
            while files_processed < max_files:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    no_message_count += 1
                    if no_message_count >= max_no_message_count:
                        logger.info(f"No messages received for {max_no_message_count} seconds. Exiting parser demo.")
                        break
                    continue
                
                # Reset counter when we get a message
                no_message_count = 0
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info("Reached end of partition")
                        break
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                try:
                    # Deserialize the message
                    parsed_file = self.deserializer(
                        msg.value(), 
                        SerializationContext(get_parsed_files_topic(), MessageField.VALUE)
                    )
                    
                    # Check parsing status
                    if parsed_file.get('parseStatus') != 'success':
                        logger.info(f"Skipping file with failed parsing: {parsed_file['name']}")
                        continue
                    
                    # Load content from storage
                    storage_reference = parsed_file.get('storagePath')
                    if not storage_reference:
                        logger.warning(f"No storage path for file: {parsed_file['name']}")
                        continue
                    
                    text_content = self.storage_adapter.load(storage_reference)
                    if not text_content:
                        # Silently skip files that don't have parsed content
                        continue
                    
                    # Chunk the text
                    logger.info(f"Processing parsed content for: {parsed_file['name']}")
                    chunks = self.text_chunker.chunk_text(
                        text_content, 
                        parsed_file['id']
                    )
                    
                    # Send each chunk to Kafka
                    for chunk in chunks:
                        chunk_dict = {
                            'chunkId': chunk.chunk_id,
                            'chunkIndex': chunk.chunk_index,
                            'chunkText': chunk.text,
                            'startPosition': chunk.start_position,
                            'endPosition': chunk.end_position,
                            'totalChunks': chunk.total_chunks,
                            'fileId': parsed_file['id'],
                            'fileName': parsed_file['name'],
                            'chunkTimestamp': datetime.utcnow().isoformat() + "Z"
                        }
                        
                        self.producer.produce(
                            topic=get_chunks_topic(),
                            value=self.file_chunk_serializer(
                                chunk_dict,
                                SerializationContext(get_chunks_topic(), MessageField.VALUE)
                            ),
                            key=chunk.chunk_id.encode('utf-8')
                        )
                    
                    files_processed += 1
                    logger.info(f"Successfully chunked file {files_processed}/{max_files}: {parsed_file['name']} into {len(chunks)} chunks")
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    logger.error(traceback.format_exc())
                    continue
        
        finally:
            self.consumer.close()
            self.producer.flush()
        
        logger.info(f"Chunker demo completed - processed {files_processed} files")
    
    def run_full_pipeline(self, max_files: int = 10):
        """Run the complete pipeline demo."""
        logger.info("Starting full pipeline demo")
        
        # Step 1: Run parser
        logger.info("=== STEP 1: PARSER ===")
        self.run_parser_demo(max_files)
        
        # Small delay to ensure messages are processed
        time.sleep(2)
        
        # Step 2: Run chunker
        logger.info("=== STEP 2: CHUNKER ===")
        self.run_chunker_demo(max_files)
        
        logger.info("Full pipeline demo completed!")
        
        # Clean up resources
        self.consumer.close()
        self.producer.flush()

def main():
    """Main function to run the pipeline demo."""
    try:
        demo = PipelineDemo()
        demo.run_full_pipeline(max_files=10)
    except KeyboardInterrupt:
        logger.info("Demo interrupted by user")
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise

if __name__ == "__main__":
    main()
