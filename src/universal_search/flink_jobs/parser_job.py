"""
Flink Parser Job for processing Google Drive files.

This module provides a PyFlink streaming job that consumes drive-files messages,
filters for PDF files, downloads and parses them, stores content to storage,
and produces parsed file references to Kafka.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
from pyflink.common.time import Time
from pyflink.datastream.checkpointing_mode import CheckpointingMode

from ..clients.drive_client import DriveClient
from ..parsers.pdf_parser import PDFParser
from ..storage import StorageFactory
from ..config.kafka_config import (
    get_consumer_config,
    get_producer_config,
    get_drive_files_topic,
    get_parsed_files_topic
)
from ..config.schema_registry_config import (
    get_parsed_schema_name,
    get_schema_namespace,
    get_schema_registry_config,
    get_avro_serializer_config
)
from ..config.flink_config import get_flink_config
from ..config.storage_config import get_storage_config


class DriveFileParserFunction(KeyedProcessFunction):
    """KeyedProcessFunction for parsing Google Drive files."""
    
    def __init__(self):
        """Initialize the parser function."""
        self.drive_client = None
        self.pdf_parser = None
        self.storage_adapter = None
        self.service = None
        self.last_processed_time_state = None
        self.logger = None
    
    def open(self, runtime_context: RuntimeContext):
        """Initialize resources when the function starts."""
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.drive_client = DriveClient()
        self.pdf_parser = PDFParser()
        
        # Initialize storage adapter using factory
        storage_config = get_storage_config()
        self.storage_adapter = StorageFactory.create_adapter(storage_config)
        
        # Initialize state for tracking last processed timestamp
        self.last_processed_time_state = runtime_context.get_state(
            ValueStateDescriptor("last_processed_time", Types.STRING())
        )
        
        # Initialize Google Drive service
        try:
            credentials_config = self.drive_client._load_credentials()
            credentials = self.drive_client.authenticate(credentials_config)
            self.service = self.drive_client.get_drive_service(credentials)
            self.logger.info("Google Drive service initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Google Drive service: {str(e)}")
            raise
    
    def process_element(self, value: Dict[str, Any], ctx) -> Optional[Dict[str, Any]]:
        """
        Process a single drive file message.
        
        Args:
            value: Drive file message dictionary
            ctx: Process function context
            
        Returns:
            Parsed file message or None if skipped/failed
        """
        try:
            file_id = value.get('id')
            file_name = value.get('name', 'Unknown')
            mime_type = value.get('mimeType')
            modified_time = value.get('modifiedTime')
            
            self.logger.info(f"Processing file: {file_name} (ID: {file_id})")
            
            # Check if it's a PDF file (using DriveClient to know how Drive represents PDFs)
            if not self.drive_client.is_pdf_file(mime_type):
                self.logger.info(f"Skipping non-PDF file: {file_name} (mimeType: {mime_type})")
                return None
            
            # Check if we've already processed this file with a newer timestamp
            last_processed_time = self.last_processed_time_state.value()
            if last_processed_time and modified_time:
                if modified_time <= last_processed_time:
                    self.logger.info(f"Skipping already processed file: {file_name}")
                    return None
            
            # Download and parse PDF
            self.logger.info(f"Downloading and parsing PDF: {file_name}")
            parsed_text, parse_status = self.pdf_parser.parse_pdf_from_drive(
                self.service, file_id
            )
            
            # Generate storage path
            storage_path = f"parsed/{file_id}.txt"
            
            # Create parsed file message
            parsed_file_msg = {
                'id': file_id,
                'name': file_name,
                'mimeType': mime_type,
                'modifiedTime': modified_time,
                'storagePath': storage_path,
                'textLength': len(parsed_text) if parsed_text else None,
                'parseTimestamp': datetime.utcnow().isoformat() + 'Z',
                'parseStatus': parse_status,
                'errorMessage': None
            }
            
            if parse_status == 'success' and parsed_text:
                # Save parsed content to storage
                metadata = {
                    'file_id': file_id,
                    'file_name': file_name,
                    'parse_timestamp': parsed_file_msg['parseTimestamp'],
                    'text_length': len(parsed_text)
                }
                
                try:
                    self.storage_adapter.save(storage_path, parsed_text, metadata)
                    self.logger.info(f"Successfully saved parsed content for {file_name}")
                except Exception as e:
                    error_msg = f"Failed to save parsed content for {file_name}: {e}"
                    self.logger.error(error_msg)
                    parsed_file_msg['parseStatus'] = 'failed'
                    parsed_file_msg['errorMessage'] = f'Failed to save to storage: {str(e)}'
            elif parse_status == 'failed':
                parsed_file_msg['errorMessage'] = 'PDF parsing failed'
            elif parse_status == 'empty':
                parsed_file_msg['errorMessage'] = 'PDF contains no extractable text'
            elif parse_status == 'download_failed':
                parsed_file_msg['errorMessage'] = 'Failed to download PDF from Google Drive'
            
            # Update state with current timestamp
            if modified_time:
                self.last_processed_time_state.update(modified_time)
            
            self.logger.info(f"Processed file {file_name} with status: {parsed_file_msg['parseStatus']}")
            return parsed_file_msg
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_id}: {str(e)}")
            # Return a failed message
            return {
                'id': value.get('id', 'unknown'),
                'name': value.get('name', 'Unknown'),
                'mimeType': value.get('mimeType'),
                'modifiedTime': value.get('modifiedTime'),
                'storagePath': '',
                'textLength': None,
                'parseTimestamp': datetime.utcnow().isoformat() + 'Z',
                'parseStatus': 'failed',
                'errorMessage': str(e)
            }


class DriveFileParserJob:
    """Flink job for parsing Google Drive files."""
    
    def __init__(self):
        """Initialize the parser job."""
        self.env = None
        self.logger = logging.getLogger(__name__)
        self.flink_config = get_flink_config()
    
    def create_environment(self) -> StreamExecutionEnvironment:
        """Create and configure Flink execution environment."""
        # Create execution environment
        self.env = StreamExecutionEnvironment.get_execution_environment()

        # Add Kafka connector JAR to the environment (using SQL connector which includes all dependencies)
        jar_path = os.path.join(os.getcwd(), "jars", "flink-sql-connector-kafka-4.0.1-2.0.jar")
        self.env.add_jars(f"file://{jar_path}")
        
        # Set parallelism
        self.env.set_parallelism(int(self.flink_config['parser_parallelism']))
        
        # Enable checkpointing
        self.env.enable_checkpointing(
            int(self.flink_config['checkpoint_interval']),
            CheckpointingMode.EXACTLY_ONCE
        )
        
        # Configure state backend - skip for now as API has changed
        # Using default state backend provided by Flink
        if self.flink_config['state_backend'] == 'rocksdb':
            self.logger.info("State backend configuration skipped - using default state backend")
        
        self.logger.info("Flink execution environment configured")
        return self.env
    
    def create_kafka_source(self) -> DataStream:
        """Create Kafka source for drive-files topic."""
        from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.common.watermark_strategy import WatermarkStrategy

        # Get consumer config
        consumer_config = get_consumer_config('flink-parser-consumer', 'flink-parser-group')

        # Build Kafka source using the new KafkaSource API
        kafka_source = KafkaSource.builder() \
            .set_bootstrap_servers(consumer_config.get('bootstrap.servers', 'localhost:9092')) \
            .set_topics(get_drive_files_topic()) \
            .set_group_id('flink-parser-group') \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        return self.env.from_source(
            kafka_source,
            WatermarkStrategy.no_watermarks(),
            "Kafka Source - Drive Files"
        )
    
    def create_kafka_sink(self, data_stream: DataStream):
        """Create Kafka sink for parsed-files topic."""
        from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
        from pyflink.common.serialization import SimpleStringSchema

        # Get producer config
        producer_config = get_producer_config('flink-parser-producer')

        # Build Kafka sink using the new KafkaSink API
        kafka_sink = KafkaSink.builder() \
            .set_bootstrap_servers(producer_config.get('bootstrap.servers', 'localhost:9092')) \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                    .set_topic(get_parsed_files_topic())
                    .set_value_serialization_schema(SimpleStringSchema())
                    .build()
            ) \
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
            .build()

        data_stream.sink_to(kafka_sink)
    
    def build_job(self):
        """Build the complete Flink job."""
        # Create environment
        self.create_environment()
        
        # Create source
        source_stream = self.create_kafka_source()
        
        # Parse JSON strings to dictionaries
        parsed_stream = source_stream.map(
            lambda x: json.loads(x),
            output_type=Types.MAP(Types.STRING(), Types.STRING())
        )
        
        # Key by file ID and process with parser function
        keyed_stream = parsed_stream.key_by(lambda x: x['id'])
        
        processed_stream = keyed_stream.process(
            DriveFileParserFunction(),
            output_type=Types.MAP(Types.STRING(), Types.STRING())
        )
        
        # Filter out None values (skipped files)
        filtered_stream = processed_stream.filter(lambda x: x is not None)
        
        # Convert back to JSON strings for Kafka
        output_stream = filtered_stream.map(
            lambda x: json.dumps(x),
            output_type=Types.STRING()
        )
        
        # Create sink
        self.create_kafka_sink(output_stream)
        
        self.logger.info("Flink parser job built successfully")
    
    def run(self):
        """Run the Flink job."""
        try:
            self.build_job()
            self.logger.info("Starting Flink parser job...")
            self.env.execute("Drive File Parser Job")
        except Exception as e:
            self.logger.error(f"Flink parser job failed: {str(e)}")
            raise


def main():
    """Main entry point for the parser job."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Drive File Parser Flink Job')
    parser.add_argument('--parallelism', type=int, default=None,
                       help='Override parallelism setting')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run job
    job = DriveFileParserJob()
    
    if args.parallelism:
        job.flink_config['parser_parallelism'] = args.parallelism
    
    try:
        job.run()
    except Exception as e:
        print(f"Job failed: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()
