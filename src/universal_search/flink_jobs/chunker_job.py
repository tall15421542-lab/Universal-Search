"""
Flink Chunker Job for processing parsed files.

This module provides a PyFlink streaming job that consumes parsed file messages,
loads content from storage, chunks the text, and produces individual chunks to Kafka.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Iterator

from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.datastream.functions import FlatMapFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.common.time import Time
from pyflink.datastream.checkpointing_mode import CheckpointingMode

from ..chunkers.text_chunker import TextChunker, TextChunk
from ..storage import StorageFactory
from ..config.kafka_config import (
    get_consumer_config,
    get_producer_config,
    get_parsed_files_topic,
    get_chunks_topic
)
from ..config.schema_registry_config import (
    get_chunk_schema_name,
    get_schema_namespace,
    get_schema_registry_config,
    get_avro_serializer_config
)
from ..config.flink_config import get_flink_config
from ..config.storage_config import get_storage_config


class FileChunkerFunction(FlatMapFunction):
    """FlatMapFunction for chunking parsed files."""
    
    def __init__(self, window_size: int = 1000, overlap: int = 200):
        """
        Initialize the chunker function.
        
        Args:
            window_size: Size of each chunk in characters
            overlap: Number of characters to overlap between chunks
        """
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.text_chunker = TextChunker(window_size, overlap)
        
        # Initialize storage adapter using factory
        storage_config = get_storage_config()
        self.storage_adapter = StorageFactory.create_adapter(storage_config)
        
        self.logger.info("File chunker function initialized")
    
    def open(self, runtime_context: RuntimeContext):
        """Initialize resources when the function starts."""
        pass
    
    def flat_map(self, value: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Process a parsed file message and yield chunk messages.
        
        Args:
            value: Parsed file message dictionary
            
        Yields:
            Individual chunk messages
        """
        try:
            file_id = value.get('id')
            file_name = value.get('name', 'Unknown')
            storage_path = value.get('storagePath')
            parse_status = value.get('parseStatus')
            
            self.logger.info(f"Processing parsed file: {file_name} (ID: {file_id})")
            
            # Skip if parsing failed
            if parse_status != 'success':
                self.logger.info(f"Skipping file with parse status '{parse_status}': {file_name}")
                return
            
            # Load parsed content from storage
            if not storage_path:
                self.logger.error(f"No storage path for file: {file_name}")
                return
            
            parsed_text = self.storage_adapter.load(storage_path)
            if not parsed_text:
                self.logger.error(f"Failed to load parsed content for file: {file_name}")
                return
            
            self.logger.info(f"Loaded {len(parsed_text)} characters for chunking: {file_name}")
            
            # Chunk the text
            chunks = self.text_chunker.chunk_text(parsed_text, file_id)
            
            if not chunks:
                self.logger.warning(f"No chunks created for file: {file_name}")
                return
            
            # Generate chunk messages
            chunk_timestamp = datetime.utcnow().isoformat() + 'Z'
            
            for chunk in chunks:
                chunk_msg = {
                    'fileId': file_id,
                    'fileName': file_name,
                    'chunkId': chunk.chunk_id,
                    'chunkIndex': chunk.chunk_index,
                    'chunkText': chunk.text,
                    'startPosition': chunk.start_position,
                    'endPosition': chunk.end_position,
                    'chunkTimestamp': chunk_timestamp,
                    'totalChunks': chunk.total_chunks
                }
                
                self.logger.debug(f"Generated chunk {chunk.chunk_index}/{chunk.total_chunks} for {file_name}")
                yield chunk_msg
            
            self.logger.info(f"Generated {len(chunks)} chunks for file: {file_name}")
            
        except Exception as e:
            self.logger.error(f"Error chunking file {file_id}: {str(e)}")
            # Don't yield anything on error


class DriveFileChunkerJob:
    """Flink job for chunking parsed files."""
    
    def __init__(self, window_size: int = 1000, overlap: int = 200):
        """
        Initialize the chunker job.
        
        Args:
            window_size: Size of each chunk in characters
            overlap: Number of characters to overlap between chunks
        """
        self.window_size = window_size
        self.overlap = overlap
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
        self.env.set_parallelism(int(self.flink_config['chunker_parallelism']))

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
        """Create Kafka source for parsed-files topic."""
        from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.common.watermark_strategy import WatermarkStrategy

        # Get consumer config
        consumer_config = get_consumer_config('flink-chunker-consumer', 'flink-chunker-group')

        # Build Kafka source using the new KafkaSource API
        kafka_source = KafkaSource.builder() \
            .set_bootstrap_servers(consumer_config.get('bootstrap.servers', 'localhost:9092')) \
            .set_topics(get_parsed_files_topic()) \
            .set_group_id('flink-chunker-group') \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        return self.env.from_source(
            kafka_source,
            WatermarkStrategy.no_watermarks(),
            "Kafka Source - Parsed Files"
        )
    
    def create_kafka_sink(self, data_stream: DataStream):
        """Create Kafka sink for chunks topic."""
        from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
        from pyflink.common.serialization import SimpleStringSchema

        # Get producer config
        producer_config = get_producer_config('flink-chunker-producer')

        # Build Kafka sink using the new KafkaSink API
        kafka_sink = KafkaSink.builder() \
            .set_bootstrap_servers(producer_config.get('bootstrap.servers', 'localhost:9092')) \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                    .set_topic(get_chunks_topic())
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
        
        # Flat map to generate chunks
        chunk_stream = parsed_stream.flat_map(
            FileChunkerFunction(self.window_size, self.overlap),
            output_type=Types.MAP(Types.STRING(), Types.STRING())
        )
        
        # Convert back to JSON strings for Kafka
        output_stream = chunk_stream.map(
            lambda x: json.dumps(x),
            output_type=Types.STRING()
        )
        
        # Create sink
        self.create_kafka_sink(output_stream)
        
        self.logger.info("Flink chunker job built successfully")
    
    def run(self):
        """Run the Flink job."""
        try:
            self.build_job()
            self.logger.info("Starting Flink chunker job...")
            self.env.execute("Drive File Chunker Job")
        except Exception as e:
            self.logger.error(f"Flink chunker job failed: {str(e)}")
            raise


def main():
    """Main entry point for the chunker job."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Drive File Chunker Flink Job')
    parser.add_argument('--parallelism', type=int, default=None,
                       help='Override parallelism setting')
    parser.add_argument('--window-size', type=int, default=1000,
                       help='Chunk window size in characters')
    parser.add_argument('--overlap', type=int, default=200,
                       help='Chunk overlap in characters')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run job
    job = DriveFileChunkerJob(args.window_size, args.overlap)
    
    if args.parallelism:
        job.flink_config['chunker_parallelism'] = args.parallelism
    
    try:
        job.run()
    except Exception as e:
        print(f"Job failed: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()
