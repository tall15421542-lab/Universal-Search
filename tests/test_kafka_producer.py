"""
Test suite for Kafka producer functionality.

This module contains comprehensive tests for the Kafka producer
that handles Google Drive file metadata serialization and sending.
"""

import pytest
import json
from unittest.mock import Mock, patch, mock_open
from universal_search.producers.kafka_producer import DriveFileKafkaProducer
from universal_search.config.kafka_config import get_topic_name


class TestDriveFileKafkaProducer:
    """Test class for Kafka producer functionality."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.test_file_data = {
            'id': 'test_file_123',
            'name': 'Test Document.pdf',
            'mimeType': 'application/pdf',
            'createdTime': '2024-01-01T10:00:00.000Z',
            'modifiedTime': '2024-01-01T12:00:00.000Z',
            'size': 1024000,
            'webViewLink': 'https://drive.google.com/file/d/test_file_123/view',
            'webContentLink': 'https://drive.google.com/uc?id=test_file_123',
            'parents': ['parent_folder_1'],
            'owners': [
                {
                    'displayName': 'Test User',
                    'emailAddress': 'test@example.com'
                }
            ]
        }
    
    @patch('kafka_producer.SchemaRegistryClient')
    @patch('kafka_producer.AvroSerializer')
    @patch('kafka_producer.Producer')
    def test_producer_initialization_success(self, mock_producer, mock_serializer, mock_schema_registry):
        """Test successful producer initialization."""
        # Mock schema registry client
        mock_schema_client = Mock()
        mock_schema_registry.return_value = mock_schema_client
        
        # Mock Avro serializer
        mock_avro_serializer = Mock()
        mock_serializer.return_value = mock_avro_serializer
        
        # Mock producer
        mock_kafka_producer = Mock()
        mock_producer.return_value = mock_kafka_producer
        
        # Create producer instance
        producer = DriveFileKafkaProducer()
        
        # Verify initialization
        assert producer.topic_name == get_topic_name()
        assert producer.producer == mock_kafka_producer
        assert producer.schema_registry_client == mock_schema_client
        assert producer.avro_serializer == mock_avro_serializer
        
    
    @patch('kafka_producer.DriveFileKafkaProducer._initialize_schema_registry')
    @patch('kafka_producer.DriveFileKafkaProducer._initialize_producer')
    def test_send_file_success(self, mock_init_producer, mock_init_schema):
        """Test successful file sending to Kafka."""
        # Mock producer and serializer
        mock_producer = Mock()
        mock_serializer = Mock()
        mock_serializer.return_value = b'serialized_data'
        
        producer = DriveFileKafkaProducer()
        producer.producer = mock_producer
        producer.avro_serializer = mock_serializer
        
        # Test sending file
        result = producer.send_file(self.test_file_data)
        
        # Verify success
        assert result is True
        mock_serializer.assert_called_once()
        mock_producer.produce.assert_called_once()
        
        # Verify the produce call includes the file ID as key
        call_args = mock_producer.produce.call_args
        assert call_args[1]['key'] == b'test_file_123'  # File ID encoded as bytes
        assert call_args[1]['topic'] == get_topic_name()
        assert call_args[1]['value'] == b'serialized_data'
        
        # Verify that defaults were set on the original file_data
        assert self.test_file_data['id'] == 'test_file_123'  # Should remain unchanged
        assert self.test_file_data['parents'] == ['parent_folder_1']  # Should remain unchanged
    
    @patch('kafka_producer.DriveFileKafkaProducer._initialize_schema_registry')
    @patch('kafka_producer.DriveFileKafkaProducer._initialize_producer')
    def test_send_file_failure(self, mock_init_producer, mock_init_schema):
        """Test file sending failure."""
        # Mock producer and serializer to raise exception
        mock_producer = Mock()
        mock_serializer = Mock()
        mock_serializer.side_effect = Exception("Serialization failed")
        
        producer = DriveFileKafkaProducer()
        producer.producer = mock_producer
        producer.avro_serializer = mock_serializer
        
        # Test sending file
        result = producer.send_file(self.test_file_data)
        
        # Verify failure
        assert result is False
    
    @patch('kafka_producer.DriveFileKafkaProducer._initialize_schema_registry')
    @patch('kafka_producer.DriveFileKafkaProducer._initialize_producer')
    def test_send_file_missing_id(self, mock_init_producer, mock_init_schema):
        """Test file sending when file ID is missing."""
        # Mock producer and serializer
        mock_producer = Mock()
        mock_serializer = Mock()
        mock_serializer.return_value = b'serialized_data'
        
        producer = DriveFileKafkaProducer()
        producer.producer = mock_producer
        producer.avro_serializer = mock_serializer
        
        # File data without ID
        file_data_no_id = {
            'name': 'File without ID',
            'mimeType': 'text/plain'
        }
        
        # Test sending file
        result = producer.send_file(file_data_no_id)
        
        # Verify success with fallback key
        assert result is True
        mock_producer.produce.assert_called_once()
        
        # Verify the produce call includes a fallback key
        call_args = mock_producer.produce.call_args
        key = call_args[1]['key']
        assert key.startswith(b'unknown_')  # Should start with fallback prefix
        assert len(key) > len(b'unknown_')  # Should have timestamp suffix
        
        # Verify that defaults were set
        assert file_data_no_id['id'] == ''  # Default empty string
        assert file_data_no_id['parents'] == []  # Default empty list
    
    
    @patch('kafka_producer.DriveFileKafkaProducer._initialize_schema_registry')
    @patch('kafka_producer.DriveFileKafkaProducer._initialize_producer')
    def test_send_files_multiple(self, mock_init_producer, mock_init_schema):
        """Test sending multiple files to Kafka."""
        # Mock producer and serializer
        mock_producer = Mock()
        mock_serializer = Mock()
        mock_serializer.return_value = b'serialized_data'
        
        producer = DriveFileKafkaProducer()
        producer.producer = mock_producer
        producer.avro_serializer = mock_serializer
        
        # Test data
        files_data = [self.test_file_data, self.test_file_data]
        
        # Test sending files
        result = producer.send_files(files_data)
        
        # Verify results
        assert result['total'] == 2
        assert result['success'] == 2
        assert result['failure'] == 0
        assert mock_producer.produce.call_count == 2
        mock_producer.flush.assert_called_once()
    
    @patch('kafka_producer.DriveFileKafkaProducer._initialize_schema_registry')
    @patch('kafka_producer.DriveFileKafkaProducer._initialize_producer')
    def test_close_producer(self, mock_init_producer, mock_init_schema):
        """Test producer close functionality."""
        mock_producer = Mock()
        
        producer = DriveFileKafkaProducer()
        producer.producer = mock_producer
        
        # Test close
        producer.close()
        
        # Verify flush was called
        mock_producer.flush.assert_called_once()
    
    @patch('kafka_producer.DriveFileKafkaProducer._initialize_schema_registry')
    @patch('kafka_producer.DriveFileKafkaProducer._initialize_producer')
    def test_context_manager(self, mock_init_producer, mock_init_schema):
        """Test producer as context manager."""
        mock_producer = Mock()
        
        with DriveFileKafkaProducer() as producer:
            producer.producer = mock_producer
            pass  # Context manager should call close automatically
        
        # Verify flush was called on exit
        mock_producer.flush.assert_called_once()
    
    def test_delivery_callback_success(self):
        """Test delivery callback for successful message delivery."""
        with patch('kafka_producer.DriveFileKafkaProducer._initialize_schema_registry'), \
             patch('kafka_producer.DriveFileKafkaProducer._initialize_producer'):
            
            producer = DriveFileKafkaProducer()
            
            # Mock message object
            mock_msg = Mock()
            mock_msg.topic.return_value = 'test-topic'
            mock_msg.partition.return_value = 0
            mock_msg.offset.return_value = 123
            mock_msg.key.return_value = b'test_file_123'
            
            # Test successful delivery callback
            producer._delivery_callback(None, mock_msg)
            
            # Should not raise any exceptions
            mock_msg.topic.assert_called_once()
            mock_msg.partition.assert_called_once()
            mock_msg.offset.assert_called_once()
            mock_msg.key.assert_called_once()
    
    def test_delivery_callback_failure(self):
        """Test delivery callback for failed message delivery."""
        with patch('kafka_producer.DriveFileKafkaProducer._initialize_schema_registry'), \
             patch('kafka_producer.DriveFileKafkaProducer._initialize_producer'):
            
            producer = DriveFileKafkaProducer()
            
            # Test failed delivery callback
            error = Exception("Delivery failed")
            producer._delivery_callback(error, None)
            
            # Should not raise any exceptions
            assert True  # If we get here, no exception was raised


if __name__ == "__main__":
    pytest.main([__file__])
