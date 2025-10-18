#!/usr/bin/env python3
"""
Test script for the Drive Streaming Job.

This script provides basic tests for the DriveStreamingJob functionality.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from universal_search.jobs.drive_streaming_job import DriveStreamingJob


class TestDriveStreamingJob(unittest.TestCase):
    """Test cases for DriveStreamingJob."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Don't create real job instance in setUp since initialization happens in __init__
        # Individual tests will create job instances with proper mocking
        pass
    
    @patch('drive_streaming_job.DriveClient')
    @patch('drive_streaming_job.DriveFileKafkaProducer')
    def test_initialization_success(self, mock_producer_class, mock_client_class):
        """Test successful initialization and basic properties."""
        # Mock the components
        mock_client = Mock()
        mock_client._load_credentials.return_value = {'web': {}}
        mock_client.authenticate.return_value = Mock()
        mock_client.get_drive_service.return_value = Mock()
        mock_client_class.return_value = mock_client
        
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        # Create job with mocked components
        job = DriveStreamingJob(batch_size=10, max_files_per_run=50)
        
        # Test basic properties
        self.assertEqual(job.batch_size, 10)
        self.assertEqual(job.max_files_per_run, 50)
        self.assertFalse(job.is_running)
        self.assertEqual(job.total_files_processed, 0)
        self.assertEqual(job.total_files_sent, 0)
        self.assertEqual(job.total_files_failed, 0)
        
        # Verify components are initialized
        self.assertIsNotNone(job.service)
        self.assertIsNotNone(job.kafka_producer)
        
        # Verify mock methods were called
        mock_client._load_credentials.assert_called_once()
        mock_client.authenticate.assert_called_once()
        mock_client.get_drive_service.assert_called_once()
    
    @patch('drive_streaming_job.DriveClient')
    @patch('drive_streaming_job.DriveFileKafkaProducer')
    def test_status_reporting(self, mock_producer_class, mock_client_class):
        """Test status reporting functionality."""
        # Mock the components
        mock_client = Mock()
        mock_client._load_credentials.return_value = {'web': {}}
        mock_client.authenticate.return_value = Mock()
        mock_client.get_drive_service.return_value = Mock()
        mock_client_class.return_value = mock_client
        
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        # Create job with mocked components
        job = DriveStreamingJob(batch_size=10, max_files_per_run=50)
        
        status = job.get_status()
        
        # Test status structure
        self.assertIn('is_running', status)
        self.assertIn('total_files_processed', status)
        self.assertIn('batch_size', status)
        self.assertIn('max_files_per_run', status)
        self.assertIn('current_page_token', status)
        
        # Test status values
        self.assertEqual(status['batch_size'], 10)
        self.assertEqual(status['max_files_per_run'], 50)
        self.assertFalse(status['is_running'])
        self.assertEqual(status['total_files_processed'], 0)
        self.assertEqual(status['total_files_sent'], 0)
        self.assertEqual(status['total_files_failed'], 0)
    
    @patch('drive_streaming_job.DriveClient')
    def test_initialization_failure(self, mock_client_class):
        """Test initialization failure."""
        # Mock client to raise exception
        mock_client = Mock()
        mock_client._load_credentials.side_effect = Exception("Auth failed")
        mock_client_class.return_value = mock_client
        
        # Test initialization failure in __init__
        with self.assertRaises(Exception):
            DriveStreamingJob()
    
    @patch('drive_streaming_job.DriveClient')
    @patch('drive_streaming_job.DriveFileKafkaProducer')
    def test_stop(self, mock_producer_class, mock_client_class):
        """Test job stopping."""
        # Mock the components
        mock_client = Mock()
        mock_client._load_credentials.return_value = {'web': {}}
        mock_client.authenticate.return_value = Mock()
        mock_client.get_drive_service.return_value = Mock()
        mock_client_class.return_value = mock_client
        
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        # Create job with mocked components
        job = DriveStreamingJob()
        job.is_running = True
        
        job.stop()
        
        self.assertFalse(job.is_running)
        mock_producer.close.assert_called_once()


class TestDriveStreamingJobIntegration(unittest.TestCase):
    """Integration tests for DriveStreamingJob."""
    
    @patch('drive_streaming_job.DriveClient')
    @patch('drive_streaming_job.DriveFileKafkaProducer')
    def test_process_files_batch(self, mock_producer_class, mock_client_class):
        """Test processing a batch of files."""
        # Setup mocks
        mock_client = Mock()
        mock_client._load_credentials.return_value = {'web': {}}
        mock_client.authenticate.return_value = Mock()
        mock_client.get_drive_service.return_value = Mock()
        
        # Mock list_files to return sample data
        sample_files = [
            {'id': '1', 'name': 'file1.txt', 'mimeType': 'text/plain'},
            {'id': '2', 'name': 'file2.pdf', 'mimeType': 'application/pdf'}
        ]
        mock_client.list_files.return_value = (sample_files, 'next_token')
        mock_client_class.return_value = mock_client
        
        mock_producer = Mock()
        mock_producer.send_files.return_value = {'success': 2, 'failure': 0}
        mock_producer_class.return_value = mock_producer
        
        # Create job (initialization happens in __init__)
        job = DriveStreamingJob(batch_size=10)
        
        # Process batch
        result = job.process_files_batch()
        
        # Verify results
        self.assertEqual(result['processed'], 2)
        self.assertEqual(result['sent'], 2)
        self.assertEqual(result['failed'], 0)
        self.assertEqual(job.total_files_processed, 2)
        self.assertEqual(job.total_files_sent, 2)
    
    @patch('drive_streaming_job.DriveClient')
    @patch('drive_streaming_job.DriveFileKafkaProducer')
    def test_job_run_complete_flow(self, mock_producer_class, mock_client_class):
        """Test complete job run flow."""
        # Setup mocks
        mock_client = Mock()
        mock_client._load_credentials.return_value = {'web': {}}
        mock_client.authenticate.return_value = Mock()
        mock_client.get_drive_service.return_value = Mock()
        
        # Mock list_files to return sample data with pagination
        sample_files_batch1 = [
            {'id': '1', 'name': 'file1.txt', 'mimeType': 'text/plain'},
            {'id': '2', 'name': 'file2.pdf', 'mimeType': 'application/pdf'}
        ]
        sample_files_batch2 = [
            {'id': '3', 'name': 'file3.docx', 'mimeType': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'}
        ]
        
        # First call returns batch1 with next token, second call returns batch2 with no token
        mock_client.list_files.side_effect = [
            (sample_files_batch1, 'next_token'),
            (sample_files_batch2, None)
        ]
        mock_client_class.return_value = mock_client
        
        mock_producer = Mock()
        mock_producer.send_files.side_effect = [
            {'success': 2, 'failure': 0},  # First batch
            {'success': 1, 'failure': 0}   # Second batch
        ]
        mock_producer_class.return_value = mock_producer
        
        # Create job with small batch size to test pagination
        job = DriveStreamingJob(batch_size=2, max_files_per_run=5)
        
        # Run the complete job
        result = job.run()
        
        # Verify final results
        self.assertEqual(result['processed'], 3)  # Total files processed
        self.assertEqual(result['sent'], 3)       # Total files sent
        self.assertEqual(result['failed'], 0)     # Total files failed
        self.assertGreater(result['duration'], 0) # Duration should be positive
        
        # Verify job state
        self.assertEqual(job.total_files_processed, 3)
        self.assertEqual(job.total_files_sent, 3)
        self.assertEqual(job.total_files_failed, 0)
        self.assertFalse(job.is_running)  # Job should be stopped after run
        
        # Verify mock calls
        self.assertEqual(mock_client.list_files.call_count, 2)  # Two batches
        self.assertEqual(mock_producer.send_files.call_count, 2)  # Two batches sent
        mock_producer.close.assert_called_once()  # Producer should be closed
    
    @patch('drive_streaming_job.DriveClient')
    @patch('drive_streaming_job.DriveFileKafkaProducer')
    def test_job_run_with_max_files_limit(self, mock_producer_class, mock_client_class):
        """Test job run with max files limit."""
        # Setup mocks
        mock_client = Mock()
        mock_client._load_credentials.return_value = {'web': {}}
        mock_client.authenticate.return_value = Mock()
        mock_client.get_drive_service.return_value = Mock()
        
        # Mock list_files to return more files than the limit
        sample_files = [
            {'id': '1', 'name': 'file1.txt', 'mimeType': 'text/plain'},
            {'id': '2', 'name': 'file2.pdf', 'mimeType': 'application/pdf'},
            {'id': '3', 'name': 'file3.docx', 'mimeType': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'}
        ]
        mock_client.list_files.return_value = (sample_files, 'next_token')
        mock_client_class.return_value = mock_client
        
        mock_producer = Mock()
        mock_producer.send_files.return_value = {'success': 3, 'failure': 0}
        mock_producer_class.return_value = mock_producer
        
        # Create job with max files limit
        job = DriveStreamingJob(batch_size=10, max_files_per_run=2)
        
        # Run the job
        result = job.run()
        
        # Verify results - should stop at max limit
        # Note: The limit is checked after processing a batch, so if batch contains 3 files
        # and limit is 2, it will process all 3 files before checking the limit
        self.assertEqual(result['processed'], 3)  # Processes the full batch before checking limit
        self.assertEqual(result['sent'], 3)
        self.assertEqual(result['failed'], 0)
        
        # Verify job state
        self.assertEqual(job.total_files_processed, 3)  # Processes the full batch
        self.assertEqual(job.total_files_sent, 3)
        self.assertEqual(job.total_files_failed, 0)
        self.assertFalse(job.is_running)
        
        # Verify mock calls
        mock_client.list_files.assert_called_once()  # Only one call before hitting limit
        mock_producer.send_files.assert_called_once()
        mock_producer.close.assert_called_once()
    
    @patch('drive_streaming_job.DriveClient')
    @patch('drive_streaming_job.DriveFileKafkaProducer')
    def test_job_run_with_no_files(self, mock_producer_class, mock_client_class):
        """Test job run when no files are found."""
        # Setup mocks
        mock_client = Mock()
        mock_client._load_credentials.return_value = {'web': {}}
        mock_client.authenticate.return_value = Mock()
        mock_client.get_drive_service.return_value = Mock()
        
        # Mock list_files to return no files
        mock_client.list_files.return_value = ([], None)
        mock_client_class.return_value = mock_client
        
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        # Create job
        job = DriveStreamingJob()
        
        # Run the job
        result = job.run()
        
        # Verify results
        self.assertEqual(result['processed'], 0)
        self.assertEqual(result['sent'], 0)
        self.assertEqual(result['failed'], 0)
        self.assertGreater(result['duration'], 0)
        
        # Verify job state
        self.assertEqual(job.total_files_processed, 0)
        self.assertEqual(job.total_files_sent, 0)
        self.assertEqual(job.total_files_failed, 0)
        self.assertFalse(job.is_running)
        
        # Verify mock calls
        mock_client.list_files.assert_called_once()
        mock_producer.send_files.assert_not_called()  # No files to send
        mock_producer.close.assert_called_once()


if __name__ == "__main__":
    # Run tests using unittest directly
    unittest.main(verbosity=2)
