"""
Drive Streaming Job for Google Drive to Kafka.

This module provides a job that continuously streams Google Drive files
to Kafka using the DriveClient and KafkaProducer components.
"""

import json
import time
import logging
import signal
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime
from ..clients.drive_client import DriveClient
from ..producers.kafka_producer import DriveFileKafkaProducer
from ..config.kafka_config import get_drive_files_topic


class DriveStreamingJob:
    """Job for streaming Google Drive files to Kafka."""
    
    def __init__(self, 
                 batch_size: int = 100,
                 max_files_per_run: Optional[int] = None,
                 current_page_token: Optional[str] = None):
        """
        Initialize the streaming job.
        
        Args:
            batch_size: Number of files to process in each batch.
            max_files_per_run: Maximum files to process per run (None for unlimited).
            current_page_token: Page token to start from (for pagination).
        """
        self.batch_size = batch_size
        self.max_files_per_run = max_files_per_run
        self.current_page_token = current_page_token
        
        # Initialize components
        self.drive_client = DriveClient()
        self.kafka_producer = None
        self.service = None
        
        # Job state
        self.is_running = False
        self.total_files_processed = 0
        self.total_files_sent = 0
        self.total_files_failed = 0
        
        # Setup logging
        self._setup_logging()
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        # Initialize job components
        self._initialize_components()
    
    def _setup_logging(self) -> None:
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('drive_streaming_job.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
            self.stop()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def _initialize_components(self) -> None:
        """
        Initialize the job components.
        
        Raises:
            Exception: If initialization fails.
        """
        try:
            self.logger.info("Initializing Drive Streaming Job...")
            
            # Load and validate credentials
            self.logger.info("Loading Google Drive credentials...")
            credentials_config = self.drive_client._load_credentials()
            
            # Authenticate with Google Drive
            self.logger.info("Authenticating with Google Drive...")
            credentials = self.drive_client.authenticate(credentials_config)
            
            # Create Drive service
            self.logger.info("Creating Google Drive service...")
            self.service = self.drive_client.get_drive_service(credentials)
            
            # Initialize Kafka producer
            self.logger.info("Initializing Kafka producer...")
            self.kafka_producer = DriveFileKafkaProducer(client_id='drive-streaming-producer')
            
            self.logger.info("Job initialization completed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize job: {str(e)}")
            raise
    
    def process_files_batch(self) -> Dict[str, int]:
        """
        Process a batch of files from Google Drive.
        
        Returns:
            Dictionary with processing statistics.
        """
        try:
            self.logger.info(f"Fetching batch of {self.batch_size} files...")
            
            # Fetch files from Google Drive
            files, next_page_token = self.drive_client.list_files(
                self.service, 
                page_size=self.batch_size, 
                page_token=self.current_page_token
            )
            
            if not files:
                self.logger.info("No files found in current batch")
                return {'processed': 0, 'sent': 0, 'failed': 0}
            
            self.logger.info(f"Retrieved {len(files)} files from Google Drive")
            
            # Send files to Kafka
            result = self.kafka_producer.send_files(files)
            
            # Update statistics
            processed = len(files)
            sent = result['success']
            failed = result['failure']
            
            self.total_files_processed += processed
            self.total_files_sent += sent
            self.total_files_failed += failed
            
            # Update page token for next batch
            self.current_page_token = next_page_token
            
            self.logger.info(f"Batch processed: {processed} files, {sent} sent, {failed} failed")
            
            return {
                'processed': processed,
                'sent': sent,
                'failed': failed,
                'next_page_token': next_page_token
            }
            
        except Exception as e:
            self.logger.error(f"Error processing files batch: {str(e)}")
            return {'processed': 0, 'sent': 0, 'failed': 0}
    
    def run_single_cycle(self) -> Dict[str, int]:
        """
        Run a single cycle to process files.
        
        Returns:
            Dictionary with cycle statistics.
        """
        cycle_start_time = time.time()
        self.logger.info("Starting file processing cycle...")
        
        total_processed = 0
        total_sent = 0
        total_failed = 0
        
        # Process files in batches until we hit limits or run out of files
        while True:
            # Check if we've hit the max files limit
            if (self.max_files_per_run and 
                self.total_files_processed >= self.max_files_per_run):
                self.logger.info(f"Reached max files limit: {self.max_files_per_run}")
                break
            
            # Process a batch
            batch_result = self.process_files_batch()
            
            total_processed += batch_result['processed']
            total_sent += batch_result['sent']
            total_failed += batch_result['failed']
            
            # If no files were processed or no next page token, we're done
            if batch_result['processed'] == 0 or not self.current_page_token:
                self.logger.info("No more files to process")
                break
            
        
        cycle_duration = time.time() - cycle_start_time
        self.logger.info(f"Cycle completed in {cycle_duration:.2f}s: "
                        f"{total_processed} processed, {total_sent} sent, {total_failed} failed")
        
        return {
            'processed': total_processed,
            'sent': total_sent,
            'failed': total_failed,
            'duration': cycle_duration
        }
    
    def run(self) -> Dict[str, int]:
        """
        Run the job to process files.
        
        Returns:
            Dictionary with run statistics.
        """
        self.is_running = True
        self.logger.info("Starting file processing job...")
        
        try:
            result = self.run_single_cycle()
            return result
        finally:
            self.stop()
    
    def stop(self) -> None:
        """Stop the job gracefully."""
        if not self.is_running:
            return
        
        self.logger.info("Stopping job...")
        self.is_running = False
        
        # Close Kafka producer
        if self.kafka_producer:
            self.kafka_producer.close()
        
        # Log final statistics
        self.logger.info(f"Job stopped. Final stats: {self.total_files_processed} processed, "
                        f"{self.total_files_sent} sent, {self.total_files_failed} failed")
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current job status.
        
        Returns:
            Dictionary with current job status.
        """
        return {
            'is_running': self.is_running,
            'total_files_processed': self.total_files_processed,
            'total_files_sent': self.total_files_sent,
            'total_files_failed': self.total_files_failed,
            'current_page_token': self.current_page_token,
            'batch_size': self.batch_size,
            'max_files_per_run': self.max_files_per_run
        }


def main():
    """Main entry point for the streaming job."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Drive Streaming Job')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Number of files to process per batch')
    parser.add_argument('--max-files', type=int, default=None,
                       help='Maximum files to process per run')
    parser.add_argument('--page-token', type=str, default=None,
                       help='Page token to start from')
    
    args = parser.parse_args()
    
    # Create and run the job
    job = DriveStreamingJob(
        batch_size=args.batch_size,
        max_files_per_run=args.max_files,
        current_page_token=args.page_token
    )
    
    try:
        result = job.run()
        print(f"Job completed: {result}")
    except Exception as e:
        print(f"Job failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
