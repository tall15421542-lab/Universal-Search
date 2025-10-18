# Universal Search - Google Drive to Kafka

This project provides a Google Drive client that can list files from Google Drive and send them to Apache Kafka using Avro serialization and Confluent Schema Registry.

## Features

- **Google Drive Integration**: Authenticate and list files from Google Drive using OAuth2
- **Kafka Producer**: Send Google Drive file metadata to Kafka topics
- **Streaming Job**: Continuous or one-time streaming of Drive files to Kafka
- **Avro Serialization**: Use Avro format for message serialization
- **Schema Registry**: Integrate with Confluent Schema Registry for schema management
- **Message Keys**: Use file IDs as Kafka message keys for proper partitioning
- **Comprehensive Testing**: Full test suite with mocking for reliable testing
- **Monitoring & Logging**: Built-in progress tracking and detailed logging

## Prerequisites

- Python 3.8+
- Docker and Docker Compose (for Kafka and Schema Registry)
- Google Cloud Project with Drive API enabled
- OAuth2 credentials for Google Drive API

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd universal-search
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up Google Drive credentials:
   - Create a Google Cloud Project
   - Enable the Google Drive API
   - Create OAuth2 credentials and download as `credentials.json`
   - Place `credentials.json` in the project root

4. Start Kafka and Schema Registry:
```bash
docker-compose up -d
```

## Usage

### Streaming Job (Recommended)

The streaming job provides an easy way to stream Google Drive files to Kafka:

```bash
# Run with default settings
python drive_streaming_job.py

# Custom batch size and file limit
python drive_streaming_job.py --batch-size 50 --max-files 200

# Start from a specific page token
python drive_streaming_job.py --page-token "your_page_token"
```

### Command Line Options for Streaming Job

```bash
# Basic usage
python drive_streaming_job.py

# Custom batch size
python drive_streaming_job.py --batch-size 25

# Process specific number of files
python drive_streaming_job.py --max-files 1000

# Start from specific page token
python drive_streaming_job.py --page-token "CAEQ..."
```

### Basic Usage (Legacy)

Run the example with Kafka integration:
```bash
python example_usage.py
```

### Command Line Options (Legacy)

```bash
# Run with Kafka integration (default)
python example_usage.py

# Run without Kafka (only list files)
python example_usage.py --no-kafka

# Demonstrate Kafka functionality with sample data
python example_usage.py --kafka-only

# Run Kafka producer demonstration
python example_usage.py --demo
```

### Programmatic Usage

#### Using the Streaming Job

```python
from drive_streaming_job import DriveStreamingJob

# Create and run streaming job with default settings
job = DriveStreamingJob()
result = job.run()
print(f"Processed {result['processed']} files")

# Custom configuration
job = DriveStreamingJob(
    batch_size=100,
    max_files_per_run=500,
    current_page_token="your_page_token"
)
result = job.run()
print(f"Processed {result['processed']} files")
```

#### Using Individual Components

```python
from drive_client import DriveClient

# Create client and run with Kafka
client = DriveClient()
client.main(enable_kafka=True)

# Or disable Kafka
client.main(enable_kafka=False)
```

### Using Kafka Producer Directly

```python
from kafka_producer import DriveFileKafkaProducer

# Sample file data
file_data = {
    'id': 'file_123',
    'name': 'Document.pdf',
    'mimeType': 'application/pdf',
    'createdTime': '2024-01-01T10:00:00.000Z',
    'modifiedTime': '2024-01-01T12:00:00.000Z',
    'size': 1024000,
    'webViewLink': 'https://drive.google.com/file/d/file_123/view',
    'webContentLink': 'https://drive.google.com/uc?id=file_123',
    'parents': ['folder_1'],
    'owners': [
        {
            'displayName': 'John Doe',
            'emailAddress': 'john@example.com'
        }
    ]
}

# Send to Kafka
with DriveFileKafkaProducer() as producer:
    result = producer.send_files([file_data])
    print(f"Sent {result['success']} files successfully")
```

## Configuration

### Kafka Configuration

The Kafka configuration is managed in `kafka_config.py`:

```python
# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'drive-files'

# Schema Registry settings
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
```

### Environment Variables

You can override configuration using environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS=your-kafka-server:9092
export SCHEMA_REGISTRY_URL=http://your-schema-registry:8081
export DRIVE_FILES_TOPIC=your-topic-name
```

## Message Keys and Partitioning

The Kafka producer uses Google Drive file IDs as message keys, which provides several benefits:

- **Consistent Partitioning**: Messages for the same file always go to the same partition
- **Ordering Guarantees**: Messages for a specific file maintain order within their partition
- **Efficient Processing**: Consumers can process files in parallel while maintaining per-file ordering
- **Deduplication**: Duplicate messages for the same file can be easily identified

If a file ID is missing, a fallback key is generated using the format `unknown_{timestamp}`.

## Avro Schema

The project uses a comprehensive Avro schema for Google Drive file metadata:

```json
{
  "type": "record",
  "name": "DriveFile",
  "namespace": "com.universalsearch.drive",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "mimeType", "type": ["null", "string"], "default": null},
    {"name": "createdTime", "type": ["null", "string"], "default": null},
    {"name": "modifiedTime", "type": ["null", "string"], "default": null},
    {"name": "size", "type": ["null", "long"], "default": null},
    {"name": "webViewLink", "type": ["null", "string"], "default": null},
    {"name": "webContentLink", "type": ["null", "string"], "default": null},
    {"name": "parents", "type": {"type": "array", "items": "string"}, "default": []},
    {"name": "owners", "type": {"type": "array", "items": {"type": "record", "name": "Owner", "fields": [...]}}, "default": []},
    {"name": "timestamp", "type": "long"}
  ]
}
```

## Testing

Run the test suite:

```bash
# Run all tests
python -m pytest -v

# Run only Drive client tests
python -m pytest test_drive_client.py -v

# Run only Kafka producer tests
python -m pytest test_kafka_producer.py -v

# Run streaming job tests
python test_streaming_job.py

# Run with coverage
python -m pytest --cov=kafka_producer --cov=drive_client --cov=drive_streaming_job
```

### Example Usage

Run the example scripts to see the streaming job in action:

```bash
# Run streaming job examples
python example_streaming_job.py

# Run legacy examples
python example_usage.py
```

## Docker Services

The `docker-compose.yml` includes:

- **Kafka**: Apache Kafka broker (port 9092)
- **Schema Registry**: Confluent Schema Registry (port 8081)
- **Kafka UI**: Web interface for Kafka management (port 8080)

### Starting Services

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs kafka
docker-compose logs schema-registry
```

### Stopping Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

## Monitoring

### Kafka UI

Access the Kafka UI at http://localhost:8080 to:
- View topics and messages
- Monitor consumer groups
- Browse schemas in Schema Registry

### Schema Registry API

Access Schema Registry directly at http://localhost:8081:
- View registered schemas
- Check schema versions
- Validate schema compatibility

## Troubleshooting

### Common Issues

1. **Kafka Connection Failed**
   - Ensure Docker services are running: `docker-compose ps`
   - Check Kafka logs: `docker-compose logs kafka`

2. **Schema Registry Connection Failed**
   - Verify Schema Registry is healthy: `curl http://localhost:8081/subjects`
   - Check Schema Registry logs: `docker-compose logs schema-registry`

3. **Google Drive Authentication Failed**
   - Ensure `credentials.json` exists and is valid
   - Check OAuth2 scopes include Drive API access

4. **Avro Serialization Errors**
   - Verify schema file exists: `schemas/drive_file.avsc`
   - Check schema compatibility with Schema Registry

### Debug Mode

Enable debug logging by setting environment variables:

```bash
export KAFKA_LOG_LEVEL=DEBUG
export SCHEMA_REGISTRY_LOG_LEVEL=DEBUG
```

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Google Drive  │───▶│   Drive Client   │───▶│  Kafka Producer │
│      API        │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
                                               ┌─────────────────┐
                                               │ Schema Registry │
                                               │                 │
                                               └─────────────────┘
                                                         │
                                                         ▼
                                               ┌─────────────────┐
                                               │   Kafka Topic   │
                                               │  (drive-files)  │
                                               └─────────────────┘
```

### Streaming Job Architecture

The streaming job orchestrates the entire process:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Streaming Job   │───▶│   Drive Client   │───▶│  Kafka Producer │
│                 │    │                  │    │                 │
│ • Batch Control │    │ • Authentication │    │ • Avro Serial.  │
│ • Pagination    │    │ • File Listing   │    │ • Message Keys  │
│ • Error Handling│    │ • Rate Limiting  │    │ • Delivery Conf │
│ • Monitoring    │    │ • Retry Logic    │    │ • Resource Mgmt │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                                               │
         ▼                                               ▼
┌─────────────────┐                            ┌─────────────────┐
│   Logging &     │                            │   Kafka Topic   │
│   Monitoring    │                            │  (drive-files)  │
└─────────────────┘                            └─────────────────┘
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License.