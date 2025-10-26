# Universal Search

A Python package for streaming Google Drive file metadata to Apache Kafka using Avro serialization and Confluent Schema Registry, with Flink-based parsing and chunking pipeline.

## Architecture

The system consists of multiple components working together:

```
Google Drive → Drive Streaming Job → Kafka (drive-files) → Parser Job → Kafka (drive-files-parsed) → Chunker Job → Kafka (drive-files-chunks)
                                                                    ↓
                                                              Storage Layer
```

### Components

1. **Drive Streaming Job**: Continuously streams Google Drive file metadata to Kafka
2. **Parser Job**: Consumes drive-files, filters PDFs, downloads and parses them, stores content to storage
3. **Chunker Job**: Consumes parsed files, chunks text with configurable window size and overlap
4. **Storage Layer**: Abstracts file storage (local filesystem, S3, etc.)

## Quick Start

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Google Cloud Project with Drive API enabled

### Installation

1. **Clone and install:**
   ```bash
   git clone <repository-url>
   cd universal-search
   pip install -e .
   ```

2. **Set up Google Drive credentials:**
   - Download OAuth2 credentials as `credentials.json`
   - Place in project root

3. **Start services:**
   ```bash
   make docker-up
   ```

4. **Run streaming job:**
   ```bash
   make run-streaming-job
   ```

5. **Run Flink parser job:**
   ```bash
   python -m universal_search.flink_jobs.parser_job
   ```

6. **Run Flink chunker job:**
   ```bash
   python -m universal_search.flink_jobs.chunker_job
   ```

### Development Setup

```bash
# Install development dependencies
make install-dev

# Run tests
make test

# Format code
make format

# Run all checks
make check
```

## Configuration

### Environment Variables

- `STORAGE_TYPE`: Storage backend (`local`, `s3`) - default: `local`
- `STORAGE_ROOT`: Local storage root directory - default: `./storage`
- `FLINK_PARSER_PARALLELISM`: Parser job parallelism - default: `2`
- `FLINK_CHUNKER_PARALLELISM`: Chunker job parallelism - default: `4`
- `FLINK_CHECKPOINT_INTERVAL`: Checkpoint interval in ms - default: `60000`

### Kafka Topics

- `drive-files`: Original Google Drive file metadata
- `drive-files-parsed`: Parsed file references with storage paths
- `drive-files-chunks`: Individual text chunks with metadata

## Project Structure

```
src/universal_search/     # Source code
├── clients/               # Google Drive client
├── producers/             # Kafka producer
├── jobs/                  # Streaming jobs
├── flink_jobs/            # Flink streaming jobs
├── parsers/               # PDF parsing components
├── chunkers/              # Text chunking components
├── storage/                # Storage abstraction layer
└── config/                # Configuration

schemas/                   # Avro schemas
├── drive_file.avsc        # Original file metadata
├── parsed_file.avsc       # Parsed file references
└── file_chunk.avsc        # Text chunks

tests/                     # Test suite
docker/                    # Docker services
docs/                      # Documentation
scripts/                   # Utility scripts
```

## Available Commands

- `make help` - Show all available commands
- `make install` - Install package
- `make install-dev` - Install with dev dependencies
- `make test` - Run tests
- `make lint` - Run linting
- `make format` - Format code
- `make docker-up` - Start Docker services
- `make run-streaming-job` - Run the streaming job

## Flink Jobs

### Parser Job

Processes Google Drive files:
- Filters for PDF files only
- Checks timestamps to avoid reprocessing
- Downloads PDFs from Google Drive
- Extracts text using PyMuPDF
- Stores content in storage layer
- Produces parsed file references to Kafka

### Chunker Job

Processes parsed files:
- Loads text content from storage
- Chunks text with configurable window size (default: 1000 chars) and overlap (default: 200 chars)
- Preserves sentence boundaries where possible
- Produces individual chunks to Kafka

## Storage Layer

The storage layer provides abstraction for storing parsed content:

- **LocalStorageAdapter**: Stores files locally (default)
- **S3StorageAdapter**: Placeholder for S3 storage (not implemented)

To migrate to cloud storage, simply change the `STORAGE_TYPE` environment variable and implement the corresponding adapter.

## Documentation

For detailed documentation, see [docs/README.md](docs/README.md).

## License

MIT License