# Universal Search

A Python package for streaming Google Drive file metadata to Apache Kafka using Avro serialization and Confluent Schema Registry.

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

## Project Structure

```
src/universal_search/     # Source code
├── clients/               # Google Drive client
├── producers/             # Kafka producer
├── jobs/                  # Streaming jobs
└── config/                # Configuration

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

## Documentation

For detailed documentation, see [docs/README.md](docs/README.md).

## License

MIT License