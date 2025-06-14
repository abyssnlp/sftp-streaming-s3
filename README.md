SFTP Streaming to S3
========================
Example for async SFTP to S3 streaming with real-time monitoring.

## Features

- 🚀 **Async streaming**: Direct SFTP to S3 transfer without disk buffering
- 📊 **Real-time metrics**: Process memory and disk I/O monitoring
- 🔄 **Concurrent processing**: Configurable concurrent reads and uploads
- ✅ **Integrity verification**: Automatic file size validation
- 📈 **Grafana dashboard**: Visual performance monitoring

## Quick Start

### Prerequisites

- Python 3.9+
- Poetry
- Running SFTP server
- Running S3-compatible storage (MinIO/AWS S3)
- Prometheus Push Gateway (optional, for metrics)

### Installation

```bash
poetry install
poetry shell
```

### Configuration

Set environment variables or create a `.env` file:

```bash
SFTP_HOST=localhost
SFTP_PORT=30022
SFTP_USER=airflow
SFTP_PASSWORD=airflow123
S3_BUCKET=data
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
PUSH_GATEWAY=http://localhost:9091
```

### Running the Streamer

```bash
poetry run python run_streamer.py
# or
poetry shell
python run_streamer.py
```

## Monitoring

### Grafana Dashboard

1. **Import Dashboard**: Load `monitoring/grafana/dashboards/sftp-s3-streaming.json`
2. **Configure Prometheus**: Point to your Prometheus instance
3. **View Metrics**: Monitor real-time performance

### Key Metrics

- **Memory Usage**: Process RAM consumption (should stay stable)
- **Process Write Bytes**: Disk writes by the process (should stay near 0 for true streaming)
- **Transfer Throughput**: Real-time MB/s transfer rate
- **Chunks Processed**: Progress indicator


## Performance Tuning

Adjust concurrency settings in `sftp_streaming/main.py`:

```python
CHUNK_SIZE = 64 * 1024 * 1024        
MAX_CONCURRENT_UPLOADS = 10          
MAX_CONCURRENT_READS = 6 
```

## Troubleshooting

### Common Issues

- **SSH host key errors**: Set `known_hosts=None` in connection params
- **High disk write bytes**: Check if MinIO and app are on same disk
- **Low throughput**: Increase concurrent workers or adjust chunk size
- **Memory issues**: Reduce chunk size or concurrent operations

## Architecture

```
SFTP Server → [Async Readers] → [Memory Buffer] → [Async Uploaders] → S3 Storage
                     ↓
            [Prometheus Metrics] → [Grafana Dashboard]
```

The tool streams data directly from SFTP to S3 without writing to local disk, using async I/O for optimal performance.
