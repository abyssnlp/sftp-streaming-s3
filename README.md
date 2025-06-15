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
- Kubernetes cluster (local or remote)
- kubectl configured

### Installation & Deployment

```bash
make sftp-server s3-server monitoring run-streamer

# Or step by step:
make sftp-server  
make s3-server    
make monitoring   
make run-streamer 
```

### Configuration

Create a `.env` file in the project root:

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
make run-streamer
make install

# Check infrastructure status
make status
```

## Monitoring

### Grafana Dashboard

The monitoring stack is automatically deployed with:

```bash
make monitoring
```

1. **Access Grafana**: `http://localhost:3000` (admin/admin)
2. **Import Dashboard**: Use `monitoring/grafana/dashboards/sftp-s3-streaming.json`
3. **Configure Prometheus**: Data source should auto-configure to `http://prometheus:9090`

### Key Metrics

- **Memory Usage**: Process RAM consumption (should stay stable)
- **Process Write Bytes**: Disk writes by the process (should stay near 0 for true streaming)
- **Transfer Throughput**: Real-time MB/s transfer rate
- **Chunks Processed**: Progress indicator

### Access URLs

After running `make monitoring`, make sure you port forward the necessary services.
example:
```bash
kubectl port-forward -n monitoring svc/grafana 3000:3000
```

To access the services, use the following URLs:
```bash
- **Grafana**: `http://localhost:3000`
- **Prometheus**: `http://localhost:9090`
- **MinIO Console**: `http://localhost:9001`
- **S3 API**: `http://localhost:9000`

## Make Commands

```bash
# Infrastructure
make sftp-server 
make s3-server   
make monitoring  
make status      

# Application  
make install        
make shell          
make run-streamer   

# Utilities
make dev-setup      
make generate-test-file  
make clean-all      
make help           
```

## Performance Tuning

Adjust concurrency settings in `sftp_streaming/main.py`:

```python
CHUNK_SIZE = 64 * 1024 * 1024        
MAX_CONCURRENT_UPLOADS = 10          
MAX_CONCURRENT_READS = 6             
```

## Troubleshooting

### Check Deployment Status

```bash
make status
```

### Common Issues

- **Pod not starting**: Check with `kubectl get pods --all-namespaces`
- **SSH host key errors**: Set `known_hosts=None` in connection params
- **High disk write bytes**: Check if MinIO and app are on same disk
- **Low throughput**: Increase concurrent workers or adjust chunk size
- **Memory issues**: Reduce chunk size or concurrent operations

### Logs

```bash
# Check application logs
kubectl logs -n sftp-ns deployment/sftp-server
kubectl logs -n s3-ns deployment/minio
kubectl logs -n monitoring deployment/grafana
```

### Clean Restart

```bash
make clean-all
make dev-setup
make run-streamer
```

## Development Workflow

```bash
make dev-setup

# to generate test file
make generate-test-file

make run-streamer

# Open http://localhost:3000 and import the dashboard
```

## Architecture

```
SFTP Server (K8s) → [Async Readers] → [Memory Buffer] → [Async Uploaders] → S3 Storage (K8s)
                              ↓
                    [Prometheus Metrics] → [Grafana Dashboard (K8s)]
```

The tool streams data directly from SFTP to S3 without writing to local disk, using async I/O for optimal performance. All infrastructure runs in Kubernetes for easy deployment and scaling.
