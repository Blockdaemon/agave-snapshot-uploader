# Solana Snapshot Uploader

A Golang application that monitors a directory for Solana snapshot files, processes them based on specific rules, and uploads valid snapshots to an S3-compatible storage.

## Features

- Monitors a directory for new Solana snapshot files
- Validates snapshots based on configurable rules:
  - Full Snapshots: Only uploads if the difference between the last uploaded full snapshot's slot and the new snapshot's slot is at least 25,000 slots (configurable)
  - Incremental Snapshots: Ensures the first slot matches an existing full snapshot and the second slot is within 500 slots of the previous incremental snapshot (configurable)
- Generates and uploads metadata files with:
  - Solana client version
  - Snapshot slot number
  - Timestamp of when the snapshot was generated
  - SHA256 hash of the snapshot file for verification
- Supports configuration through a config file or environment variables
- Can run as a systemd service or inside a Docker container

## Installation

### Prerequisites

- Go 1.23 or higher
- Access to an S3-compatible storage

### Building from Source

```bash
git clone https://github.com/maestroi/anza-snapshot-uploader.git
cd anza-snapshot-uploader
go build -o snapshot-monitor ./cmd/snapshot-monitor
```

### Using Pre-built Binaries

You can download pre-built binaries from the [Releases](https://github.com/maestroi/anza-snapshot-uploader/releases) page. Each release includes:

- A Linux binary (`snapshot-monitor-linux-amd64`)
- A deployment package (`snapshot-monitor-vX.Y.Z.tar.gz`) containing:
  - The binary
  - Configuration examples
  - Deployment files for systemd and Docker
  - Documentation

To use the deployment package:

```bash
tar -xzvf snapshot-monitor-vX.Y.Z.tar.gz
cd snapshot-monitor-vX.Y.Z
```

## Configuration

The application can be configured using a YAML configuration file or environment variables.

### Configuration File

Create a `config.yaml` file (see `deployment/config.yaml` for an example):

```yaml
# Directory to monitor for snapshots
watch_dir: "/snapshots"

# S3-compatible storage configuration
s3_endpoint: "https://s3.example.com"
s3_bucket: "solana-snapshots"
s3_access_key: "your-access-key"
s3_secret_key: "your-secret-key"

# Solana version to include in metadata (optional, auto-detected if not specified)
solana_version: "1.18.5"

# Minimum slot gap between full snapshots
full_snapshot_gap: 25000

# Maximum slot gap for incremental snapshots
incremental_gap: 500

# Log level (debug, info, warn, error)
log_level: "info"
```

### Environment Variables

Alternatively, you can use environment variables:

- `WATCH_DIR`: Directory to monitor for snapshots
- `S3_ENDPOINT`: S3-compatible storage URL
- `S3_BUCKET`: S3 bucket name
- `S3_ACCESS_KEY`: S3 access key
- `S3_SECRET_KEY`: S3 secret key
- `SOLANA_VERSION`: Solana version to include in metadata
- `FULL_SNAPSHOT_GAP`: Minimum slot gap between full snapshots
- `INCREMENTAL_GAP`: Maximum slot gap for incremental snapshots
- `LOG_LEVEL`: Log level (debug, info, warn, error)

## Usage

### Running Directly

```bash
./snapshot-monitor -config config.yaml
```

Or with environment variables:

```bash
export WATCH_DIR=/snapshots
export S3_ENDPOINT=https://s3.example.com
export S3_BUCKET=solana-snapshots
export S3_ACCESS_KEY=your-access-key
export S3_SECRET_KEY=your-secret-key
./snapshot-monitor
```

### Running as a Systemd Service

1. Copy the binary to `/usr/local/bin/`:

```bash
sudo cp snapshot-monitor /usr/local/bin/
```

2. Copy the systemd service file and environment file:

```bash
sudo cp deployment/systemd/snapshot-monitor.service /etc/systemd/system/
sudo cp deployment/systemd/snapshot-monitor.env /etc/
```

3. Edit the environment file with your configuration:

```bash
sudo nano /etc/snapshot-monitor.env
```

4. Enable and start the service:

```bash
sudo systemctl enable snapshot-monitor
sudo systemctl start snapshot-monitor
```

### Running with Docker

1. Pull the Docker image:

```bash
docker pull ghcr.io/maestroi/anza-snapshot-uploader:latest
```

2. Run the container:

```bash
docker run -d \
  -v /data/snapshots:/snapshots \
  -e WATCH_DIR=/snapshots \
  -e S3_ENDPOINT=https://s3.example.com \
  -e S3_BUCKET=solana-snapshots \
  -e S3_ACCESS_KEY=your-access-key \
  -e S3_SECRET_KEY=your-secret-key \
  ghcr.io/maestroi/anza-snapshot-uploader:latest
```

### Running with Docker Compose

1. Copy the Docker Compose file:

```bash
cp deployment/docker/docker-compose.yml /your/preferred/location/
```

2. Edit the Docker Compose file with your configuration:

```bash
nano /your/preferred/location/docker-compose.yml
```

3. Start the service:

```bash
cd /your/preferred/location/
docker-compose up -d
```

## CI/CD

This project uses GitHub Actions for continuous integration and deployment:

- **Test Workflow**: Runs tests and builds the application on pull requests and pushes to the main branch.
- **Docker Workflow**: Builds and pushes a Docker image to GitHub Container Registry on pushes to the main branch.
- **Release Workflow**: Builds and uploads a Linux binary and Docker image when a new release is created.

### Docker Images

Docker images are available from the GitHub Container Registry:

```bash
docker pull ghcr.io/maestroi/anza-snapshot-uploader:latest
```

You can also use a specific version:

```bash
docker pull ghcr.io/maestroi/anza-snapshot-uploader:v1.0.0
```

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.
