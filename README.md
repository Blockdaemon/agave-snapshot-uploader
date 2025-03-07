# Solana Snapshot Uploader

A Golang application that monitors a directory for Solana snapshot files, processes them based on specific rules, and uploads valid snapshots to an S3-compatible storage.

## Features

- Monitors a directory for new Solana snapshot files
- Validates snapshots based on configurable rules:
  - Full Snapshots: Only uploads if the difference between the last uploaded full snapshot's slot and the new snapshot's slot is at least 25,000 slots (configurable)
  - Incremental Snapshots: Ensures the first slot matches an existing full snapshot and the second slot is within 500 slots of the previous incremental snapshot (configurable)
  - Option to disable incremental snapshot uploads entirely
- Generates and uploads metadata files with:
  - Solana client version and feature set
  - Snapshot slot number
  - Timestamp of when the snapshot was generated (both Unix and human-readable formats)
  - SHA256 hash of the snapshot file for verification
  - Upload status and uploader information
- Auto-detects Solana version and feature set via RPC
- Automatic retention management to delete old snapshots based on configurable time period
- Supports configuration through a config file or environment variables
- Can run as a systemd service or inside a Docker container
- Advanced features:
  - Progress reporting during uploads with file size and percentage complete
  - Status tracking to prevent duplicate uploads across multiple instances
  - Automatic exclusion of temporary files and files in the remote directory
  - Robust multipart upload system with automatic resumption of failed uploads
  - Cleanup of abandoned multipart uploads to prevent storage waste

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

# Solana configuration
solana_version: "1.18.5"  # Optional, auto-detected if not specified
solana_rpc_url: "http://localhost:8899"  # Used to auto-detect Solana version and feature set

# Minimum slot gap between full snapshots
full_snapshot_gap: 25000

# Maximum slot gap for incremental snapshots
incremental_gap: 500

# Enable or disable incremental snapshot uploads (true/false)
enable_incremental_snap: true

# Retention settings
enable_retention: true  # Enable automatic deletion of old snapshots
retention_period_hours: 168  # Delete snapshots older than 7 days

# Log level (debug, info, warn, error)
log_level: "info"

# Hostname to identify this instance (optional, auto-detected if not specified)
hostname: "node-1"
```

### Environment Variables

Alternatively, you can use environment variables:

- `WATCH_DIR`: Directory to monitor for snapshots
- `S3_ENDPOINT`: S3-compatible storage URL
- `S3_BUCKET`: S3 bucket name
- `S3_ACCESS_KEY`: S3 access key
- `S3_SECRET_KEY`: S3 secret key
- `SOLANA_VERSION`: Solana version to include in metadata (optional if `SOLANA_RPC_URL` is provided)
- `SOLANA_RPC_URL`: Solana RPC URL for auto-detecting version and feature set
- `FULL_SNAPSHOT_GAP`: Minimum slot gap between full snapshots
- `INCREMENTAL_GAP`: Maximum slot gap for incremental snapshots
- `ENABLE_INCREMENTAL_SNAP`: Enable or disable incremental snapshot uploads (true/false)
- `ENABLE_RETENTION`: Enable or disable automatic deletion of old snapshots (true/false)
- `RETENTION_PERIOD_HOURS`: Number of hours to keep snapshots before deleting them (default: 168, which is 7 days)
- `LOG_LEVEL`: Log level (debug, info, warn, error)
- `HOSTNAME`: Hostname to identify this instance

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

## Advanced Features

### Status Tracking

The application uses a status tracking system to prevent duplicate uploads across multiple instances:

1. Before uploading a snapshot, it creates a metadata file with an "uploading" status
2. After successfully uploading the snapshot, it updates the metadata file with a "completed" status
3. When checking if a snapshot exists, it also checks its status to avoid duplicate uploads
4. If another instance is already uploading a snapshot, it will skip it

This allows multiple instances to run simultaneously without conflicts.

### File Exclusion Rules

The application automatically excludes the following files from processing:

- Files with names containing "tmp" or "temp" (case-insensitive)
- Files starting with a dot (hidden files)
- Files ending with `.tmp`
- Files in the `watchdir/remote` directory

### Progress Reporting

The application provides detailed progress reporting during uploads:

- Shows the file size in a human-readable format (e.g., 1.2 GiB)
- Displays progress updates during the upload, showing:
  - The percentage of the file uploaded
  - The amount uploaded so far (e.g., 500 MiB)
  - The total file size (e.g., 1.2 GiB)

### Directory Structure

The application creates a `remote` directory inside the watch directory. Files in this directory are never processed or uploaded, making it a safe place to store files that should not be uploaded.

### Retention Management

The application can automatically delete old snapshots from the S3 bucket based on a configurable retention period:

- Enable retention management with the `enable_retention` configuration option
- Set the retention period in hours with the `retention_period_hours` option (default: 168 hours, which is 7 days)
- The application will delete snapshots older than the retention period
- The latest full snapshot is always preserved, regardless of age
- When a full snapshot is deleted, all its incremental snapshots are also deleted
- Cleanup runs on startup and then every 6 hours

This helps manage storage costs and prevents the S3 bucket from growing indefinitely.

### Resumable Uploads

The application includes a robust multipart upload system that can automatically resume failed uploads:

- Large files (>5MB) are automatically uploaded using multipart uploads
- Upload progress is tracked in a local state file
- If an upload is interrupted, it will automatically resume from where it left off when the application restarts
- Abandoned multipart uploads are automatically cleaned up after 24 hours to prevent storage waste

This feature ensures that even in case of network issues or application restarts, snapshot uploads will complete successfully without having to start from the beginning.

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
