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
- Automatic upload of genesis.tar.bz2 file if not present in the bucket
- Advanced retention management:
  - Configurable count-based retention: keep N most recent full and incremental snapshots
  - Configurable time-based retention: keep snapshots newer than X hours
  - Independent retention policies for full and incremental snapshots
- Organized latest.json file with optimized field ordering and no duplicate fields
- Supports configuration through a config file or environment variables
- Can run as a systemd service or inside a Docker container
- Advanced features:
  - High-performance pure Go implementation for S3 uploads with multipart support
  - Configurable upload performance parameters (coming in a future version):
    - Parallel uploads (currently fixed at 5)
    - Chunk size for multipart uploads (currently fixed at 32MB)
  - Progress reporting during uploads with file size and percentage complete
  - Status tracking to prevent duplicate uploads across multiple instances
  - Automatic exclusion of temporary files and files in the remote directory
  - Robust multipart upload system with automatic resumption of failed uploads
  - Cleanup of abandoned multipart uploads to prevent storage waste
  - Enhanced S3 compatibility with proper Content-Length headers for all uploads
  - Improved error handling and retry mechanism for reliable uploads

## Installation

### Prerequisites

- Go 1.23 or higher
- Access to an S3-compatible storage

### Building from Source

```bash
git clone https://github.com/maestroi/agave-snapshot-uploader.git
cd agave-snapshot-uploader
go build -o snapshot-monitor ./cmd/snapshot-monitor
```

### Using Pre-built Binaries

You can download pre-built binaries from the [Releases](https://github.com/maestroi/agave-snapshot-uploader/releases) page. Each release includes:

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
s3_namespace: "mainnet"  # Optional: namespace prefix for all S3 remote paths
s3_access_key: "your-access-key"
s3_secret_key: "your-secret-key"
s3_public_endpoint: "https://snapshots.example.com"  # Optional public endpoint for URLs in metadata

# Solana configuration
solana_version: "1.18.5"  # Optional, auto-detected if not specified
solana_rpc_url: "http://localhost:8899"  # Used to auto-detect Solana version and feature set

# Minimum slot gap between full snapshots
full_snapshot_gap: 25000

# Maximum slot gap for incremental snapshots
incremental_gap: 500

# Enable or disable incremental snapshot uploads (true/false)
enable_incremental_snap: true

# Enable or disable metadata cleanup (true/false)
enable_metadata_cleanup: true

# Retention settings
enable_retention: true  # Enable automatic deletion of old snapshots (in multi-instance setups, enable on only one instance)

# Legacy retention setting - will be used if the more specific settings below are not provided
retention_period_hours: 168  # Delete snapshots older than 7 days

# Full snapshot retention (count and time-based)
full_snapshot_retain_count: 3  # Keep at least this many recent full snapshots
full_snapshot_retain_hours: 672  # Keep full snapshots for 28 days (4 weeks)

# Incremental snapshot retention (count and time-based)
incremental_retain_count: 5  # Keep this many recent incremental snapshots per full snapshot
incremental_retain_hours: 168  # Keep incremental snapshots for 7 days (1 week)

# Log level (debug, info, warn, error)
log_level: "info"

# Hostname to identify this instance (optional, auto-detected if not specified)
hostname: "node-1"
```

### Environment Variables

Alternatively, you can use environment variables:

- `WATCH_DIR`: Directory to monitor for snapshots
- `S3_ENDPOINT`: S3-compatible storage URL
- `S3_PUBLIC_ENDPOINT`: Public endpoint URL for snapshot access
- `S3_BUCKET`: S3 bucket name
- `S3_NAMESPACE`: Optional namespace prefix for all S3 remote paths
- `S3_ACCESS_KEY`: S3 access key
- `S3_SECRET_KEY`: S3 secret key
- `S3_UPLOAD_CONCURRENCY`: Number of parallel upload operations (will be supported in a future version)
- `S3_CHUNK_SIZE_MB`: Size of each upload chunk in MB (will be supported in a future version)
- `SOLANA_VERSION`: Solana version to include in metadata (optional if `SOLANA_RPC_URL` is provided)
- `SOLANA_RPC_URL`: Solana RPC URL for auto-detecting version and feature set
- `FULL_SNAPSHOT_GAP`: Minimum slot gap between full snapshots
- `INCREMENTAL_GAP`: Maximum slot gap for incremental snapshots
- `ENABLE_INCREMENTAL_SNAP`: Enable or disable incremental snapshot uploads (true/false)
- `ENABLE_RETENTION`: Enable or disable automatic deletion of old snapshots (true/false)
- `RETENTION_PERIOD_HOURS`: Number of hours to keep snapshots before deleting them (legacy setting)
- `FULL_SNAPSHOT_RETAIN_COUNT`: Number of recent full snapshots to keep
- `FULL_SNAPSHOT_RETAIN_HOURS`: Number of hours to keep full snapshots
- `INCREMENTAL_RETAIN_COUNT`: Number of recent incremental snapshots to keep per full snapshot
- `INCREMENTAL_RETAIN_HOURS`: Number of hours to keep incremental snapshots
- `ENABLE_METADATA_CLEANUP`: Enable or disable metadata cleanup (true/false)
- `LOG_LEVEL`: Log level (debug, info, warn, error)
- `HOSTNAME`: Hostname to identify this instance

## Usage

### Running Directly

```bash
./snapshot-monitor -config config.yaml
```

The `-config` flag defaults to `config.yaml` in the current directory, so you can also run:

```bash
./snapshot-monitor
```

Or with a custom config file:

```bash
./snapshot-monitor -config /path/to/custom-config.yaml
```

Or with environment variables:

```bash
export WATCH_DIR=/snapshots
export S3_ENDPOINT=https://s3.example.com
export S3_BUCKET=solana-snapshots
export S3_NAMESPACE=mainnet
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
docker pull ghcr.io/maestroi/agave-snapshot-uploader:latest
```

2. Run the container:

```bash
docker run -d \
  -v /data/snapshots:/snapshots \
  -e WATCH_DIR=/snapshots \
  -e S3_ENDPOINT=https://s3.example.com \
  -e S3_BUCKET=solana-snapshots \
  -e S3_NAMESPACE=mainnet \
  -e S3_ACCESS_KEY=your-access-key \
  -e S3_SECRET_KEY=your-secret-key \
  ghcr.io/maestroi/agave-snapshot-uploader:latest
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

### Genesis File Auto-Upload

The application automatically checks for and uploads the genesis.tar.bz2 file if it doesn't exist in the S3 bucket:

- On startup, it checks if genesis.tar.bz2 exists in the S3 bucket
- If not, it looks for the file in the watch directory and its parent directory
- If found, it uploads the file to the S3 bucket
- This ensures that validators can access both snapshots and the genesis file from the same bucket

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
  - The current upload speed (e.g., 25 MiB/s)
  - The estimated time remaining (e.g., 2.5 min)

This real-time feedback helps users monitor the progress of large snapshot uploads and estimate completion times.

### Directory Structure

The application creates a `remote` directory inside the watch directory. Files in this directory are never processed or uploaded, making it a safe place to store files that should not be uploaded.

### Advanced Retention Management

The application offers sophisticated retention controls to manage storage costs:

- **Dual Retention Policies**: Both count-based and time-based retention can be applied simultaneously
- **Granular Controls**: Separate settings for full and incremental snapshots
- **Count-based Retention**:
  - `full_snapshot_retain_count`: Keep N most recent full snapshots (default: 3)
  - `incremental_retain_count`: Keep N most recent incremental snapshots per full snapshot (default: 5)
- **Time-based Retention**:
  - `full_snapshot_retain_hours`: Keep full snapshots newer than X hours (default: 672 hours / 28 days)
  - `incremental_retain_hours`: Keep incremental snapshots newer than X hours (default: 168 hours / 7 days)
- **Implementation Details**:
  - The latest full snapshot is always preserved, regardless of age
  - When a full snapshot is deleted, all its incremental snapshots are also deleted
  - Snapshots are kept if they satisfy either the count OR time-based retention
  - Cleanup runs on startup and then every 6 hours

This advanced retention system allows fine-tuned control over storage usage while ensuring critical snapshots remain available.

### Optimized latest.json File

The application generates an optimized latest.json file with:

- Organized field ordering for better readability
- Removal of duplicate fields (no more redundant incremental snapshot information)
- Proper handling of both full and incremental snapshot information
- Inclusion of genesis hash for validator setup verification
- Backward compatibility with older clients

### Performance Tuning

The application will support configurable upload performance tuning in a future version. Currently, the following settings are fixed:

- **Parallel Upload Operations**: 
  - Fixed at 5 concurrent uploads
  - Will be configurable in a future version
  
- **Chunk Size**:
  - Fixed at 32MB
  - Will be configurable in a future version

When these settings become configurable, they will allow you to fine-tune the performance based on your specific network conditions, server resources, and the S3-compatible storage provider's capabilities.

### Multi-Instance Retention Management

When running multiple instances of the snapshot uploader that share the same S3 bucket, special consideration should be given to retention management:

- **Single Retention Manager**: Configure retention (`enable_retention: true`) on only one instance to prevent concurrent cleanup operations that could conflict with each other.

- **Recommended Setup**:
  - **Primary Instance**: Enable retention with your desired settings (`enable_retention: true`)
  - **Secondary Instances**: Disable retention (`enable_retention: false`)

- **Alternative Approach**: If you need high availability for retention management, you can enable retention on multiple instances but should:
  - Use identical retention settings across all instances
  - Set longer minimum intervals between cleanups (the default 5-minute cooldown helps prevent overlapping operations)
  - Be aware that multiple instances may occasionally attempt cleanup operations in close succession

- **Centralized Configuration**: For larger deployments, consider implementing a central configuration service that:
  - Stores retention settings in a shared database or configuration file
  - Designates a single "leader" instance responsible for retention
  - Implements leader election if the designated retention manager goes offline

The default implementation includes safeguards (cleanup is only performed if it hasn't run in the last 5 minutes) to minimize the risk of concurrent deletion operations, but for optimal operation, retention management should be assigned to a single instance when possible.

### Resumable Uploads

The application includes a robust multipart upload system that can automatically resume failed uploads:

- Large files (>5MB) are automatically uploaded using multipart uploads
- Upload progress is tracked in a local state file stored in the system's temporary directory
- If an upload is interrupted, it will automatically resume from where it left off when the application restarts
- Abandoned multipart uploads are automatically cleaned up after 24 hours to prevent storage waste
- Fallback mechanisms ensure uploads work even with limited permissions:
  - If the application can't write to the snapshot directory, it uses the system's temporary directory
  - If multipart uploads can't be used due to permission issues, it falls back to regular uploads

This feature ensures that even in case of network issues, application restarts, or permission constraints, snapshot uploads will complete successfully without having to start from the beginning.

## CI/CD

This project uses GitHub Actions for continuous integration and deployment:

- **Test Workflow**: Runs tests and builds the application on pull requests and pushes to the main branch.
- **Docker Workflow**: Builds and pushes a Docker image to GitHub Container Registry on pushes to the main branch.
- **Release Workflow**: Builds and uploads a Linux binary and Docker image when a new release is created.

### Docker Images

Docker images are available from the GitHub Container Registry:

```bash
docker pull ghcr.io/maestroi/agave-snapshot-uploader:latest
```

You can also use a specific version:

```bash
docker pull ghcr.io/maestroi/agave-snapshot-uploader:v1.0.0
```

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

### S3 Compatibility

The application is designed to work with any S3-compatible storage service, including:

- Amazon S3
- MinIO
- Ceph Object Gateway
- Wasabi
- Backblaze B2 (with S3 compatibility)
- DigitalOcean Spaces
- Linode Object Storage

The application ensures proper Content-Length headers are included in all S3 requests, which is required by many S3-compatible services. This prevents common "411 Length Required" errors that can occur with some S3 implementations.
