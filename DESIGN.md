# Solana Snapshot Uploader - Design Document

## Overview

The Solana Snapshot Uploader is a robust Go application designed to monitor, process, and upload Solana blockchain snapshots to S3-compatible storage. It provides automated snapshot management with configurable rules, metadata generation, and retention policies.

## Architecture

### Core Components

1. **Snapshot Monitor (`pkg/monitor`)**
   - Main monitoring service that watches for new snapshot files
   - Implements file system event handling
   - Manages snapshot processing pipeline
   - Handles concurrent upload operations

2. **Snapshot Processor (`pkg/snapshot`)**
   - Validates and processes snapshot files
   - Implements slot gap validation rules
   - Manages snapshot metadata generation
   - Handles snapshot type detection (full vs incremental)

3. **S3 Storage (`pkg/s3`)**
   - Manages S3-compatible storage interactions
   - Implements multipart upload functionality
   - Handles upload retries and resumption
   - Manages cleanup of abandoned uploads

4. **Metadata Management (`pkg/metadata`)**
   - Generates and manages snapshot metadata
   - Tracks upload status and progress
   - Maintains version and feature set information
   - Implements status tracking for concurrent operations

5. **Configuration (`pkg/config`)**
   - Manages application configuration
   - Supports both YAML and environment variable configuration
   - Validates configuration parameters
   - Provides default values and configuration merging

6. **Solana Integration (`pkg/solana`)**
   - Handles Solana RPC interactions
   - Auto-detects Solana version and feature set
   - Manages Solana-specific operations

### Data Flow

1. **Snapshot Detection**
   ```
   File System → Monitor → Snapshot Processor
   ```

2. **Snapshot Processing**
   ```
   Snapshot Processor → Validation → Metadata Generation → S3 Upload
   ```

3. **Retention Management**
   ```
   Monitor → Retention Check → S3 Cleanup
   ```

## Key Features

### 1. Snapshot Monitoring
- Real-time directory monitoring
- Automatic detection of new snapshots
- File exclusion rules for temporary files
- Concurrent processing support

### 2. Snapshot Validation
- Full snapshot slot gap validation (configurable)
- Incremental snapshot validation
- Slot continuity checking
- Optional incremental snapshot support

### 3. Metadata Management
- Automatic metadata generation
- Version and feature set tracking
- Upload status tracking
- Progress reporting
- Automatic cleanup of orphaned metadata (metadata without corresponding snapshot files)

### 4. Storage Management
- S3-compatible storage support
- Multipart upload handling
- Upload retry and resumption
- Abandoned upload cleanup

### 5. Retention Management
- Configurable retention periods
- Automatic cleanup of old snapshots
- Retention policy enforcement
- Orphaned metadata cleanup (removes metadata files when corresponding snapshots are missing)

### 6. Metadata Cleanup
- Automatic detection of orphaned metadata files
- Periodic scanning for metadata-snapshot consistency
- Safe removal of metadata without corresponding snapshots
- Configurable cleanup intervals
- Logging of cleanup operations for audit purposes

## Configuration

The application supports configuration through:
- YAML configuration file
- Environment variables
- Command-line arguments

Key configuration parameters:
- Watch directory
- S3 credentials and endpoint
- Solana RPC URL
- Slot gap settings
- Retention policies
- Logging levels

## Deployment Options

1. **Direct Execution**
   - Standalone binary execution
   - Environment variable configuration
   - Custom configuration file support

2. **Systemd Service**
   - Systemd service file included
   - Environment file configuration
   - Automatic startup and recovery

3. **Docker Container**
   - Dockerfile included
   - Volume mounting support
   - Environment variable configuration
   - Docker Compose support

## Error Handling

- Graceful error recovery
- Upload retry mechanism
- Failed upload cleanup
- Logging and monitoring
- Status tracking for failed operations
- Metadata consistency checks and cleanup

## Security Considerations

- Secure credential management
- S3 access key protection
- File permission management
- Secure logging practices

## Monitoring and Logging

- Configurable log levels
- Progress reporting
- Status tracking
- Error logging
- Performance metrics

## Future Considerations

1. **Scalability**
   - Distributed monitoring support
   - Load balancing capabilities
   - Performance optimization

2. **Enhanced Features**
   - Additional storage backend support
   - Advanced retention policies
   - Custom validation rules
   - Enhanced metadata support

3. **Integration**
   - Monitoring system integration
   - Alerting capabilities
   - API endpoints for control
   - Web interface for management

## Dependencies

- Go 1.23 or higher
- S3-compatible storage
- Solana RPC access
- System requirements for deployment method

## Development Guidelines

1. **Code Organization**
   - Package-based structure
   - Clear separation of concerns
   - Modular design

2. **Documentation**
   - Code documentation
   - API documentation
   - Configuration documentation
   - Deployment guides 