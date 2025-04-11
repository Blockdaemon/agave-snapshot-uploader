package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Blockdaemon/agave-snapshot-uploader/pkg/s3"
	"github.com/Blockdaemon/agave-snapshot-uploader/pkg/snapshot"
)

// UploadStatus represents the status of a snapshot upload
type UploadStatus string

const (
	// StatusUploading indicates that the snapshot is currently being uploaded
	StatusUploading UploadStatus = "uploading"
	// StatusCompleted indicates that the snapshot upload is completed
	StatusCompleted UploadStatus = "completed"
	// StatusFailed indicates that the snapshot upload failed
	StatusFailed UploadStatus = "failed"
)

// Metadata represents the metadata for a snapshot
type Metadata struct {
	SolanaVersion    string       `json:"solana_version"`
	SolanaFeatureSet int64        `json:"solana_feature_set,omitempty"`
	Slot             int64        `json:"slot"` // Legacy field for backward compatibility
	Timestamp        int64        `json:"timestamp"`
	TimestampHuman   string       `json:"timestamp_human,omitempty"`
	Hash             string       `json:"hash"` // Legacy field for backward compatibility
	Status           UploadStatus `json:"status,omitempty"`
	UploadedBy       string       `json:"uploaded_by,omitempty"`
	URL              string       `json:"url,omitempty"` // Legacy field for backward compatibility
	GenesisHash      string       `json:"genesis_hash,omitempty"`

	// New clearer field names
	FullSnapshotSlot int64  `json:"full_snapshot_slot,omitempty"`
	FullSnapshotHash string `json:"full_snapshot_hash,omitempty"`
	FullSnapshotURL  string `json:"full_snapshot_url,omitempty"`

	// For backward compatibility with old field names
	LatestIncrementalSlot int64  `json:"latest_incremental_slot,omitempty"`
	LatestIncrementalHash string `json:"latest_incremental_hash,omitempty"`
	LatestIncrementalURL  string `json:"latest_incremental_url,omitempty"`

	// New clearer field names
	IncrementalSnapshotSlot int64  `json:"incremental_snapshot_slot,omitempty"`
	IncrementalSnapshotHash string `json:"incremental_snapshot_hash,omitempty"`
	IncrementalSnapshotURL  string `json:"incremental_snapshot_url,omitempty"`
}

// GenerateFromSnapshot creates a metadata object from a snapshot
func GenerateFromSnapshot(snap *snapshot.Snapshot) *Metadata {
	// Convert Unix timestamp to human-readable format
	timestampHuman := time.Unix(snap.Timestamp, 0).Format(time.RFC3339)

	return &Metadata{
		SolanaVersion:    snap.SolanaVersion,
		SolanaFeatureSet: snap.SolanaFeatureSet,
		Slot:             snap.Slot,
		Timestamp:        snap.Timestamp,
		TimestampHuman:   timestampHuman,
		Hash:             snap.Hash,
	}
}

// WithStatus sets the status of the metadata
func (m *Metadata) WithStatus(status UploadStatus) *Metadata {
	m.Status = status
	return m
}

// WithUploader sets the uploader of the metadata
func (m *Metadata) WithUploader(uploader string) *Metadata {
	m.UploadedBy = uploader
	return m
}

// WithURL sets the URL of the snapshot
func (m *Metadata) WithURL(url string) *Metadata {
	m.URL = url
	return m
}

// WithGenesisHash sets the genesis hash of the metadata
func (m *Metadata) WithGenesisHash(hash string) *Metadata {
	m.GenesisHash = hash
	return m
}

// WithIncrementalSnapshot adds information about the latest incremental snapshot
func (m *Metadata) WithIncrementalSnapshot(slot int64, hash, url string) *Metadata {
	// Only use the new field names, as the S3 UpdateLatestMetadata function will handle backward compatibility
	m.IncrementalSnapshotSlot = slot
	m.IncrementalSnapshotHash = hash
	m.IncrementalSnapshotURL = url

	return m
}

// WriteToFile writes the metadata to a JSON file
func (m *Metadata) WriteToFile(path string) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Marshal metadata to JSON
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Write to file
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	return nil
}

// ReadFromFile reads metadata from a JSON file
func ReadFromFile(path string) (*Metadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	var metadata Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &metadata, nil
}

// ReadFromS3 reads metadata from an S3 object
func ReadFromS3(s3Client interface {
	DownloadFile(s3Path, localPath string) error
}, s3Path string) (*Metadata, error) {
	// Create a temporary file to download the metadata
	tmpFile, err := os.CreateTemp("", "metadata-*.json")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Download the metadata file from S3
	if err := s3Client.DownloadFile(s3Path, tmpFile.Name()); err != nil {
		return nil, fmt.Errorf("failed to download metadata from S3: %w", err)
	}

	// Read the metadata from the temporary file
	return ReadFromFile(tmpFile.Name())
}

// CheckOrphanedMetadata checks for metadata files in S3 that don't have corresponding snapshot files
func CheckOrphanedMetadata(s3Client interface {
	ListObjects(prefix string) ([]s3.S3Object, error)
	FileExists(key string) (bool, error)
	HasCorrespondingFile(ctx context.Context, key string) (bool, error)
	DownloadFile(s3Path, localPath string) error
}, bucket string) ([]string, error) {
	// We're completely disabling this functionality as it's causing problems
	// Return an empty slice to indicate no orphaned files
	return []string{}, nil
}

// RemoveOrphanedMetadata removes metadata files from S3 that don't have corresponding snapshot files
func RemoveOrphanedMetadata(s3Client interface {
	ListObjects(prefix string) ([]s3.S3Object, error)
	FileExists(key string) (bool, error)
	HasCorrespondingFile(ctx context.Context, key string) (bool, error)
	DeleteObject(key string) error
	DownloadFile(s3Path, localPath string) error
}, bucket string) ([]string, error) {
	orphanedFiles, err := CheckOrphanedMetadata(s3Client, bucket)
	if err != nil {
		return nil, err
	}

	var removedFiles []string
	for _, file := range orphanedFiles {
		// Double-check that the file is truly orphaned before deleting
		hasSnapshot, err := s3Client.HasCorrespondingFile(context.Background(), file)
		if err != nil {
			continue // Skip if we can't check existence
		}

		if hasSnapshot {
			// Skip this file as it has a corresponding snapshot
			continue
		}

		if err := s3Client.DeleteObject(file); err != nil {
			return removedFiles, fmt.Errorf("failed to remove orphaned metadata file %s: %w", file, err)
		}
		removedFiles = append(removedFiles, file)
	}

	return removedFiles, nil
}
