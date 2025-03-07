package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/maestroi/anza-snapshot-uploader/pkg/snapshot"
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
	Slot             int64        `json:"slot"`
	Timestamp        int64        `json:"timestamp"`
	TimestampHuman   string       `json:"timestamp_human,omitempty"`
	Hash             string       `json:"hash"`
	Status           UploadStatus `json:"status,omitempty"`
	UploadedBy       string       `json:"uploaded_by,omitempty"`
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
