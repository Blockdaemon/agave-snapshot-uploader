package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/maestroi/anza-snapshot-uploader/pkg/snapshot"
)

// Metadata represents the metadata for a snapshot
type Metadata struct {
	SolanaVersion string `json:"solana_version"`
	Slot          int64  `json:"slot"`
	Timestamp     int64  `json:"timestamp"`
	Hash          string `json:"hash"`
}

// GenerateFromSnapshot creates a metadata object from a snapshot
func GenerateFromSnapshot(snap *snapshot.Snapshot) *Metadata {
	return &Metadata{
		SolanaVersion: snap.SolanaVersion,
		Slot:          snap.Slot,
		Timestamp:     snap.Timestamp,
		Hash:          snap.Hash,
	}
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
