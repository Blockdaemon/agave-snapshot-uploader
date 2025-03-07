package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// SnapshotType represents the type of snapshot
type SnapshotType int

const (
	// FullSnapshot represents a full snapshot
	FullSnapshot SnapshotType = iota
	// IncrementalSnapshot represents an incremental snapshot
	IncrementalSnapshot
)

// Snapshot represents a Solana snapshot file
type Snapshot struct {
	Path             string       `json:"path"`
	Filename         string       `json:"filename"`
	Type             SnapshotType `json:"type"`
	Slot             int64        `json:"slot"`
	FullSlot         int64        `json:"full_slot,omitempty"` // Only for incremental snapshots
	ValidatorID      string       `json:"validator_id"`
	Hash             string       `json:"hash,omitempty"`
	SolanaVersion    string       `json:"solana_version,omitempty"`
	SolanaFeatureSet int64        `json:"solana_feature_set,omitempty"`
	Timestamp        int64        `json:"timestamp,omitempty"`
}

var (
	// Regular expression for full snapshots: snapshot-<slot>-<validator_id>.tar.zst
	fullSnapshotRegex = regexp.MustCompile(`^snapshot-(\d+)-([A-Za-z0-9]{32,44})\.tar\.zst$`)

	// Regular expression for incremental snapshots: incremental-snapshot-<full_snapshot_slot>-<slot>-<validator_id>.tar.zst
	incrementalSnapshotRegex = regexp.MustCompile(`^incremental-snapshot-(\d+)-(\d+)-([A-Za-z0-9]{32,44})\.tar\.zst$`)
)

// ParseFilename parses a snapshot filename and returns a Snapshot object
func ParseFilename(path string) (*Snapshot, error) {
	filename := filepath.Base(path)

	// Try to match full snapshot pattern
	if matches := fullSnapshotRegex.FindStringSubmatch(filename); matches != nil {
		slot, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid slot number: %w", err)
		}

		return &Snapshot{
			Path:        path,
			Filename:    filename,
			Type:        FullSnapshot,
			Slot:        slot,
			ValidatorID: matches[2],
			Timestamp:   time.Now().Unix(),
		}, nil
	}

	// Try to match incremental snapshot pattern
	if matches := incrementalSnapshotRegex.FindStringSubmatch(filename); matches != nil {
		fullSlot, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid full slot number: %w", err)
		}

		slot, err := strconv.ParseInt(matches[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid slot number: %w", err)
		}

		return &Snapshot{
			Path:        path,
			Filename:    filename,
			Type:        IncrementalSnapshot,
			Slot:        slot,
			FullSlot:    fullSlot,
			ValidatorID: matches[3],
			Timestamp:   time.Now().Unix(),
		}, nil
	}

	return nil, fmt.Errorf("filename does not match snapshot pattern: %s", filename)
}

// CalculateHash calculates the SHA256 hash of the snapshot file
func (s *Snapshot) CalculateHash() error {
	file, err := os.Open(s.Path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return fmt.Errorf("failed to calculate hash: %w", err)
	}

	s.Hash = hex.EncodeToString(hash.Sum(nil))
	return nil
}

// IsValid checks if the snapshot is valid based on the provided rules
func (s *Snapshot) IsValid(lastFullSnapshot *Snapshot, lastIncrementalSnapshot *Snapshot, fullSnapshotGap int, incrementalGap int) (bool, string) {
	if s.Type == FullSnapshot {
		// If there's no previous full snapshot, this one is valid
		if lastFullSnapshot == nil {
			return true, ""
		}

		// Check if the slot difference is at least fullSnapshotGap
		if s.Slot-lastFullSnapshot.Slot < int64(fullSnapshotGap) {
			return false, fmt.Sprintf("slot difference (%d) is less than required gap (%d)", s.Slot-lastFullSnapshot.Slot, fullSnapshotGap)
		}

		return true, ""
	} else if s.Type == IncrementalSnapshot {
		// Check if there's a corresponding full snapshot
		if lastFullSnapshot == nil || s.FullSlot != lastFullSnapshot.Slot {
			return false, fmt.Sprintf("no matching full snapshot found for slot %d", s.FullSlot)
		}

		// If there's no previous incremental snapshot, this one is valid
		if lastIncrementalSnapshot == nil {
			return true, ""
		}

		// Check if the slot difference is within incrementalGap
		slotDiff := s.Slot - lastIncrementalSnapshot.Slot
		if slotDiff < 0 || slotDiff > int64(incrementalGap) {
			return false, fmt.Sprintf("slot difference (%d) is outside the allowed range (0-%d)", slotDiff, incrementalGap)
		}

		return true, ""
	}

	return false, "unknown snapshot type"
}

// GetMetadataFilename returns the filename for the metadata JSON file
func (s *Snapshot) GetMetadataFilename() string {
	return fmt.Sprintf("%s.json", s.Filename[:len(s.Filename)-8]) // Remove .tar.zst
}
