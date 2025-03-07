package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/maestroi/anza-snapshot-uploader/pkg/config"
	"github.com/maestroi/anza-snapshot-uploader/pkg/metadata"
	"github.com/maestroi/anza-snapshot-uploader/pkg/s3"
	"github.com/maestroi/anza-snapshot-uploader/pkg/snapshot"
)

// Monitor watches a directory for new snapshot files
type Monitor struct {
	cfg              *config.Config
	s3Client         *s3.Client
	watcher          *fsnotify.Watcher
	lastFullSnapshot *snapshot.Snapshot
	lastIncSnapshot  *snapshot.Snapshot
	mu               sync.Mutex
	logger           *slog.Logger
}

// New creates a new monitor
func New(cfg *config.Config, s3Client *s3.Client, logger *slog.Logger) (*Monitor, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher: %w", err)
	}

	return &Monitor{
		cfg:      cfg,
		s3Client: s3Client,
		watcher:  watcher,
		logger:   logger,
	}, nil
}

// Start starts the monitor
func (m *Monitor) Start(ctx context.Context) error {
	// Check if the watch directory exists
	if _, err := os.Stat(m.cfg.WatchDir); os.IsNotExist(err) {
		return fmt.Errorf("watch directory does not exist: %s", m.cfg.WatchDir)
	}

	// Add the watch directory to the watcher
	if err := m.watcher.Add(m.cfg.WatchDir); err != nil {
		return fmt.Errorf("failed to add watch directory: %w", err)
	}

	// Process existing files in the directory
	if err := m.processExistingFiles(); err != nil {
		m.logger.Error("Failed to process existing files", "error", err)
	}

	// Start watching for new files
	go m.watchForNewFiles(ctx)

	return nil
}

// Stop stops the monitor
func (m *Monitor) Stop() error {
	return m.watcher.Close()
}

// processExistingFiles processes existing snapshot files in the watch directory
func (m *Monitor) processExistingFiles() error {
	entries, err := os.ReadDir(m.cfg.WatchDir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(m.cfg.WatchDir, entry.Name())
		if err := m.processFile(path); err != nil {
			m.logger.Error("Failed to process file", "path", path, "error", err)
		}
	}

	return nil
}

// watchForNewFiles watches for new files in the watch directory
func (m *Monitor) watchForNewFiles(ctx context.Context) {
	for {
		select {
		case event, ok := <-m.watcher.Events:
			if !ok {
				return
			}

			// Only process create and write events
			if event.Op&(fsnotify.Create|fsnotify.Write) != 0 {
				// Process the file
				if err := m.processFile(event.Name); err != nil {
					m.logger.Error("Failed to process file", "path", event.Name, "error", err)
				}
			}

		case err, ok := <-m.watcher.Errors:
			if !ok {
				return
			}
			m.logger.Error("Watcher error", "error", err)

		case <-ctx.Done():
			return
		}
	}
}

// processFile processes a snapshot file
func (m *Monitor) processFile(path string) error {
	// Parse the filename
	snap, err := snapshot.ParseFilename(path)
	if err != nil {
		// Not a snapshot file, ignore
		return nil
	}

	// Lock to prevent concurrent processing
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if the snapshot is valid
	valid, reason := snap.IsValid(m.lastFullSnapshot, m.lastIncSnapshot, m.cfg.FullSnapshotGap, m.cfg.IncrementalGap)
	if !valid {
		m.logger.Info("Skipping invalid snapshot", "path", path, "reason", reason)
		return nil
	}

	// Calculate the hash
	if err := snap.CalculateHash(); err != nil {
		return fmt.Errorf("failed to calculate hash: %w", err)
	}

	// Set the Solana version
	snap.SolanaVersion = m.cfg.SolanaVersion

	// Generate metadata
	meta := metadata.GenerateFromSnapshot(snap)

	// Create a temporary file for the metadata
	metaFilename := snap.GetMetadataFilename()
	metaPath := filepath.Join(os.TempDir(), metaFilename)
	if err := meta.WriteToFile(metaPath); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}
	defer os.Remove(metaPath)

	// Upload the snapshot file
	m.logger.Info("Uploading snapshot", "path", path)
	if err := m.s3Client.UploadFile(path, snap.Filename); err != nil {
		return fmt.Errorf("failed to upload snapshot: %w", err)
	}

	// Upload the metadata file
	m.logger.Info("Uploading metadata", "path", metaPath)
	if err := m.s3Client.UploadFile(metaPath, metaFilename); err != nil {
		return fmt.Errorf("failed to upload metadata: %w", err)
	}

	// Update the last snapshot
	if snap.Type == snapshot.FullSnapshot {
		m.lastFullSnapshot = snap
	} else if snap.Type == snapshot.IncrementalSnapshot {
		m.lastIncSnapshot = snap
	}

	m.logger.Info("Successfully processed snapshot", "path", path)
	return nil
}
