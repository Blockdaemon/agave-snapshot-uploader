package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/maestroi/anza-snapshot-uploader/pkg/config"
	"github.com/maestroi/anza-snapshot-uploader/pkg/metadata"
	"github.com/maestroi/anza-snapshot-uploader/pkg/s3"
	"github.com/maestroi/anza-snapshot-uploader/pkg/snapshot"
	"github.com/maestroi/anza-snapshot-uploader/pkg/solana"
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
	solanaFeatureSet int64
}

// New creates a new monitor
func New(cfg *config.Config, s3Client *s3.Client, logger *slog.Logger) (*Monitor, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher: %w", err)
	}

	// Fetch Solana version if not already fetched
	solanaFeatureSet := int64(0)
	if cfg.SolanaRpcUrl != "" {
		version, err := solana.GetVersion(cfg.SolanaRpcUrl)
		if err == nil {
			solanaFeatureSet = version.FeatureSet
			if cfg.SolanaVersion == "" {
				cfg.SolanaVersion = version.SolanaCore
			}
		}
	}

	return &Monitor{
		cfg:              cfg,
		s3Client:         s3Client,
		watcher:          watcher,
		logger:           logger,
		solanaFeatureSet: solanaFeatureSet,
	}, nil
}

// Start starts the monitor
func (m *Monitor) Start(ctx context.Context) error {
	// Check if the watch directory exists
	if _, err := os.Stat(m.cfg.WatchDir); os.IsNotExist(err) {
		return fmt.Errorf("watch directory does not exist: %s", m.cfg.WatchDir)
	}

	// Process existing files in the directory (this will create the remote directory)
	if err := m.processExistingFiles(); err != nil {
		m.logger.Error("Failed to process existing files", "error", err)
	}

	// Add the watch directory to the watcher
	if err := m.watcher.Add(m.cfg.WatchDir); err != nil {
		return fmt.Errorf("failed to add watch directory: %w", err)
	}

	// Add the remote directory to the watcher
	remoteDir := filepath.Join(m.cfg.WatchDir, "remote")
	if err := m.watcher.Add(remoteDir); err != nil {
		m.logger.Warn("Failed to add remote directory to watcher", "error", err)
	}

	// Start watching for new files
	go m.watchForNewFiles(ctx)

	// Start cleanup routine if retention is enabled
	if m.cfg.EnableRetention {
		go m.runPeriodicCleanup(ctx)
	}

	return nil
}

// Stop stops the monitor
func (m *Monitor) Stop() error {
	return m.watcher.Close()
}

// processExistingFiles processes existing snapshot files in the watch directory
func (m *Monitor) processExistingFiles() error {
	// Create remote directory if it doesn't exist
	remoteDir := filepath.Join(m.cfg.WatchDir, "remote")
	if err := os.MkdirAll(remoteDir, 0755); err != nil {
		return fmt.Errorf("failed to create remote directory: %w", err)
	}

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
	// Skip temporary files
	filename := filepath.Base(path)
	if strings.HasPrefix(filename, ".") ||
		strings.HasSuffix(path, ".tmp") ||
		strings.Contains(strings.ToLower(filename), "tmp") ||
		strings.Contains(strings.ToLower(filename), "temp") {
		m.logger.Debug("Skipping temporary file", "path", path)
		return nil
	}

	// Skip files in the remote directory
	remoteDir := filepath.Join(m.cfg.WatchDir, "remote")
	if strings.HasPrefix(path, remoteDir) {
		m.logger.Debug("Skipping file in remote directory", "path", path)
		return nil
	}

	// Parse the filename
	snap, err := snapshot.ParseFilename(path)
	if err != nil {
		// Not a snapshot file, ignore
		return nil
	}

	// Skip incremental snapshots if they are disabled
	if snap.Type == snapshot.IncrementalSnapshot && !m.cfg.EnableIncrementalSnap {
		m.logger.Info("Skipping incremental snapshot (disabled in config)", "path", path)
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

	// Get file info for size
	fileInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Format file size in human-readable format
	fileSize := formatFileSize(fileInfo.Size())

	// Create metadata filename
	metaFilename := snap.GetMetadataFilename()

	// Check if the metadata already exists in the bucket and its status
	metaExists, err := m.s3Client.FileExists(metaFilename)
	if err != nil {
		m.logger.Warn("Failed to check if metadata exists in bucket", "error", err)
	} else if metaExists {
		// Try to read the metadata to check its status
		meta, err := metadata.ReadFromS3(m.s3Client, metaFilename)
		if err != nil {
			m.logger.Warn("Failed to read metadata from S3", "error", err)
		} else {
			// If the metadata exists and has a status of "completed", skip the upload
			if meta.Status == metadata.StatusCompleted {
				m.logger.Info("Snapshot already uploaded, skipping", "path", path, "size", fileSize)

				// Update our tracking
				if snap.Type == snapshot.FullSnapshot {
					m.lastFullSnapshot = snap
				} else if snap.Type == snapshot.IncrementalSnapshot {
					m.lastIncSnapshot = snap
				}

				return nil
			}

			// If the metadata exists and has a status of "uploading", check who is uploading it
			if meta.Status == metadata.StatusUploading {
				// If it's being uploaded by someone else, skip it
				if meta.UploadedBy != m.cfg.Hostname {
					m.logger.Info("Snapshot is being uploaded by another instance, skipping",
						"path", path,
						"size", fileSize,
						"uploader", meta.UploadedBy)
					return nil
				}

				// If it's being uploaded by us, continue with the upload (maybe a previous attempt failed)
				m.logger.Info("Resuming upload of snapshot", "path", path, "size", fileSize)
			}
		}
	}

	// Calculate the hash
	if err := snap.CalculateHash(); err != nil {
		return fmt.Errorf("failed to calculate hash: %w", err)
	}

	// Set the Solana version
	snap.SolanaVersion = m.cfg.SolanaVersion
	snap.SolanaFeatureSet = m.solanaFeatureSet

	// Generate metadata with "uploading" status
	meta := metadata.GenerateFromSnapshot(snap).
		WithStatus(metadata.StatusUploading).
		WithUploader(m.cfg.Hostname)

	// Create a temporary file for the metadata
	metaPath := filepath.Join(os.TempDir(), metaFilename)
	if err := meta.WriteToFile(metaPath); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}
	defer os.Remove(metaPath)

	// Upload the metadata file with "uploading" status first
	metaInfo, err := os.Stat(metaPath)
	if err != nil {
		return fmt.Errorf("failed to get metadata file info: %w", err)
	}

	metaSize := formatFileSize(metaInfo.Size())
	m.logger.Info("Uploading metadata with 'uploading' status", "path", metaPath, "size", metaSize)

	// Define progress function for metadata upload
	metaProgressFunc := func(uploaded, total int64) {
		// For small metadata files, we don't need to log progress
		if total > 1024*1024 { // Only log progress for files larger than 1MB
			percentage := float64(uploaded) / float64(total) * 100
			uploadedSize := formatFileSize(uploaded)
			totalSize := formatFileSize(total)
			m.logger.Info("Upload progress",
				"file", metaFilename,
				"progress", fmt.Sprintf("%.1f%%", percentage),
				"uploaded", uploadedSize,
				"total", totalSize)
		}
	}

	if err := m.s3Client.UploadFile(metaPath, metaFilename, metaProgressFunc); err != nil {
		return fmt.Errorf("failed to upload metadata: %w", err)
	}

	// Upload the snapshot file
	m.logger.Info("Uploading snapshot", "path", path, "size", fileSize)

	// Define progress function for snapshot upload
	progressFunc := func(uploaded, total int64) {
		percentage := float64(uploaded) / float64(total) * 100
		uploadedSize := formatFileSize(uploaded)
		totalSize := formatFileSize(total)
		m.logger.Info("Upload progress",
			"file", filepath.Base(path),
			"progress", fmt.Sprintf("%.1f%%", percentage),
			"uploaded", uploadedSize,
			"total", totalSize)
	}

	// Use multipart upload for snapshot files to handle potential failures
	if err := m.s3Client.UploadFileMultipart(path, snap.Filename, progressFunc); err != nil {
		return fmt.Errorf("failed to upload snapshot: %w", err)
	}

	// Update metadata with "completed" status
	meta.Status = metadata.StatusCompleted
	if err := meta.WriteToFile(metaPath); err != nil {
		return fmt.Errorf("failed to write updated metadata file: %w", err)
	}

	// Upload the updated metadata file
	m.logger.Info("Uploading metadata with 'completed' status", "path", metaPath, "size", metaSize)
	if err := m.s3Client.UploadFile(metaPath, metaFilename, metaProgressFunc); err != nil {
		return fmt.Errorf("failed to upload updated metadata: %w", err)
	}

	// Update the last snapshot
	if snap.Type == snapshot.FullSnapshot {
		m.lastFullSnapshot = snap
	} else if snap.Type == snapshot.IncrementalSnapshot {
		m.lastIncSnapshot = snap
	}

	m.logger.Info("Successfully processed snapshot", "path", path, "size", fileSize)
	return nil
}

// formatFileSize formats a file size in bytes to a human-readable format
func formatFileSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(size)/float64(div), "KMGTPE"[exp])
}

// CleanupOldSnapshots deletes old snapshots from the S3 bucket based on retention period
func (m *Monitor) CleanupOldSnapshots() error {
	if !m.cfg.EnableRetention {
		return nil
	}

	m.logger.Info("Starting cleanup of old snapshots",
		"retention_period_hours", m.cfg.RetentionPeriodHours)

	// Calculate the cutoff time
	cutoffTime := time.Now().Add(-time.Duration(m.cfg.RetentionPeriodHours) * time.Hour)
	m.logger.Info("Retention cutoff time", "time", cutoffTime.Format(time.RFC3339))

	// List all snapshot files in the bucket
	fullSnapshots, err := m.s3Client.ListObjects("snapshot-")
	if err != nil {
		return fmt.Errorf("failed to list full snapshots: %w", err)
	}

	incSnapshots, err := m.s3Client.ListObjects("incremental-snapshot-")
	if err != nil {
		return fmt.Errorf("failed to list incremental snapshots: %w", err)
	}

	// Combine the lists
	allSnapshots := append(fullSnapshots, incSnapshots...)

	// Group snapshots by slot
	type SnapshotObject struct {
		Key          string
		LastModified time.Time
		Size         int64
		Slot         int64
		FullSlot     int64
		Type         snapshot.SnapshotType
	}

	var snapshotObjects []SnapshotObject
	fullSnapshotsBySlot := make(map[int64][]SnapshotObject)
	incSnapshotsByFullSlot := make(map[int64][]SnapshotObject)

	for _, obj := range allSnapshots {
		// Skip metadata files
		if strings.HasSuffix(obj.Key, ".json") {
			continue
		}

		// Parse the snapshot filename
		snap, err := snapshot.ParseFilename(obj.Key)
		if err != nil {
			m.logger.Warn("Failed to parse snapshot filename", "key", obj.Key, "error", err)
			continue
		}

		snapshotObj := SnapshotObject{
			Key:          obj.Key,
			LastModified: obj.LastModified,
			Size:         obj.Size,
			Slot:         snap.Slot,
			Type:         snap.Type,
		}

		if snap.Type == snapshot.IncrementalSnapshot {
			snapshotObj.FullSlot = snap.FullSlot
		}

		snapshotObjects = append(snapshotObjects, snapshotObj)

		if snap.Type == snapshot.FullSnapshot {
			fullSnapshotsBySlot[snap.Slot] = append(fullSnapshotsBySlot[snap.Slot], snapshotObj)
		} else if snap.Type == snapshot.IncrementalSnapshot {
			incSnapshotsByFullSlot[snap.FullSlot] = append(incSnapshotsByFullSlot[snap.FullSlot], snapshotObj)
		}
	}

	// Find the latest full snapshot
	var latestFullSlot int64
	for slot := range fullSnapshotsBySlot {
		if slot > latestFullSlot {
			latestFullSlot = slot
		}
	}

	// Collect objects to delete
	var objectsToDelete []string
	var totalSize int64

	// Process full snapshots
	for slot, objects := range fullSnapshotsBySlot {
		// Keep the latest full snapshot regardless of age
		if slot == latestFullSlot {
			continue
		}

		for _, obj := range objects {
			// Delete if older than retention period
			if obj.LastModified.Before(cutoffTime) {
				objectsToDelete = append(objectsToDelete, obj.Key)
				// Also delete the metadata file
				metadataKey := obj.Key[:len(obj.Key)-8] + ".json" // Remove .tar.zst and add .json
				objectsToDelete = append(objectsToDelete, metadataKey)
				totalSize += obj.Size
				m.logger.Info("Marking full snapshot for deletion",
					"key", obj.Key,
					"slot", slot,
					"age", time.Since(obj.LastModified).Hours(),
					"size", formatFileSize(obj.Size))
			}
		}
	}

	// Process incremental snapshots
	for fullSlot, objects := range incSnapshotsByFullSlot {
		// If the full snapshot is deleted or marked for deletion, delete all its incremental snapshots
		if _, exists := fullSnapshotsBySlot[fullSlot]; !exists || containsFullSlot(objectsToDelete, fullSlot) {
			for _, obj := range objects {
				objectsToDelete = append(objectsToDelete, obj.Key)
				// Also delete the metadata file
				metadataKey := obj.Key[:len(obj.Key)-8] + ".json" // Remove .tar.zst and add .json
				objectsToDelete = append(objectsToDelete, metadataKey)
				totalSize += obj.Size
				m.logger.Info("Marking incremental snapshot for deletion (full snapshot deleted)",
					"key", obj.Key,
					"full_slot", fullSlot,
					"size", formatFileSize(obj.Size))
			}
			continue
		}

		// Delete incremental snapshots older than retention period
		for _, obj := range objects {
			if obj.LastModified.Before(cutoffTime) {
				objectsToDelete = append(objectsToDelete, obj.Key)
				// Also delete the metadata file
				metadataKey := obj.Key[:len(obj.Key)-8] + ".json" // Remove .tar.zst and add .json
				objectsToDelete = append(objectsToDelete, metadataKey)
				totalSize += obj.Size
				m.logger.Info("Marking incremental snapshot for deletion (age)",
					"key", obj.Key,
					"full_slot", fullSlot,
					"age", time.Since(obj.LastModified).Hours(),
					"size", formatFileSize(obj.Size))
			}
		}
	}

	// Delete the objects
	if len(objectsToDelete) > 0 {
		m.logger.Info("Deleting old snapshots",
			"count", len(objectsToDelete),
			"total_size", formatFileSize(totalSize))

		if err := m.s3Client.DeleteObjects(objectsToDelete); err != nil {
			return fmt.Errorf("failed to delete old snapshots: %w", err)
		}

		m.logger.Info("Successfully deleted old snapshots",
			"count", len(objectsToDelete),
			"total_size", formatFileSize(totalSize))
	} else {
		m.logger.Info("No snapshots to delete")
	}

	return nil
}

// containsFullSlot checks if any of the keys to delete contains the given full slot
func containsFullSlot(keys []string, fullSlot int64) bool {
	fullSlotStr := fmt.Sprintf("snapshot-%d-", fullSlot)
	for _, key := range keys {
		if strings.Contains(key, fullSlotStr) {
			return true
		}
	}
	return false
}

// runPeriodicCleanup runs the cleanup process periodically
func (m *Monitor) runPeriodicCleanup(ctx context.Context) {
	// Run cleanup immediately on startup
	if m.cfg.EnableRetention {
		if err := m.CleanupOldSnapshots(); err != nil {
			m.logger.Error("Failed to clean up old snapshots", "error", err)
		}
	}

	// Clean up abandoned multipart uploads
	if err := m.CleanupAbandonedUploads(); err != nil {
		m.logger.Error("Failed to clean up abandoned multipart uploads", "error", err)
	}

	// Set up ticker for periodic cleanup
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Run snapshot retention cleanup if enabled
			if m.cfg.EnableRetention {
				if err := m.CleanupOldSnapshots(); err != nil {
					m.logger.Error("Failed to clean up old snapshots", "error", err)
				}
			}

			// Clean up abandoned multipart uploads
			if err := m.CleanupAbandonedUploads(); err != nil {
				m.logger.Error("Failed to clean up abandoned multipart uploads", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// CleanupAbandonedUploads cleans up abandoned multipart uploads
func (m *Monitor) CleanupAbandonedUploads() error {
	m.logger.Info("Starting cleanup of abandoned multipart uploads")

	// Clean up uploads older than 24 hours
	if err := m.s3Client.CleanupAbandonedUploads(24 * time.Hour); err != nil {
		return fmt.Errorf("failed to clean up abandoned multipart uploads: %w", err)
	}

	m.logger.Info("Completed cleanup of abandoned multipart uploads")
	return nil
}
