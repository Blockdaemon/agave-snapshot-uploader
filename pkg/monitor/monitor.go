package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Blockdaemon/agave-snapshot-uploader/pkg/config"
	"github.com/Blockdaemon/agave-snapshot-uploader/pkg/metadata"
	"github.com/Blockdaemon/agave-snapshot-uploader/pkg/s3"
	"github.com/Blockdaemon/agave-snapshot-uploader/pkg/snapshot"
	"github.com/Blockdaemon/agave-snapshot-uploader/pkg/solana"
	"github.com/fsnotify/fsnotify"
)

// Monitor watches a directory for new snapshot files
type Monitor struct {
	cfg                 *config.Config
	s3Client            *s3.Client
	watcher             *fsnotify.Watcher
	lastFullSnapshot    *snapshot.Snapshot
	lastIncSnapshot     *snapshot.Snapshot
	mu                  sync.Mutex
	logger              *slog.Logger
	solanaFeatureSet    int64
	solanaGenesisHash   string
	uploadStartTime     time.Time
	metaUploadStartTime time.Time
	lastCleanupTime     time.Time
}

// SnapshotMetadata represents metadata about a snapshot
type SnapshotMetadata struct {
	Path     string    `json:"path"`
	Status   string    `json:"status"`
	Uploaded time.Time `json:"uploaded"`
}

// New creates a new monitor
func New(cfg *config.Config, s3Client *s3.Client, logger *slog.Logger) (*Monitor, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher: %w", err)
	}

	// Fetch Solana version if not already fetched
	solanaFeatureSet := int64(0)
	var solanaGenesisHash string
	if cfg.SolanaRpcUrl != "" {
		// Get Solana version
		version, err := solana.GetVersion(cfg.SolanaRpcUrl)
		if err == nil {
			solanaFeatureSet = version.FeatureSet
			if cfg.SolanaVersion == "" {
				cfg.SolanaVersion = version.SolanaCore
			}
		}

		// Get Genesis hash
		genesisHash, err := solana.GetGenesisHash(cfg.SolanaRpcUrl)
		if err == nil {
			solanaGenesisHash = genesisHash
			logger.Info("Fetched Solana genesis hash", "genesis_hash", genesisHash)
		} else {
			logger.Warn("Failed to fetch Solana genesis hash", "error", err)
		}
	}

	return &Monitor{
		cfg:               cfg,
		s3Client:          s3Client,
		watcher:           watcher,
		logger:            logger,
		solanaFeatureSet:  solanaFeatureSet,
		solanaGenesisHash: solanaGenesisHash,
	}, nil
}

// Start starts the monitor
func (m *Monitor) Start(ctx context.Context) error {
	// Check if the watch directory exists
	if _, err := os.Stat(m.cfg.WatchDir); os.IsNotExist(err) {
		return fmt.Errorf("watch directory does not exist: %s", m.cfg.WatchDir)
	}

	// Check and upload genesis file if it doesn't exist in the bucket
	if err := m.checkAndUploadGenesis(ctx); err != nil {
		m.logger.Warn("Failed to upload genesis file", "error", err)
		// Continue with monitor startup even if genesis upload fails
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

	// Create a context for the upload
	ctx := context.Background()

	// Upload the snapshot
	if err := m.uploadSnapshot(ctx, path); err != nil {
		return fmt.Errorf("failed to upload snapshot: %w", err)
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

// CleanupOldSnapshots cleans up old snapshots and their metadata
func (m *Monitor) CleanupOldSnapshots() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Disable orphaned metadata cleanup as it's causing issues
	// Only log that we're skipping this step
	if m.cfg.EnableMetadataCleanup {
		m.logger.Info("Skipping orphaned metadata cleanup as it's currently disabled")
	}

	if !m.cfg.EnableRetention {
		return nil
	}

	startTime := time.Now()
	m.logger.Info("Starting cleanup of old snapshots",
		"full_retain_count", m.cfg.FullSnapshotRetainCount,
		"full_retain_hours", m.cfg.FullSnapshotRetainHours,
		"incremental_retain_count", m.cfg.IncrementalRetainCount,
		"incremental_retain_hours", m.cfg.IncrementalRetainHours)

	// Calculate the cutoff times
	fullCutoffTime := time.Now().Add(-time.Duration(m.cfg.FullSnapshotRetainHours) * time.Hour)
	incCutoffTime := time.Now().Add(-time.Duration(m.cfg.IncrementalRetainHours) * time.Hour)

	m.logger.Info("Retention cutoff times",
		"full_cutoff", fullCutoffTime.Format(time.RFC3339),
		"incremental_cutoff", incCutoffTime.Format(time.RFC3339))

	// List all snapshot files in the bucket
	m.logger.Debug("Listing full snapshots")
	listStart := time.Now()
	fullSnapshots, err := m.s3Client.ListObjects("snapshot-")
	if err != nil {
		return fmt.Errorf("failed to list full snapshots: %w", err)
	}
	m.logger.Info("Listed full snapshots",
		"count", len(fullSnapshots),
		"duration", time.Since(listStart).String())

	m.logger.Debug("Listing incremental snapshots")
	listStart = time.Now()
	incSnapshots, err := m.s3Client.ListObjects("incremental-snapshot-")
	if err != nil {
		return fmt.Errorf("failed to list incremental snapshots: %w", err)
	}
	m.logger.Info("Listed incremental snapshots",
		"count", len(incSnapshots),
		"duration", time.Since(listStart).String())

	// Combine the lists
	allSnapshots := append(fullSnapshots, incSnapshots...)
	m.logger.Info("Total snapshots found", "count", len(allSnapshots))

	// Group snapshots by slot
	m.logger.Debug("Grouping snapshots by slot")
	groupStart := time.Now()
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
	m.logger.Info("Grouped snapshots",
		"full_snapshots", len(fullSnapshotsBySlot),
		"inc_snapshots_groups", len(incSnapshotsByFullSlot),
		"duration", time.Since(groupStart).String())

	// Find the latest full snapshot
	var latestFullSlot int64
	for slot := range fullSnapshotsBySlot {
		if slot > latestFullSlot {
			latestFullSlot = slot
		}
	}
	m.logger.Info("Latest full snapshot identified", "slot", latestFullSlot)

	// Collect objects to delete
	m.logger.Debug("Identifying snapshots to delete")
	markStart := time.Now()
	var objectsToDelete []string
	var totalSize int64

	// Sort full snapshots by slot (descending)
	var fullSlots []int64
	for slot := range fullSnapshotsBySlot {
		fullSlots = append(fullSlots, slot)
	}
	sort.Slice(fullSlots, func(i, j int) bool {
		return fullSlots[i] > fullSlots[j]
	})
	m.logger.Debug("Sorted full snapshots by slot", "count", len(fullSlots))

	// Process full snapshots: keep the N most recent and respect age
	m.logger.Debug("Processing full snapshots for retention")
	for i, slot := range fullSlots {
		objects := fullSnapshotsBySlot[slot]

		// Always keep the latest full snapshot
		if slot == latestFullSlot {
			m.logger.Debug("Keeping latest full snapshot", "slot", slot)
			continue
		}

		// For others, apply both count and age retention
		if i < m.cfg.FullSnapshotRetainCount {
			// Within count retention, check age
			for _, obj := range objects {
				if obj.LastModified.Before(fullCutoffTime) {
					objectsToDelete = append(objectsToDelete, obj.Key)
					metadataKey := obj.Key[:len(obj.Key)-8] + ".json" // Remove .tar.zst and add .json
					objectsToDelete = append(objectsToDelete, metadataKey)
					totalSize += obj.Size
					m.logger.Info("Marking full snapshot for deletion (age)",
						"key", obj.Key,
						"slot", slot,
						"age_hours", time.Since(obj.LastModified).Hours(),
						"size", formatFileSize(obj.Size))
				}
			}
		} else {
			// Beyond count retention, delete regardless of age
			for _, obj := range objects {
				objectsToDelete = append(objectsToDelete, obj.Key)
				metadataKey := obj.Key[:len(obj.Key)-8] + ".json" // Remove .tar.zst and add .json
				objectsToDelete = append(objectsToDelete, metadataKey)
				totalSize += obj.Size
				m.logger.Info("Marking full snapshot for deletion (count)",
					"key", obj.Key,
					"slot", slot,
					"size", formatFileSize(obj.Size))
			}
		}
	}
	m.logger.Debug("Completed full snapshot retention processing")

	// Process incremental snapshots
	m.logger.Debug("Processing incremental snapshots for retention")
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

		// Keep only the N most recent incremental snapshots for each full snapshot
		keepCount := m.cfg.IncrementalRetainCount

		// Sort incremental snapshots by slot (descending)
		sort.Slice(objects, func(i, j int) bool {
			return objects[i].Slot > objects[j].Slot
		})

		// Keep the N most recent, delete the rest (unless they're within retention period)
		for i, obj := range objects {
			if i < keepCount {
				// Keep the N most recent snapshots, unless they're older than the retention period
				if obj.LastModified.Before(incCutoffTime) {
					objectsToDelete = append(objectsToDelete, obj.Key)
					metadataKey := obj.Key[:len(obj.Key)-8] + ".json" // Remove .tar.zst and add .json
					objectsToDelete = append(objectsToDelete, metadataKey)
					totalSize += obj.Size
					m.logger.Info("Marking recent incremental snapshot for deletion (age)",
						"key", obj.Key,
						"full_slot", fullSlot,
						"slot", obj.Slot,
						"age_hours", time.Since(obj.LastModified).Hours(),
						"size", formatFileSize(obj.Size))
				}
			} else {
				// Always delete snapshots beyond the count retention
				objectsToDelete = append(objectsToDelete, obj.Key)
				metadataKey := obj.Key[:len(obj.Key)-8] + ".json" // Remove .tar.zst and add .json
				objectsToDelete = append(objectsToDelete, metadataKey)
				totalSize += obj.Size
				m.logger.Info("Marking incremental snapshot for deletion (beyond retention count)",
					"key", obj.Key,
					"full_slot", fullSlot,
					"slot", obj.Slot,
					"size", formatFileSize(obj.Size))
			}
		}
	}
	m.logger.Info("Completed marking snapshots for deletion",
		"count", len(objectsToDelete),
		"total_size", formatFileSize(totalSize),
		"duration", time.Since(markStart).String())

	// Delete the objects
	if len(objectsToDelete) > 0 {
		m.logger.Info("Deleting old snapshots",
			"count", len(objectsToDelete),
			"total_size", formatFileSize(totalSize))

		deleteStart := time.Now()
		if err := m.s3Client.DeleteObjects(objectsToDelete); err != nil {
			return fmt.Errorf("failed to delete old snapshots: %w", err)
		}
		m.logger.Info("Successfully deleted old snapshots",
			"count", len(objectsToDelete),
			"total_size", formatFileSize(totalSize),
			"duration", time.Since(deleteStart).String())
	} else {
		m.logger.Info("No snapshots to delete")
	}

	m.logger.Info("Cleanup completed successfully",
		"total_duration", time.Since(startTime).String())
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
		m.logger.Info("Running initial cleanup on startup")

		// Create a context with a 10-minute timeout
		cleanupCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)

		// Run cleanup in a goroutine so we can monitor for timeout
		cleanupDone := make(chan error, 1)
		go func() {
			cleanupDone <- m.CleanupOldSnapshots()
			close(cleanupDone)
		}()

		// Wait for cleanup to finish or timeout
		select {
		case err := <-cleanupDone:
			if err != nil {
				m.logger.Error("Failed to clean up old snapshots", "error", err)
			} else {
				m.logger.Info("Initial cleanup completed successfully")
			}
		case <-cleanupCtx.Done():
			m.logger.Warn("Initial cleanup operation timed out after 10 minutes", "error", cleanupCtx.Err())
		}

		cancel() // Ensure the context is canceled
		m.lastCleanupTime = time.Now()
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
				m.logger.Info("Running periodic cleanup")

				// Create a context with a 10-minute timeout
				cleanupCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)

				// Run cleanup in a goroutine so we can monitor for timeout
				cleanupDone := make(chan error, 1)
				go func() {
					cleanupDone <- m.CleanupOldSnapshots()
					close(cleanupDone)
				}()

				// Wait for cleanup to finish or timeout
				select {
				case err := <-cleanupDone:
					if err != nil {
						m.logger.Error("Failed to clean up old snapshots", "error", err)
					} else {
						m.logger.Info("Periodic cleanup completed successfully")
					}
				case <-cleanupCtx.Done():
					m.logger.Warn("Periodic cleanup operation timed out after 10 minutes", "error", cleanupCtx.Err())
				}

				cancel() // Ensure the context is canceled
				m.lastCleanupTime = time.Now()
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

// findLatestIncrementalSnapshot finds the latest incremental snapshot for a full snapshot
func (m *Monitor) findLatestIncrementalSnapshot(fullSlot int64) (*snapshot.Snapshot, error) {
	// If incremental snapshots are disabled, return nil
	if !m.cfg.EnableIncrementalSnap {
		return nil, nil
	}

	// List all incremental snapshots for this full slot
	prefix := fmt.Sprintf("incremental-snapshot-%d-", fullSlot)
	objects, err := m.s3Client.ListObjects(prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list incremental snapshots: %w", err)
	}

	if len(objects) == 0 {
		return nil, nil
	}

	// Find the latest incremental snapshot by slot
	var latestSlot int64
	var latestObject s3.S3Object

	for _, obj := range objects {
		// Skip metadata files
		if strings.HasSuffix(obj.Key, ".json") {
			continue
		}

		// Parse the snapshot filename
		snap, err := snapshot.ParseFilename(obj.Key)
		if err != nil {
			m.logger.Warn("Failed to parse incremental snapshot filename", "key", obj.Key, "error", err)
			continue
		}

		// Update if this snapshot has a higher slot
		if snap.Slot > latestSlot {
			latestSlot = snap.Slot
			latestObject = obj
		}
	}

	// If we didn't find any valid incremental snapshots, return nil
	if latestSlot == 0 {
		return nil, nil
	}

	// Parse the snapshot to return
	snap, err := snapshot.ParseFilename(latestObject.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse incremental snapshot filename: %w", err)
	}

	// Calculate hash if needed
	if snap.Hash == "" {
		// Skip hash calculation for remote files, use the filename hash
		parts := strings.Split(latestObject.Key, "-")
		if len(parts) >= 4 {
			snap.Hash = strings.TrimSuffix(parts[len(parts)-1], ".tar.zst")
		}
	}

	return snap, nil
}

// uploadMetadata uploads metadata about a snapshot
func (m *Monitor) uploadMetadata(ctx context.Context, metaPath, snapshotPath, status string) error {
	// Parse the snapshot filename
	snap, err := snapshot.ParseFilename(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to parse snapshot filename: %w", err)
	}

	// Calculate hash if not already done
	if snap.Hash == "" {
		if err := snap.CalculateHash(); err != nil {
			return fmt.Errorf("failed to calculate hash: %w", err)
		}
	}

	// Set Solana version and feature set
	snap.SolanaVersion = m.cfg.SolanaVersion
	snap.SolanaFeatureSet = m.solanaFeatureSet

	// Construct URL for the snapshot using the config's public endpoint
	var snapshotURL string
	if m.cfg.S3PublicEndpoint != "" {
		// Use the configured public endpoint
		baseURL := m.cfg.S3PublicEndpoint
		// Ensure the URL doesn't end with a slash
		baseURL = strings.TrimSuffix(baseURL, "/")
		// Construct the full URL - don't include bucket name in URL since it's part of the domain
		snapshotURL = fmt.Sprintf("%s/%s", baseURL, snap.Filename)
	} else {
		// Fall back to constructing URL from endpoint and bucket
		snapshotURL = fmt.Sprintf("%s/%s/%s", m.cfg.S3Endpoint, m.cfg.S3Bucket, snap.Filename)
	}

	// Remove any double slashes except for the http:// or https:// prefix
	snapshotURL = strings.Replace(snapshotURL, "://", "###PLACEHOLDER###", 1)
	snapshotURL = strings.Replace(snapshotURL, "//", "/", -1)
	snapshotURL = strings.Replace(snapshotURL, "###PLACEHOLDER###", "://", 1)

	// Create metadata
	meta := metadata.GenerateFromSnapshot(snap).
		WithStatus(metadata.UploadStatus(status)).
		WithUploader(m.cfg.Hostname).
		WithURL(snapshotURL)

	// Set the appropriate fields based on snapshot type
	if snap.Type == snapshot.FullSnapshot {
		// For full snapshots, set the full snapshot fields
		meta.FullSnapshotSlot = snap.Slot
		meta.FullSnapshotHash = snap.Hash
		meta.FullSnapshotURL = snapshotURL

		// For full snapshots, add the latest incremental snapshot information if available
		if status == string(metadata.StatusCompleted) {
			incSnap, err := m.findLatestIncrementalSnapshot(snap.Slot)
			if err != nil {
				m.logger.Warn("Failed to find latest incremental snapshot", "error", err)
			} else if incSnap != nil {
				// Construct URL for the incremental snapshot
				var incSnapshotURL string
				if m.cfg.S3PublicEndpoint != "" {
					baseURL := m.cfg.S3PublicEndpoint
					baseURL = strings.TrimSuffix(baseURL, "/")
					incSnapshotURL = fmt.Sprintf("%s/%s", baseURL, incSnap.Filename)
				} else {
					incSnapshotURL = fmt.Sprintf("%s/%s/%s", m.cfg.S3Endpoint, m.cfg.S3Bucket, incSnap.Filename)
				}

				// Remove any double slashes
				incSnapshotURL = strings.Replace(incSnapshotURL, "://", "###PLACEHOLDER###", 1)
				incSnapshotURL = strings.Replace(incSnapshotURL, "//", "/", -1)
				incSnapshotURL = strings.Replace(incSnapshotURL, "###PLACEHOLDER###", "://", 1)

				// Add to metadata
				meta.IncrementalSnapshotSlot = incSnap.Slot
				meta.IncrementalSnapshotHash = incSnap.Hash
				meta.IncrementalSnapshotURL = incSnapshotURL

				m.logger.Info("Added latest incremental snapshot to metadata",
					"full_slot", snap.Slot,
					"inc_slot", incSnap.Slot,
					"inc_hash", incSnap.Hash)
			}
		}
	} else if snap.Type == snapshot.IncrementalSnapshot {
		// For incremental snapshots, set the incremental snapshot fields
		meta.IncrementalSnapshotSlot = snap.Slot
		meta.IncrementalSnapshotHash = snap.Hash
		meta.IncrementalSnapshotURL = snapshotURL
	}

	// Add genesis hash if available
	if m.solanaGenesisHash != "" {
		meta.WithGenesisHash(m.solanaGenesisHash)
	}

	// Write metadata to temporary file
	if err := meta.WriteToFile(metaPath); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Generate the remote metadata key
	metadataKey := filepath.Base(metaPath)

	// Upload metadata
	if err := m.s3Client.UploadFile(ctx, metaPath, metadataKey); err != nil {
		return fmt.Errorf("failed to upload metadata: %w", err)
	}

	// If this is a completed snapshot, update the latest.json file
	if status == string(metadata.StatusCompleted) {
		m.logger.Info("Updating latest.json with metadata for completed snapshot",
			"slot", snap.Slot,
			"hash", snap.Hash)

		if err := m.s3Client.UpdateLatestMetadata(ctx, metadataKey); err != nil {
			m.logger.Warn("Failed to update latest.json", "error", err)
			// Don't fail the whole operation if updating latest.json fails
		}
	}

	return nil
}

// uploadSnapshot uploads a snapshot file to S3
func (m *Monitor) uploadSnapshot(ctx context.Context, path string) error {
	// Get file info for size
	fileInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Parse the snapshot filename
	snap, err := snapshot.ParseFilename(path)
	if err != nil {
		return fmt.Errorf("failed to parse snapshot filename: %w", err)
	}

	// Calculate hash
	if err := snap.CalculateHash(); err != nil {
		return fmt.Errorf("failed to calculate hash: %w", err)
	}

	// Set Solana version and feature set
	snap.SolanaVersion = m.cfg.SolanaVersion
	snap.SolanaFeatureSet = m.solanaFeatureSet

	// Create metadata filename
	metaPath := filepath.Join(os.TempDir(), snap.GetMetadataFilename())

	// Upload metadata with 'uploading' status
	if err := m.uploadMetadata(ctx, metaPath, path, string(metadata.StatusUploading)); err != nil {
		return fmt.Errorf("failed to upload metadata: %w", err)
	}

	// Log the start of the upload
	m.logger.Info("Starting snapshot upload",
		"file", filepath.Base(path),
		"size", formatFileSize(fileInfo.Size()),
		"solana_version", snap.SolanaVersion,
		"feature_set", snap.SolanaFeatureSet,
		"slot", snap.Slot,
		"hash", snap.Hash)

	// Record the start time of the upload
	m.uploadStartTime = time.Now()

	// Upload the snapshot
	if err := m.s3Client.UploadFile(ctx, path, snap.Filename); err != nil {
		return fmt.Errorf("failed to upload snapshot: %w", err)
	}

	// Calculate upload statistics
	uploadDuration := time.Since(m.uploadStartTime)
	uploadSpeedMBps := float64(fileInfo.Size()) / (1024 * 1024) / uploadDuration.Seconds()

	// Log upload completion
	m.logger.Info("Successfully uploaded snapshot",
		"file", filepath.Base(path),
		"size", formatFileSize(fileInfo.Size()),
		"duration", uploadDuration.Round(time.Second).String(),
		"speed", fmt.Sprintf("%.2f MB/s", uploadSpeedMBps))

	// Upload metadata with 'completed' status
	if err := m.uploadMetadata(ctx, metaPath, path, string(metadata.StatusCompleted)); err != nil {
		return fmt.Errorf("failed to upload metadata: %w", err)
	}

	// Run cleanup after successful upload if retention is enabled
	// Only run cleanup if it hasn't been run in the last 5 minutes
	if m.cfg.EnableRetention && time.Since(m.lastCleanupTime) > 5*time.Minute {
		m.logger.Info("Running cleanup after successful upload")

		// Create a context with a 10-minute timeout
		cleanupCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()

		// Run cleanup in a goroutine so we can monitor for timeout
		cleanupDone := make(chan error, 1)
		go func() {
			cleanupDone <- m.CleanupOldSnapshots()
		}()

		// Wait for cleanup to finish or timeout
		select {
		case err := <-cleanupDone:
			if err != nil {
				m.logger.Warn("Failed to clean up old snapshots after upload", "error", err)
			} else {
				m.logger.Info("Cleanup completed successfully")
			}
		case <-cleanupCtx.Done():
			m.logger.Warn("Cleanup operation timed out after 10 minutes", "error", cleanupCtx.Err())
		}

		m.lastCleanupTime = time.Now()
	}

	return nil
}

// ProcessSnapshot processes a single snapshot file
func (m *Monitor) ProcessSnapshot(path string) error {
	// Create a context for the upload
	ctx := context.Background()

	// Upload the snapshot
	return m.uploadSnapshot(ctx, path)
}

// checkAndUploadGenesis checks if genesis.tar.bz2 file exists in the S3 bucket
// and uploads it if it doesn't
func (m *Monitor) checkAndUploadGenesis(ctx context.Context) error {
	const genesisFileName = "genesis.tar.bz2"

	// First check if genesis.tar.bz2 already exists in the bucket
	exists, err := m.s3Client.FileExists(genesisFileName)
	if err != nil {
		return fmt.Errorf("failed to check if genesis file exists in bucket: %w", err)
	}

	if exists {
		m.logger.Info("Genesis file already exists in bucket, skipping upload")
		return nil
	}

	// Genesis file doesn't exist in bucket, look for it in the ledger directory
	genesisPath := filepath.Join(m.cfg.WatchDir, genesisFileName)
	if _, err := os.Stat(genesisPath); os.IsNotExist(err) {
		// Try the parent directory of the watch directory
		parentDir := filepath.Dir(m.cfg.WatchDir)
		genesisPath = filepath.Join(parentDir, genesisFileName)

		if _, err := os.Stat(genesisPath); os.IsNotExist(err) {
			return fmt.Errorf("genesis file not found in %s or %s", m.cfg.WatchDir, parentDir)
		}
	}

	// Get file info for logging
	fileInfo, err := os.Stat(genesisPath)
	if err != nil {
		return fmt.Errorf("failed to get genesis file info: %w", err)
	}

	m.logger.Info("Uploading genesis file",
		"path", genesisPath,
		"size", formatFileSize(fileInfo.Size()))

	// Upload the genesis file
	uploadStartTime := time.Now()
	if err := m.s3Client.UploadFile(ctx, genesisPath, genesisFileName); err != nil {
		return fmt.Errorf("failed to upload genesis file: %w", err)
	}

	// Calculate upload statistics
	uploadDuration := time.Since(uploadStartTime)
	uploadSpeedMBps := float64(fileInfo.Size()) / (1024 * 1024) / uploadDuration.Seconds()

	m.logger.Info("Successfully uploaded genesis file",
		"path", genesisPath,
		"size", formatFileSize(fileInfo.Size()),
		"duration", uploadDuration.Round(time.Second).String(),
		"speed", fmt.Sprintf("%.2f MB/s", uploadSpeedMBps))

	return nil
}
