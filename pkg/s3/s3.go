package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
)

// getFileSize returns the size of a file in bytes
func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// Constants copied directly from the faster implementation
const (
	PartSize       int64 = 32 * 1024 * 1024 // 32MB part size for optimal performance
	MaxConcurrency       = 5                // 5 concurrent uploads
)

// Client is a wrapper around AWS S3 client
type Client struct {
	client     *s3.Client
	bucketName string
	logger     *slog.Logger
}

// ProgressReader is a wrapper around an io.Reader that reports progress
type ProgressReader struct {
	io.Reader
	io.ReaderAt
	Total        int64
	Uploaded     int64
	ProgressFunc func(uploaded, total int64, speed float64, eta time.Duration)
	LastUpdate   time.Time
	StartTime    time.Time
	SpeedHistory []struct {
		Timestamp time.Time
		Bytes     int64
	}
}

// ReadAt implements the io.ReaderAt interface
func (pr *ProgressReader) ReadAt(p []byte, off int64) (n int, err error) {
	if pr.ReaderAt == nil {
		return 0, fmt.Errorf("underlying reader does not support ReadAt")
	}
	return pr.ReaderAt.ReadAt(p, off)
}

// Read reads data from the underlying reader and reports progress
func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.Reader.Read(p)
	if n > 0 {
		pr.Uploaded += int64(n)
		now := time.Now()

		// Initialize start time if not set
		if pr.StartTime.IsZero() {
			pr.StartTime = now
			pr.SpeedHistory = make([]struct {
				Timestamp time.Time
				Bytes     int64
			}, 0, 10) // Pre-allocate for 10 entries
		}

		// Only call progress function every 100ms to avoid too many updates
		if now.Sub(pr.LastUpdate) >= 100*time.Millisecond {
			// Add current data point to speed history
			pr.SpeedHistory = append(pr.SpeedHistory, struct {
				Timestamp time.Time
				Bytes     int64
			}{
				Timestamp: now,
				Bytes:     pr.Uploaded,
			})

			// Keep only the last 10 data points for speed calculation
			if len(pr.SpeedHistory) > 10 {
				pr.SpeedHistory = pr.SpeedHistory[len(pr.SpeedHistory)-10:]
			}

			// Calculate speed in bytes per second
			var speed float64
			if len(pr.SpeedHistory) > 1 {
				first := pr.SpeedHistory[0]
				last := pr.SpeedHistory[len(pr.SpeedHistory)-1]
				duration := last.Timestamp.Sub(first.Timestamp).Seconds()
				if duration > 0 {
					speed = float64(last.Bytes-first.Bytes) / duration
				}
			}

			// Calculate ETA
			var eta time.Duration
			if speed > 0 {
				remainingBytes := pr.Total - pr.Uploaded
				etaSeconds := float64(remainingBytes) / speed
				eta = time.Duration(etaSeconds * float64(time.Second))
			}

			if pr.ProgressFunc != nil {
				pr.ProgressFunc(pr.Uploaded, pr.Total, speed, eta)
			}
			pr.LastUpdate = now
		}
	}
	return n, err
}

// filteringLogger is a custom logger that filters out specific messages
type filteringLogger struct {
	logger        logging.Logger
	filterStrings []string
}

// Logf implements the logging.Logger interface
func (l *filteringLogger) Logf(classification logging.Classification, format string, v ...interface{}) {
	message := fmt.Sprintf(format, v...)
	for _, filter := range l.filterStrings {
		if strings.Contains(message, filter) {
			return // Skip logging this message
		}
	}
	l.logger.Logf(classification, format, v...)
}

// NewClient creates a new S3 client
func NewClient(endpoint, bucketName, accessKey, secretKey string) (*Client, error) {
	// Create exactly the same config as the faster implementation
	cfg := aws.Config{
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		Region:      "auto", // Default region as in the faster implementation
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               endpoint,
				HostnameImmutable: true,
			}, nil
		}),
	}

	// Create client directly from config as in the faster implementation
	client := s3.NewFromConfig(cfg)

	return &Client{
		client:     client,
		bucketName: bucketName,
		logger:     slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}, nil
}

// SetLogger sets a custom logger for the client
func (c *Client) SetLogger(logger *slog.Logger) {
	if logger != nil {
		c.logger = logger
	}
}

// PartReader is a reader that handles part uploads with proper checksum handling
type PartReader struct {
	reader     io.Reader
	size       int64
	read       int64
	progressFn func(n int64)
	buf        []byte
}

func NewPartReader(reader io.Reader, size int64, progressFn func(n int64)) *PartReader {
	return &PartReader{
		reader:     reader,
		size:       size,
		progressFn: progressFn,
	}
}

func (pr *PartReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	if n > 0 {
		pr.read += int64(n)
		if pr.progressFn != nil {
			pr.progressFn(int64(n))
		}
	}
	return n, err
}

func (pr *PartReader) Size() int64 {
	return pr.size
}

// CleanupMultipartUploads cleans up any existing multipart uploads for the given key
func (c *Client) CleanupMultipartUploads(ctx context.Context, key string) error {
	c.logger.Info("Starting cleanup of existing multipart uploads", "key", key)

	// List all multipart uploads
	listResp, err := c.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(c.bucketName),
		Prefix: aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to list multipart uploads: %w", err)
	}

	if len(listResp.Uploads) == 0 {
		c.logger.Info("No existing multipart uploads found", "key", key)
		return nil
	}

	c.logger.Info("Found existing multipart uploads to clean up",
		"key", key,
		"count", len(listResp.Uploads))

	// Abort each multipart upload
	for i, upload := range listResp.Uploads {
		c.logger.Info("Aborting multipart upload",
			"key", *upload.Key,
			"upload_id", *upload.UploadId,
			"progress", fmt.Sprintf("%d/%d", i+1, len(listResp.Uploads)))

		_, err := c.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(c.bucketName),
			Key:      upload.Key,
			UploadId: upload.UploadId,
		})
		if err != nil {
			c.logger.Warn("Failed to abort multipart upload",
				"key", *upload.Key,
				"upload_id", *upload.UploadId,
				"error", err)
		} else {
			c.logger.Info("Successfully aborted multipart upload",
				"key", *upload.Key,
				"upload_id", *upload.UploadId)
		}
	}

	c.logger.Info("Completed cleanup of multipart uploads",
		"key", key,
		"total_cleaned", len(listResp.Uploads))
	return nil
}

// UploadFile uploads a file to S3 using the fast implementation approach
func (c *Client) UploadFile(ctx context.Context, localPath, s3Path string) error {
	// Open file exactly as in the faster implementation
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file info exactly as in the faster implementation
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	fileSize := info.Size()

	c.logger.Info("Starting upload", "file", localPath, "key", s3Path, "size", fileSize)

	// Create multipart upload exactly as in the faster implementation
	uploadID, err := c.createMultipartUpload(s3Path)
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}

	// Calculate partCount exactly as in the faster implementation
	partCount := int((fileSize + PartSize - 1) / PartSize)

	c.logger.Info("Created multipart upload", "id", *uploadID, "parts", partCount)

	// Set up concurrency control exactly as in the faster implementation
	var wg sync.WaitGroup
	var mu sync.Mutex
	parts := make([]types.CompletedPart, partCount)
	semaphore := make(chan struct{}, MaxConcurrency)

	// Start upload loop exactly as in the faster implementation
	for i := 0; i < partCount; i++ {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(partNum int) {
			defer func() {
				<-semaphore
				wg.Done()
			}()

			// Calculate part bounds exactly as in the faster implementation
			offset := int64(partNum) * PartSize
			size := PartSize
			if offset+size > fileSize {
				size = fileSize - offset
			}

			// Create buffer exactly as in the faster implementation
			buf := make([]byte, size)

			// Read part data exactly as in the faster implementation
			_, err := file.ReadAt(buf, offset)
			if err != nil && err != io.EOF {
				c.logger.Error("Failed to read part", "part", partNum+1, "error", err)
				return
			}

			// Upload part exactly as in the faster implementation
			result, err := c.client.UploadPart(context.TODO(), &s3.UploadPartInput{
				Bucket:     aws.String(c.bucketName),
				Key:        aws.String(s3Path),
				UploadId:   uploadID,
				PartNumber: aws.Int32(int32(partNum + 1)),
				Body:       bytes.NewReader(buf),
			})
			if err != nil {
				c.logger.Error("Failed to upload part", "part", partNum+1, "error", err)
				return
			}

			// Store completed part exactly as in the faster implementation
			mu.Lock()
			parts[partNum] = types.CompletedPart{
				ETag:       result.ETag,
				PartNumber: aws.Int32(int32(partNum + 1)),
			}

			// Log progress
			c.logger.Info("Completed part upload",
				"part", partNum+1,
				"total", partCount,
				"progress", fmt.Sprintf("%.1f%%", float64(partNum+1)/float64(partCount)*100))

			mu.Unlock()
		}(i)
	}

	// Wait for all uploads to complete exactly as in the faster implementation
	wg.Wait()

	// Complete the multipart upload exactly as in the faster implementation
	_, err = c.client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(c.bucketName),
		Key:      aws.String(s3Path),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	c.logger.Info("Upload complete", "file", s3Path)
	return nil
}

// createMultipartUpload helper function exactly as in the faster implementation
func (c *Client) createMultipartUpload(key string) (*string, error) {
	resp, err := c.client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return resp.UploadId, nil
}

// UploadFileMultipart is an alias for UploadFile for backward compatibility
func (c *Client) UploadFileMultipart(ctx context.Context, localPath, s3Path string) error {
	return c.UploadFile(ctx, localPath, s3Path)
}

// cleanupExistingUploads aborts any existing multipart uploads for the given key
func (c *Client) cleanupExistingUploads(ctx context.Context, key string) error {
	listResp, err := c.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(c.bucketName),
		Prefix: aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to list multipart uploads: %w", err)
	}

	if len(listResp.Uploads) == 0 {
		return nil // No uploads to clean up
	}

	c.logger.Info("Found existing multipart uploads to clean up",
		"count", len(listResp.Uploads),
		"key", key)

	for _, upload := range listResp.Uploads {
		_, err := c.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(c.bucketName),
			Key:      upload.Key,
			UploadId: upload.UploadId,
		})
		if err != nil {
			c.logger.Warn("Failed to abort upload",
				"key", *upload.Key,
				"upload_id", *upload.UploadId,
				"error", err)
		} else {
			c.logger.Info("Aborted existing upload",
				"key", *upload.Key,
				"upload_id", *upload.UploadId)
		}
	}

	return nil
}

// CleanupAbandonedUploads cleans up abandoned multipart uploads
func (c *Client) CleanupAbandonedUploads(age time.Duration) error {
	c.logger.Info("Listing multipart uploads")

	listResp, err := c.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
		Bucket: aws.String(c.bucketName),
	})
	if err != nil {
		return fmt.Errorf("failed to list multipart uploads: %w", err)
	}

	if len(listResp.Uploads) == 0 {
		c.logger.Info("No multipart uploads found")
		return nil
	}

	c.logger.Info("Found multipart uploads", "count", len(listResp.Uploads))

	cutoffTime := time.Now().Add(-age)
	for _, upload := range listResp.Uploads {
		if upload.Initiated != nil && upload.Initiated.Before(cutoffTime) {
			c.logger.Info("Aborting upload", "key", *upload.Key, "id", *upload.UploadId)
			_, err := c.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(c.bucketName),
				Key:      upload.Key,
				UploadId: upload.UploadId,
			})
			if err != nil {
				c.logger.Warn("Failed to abort upload", "error", err)
			}
		}
	}

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

// FileExists checks if a file exists in S3
func (c *Client) FileExists(s3Path string) (bool, error) {
	// Create a context with timeout for the operation
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startTime := time.Now()
	_, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(s3Path),
	})

	exists := true
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NotFound" {
				exists = false
				err = nil
			}
		}
	}

	// Log slow operations for debugging
	duration := time.Since(startTime)
	if duration > 1*time.Second {
		c.logger.Warn("Slow head object operation",
			"key", s3Path,
			"exists", exists,
			"duration", duration.String())
	}

	return exists, err
}

// DownloadFile downloads a file from S3
func (c *Client) DownloadFile(s3Path, localPath string) error {
	// Create the directory if it doesn't exist
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create the file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	c.logger.Info("Downloading file", "s3_path", s3Path, "local_path", localPath)

	// Download the file
	result, err := c.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(s3Path),
	})
	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}
	defer result.Body.Close()

	// Copy the data
	written, err := io.Copy(file, result.Body)
	if err != nil {
		return fmt.Errorf("failed to save file: %w", err)
	}

	c.logger.Info("Download completed", "bytes", written)
	return nil
}

// DeleteObject deletes an object from S3, ensuring metadata files for snapshots are preserved
func (c *Client) DeleteObject(s3Path string) error {
	// If we're deleting a metadata file, first check if the snapshot exists
	if IsMetadataKey(s3Path) {
		hasSnapshot, err := c.HasCorrespondingFile(context.Background(), s3Path)
		if err != nil {
			c.logger.Warn("Failed to check if snapshot exists for metadata",
				"key", s3Path,
				"error", err)
		} else if hasSnapshot {
			c.logger.Info("Not deleting metadata file as snapshot exists", "key", s3Path)
			return nil
		}
	}

	_, err := c.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(s3Path),
	})
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}
	c.logger.Info("Deleted object", "s3_path", s3Path)
	return nil
}

// DeleteObjects deletes multiple objects from S3, ensuring metadata files for existing snapshots are preserved
func (c *Client) DeleteObjects(s3Paths []string) error {
	if len(s3Paths) == 0 {
		return nil
	}

	startTime := time.Now()
	c.logger.Info("Starting deletion of objects", "count", len(s3Paths))

	// Filter out metadata files that have corresponding snapshots
	c.logger.Debug("Filtering metadata files before deletion")
	filterStart := time.Now()
	var filteredPaths []string
	for _, path := range s3Paths {
		if IsMetadataKey(path) {
			hasSnapshot, err := c.HasCorrespondingFile(context.Background(), path)
			if err != nil {
				c.logger.Warn("Failed to check if snapshot exists for metadata",
					"key", path,
					"error", err)
				// Skip this file to be safe
				continue
			}
			if hasSnapshot {
				c.logger.Info("Not deleting metadata file as snapshot exists", "key", path)
				continue
			}
		}
		filteredPaths = append(filteredPaths, path)
	}
	c.logger.Info("Filtered paths for deletion",
		"original_count", len(s3Paths),
		"filtered_count", len(filteredPaths),
		"duration", time.Since(filterStart).String())

	// If all files were filtered out, return early
	if len(filteredPaths) == 0 {
		c.logger.Info("No objects to delete after filtering")
		return nil
	}

	const maxObjectsPerRequest = 1000
	batchCount := (len(filteredPaths) + maxObjectsPerRequest - 1) / maxObjectsPerRequest
	c.logger.Info("Preparing to delete objects in batches",
		"total_objects", len(filteredPaths),
		"batch_size", maxObjectsPerRequest,
		"batch_count", batchCount)

	for i := 0; i < len(filteredPaths); i += maxObjectsPerRequest {
		batchStart := time.Now()
		end := i + maxObjectsPerRequest
		if end > len(filteredPaths) {
			end = len(filteredPaths)
		}

		batch := filteredPaths[i:end]
		c.logger.Info("Processing deletion batch",
			"batch_number", (i/maxObjectsPerRequest)+1,
			"of", batchCount,
			"batch_size", len(batch))

		objects := make([]types.ObjectIdentifier, len(batch))
		for j, key := range batch {
			objects[j] = types.ObjectIdentifier{
				Key: aws.String(key),
			}
		}

		// Create a context with timeout for each delete operation
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)

		c.logger.Debug("Sending DeleteObjects request to S3")
		deleteStart := time.Now()
		_, err := c.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.bucketName),
			Delete: &types.Delete{
				Objects: objects,
				Quiet:   aws.Bool(true),
			},
		})
		cancel() // Always cancel the context

		if err != nil {
			c.logger.Error("Failed to delete objects batch",
				"batch", (i/maxObjectsPerRequest)+1,
				"error", err)
			return fmt.Errorf("failed to delete objects batch %d-%d: %w", i, end-1, err)
		}

		c.logger.Info("Successfully deleted batch of objects",
			"batch", (i/maxObjectsPerRequest)+1,
			"count", len(batch),
			"batch_duration", time.Since(batchStart).String(),
			"delete_request_duration", time.Since(deleteStart).String())
	}

	c.logger.Info("All deletion operations completed successfully",
		"total_objects", len(filteredPaths),
		"total_batches", batchCount,
		"total_duration", time.Since(startTime).String())
	return nil
}

// S3Object represents an object in the S3 bucket
type S3Object struct {
	Key          string
	LastModified time.Time
	Size         int64
}

// ListObjects lists objects in the S3 bucket with the given prefix
func (c *Client) ListObjects(prefix string) ([]S3Object, error) {
	var objects []S3Object
	var continuationToken *string

	for {
		result, err := c.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket:            aws.String(c.bucketName),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range result.Contents {
			objects = append(objects, S3Object{
				Key:          *obj.Key,
				LastModified: *obj.LastModified,
				Size:         *obj.Size,
			})
		}

		if result.IsTruncated == nil || !*result.IsTruncated {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	return objects, nil
}

// MultipartUploadInfo stores information about a multipart upload
type MultipartUploadInfo struct {
	UploadID     string
	Key          string
	Parts        []types.CompletedPart
	ChunkSize    int64
	TotalSize    int64
	UploadedSize int64
}

// SaveUploadInfoToFile saves multipart upload information to a file
func SaveUploadInfoToFile(info *MultipartUploadInfo, filePath string) error {
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal upload info: %w", err)
	}

	// Ensure the directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory for upload info: %w", err)
	}

	// Try to write the file
	err = os.WriteFile(filePath, data, 0644)
	if err != nil {
		// If we can't write to the specified path, try the system temp directory as a fallback
		if !strings.HasPrefix(filePath, os.TempDir()) {
			tempPath := filepath.Join(os.TempDir(), filepath.Base(filePath))
			slog.Warn("Failed to write upload info, trying alternate location",
				"original_path", filePath,
				"fallback_path", tempPath)
			if err := os.WriteFile(tempPath, data, 0644); err != nil {
				return fmt.Errorf("failed to write upload info file to fallback location: %w", err)
			}
			return nil
		}
		return fmt.Errorf("failed to write upload info file: %w", err)
	}

	return nil
}

// LoadUploadInfoFromFile loads multipart upload information from a file
func LoadUploadInfoFromFile(filePath string) (*MultipartUploadInfo, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read upload info file: %w", err)
	}

	var info MultipartUploadInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal upload info: %w", err)
	}

	return &info, nil
}

// GetUploadInfoFilePath returns the path to the upload info file for a given file
func GetUploadInfoFilePath(filePath string) string {
	// Use the system's temporary directory instead of the same directory as the file
	fileName := filepath.Base(filePath)
	return filepath.Join(os.TempDir(), fileName+".upload-info")
}

// AbortMultipartUpload aborts a multipart upload
func (c *Client) AbortMultipartUpload(s3Path, uploadID string) error {
	_, err := c.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(c.bucketName),
		Key:      aws.String(s3Path),
		UploadId: aws.String(uploadID),
	})
	if err != nil {
		return fmt.Errorf("failed to abort multipart upload: %w", err)
	}
	return nil
}

// EnsureTempDirExists ensures that the temporary directory exists and is writable
func EnsureTempDirExists() error {
	tempDir := os.TempDir()

	// Check if the directory exists
	info, err := os.Stat(tempDir)
	if err != nil {
		if os.IsNotExist(err) {
			// Try to create the directory
			if err := os.MkdirAll(tempDir, 0755); err != nil {
				return fmt.Errorf("failed to create temporary directory: %w", err)
			}
		} else {
			return fmt.Errorf("failed to check temporary directory: %w", err)
		}
	} else if !info.IsDir() {
		return fmt.Errorf("temporary path exists but is not a directory: %s", tempDir)
	}

	// Check if the directory is writable by creating a test file
	testFile := filepath.Join(tempDir, "write-test-"+time.Now().Format("20060102150405"))
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return fmt.Errorf("temporary directory is not writable: %w", err)
	}

	// Clean up the test file
	if err := os.Remove(testFile); err != nil {
		// Just log this error, don't fail
		slog.Warn("Failed to remove test file", "error", err)
	}

	return nil
}

// GetMetadataKey converts a snapshot key to its metadata key
func GetMetadataKey(snapshotKey string) string {
	return snapshotKey + ".json"
}

// GetSnapshotKeyFromMetadata converts a metadata key to its snapshot key
func GetSnapshotKeyFromMetadata(metadataKey string) string {
	if strings.HasSuffix(metadataKey, ".json") {
		return metadataKey[:len(metadataKey)-5]
	}
	return metadataKey
}

// IsMetadataKey returns true if the key is a metadata key
func IsMetadataKey(key string) bool {
	return strings.HasSuffix(key, ".json")
}

// IsSnapshotKey returns true if the key is a snapshot key
func IsSnapshotKey(key string) bool {
	return strings.HasSuffix(key, ".tar.zst")
}

// HasCorrespondingFile checks if the object has a corresponding file (snapshot for metadata or metadata for snapshot)
func (c *Client) HasCorrespondingFile(ctx context.Context, key string) (bool, error) {
	startTime := time.Now()

	// Create a timeout context if one doesn't exist
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var checkKey string
	if IsMetadataKey(key) {
		// For metadata file, check if snapshot exists
		checkKey = GetSnapshotKeyFromMetadata(key)
	} else if IsSnapshotKey(key) {
		// For snapshot file, check if metadata exists
		checkKey = GetMetadataKey(key)
	} else {
		// Not a snapshot or metadata file
		return false, nil
	}

	// Check if the corresponding file exists with timeout context
	_, err := c.client.HeadObject(timeoutCtx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(checkKey),
	})

	exists := true
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NotFound" {
				exists = false
				err = nil
			}
		}
	}

	// Log the operation for debugging
	if err != nil {
		c.logger.Warn("Error checking for corresponding file",
			"original_key", key,
			"check_key", checkKey,
			"error", err,
			"duration", time.Since(startTime).String())
	} else {
		c.logger.Debug("Checked for corresponding file",
			"original_key", key,
			"check_key", checkKey,
			"exists", exists,
			"duration", time.Since(startTime).String())
	}

	return exists, err
}

// UpdateLatestMetadata creates or updates the latest.json file
// combining data from the latest full snapshot and incremental snapshot
func (c *Client) UpdateLatestMetadata(ctx context.Context, metadataKey string) error {
	if !IsMetadataKey(metadataKey) {
		return fmt.Errorf("not a metadata file: %s", metadataKey)
	}

	c.logger.Info("Updating latest.json with metadata", "source", metadataKey)

	// Create a temporary file to download the metadata
	tmpFile, err := os.CreateTemp("", "metadata-*.json")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Download the metadata file from S3
	if err := c.DownloadFile(metadataKey, tmpFile.Name()); err != nil {
		return fmt.Errorf("failed to download metadata from S3: %w", err)
	}

	// Read current metadata
	currentData, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Parse metadata
	var currentMeta map[string]interface{}
	if err := json.Unmarshal(currentData, &currentMeta); err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	// Determine if this is a full or incremental snapshot
	isFullSnapshot := false
	if metadataKey != "latest.json" && !strings.Contains(metadataKey, "incremental") {
		isFullSnapshot = true
	}

	// Check if latest.json already exists
	latestExists, err := c.FileExists("latest.json")
	if err != nil {
		c.logger.Warn("Failed to check if latest.json exists", "error", err)
	}

	var latestMeta map[string]interface{}

	if latestExists {
		// Download existing latest.json
		latestTmpFile, err := os.CreateTemp("", "latest-*.json")
		if err != nil {
			return fmt.Errorf("failed to create temporary file for latest.json: %w", err)
		}
		defer os.Remove(latestTmpFile.Name())
		defer latestTmpFile.Close()

		if err := c.DownloadFile("latest.json", latestTmpFile.Name()); err != nil {
			c.logger.Warn("Failed to download existing latest.json", "error", err)
		} else {
			// Read existing latest.json
			latestData, err := os.ReadFile(latestTmpFile.Name())
			if err != nil {
				c.logger.Warn("Failed to read latest.json", "error", err)
			} else {
				// Parse latest.json
				if err := json.Unmarshal(latestData, &latestMeta); err != nil {
					c.logger.Warn("Failed to parse latest.json", "error", err)
				}
			}
		}
	}

	// Create a consolidated metadata object
	var consolidatedMeta map[string]interface{}

	if latestMeta == nil {
		// No existing latest.json or failed to read/parse it
		consolidatedMeta = currentMeta
	} else {
		// Start with a fresh map
		consolidatedMeta = make(map[string]interface{})

		// Add common fields from current metadata
		for k, v := range currentMeta {
			// Skip fields that we'll handle specially
			if k != "slot" && k != "hash" && k != "url" &&
				k != "latest_incremental_slot" && k != "latest_incremental_hash" && k != "latest_incremental_url" {
				consolidatedMeta[k] = v
			}
		}

		// Handle full snapshot information
		if isFullSnapshot {
			// Current metadata is from a full snapshot
			consolidatedMeta["full_snapshot_slot"] = currentMeta["slot"]
			consolidatedMeta["full_snapshot_hash"] = currentMeta["hash"]
			consolidatedMeta["full_snapshot_url"] = currentMeta["url"]

			// Preserve incremental snapshot info from latest.json if available
			if _, ok := latestMeta["incremental_snapshot_slot"].(float64); ok {
				consolidatedMeta["incremental_snapshot_slot"] = latestMeta["incremental_snapshot_slot"]
				consolidatedMeta["incremental_snapshot_hash"] = latestMeta["incremental_snapshot_hash"]
				consolidatedMeta["incremental_snapshot_url"] = latestMeta["incremental_snapshot_url"]
			} else if incSlot, ok := latestMeta["latest_incremental_slot"].(float64); ok && incSlot > 0 {
				// Legacy format migration
				consolidatedMeta["incremental_snapshot_slot"] = incSlot
				if incHash, ok := latestMeta["latest_incremental_hash"].(string); ok {
					consolidatedMeta["incremental_snapshot_hash"] = incHash
				}
				if incURL, ok := latestMeta["latest_incremental_url"].(string); ok {
					consolidatedMeta["incremental_snapshot_url"] = incURL
				}
			}
		} else {
			// Current metadata is from an incremental snapshot
			// Add incremental snapshot info from current metadata
			consolidatedMeta["incremental_snapshot_slot"] = currentMeta["slot"]
			consolidatedMeta["incremental_snapshot_hash"] = currentMeta["hash"]
			consolidatedMeta["incremental_snapshot_url"] = currentMeta["url"]

			// Preserve full snapshot info from latest.json
			if _, ok := latestMeta["full_snapshot_slot"].(float64); ok {
				consolidatedMeta["full_snapshot_slot"] = latestMeta["full_snapshot_slot"]
				consolidatedMeta["full_snapshot_hash"] = latestMeta["full_snapshot_hash"]
				consolidatedMeta["full_snapshot_url"] = latestMeta["full_snapshot_url"]
			} else if fullSlot, ok := latestMeta["slot"].(float64); ok {
				// Legacy format migration
				consolidatedMeta["full_snapshot_slot"] = fullSlot
				if fullHash, ok := latestMeta["hash"].(string); ok {
					consolidatedMeta["full_snapshot_hash"] = fullHash
				}
				if fullURL, ok := latestMeta["url"].(string); ok {
					consolidatedMeta["full_snapshot_url"] = fullURL
				}
			}
		}

		// For backward compatibility until clients are updated
		// If there's a full snapshot, set legacy slot (but not hash or url)
		if _, ok := consolidatedMeta["full_snapshot_slot"].(float64); ok {
			consolidatedMeta["slot"] = consolidatedMeta["full_snapshot_slot"]
		}

		// Remove the duplicated legacy fields for incremental snapshots
		delete(consolidatedMeta, "latest_incremental_slot")
		delete(consolidatedMeta, "latest_incremental_hash")
		delete(consolidatedMeta, "latest_incremental_url")
		delete(consolidatedMeta, "hash")
		delete(consolidatedMeta, "url")

		// Preserve genesis hash if missing in current metadata
		if _, ok := consolidatedMeta["genesis_hash"].(string); !ok {
			if genesisHash, ok := latestMeta["genesis_hash"].(string); ok {
				consolidatedMeta["genesis_hash"] = genesisHash
			}
		}
	}

	// Set status to completed
	consolidatedMeta["status"] = "completed"

	// Create an ordered map to maintain the field order
	orderedMeta := make(map[string]interface{})

	// 1. First group: slot, feature_set, version, genesis_hash
	if val, ok := consolidatedMeta["slot"].(float64); ok {
		orderedMeta["slot"] = val
	}
	if val, ok := consolidatedMeta["solana_feature_set"].(float64); ok {
		orderedMeta["solana_feature_set"] = val
	} else if val, ok := consolidatedMeta["solana_feature_set"].(int64); ok {
		orderedMeta["solana_feature_set"] = val
	}
	if val, ok := consolidatedMeta["solana_version"].(string); ok {
		orderedMeta["solana_version"] = val
	}
	if val, ok := consolidatedMeta["status"].(string); ok {
		orderedMeta["status"] = val
	}
	if val, ok := consolidatedMeta["genesis_hash"].(string); ok {
		orderedMeta["genesis_hash"] = val
	}

	// 2. Second group: full snapshot fields
	if val, ok := consolidatedMeta["full_snapshot_slot"].(float64); ok {
		orderedMeta["full_snapshot_slot"] = val
	}
	if val, ok := consolidatedMeta["full_snapshot_hash"].(string); ok {
		orderedMeta["full_snapshot_hash"] = val
	}
	if val, ok := consolidatedMeta["full_snapshot_url"].(string); ok {
		orderedMeta["full_snapshot_url"] = val
	}

	// 3. Third group: incremental snapshot fields
	if val, ok := consolidatedMeta["incremental_snapshot_slot"].(float64); ok {
		orderedMeta["incremental_snapshot_slot"] = val
	}
	if val, ok := consolidatedMeta["incremental_snapshot_hash"].(string); ok {
		orderedMeta["incremental_snapshot_hash"] = val
	}
	if val, ok := consolidatedMeta["incremental_snapshot_url"].(string); ok {
		orderedMeta["incremental_snapshot_url"] = val
	}

	// 4. Fourth group: timestamp fields
	if val, ok := consolidatedMeta["timestamp"].(float64); ok {
		orderedMeta["timestamp"] = val
	} else if val, ok := consolidatedMeta["timestamp"].(int64); ok {
		orderedMeta["timestamp"] = val
	}
	if val, ok := consolidatedMeta["timestamp_human"].(string); ok {
		orderedMeta["timestamp_human"] = val
	}

	// 5. Fifth group: uploaded_by and any remaining fields
	if val, ok := consolidatedMeta["uploaded_by"].(string); ok {
		orderedMeta["uploaded_by"] = val
	}

	// Marshal ordered metadata to JSON
	consolidatedData, err := json.MarshalIndent(orderedMeta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal consolidated metadata: %w", err)
	}

	// Create a temporary file for consolidated metadata
	consolidatedTmpFile, err := os.CreateTemp("", "consolidated-*.json")
	if err != nil {
		return fmt.Errorf("failed to create temporary file for consolidated metadata: %w", err)
	}
	defer os.Remove(consolidatedTmpFile.Name())
	defer consolidatedTmpFile.Close()

	// Write consolidated metadata to temporary file
	if err := os.WriteFile(consolidatedTmpFile.Name(), consolidatedData, 0644); err != nil {
		return fmt.Errorf("failed to write consolidated metadata: %w", err)
	}

	// Open consolidated metadata file for upload
	consolidatedFile, err := os.Open(consolidatedTmpFile.Name())
	if err != nil {
		return fmt.Errorf("failed to open consolidated metadata file: %w", err)
	}
	defer consolidatedFile.Close()

	// Upload consolidated metadata as latest.json with proper content type
	_, err = c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucketName),
		Key:         aws.String("latest.json"),
		Body:        consolidatedFile,
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload latest.json: %w", err)
	}

	c.logger.Info("Successfully updated latest.json with consolidated metadata")
	return nil
}
