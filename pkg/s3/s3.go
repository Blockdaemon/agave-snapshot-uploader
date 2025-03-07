package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
)

// Client represents an S3 client
type Client struct {
	client     *s3.Client
	bucketName string
}

// ProgressReader is a wrapper around an io.Reader that reports progress
type ProgressReader struct {
	io.Reader
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
	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               endpoint,
			SigningRegion:     region,
			HostnameImmutable: true,
		}, nil
	})

	// Create a custom HTTP client with retry configuration
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Create a custom logger that filters out the checksum warning
	customLogger := &filteringLogger{
		logger: logging.NewStandardLogger(os.Stderr),
		filterStrings: []string{
			"Response has no supported checksum",
			"Not validating response payload",
		},
	}

	// Configure the AWS SDK
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithEndpointResolverWithOptions(resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		config.WithRegion("us-east-1"), // Default region, can be overridden
		config.WithHTTPClient(httpClient),
		config.WithLogger(customLogger),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with custom options
	s3Options := []func(*s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = true // Use path-style addressing
		},
	}
	client := s3.NewFromConfig(cfg, s3Options...)

	return &Client{
		client:     client,
		bucketName: bucketName,
	}, nil
}

// UploadFile uploads a file to S3
func (c *Client) UploadFile(localPath, s3Path string, progressFunc func(uploaded, total int64)) error {
	// Open the file
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file info for size
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Create a wrapper for progress reporting
	progressReader := &ProgressReader{
		Reader: file,
		Total:  fileInfo.Size(),
		ProgressFunc: func(uploaded, total int64, speed float64, eta time.Duration) {
			if progressFunc != nil {
				progressFunc(uploaded, total)
			}
		},
	}

	// Determine content type
	contentType := "application/octet-stream"
	if ext := filepath.Ext(localPath); ext == ".json" {
		contentType = "application/json"
	} else if ext == ".zst" || ext == ".tar.zst" {
		contentType = "application/zstd"
	}

	// Upload the file with retries
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Reset file position and progress reader for each attempt
		if _, err := file.Seek(0, 0); err != nil {
			return fmt.Errorf("failed to seek file: %w", err)
		}
		progressReader.Uploaded = 0

		// Create a new context for each attempt
		ctx := context.Background()

		// Upload the file
		_, err = c.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(c.bucketName),
			Key:           aws.String(s3Path),
			Body:          progressReader,
			ContentLength: aws.Int64(fileInfo.Size()), // Explicitly set Content-Length
			ContentType:   aws.String(contentType),
		})

		if err == nil {
			return nil
		}

		// If this is not the last attempt, wait before retrying
		if attempt < maxRetries-1 {
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}

	return fmt.Errorf("failed to upload file after %d attempts: %w", maxRetries, err)
}

// FileExists checks if a file exists in S3
func (c *Client) FileExists(s3Path string) (bool, error) {
	_, err := c.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(s3Path),
	})

	if err != nil {
		// Check if the error is because the object doesn't exist
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NotFound" {
				return false, nil
			}
		}
		return false, fmt.Errorf("failed to check if file exists: %w", err)
	}

	return true, nil
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

	// Download the file with retries
	maxRetries := 3
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		// Create a new context for each attempt
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()

		// Get the object
		result, err := c.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(c.bucketName),
			Key:    aws.String(s3Path),
		})
		if err != nil {
			lastErr = err
			cancel()
			continue
		}

		// Copy the object to the file
		if _, err := io.Copy(file, result.Body); err != nil {
			result.Body.Close()
			lastErr = err
			cancel()
			continue
		}

		result.Body.Close()
		cancel()
		return nil
	}

	return fmt.Errorf("failed to download file after %d attempts: %w", maxRetries, lastErr)
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
		// List objects in the bucket
		listInput := &s3.ListObjectsV2Input{
			Bucket:            aws.String(c.bucketName),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		}

		result, err := c.client.ListObjectsV2(context.Background(), listInput)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		// Process the objects
		for _, obj := range result.Contents {
			objects = append(objects, S3Object{
				Key:          *obj.Key,
				LastModified: *obj.LastModified,
				Size:         *obj.Size,
			})
		}

		// Check if there are more objects to fetch
		if result.IsTruncated == nil || !*result.IsTruncated {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	return objects, nil
}

// DeleteObject deletes an object from the S3 bucket
func (c *Client) DeleteObject(key string) error {
	_, err := c.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}
	return nil
}

// DeleteObjects deletes multiple objects from the S3 bucket
func (c *Client) DeleteObjects(keys []string) error {
	// S3 DeleteObjects API can delete up to 1000 objects at a time
	const maxKeysPerRequest = 1000

	// Process in batches
	for i := 0; i < len(keys); i += maxKeysPerRequest {
		end := i + maxKeysPerRequest
		if end > len(keys) {
			end = len(keys)
		}

		batch := keys[i:end]
		objects := make([]types.ObjectIdentifier, len(batch))
		for j, key := range batch {
			objects[j] = types.ObjectIdentifier{
				Key: aws.String(key),
			}
		}

		_, err := c.client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
			Bucket: aws.String(c.bucketName),
			Delete: &types.Delete{
				Objects: objects,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}
	}

	return nil
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
			log.Printf("Warning: failed to write upload info to %s, trying %s instead", filePath, tempPath)
			if err := os.WriteFile(tempPath, data, 0644); err != nil {
				return fmt.Errorf("failed to write upload info file to fallback location: %w", err)
			}
			// Update the filePath for future reference
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
	// This avoids permission issues when the application doesn't have write access to the snapshot directory
	fileName := filepath.Base(filePath)
	return filepath.Join(os.TempDir(), fileName+".upload-info")
}

// UploadFileMultipart uploads a file to S3 using multipart upload
func (c *Client) UploadFileMultipart(localPath, s3Path string, progressFunc func(uploaded, total int64)) error {
	// Ensure the temporary directory exists and is writable
	if err := EnsureTempDirExists(); err != nil {
		// Fall back to regular upload if we can't use the temporary directory
		log.Printf("Warning: cannot use multipart upload due to temporary directory issues: %v. Falling back to regular upload.", err)
		return c.UploadFile(localPath, s3Path, progressFunc)
	}

	// Check if the file exists
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// If the file is small (< 5MB), use regular upload
	if fileInfo.Size() < 5*1024*1024 {
		return c.UploadFile(localPath, s3Path, progressFunc)
	}

	// Check if there's an existing upload info file
	uploadInfoPath := GetUploadInfoFilePath(localPath)
	var uploadInfo *MultipartUploadInfo
	var existingUpload bool

	if _, err := os.Stat(uploadInfoPath); err == nil {
		// Load existing upload info
		uploadInfo, err = LoadUploadInfoFromFile(uploadInfoPath)
		if err != nil {
			return fmt.Errorf("failed to load upload info: %w", err)
		}
		existingUpload = true
	} else {
		// Create new upload info
		uploadInfo = &MultipartUploadInfo{
			Key:       s3Path,
			ChunkSize: 5 * 1024 * 1024, // 5MB chunks
			TotalSize: fileInfo.Size(),
		}
	}

	// Open the file
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Determine content type
	contentType := "application/octet-stream"
	if ext := filepath.Ext(localPath); ext == ".json" {
		contentType = "application/json"
	} else if ext == ".zst" || ext == ".tar.zst" {
		contentType = "application/zstd"
	}

	// Create a new multipart upload if needed
	if !existingUpload {
		createResp, err := c.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
			Bucket:      aws.String(c.bucketName),
			Key:         aws.String(s3Path),
			ContentType: aws.String(contentType),
		})
		if err != nil {
			return fmt.Errorf("failed to create multipart upload: %w", err)
		}

		uploadInfo.UploadID = *createResp.UploadId
		uploadInfo.Parts = make([]types.CompletedPart, 0)

		// Save the upload info
		if err := SaveUploadInfoToFile(uploadInfo, uploadInfoPath); err != nil {
			return fmt.Errorf("failed to save upload info: %w", err)
		}
	}

	// Calculate the number of parts
	numParts := (fileInfo.Size() + uploadInfo.ChunkSize - 1) / uploadInfo.ChunkSize

	// Upload parts
	for partNumber := int64(1); partNumber <= numParts; partNumber++ {
		// Check if this part has already been uploaded
		partAlreadyUploaded := false
		for _, part := range uploadInfo.Parts {
			if int64(*part.PartNumber) == partNumber {
				partAlreadyUploaded = true
				break
			}
		}

		if partAlreadyUploaded {
			continue
		}

		// Calculate part size
		partSize := uploadInfo.ChunkSize
		if partNumber == numParts {
			partSize = fileInfo.Size() - (partNumber-1)*uploadInfo.ChunkSize
		}

		// Seek to the correct position in the file
		if _, err := file.Seek((partNumber-1)*uploadInfo.ChunkSize, 0); err != nil {
			return fmt.Errorf("failed to seek in file: %w", err)
		}

		// Create a limited reader for this part
		partReader := io.LimitReader(file, partSize)

		// Create a buffer to read the part
		buf := make([]byte, partSize)
		if _, err := io.ReadFull(partReader, buf); err != nil {
			return fmt.Errorf("failed to read part: %w", err)
		}

		// Upload the part
		uploadResp, err := c.client.UploadPart(context.Background(), &s3.UploadPartInput{
			Bucket:        aws.String(c.bucketName),
			Key:           aws.String(s3Path),
			PartNumber:    aws.Int32(int32(partNumber)),
			UploadId:      aws.String(uploadInfo.UploadID),
			Body:          bytes.NewReader(buf),
			ContentLength: aws.Int64(partSize), // Explicitly set Content-Length
		})
		if err != nil {
			return fmt.Errorf("failed to upload part %d: %w", partNumber, err)
		}

		// Add the part to the list
		uploadInfo.Parts = append(uploadInfo.Parts, types.CompletedPart{
			PartNumber: aws.Int32(int32(partNumber)),
			ETag:       uploadResp.ETag,
		})

		// Update uploaded size
		uploadInfo.UploadedSize += partSize

		// Call progress function
		if progressFunc != nil {
			progressFunc(uploadInfo.UploadedSize, uploadInfo.TotalSize)
		}

		// Save the upload info
		if err := SaveUploadInfoToFile(uploadInfo, uploadInfoPath); err != nil {
			return fmt.Errorf("failed to save upload info: %w", err)
		}
	}

	// Complete the multipart upload
	_, err = c.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(c.bucketName),
		Key:      aws.String(s3Path),
		UploadId: aws.String(uploadInfo.UploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: uploadInfo.Parts,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	// Delete the upload info file
	if err := os.Remove(uploadInfoPath); err != nil {
		// Just log the error, don't fail the upload
		log.Printf("Warning: failed to remove upload info file: %v", err)
	}

	return nil
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

// CleanupAbandonedUploads cleans up abandoned multipart uploads
func (c *Client) CleanupAbandonedUploads(olderThan time.Duration) error {
	// List multipart uploads
	listResp, err := c.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
		Bucket: aws.String(c.bucketName),
	})
	if err != nil {
		return fmt.Errorf("failed to list multipart uploads: %w", err)
	}

	// Get the current time
	now := time.Now()

	// Abort uploads that are older than the specified duration
	for _, upload := range listResp.Uploads {
		if upload.Initiated != nil && now.Sub(*upload.Initiated) > olderThan {
			if err := c.AbortMultipartUpload(*upload.Key, *upload.UploadId); err != nil {
				// Log the error but continue with other uploads
				log.Printf("Warning: failed to abort multipart upload for %s: %v", *upload.Key, err)
			}
		}
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
		log.Printf("Warning: failed to remove test file: %v", err)
	}

	return nil
}
