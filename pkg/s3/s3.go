package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	ProgressFunc func(uploaded, total int64)
	LastUpdate   time.Time
}

// Read reads data from the underlying reader and reports progress
func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.Reader.Read(p)
	pr.Uploaded += int64(n)

	// Only update progress every 500ms to avoid too many log messages
	if time.Since(pr.LastUpdate) > 500*time.Millisecond {
		pr.ProgressFunc(pr.Uploaded, pr.Total)
		pr.LastUpdate = time.Now()
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

// UploadFile uploads a file to S3 with progress reporting
func (c *Client) UploadFile(localPath, s3Path string, progressFunc func(uploaded, total int64)) error {
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file info for content length
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Determine content type based on file extension
	contentType := "application/octet-stream"
	if ext := filepath.Ext(localPath); ext == ".json" {
		contentType = "application/json"
	} else if ext == ".zst" || ext == ".tar.zst" {
		contentType = "application/zstd"
	}

	// Create a progress reader
	progressReader := &ProgressReader{
		Reader:       file,
		Total:        fileInfo.Size(),
		ProgressFunc: progressFunc,
		LastUpdate:   time.Now(),
	}

	// Upload the file with retries
	maxRetries := 3
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		// Create a new reader for each attempt to ensure we start from the beginning
		if _, err := file.Seek(0, 0); err != nil {
			return fmt.Errorf("failed to reset file position: %w", err)
		}

		// Reset progress reader
		progressReader.Uploaded = 0

		// Create a new context for each attempt
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()

		// Upload the file
		_, err = c.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(c.bucketName),
			Key:           aws.String(s3Path),
			Body:          progressReader,
			ContentLength: aws.Int64(fileInfo.Size()),
			ContentType:   aws.String(contentType),
		})

		if err == nil {
			// Call progress one last time to ensure 100%
			progressFunc(fileInfo.Size(), fileInfo.Size())
			return nil
		}

		lastErr = err
	}

	return fmt.Errorf("failed to upload file after %d attempts: %w", maxRetries, lastErr)
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
