package s3

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// Client represents an S3 client
type Client struct {
	client     *s3.Client
	bucketName string
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

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithEndpointResolverWithOptions(resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		config.WithRegion("us-east-1"), // Default region, can be overridden
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	return &Client{
		client:     client,
		bucketName: bucketName,
	}, nil
}

// UploadFile uploads a file to S3
func (c *Client) UploadFile(localPath, s3Path string) error {
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

	// Upload the file with retries
	maxRetries := 3
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		_, err = c.client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket:        aws.String(c.bucketName),
			Key:           aws.String(s3Path),
			Body:          file,
			ContentLength: aws.Int64(fileInfo.Size()),
			ContentType:   aws.String(contentType),
		})

		if err == nil {
			return nil
		}

		lastErr = err
		// Reset file position for retry
		if _, seekErr := file.Seek(0, 0); seekErr != nil {
			return fmt.Errorf("failed to reset file position: %w", seekErr)
		}
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
