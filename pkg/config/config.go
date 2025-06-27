package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds the application configuration
type Config struct {
	WatchDir                string        `yaml:"watch_dir" json:"watch_dir"`
	S3Endpoint              string        `yaml:"s3_endpoint" json:"s3_endpoint"`
	S3PublicEndpoint        string        `yaml:"s3_public_endpoint" json:"s3_public_endpoint"`
	S3Bucket                string        `yaml:"s3_bucket" json:"s3_bucket"`
	S3Namespace             string        `yaml:"s3_namespace" json:"s3_namespace"`
	S3AccessKey             string        `yaml:"s3_access_key" json:"s3_access_key"`
	S3SecretKey             string        `yaml:"s3_secret_key" json:"s3_secret_key"`
	S3UploadConcurrency     int           `yaml:"s3_upload_concurrency" json:"s3_upload_concurrency"`
	S3ChunkSizeMB           int           `yaml:"s3_chunk_size_mb" json:"s3_chunk_size_mb"`
	SolanaVersion           string        `yaml:"solana_version" json:"solana_version"`
	SolanaRpcUrl            string        `yaml:"solana_rpc_url" json:"solana_rpc_url"`
	FullSnapshotGap         int           `yaml:"full_snapshot_gap" json:"full_snapshot_gap"`
	IncrementalGap          int           `yaml:"incremental_gap" json:"incremental_gap"`
	EnableIncrementalSnap   bool          `yaml:"enable_incremental_snap" json:"enable_incremental_snap"`
	EnableRetention         bool          `yaml:"enable_retention" json:"enable_retention"`
	RetentionPeriodHours    int           `yaml:"retention_period_hours" json:"retention_period_hours"`
	IncrementalRetainCount  int           `yaml:"incremental_retain_count" json:"incremental_retain_count"`
	IncrementalRetainHours  int           `yaml:"incremental_retain_hours" json:"incremental_retain_hours"`
	FullSnapshotRetainCount int           `yaml:"full_snapshot_retain_count" json:"full_snapshot_retain_count"`
	FullSnapshotRetainHours int           `yaml:"full_snapshot_retain_hours" json:"full_snapshot_retain_hours"`
	LogLevel                string        `yaml:"log_level" json:"log_level"`
	Hostname                string        `yaml:"hostname" json:"hostname"`
	MetadataCleanupInterval time.Duration `yaml:"metadata_cleanup_interval" env:"METADATA_CLEANUP_INTERVAL"`
	EnableMetadataCleanup   bool          `yaml:"enable_metadata_cleanup" env:"ENABLE_METADATA_CLEANUP"`
}

// DefaultConfig returns a configuration with default values
func DefaultConfig() *Config {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	return &Config{
		WatchDir:                "/snapshots",
		SolanaRpcUrl:            "http://localhost:8899",
		S3PublicEndpoint:        "", // Default is empty, will fall back to S3Endpoint
		S3UploadConcurrency:     5,  // These are not used currently, but kept for future
		S3ChunkSizeMB:           32, // These are not used currently, but kept for future
		FullSnapshotGap:         25000,
		IncrementalGap:          500,
		EnableIncrementalSnap:   true,
		EnableRetention:         false,
		RetentionPeriodHours:    168,     // 7 days by default (legacy setting)
		IncrementalRetainCount:  5,       // Keep 5 most recent incremental snapshots
		IncrementalRetainHours:  168,     // Keep incremental snapshots for 7 days
		FullSnapshotRetainCount: 3,       // Keep 3 most recent full snapshots
		FullSnapshotRetainHours: 168 * 4, // Keep full snapshots for 28 days
		LogLevel:                "info",
		Hostname:                hostname,
		MetadataCleanupInterval: time.Hour * 24, // Check daily by default
		EnableMetadataCleanup:   true,           // Enabled by default
	}
}

// LoadFromFile loads configuration from a YAML file
func LoadFromFile(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return cfg, nil
}

// LoadFromEnv loads configuration from environment variables
func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	if val := os.Getenv("WATCH_DIR"); val != "" {
		cfg.WatchDir = val
	}
	if val := os.Getenv("S3_ENDPOINT"); val != "" {
		cfg.S3Endpoint = val
	}
	if val := os.Getenv("S3_PUBLIC_ENDPOINT"); val != "" {
		cfg.S3PublicEndpoint = val
	}
	if val := os.Getenv("S3_BUCKET"); val != "" {
		cfg.S3Bucket = val
	}
	if val := os.Getenv("S3_NAMESPACE"); val != "" {
		cfg.S3Namespace = val
	}
	if val := os.Getenv("S3_ACCESS_KEY"); val != "" {
		cfg.S3AccessKey = val
	}
	if val := os.Getenv("S3_SECRET_KEY"); val != "" {
		cfg.S3SecretKey = val
	}
	if val := os.Getenv("S3_UPLOAD_CONCURRENCY"); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			cfg.S3UploadConcurrency = intVal
		}
	}
	if val := os.Getenv("S3_CHUNK_SIZE_MB"); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			cfg.S3ChunkSizeMB = intVal
		}
	}
	if val := os.Getenv("SOLANA_VERSION"); val != "" {
		cfg.SolanaVersion = val
	}
	if val := os.Getenv("SOLANA_RPC_URL"); val != "" {
		cfg.SolanaRpcUrl = val
	}
	if val := os.Getenv("FULL_SNAPSHOT_GAP"); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			cfg.FullSnapshotGap = intVal
		}
	}
	if val := os.Getenv("INCREMENTAL_GAP"); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			cfg.IncrementalGap = intVal
		}
	}
	if val := os.Getenv("ENABLE_INCREMENTAL_SNAP"); val != "" {
		cfg.EnableIncrementalSnap = val == "true" || val == "1" || val == "yes"
	}
	if val := os.Getenv("ENABLE_RETENTION"); val != "" {
		cfg.EnableRetention = val == "true" || val == "1" || val == "yes"
	}
	if val := os.Getenv("RETENTION_PERIOD_HOURS"); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			cfg.RetentionPeriodHours = intVal
		}
	}
	if val := os.Getenv("INCREMENTAL_RETAIN_COUNT"); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			cfg.IncrementalRetainCount = intVal
		}
	}
	if val := os.Getenv("INCREMENTAL_RETAIN_HOURS"); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			cfg.IncrementalRetainHours = intVal
		}
	}
	if val := os.Getenv("FULL_SNAPSHOT_RETAIN_COUNT"); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			cfg.FullSnapshotRetainCount = intVal
		}
	}
	if val := os.Getenv("FULL_SNAPSHOT_RETAIN_HOURS"); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			cfg.FullSnapshotRetainHours = intVal
		}
	}
	if val := os.Getenv("LOG_LEVEL"); val != "" {
		cfg.LogLevel = val
	}
	if val := os.Getenv("HOSTNAME"); val != "" {
		cfg.Hostname = val
	}

	return cfg
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.S3Endpoint == "" {
		return fmt.Errorf("S3_ENDPOINT is required")
	}
	if c.S3Bucket == "" {
		return fmt.Errorf("S3_BUCKET is required")
	}
	if c.S3AccessKey == "" {
		return fmt.Errorf("S3_ACCESS_KEY is required")
	}
	if c.S3SecretKey == "" {
		return fmt.Errorf("S3_SECRET_KEY is required")
	}
	return nil
}
