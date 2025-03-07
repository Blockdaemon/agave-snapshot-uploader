package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Config holds the application configuration
type Config struct {
	WatchDir        string `yaml:"watch_dir" json:"watch_dir"`
	S3Endpoint      string `yaml:"s3_endpoint" json:"s3_endpoint"`
	S3Bucket        string `yaml:"s3_bucket" json:"s3_bucket"`
	S3AccessKey     string `yaml:"s3_access_key" json:"s3_access_key"`
	S3SecretKey     string `yaml:"s3_secret_key" json:"s3_secret_key"`
	SolanaVersion   string `yaml:"solana_version" json:"solana_version"`
	FullSnapshotGap int    `yaml:"full_snapshot_gap" json:"full_snapshot_gap"`
	IncrementalGap  int    `yaml:"incremental_gap" json:"incremental_gap"`
	LogLevel        string `yaml:"log_level" json:"log_level"`
}

// DefaultConfig returns a configuration with default values
func DefaultConfig() *Config {
	return &Config{
		WatchDir:        "/snapshots",
		FullSnapshotGap: 25000,
		IncrementalGap:  500,
		LogLevel:        "info",
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
	if val := os.Getenv("S3_BUCKET"); val != "" {
		cfg.S3Bucket = val
	}
	if val := os.Getenv("S3_ACCESS_KEY"); val != "" {
		cfg.S3AccessKey = val
	}
	if val := os.Getenv("S3_SECRET_KEY"); val != "" {
		cfg.S3SecretKey = val
	}
	if val := os.Getenv("SOLANA_VERSION"); val != "" {
		cfg.SolanaVersion = val
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
	if val := os.Getenv("LOG_LEVEL"); val != "" {
		cfg.LogLevel = val
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
