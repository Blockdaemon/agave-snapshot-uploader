package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Blockdaemon/agave-snapshot-uploader/pkg/config"
	"github.com/Blockdaemon/agave-snapshot-uploader/pkg/monitor"
	"github.com/Blockdaemon/agave-snapshot-uploader/pkg/s3"
	"github.com/Blockdaemon/agave-snapshot-uploader/pkg/solana"
)

func main() {
	// Parse command line flags
	configFile := flag.String("config", "config.yaml", "Path to config file (default: config.yaml)")
	flag.Parse()

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Load configuration
	var cfg *config.Config
	var err error

	// Load from config file
	cfg, err = config.LoadFromFile(*configFile)
	if err != nil {
		logger.Warn("Failed to load config file", "path", *configFile, "error", err)
		logger.Info("Falling back to environment variables")
		cfg = config.LoadFromEnv()
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		logger.Error("Invalid configuration", "error", err)
		os.Exit(1)
	}

	// Set log level
	var level slog.Level
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	// Fetch Solana version if RPC URL is provided and no version is specified
	if cfg.SolanaRpcUrl != "" && cfg.SolanaVersion == "" {
		logger.Info("Fetching Solana version from RPC", "url", cfg.SolanaRpcUrl)
		version, err := solana.GetVersion(cfg.SolanaRpcUrl)
		if err != nil {
			logger.Warn("Failed to fetch Solana version", "error", err)
		} else {
			cfg.SolanaVersion = version.SolanaCore
			logger.Info("Fetched Solana version",
				"version", version.SolanaCore,
				"feature_set", version.FeatureSet,
				"fetched_at", version.FetchedAt)
		}
	}

	// Create S3 client
	s3Client, err := s3.NewClient(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3Namespace, cfg.S3AccessKey, cfg.S3SecretKey)
	if err != nil {
		logger.Error("Failed to create S3 client", "error", err)
		os.Exit(1)
	}

	// Set the configured logger
	s3Client.SetLogger(logger)

	// Set default public endpoint if not specified
	if cfg.S3PublicEndpoint == "" {
		cfg.S3PublicEndpoint = "https://solana-snapshot.blockdaemon.com"
		logger.Info("Using default public endpoint", "endpoint", cfg.S3PublicEndpoint)
	}

	// Create monitor
	mon, err := monitor.New(cfg, s3Client, logger)
	if err != nil {
		logger.Error("Failed to create monitor", "error", err)
		os.Exit(1)
	}

	// Create context that can be canceled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the monitor
	if err := mon.Start(ctx); err != nil {
		logger.Error("Failed to start monitor", "error", err)
		os.Exit(1)
	}

	logger.Info("Snapshot monitor started", "watch_dir", cfg.WatchDir)

	// Handle signals for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Wait for signal
	sig := <-sigCh
	logger.Info("Received signal, shutting down", "signal", sig)

	// Stop the monitor
	cancel()
	if err := mon.Stop(); err != nil {
		logger.Error("Failed to stop monitor", "error", err)
	}

	logger.Info("Snapshot monitor stopped")
}
