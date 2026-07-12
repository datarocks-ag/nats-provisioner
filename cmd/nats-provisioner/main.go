package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"nats-provisioner/internal/client"
	"nats-provisioner/internal/config"
	"nats-provisioner/internal/provisioner"
)

var version = "dev"

func main() {
	setupLogging()
	slog.Info("Starting nats-provisioner", "version", version)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	natsURL := envOrDefault("NATS_URL", "nats://localhost:4222")
	natsUser := os.Getenv("NATS_USER")
	natsPassword := os.Getenv("NATS_PASSWORD")
	natsToken := os.Getenv("NATS_TOKEN")
	configPath := envOrDefault("NATS_CONFIG_PATH", "./config.yaml")

	slog.Info("Loading configuration", "path", configPath)
	cfg, err := config.Load(configPath)
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}
	slog.Info("Configuration loaded", "streams", len(cfg.Streams))

	slog.Info("Connecting to NATS", "url", client.RedactURL(natsURL))
	js, nc, err := client.Connect(ctx, natsURL, natsUser, natsPassword, natsToken)
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	p := provisioner.New(js, cfg)
	if err := p.Run(ctx); err != nil {
		slog.Error("Provisioning failed", "error", err)
		os.Exit(1)
	}

	slog.Info("nats-provisioner finished successfully")
}

func setupLogging() {
	level := slog.LevelInfo
	switch envOrDefault("LOG_LEVEL", "info") {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
}

func envOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
