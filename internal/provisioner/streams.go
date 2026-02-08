package provisioner

import (
	"context"
	"errors"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"

	"nats-provisioner/internal/config"
)

func (p *Provisioner) ensureStream(ctx context.Context, stream config.Stream, strategy string) error {
	cfg, err := buildStreamConfig(stream)
	if err != nil {
		return err
	}

	existing, err := p.js.Stream(ctx, stream.Name)
	if err != nil {
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return err
		}

		slog.Info("Creating stream", "stream", stream.Name)
		_, err = p.js.CreateStream(ctx, cfg)
		return err
	}

	if strategy == "create" {
		slog.Info("Skipping existing stream (strategy=create)", "stream", stream.Name)
		return nil
	}

	// Stream exists — check immutable fields, log warnings, and carry forward existing values
	info := existing.CachedInfo()
	if info.Config.Storage != cfg.Storage {
		slog.Warn("Stream storage type mismatch (immutable, cannot be changed)",
			"stream", stream.Name,
			"current", info.Config.Storage,
			"desired", cfg.Storage,
		)
	}
	cfg.Storage = info.Config.Storage

	if info.Config.Retention != cfg.Retention {
		slog.Warn("Stream retention policy mismatch (immutable, cannot be changed)",
			"stream", stream.Name,
			"current", info.Config.Retention,
			"desired", cfg.Retention,
		)
	}
	cfg.Retention = info.Config.Retention

	if stream.DenyDelete != nil && info.Config.DenyDelete != cfg.DenyDelete {
		slog.Warn("Stream deny_delete mismatch (immutable, cannot be changed)",
			"stream", stream.Name,
			"current", info.Config.DenyDelete,
			"desired", cfg.DenyDelete,
		)
	}
	cfg.DenyDelete = info.Config.DenyDelete

	if stream.DenyPurge != nil && info.Config.DenyPurge != cfg.DenyPurge {
		slog.Warn("Stream deny_purge mismatch (immutable, cannot be changed)",
			"stream", stream.Name,
			"current", info.Config.DenyPurge,
			"desired", cfg.DenyPurge,
		)
	}
	cfg.DenyPurge = info.Config.DenyPurge

	slog.Info("Updating stream", "stream", stream.Name)
	_, err = p.js.UpdateStream(ctx, cfg)
	return err
}
