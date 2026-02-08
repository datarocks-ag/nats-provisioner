package provisioner

import (
	"context"
	"errors"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"

	"nats-provisioner/internal/config"
)

func (p *Provisioner) ensureStream(ctx context.Context, stream config.Stream) error {
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

	// Stream exists — check immutable fields and log warnings
	info := existing.CachedInfo()
	if info.Config.Storage != cfg.Storage {
		slog.Warn("Stream storage type mismatch (immutable, cannot be changed)",
			"stream", stream.Name,
			"current", info.Config.Storage,
			"desired", cfg.Storage,
		)
	}
	if info.Config.Retention != cfg.Retention {
		slog.Warn("Stream retention policy mismatch (immutable, cannot be changed)",
			"stream", stream.Name,
			"current", info.Config.Retention,
			"desired", cfg.Retention,
		)
	}

	slog.Info("Updating stream", "stream", stream.Name)
	_, err = p.js.UpdateStream(ctx, cfg)
	return err
}
