package provisioner

import (
	"context"
	"log/slog"

	"nats-provisioner/internal/config"
)

// ensureConsumer always uses CreateOrUpdateConsumer which is atomic and idempotent.
// Strategy is not applicable — NATS consumers are always reconciled in a single call.
func (p *Provisioner) ensureConsumer(ctx context.Context, streamName string, consumer config.Consumer) error {
	cfg, err := buildConsumerConfig(consumer)
	if err != nil {
		return err
	}

	slog.Info("Ensuring consumer", "stream", streamName, "consumer", consumer.Name)
	created, err := p.js.CreateOrUpdateConsumer(ctx, streamName, cfg)
	if err != nil {
		return err
	}

	// Turning an ephemeral consumer (as created by release 1.1.0) into a durable
	// one keeps the server's 5s ephemeral inactive threshold on that first update;
	// a second update applies the requested threshold.
	if created.CachedInfo().Config.InactiveThreshold != cfg.InactiveThreshold {
		_, err = p.js.CreateOrUpdateConsumer(ctx, streamName, cfg)
	}
	return err
}
