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
	_, err = p.js.CreateOrUpdateConsumer(ctx, streamName, cfg)
	return err
}
