package provisioner

import (
	"context"
	"log/slog"

	"nats-provisioner/internal/config"
)

func (p *Provisioner) ensureConsumer(ctx context.Context, streamName string, consumer config.Consumer) error {
	cfg, err := buildConsumerConfig(consumer)
	if err != nil {
		return err
	}

	slog.Info("Ensuring consumer", "stream", streamName, "consumer", consumer.Name)
	_, err = p.js.CreateOrUpdateConsumer(ctx, streamName, cfg)
	return err
}
