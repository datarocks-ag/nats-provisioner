package provisioner

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"nats-provisioner/internal/config"
)

// JetStreamManager is the subset of jetstream.JetStream used by the provisioner.
type JetStreamManager interface {
	CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error)
	UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error)
	Stream(ctx context.Context, name string) (jetstream.Stream, error)
	CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error)
}

// Provisioner orchestrates idempotent NATS JetStream resource provisioning.
type Provisioner struct {
	js  JetStreamManager
	cfg *config.Config
}

// New creates a new Provisioner.
func New(js JetStreamManager, cfg *config.Config) *Provisioner {
	return &Provisioner{
		js:  js,
		cfg: cfg,
	}
}

// Run executes the full provisioning sequence:
// For each stream: Stream -> Consumers
func (p *Provisioner) Run(ctx context.Context) error {
	slog.Info("Starting provisioning")

	for _, stream := range p.cfg.Streams {
		strategy := config.EffectiveStrategy(stream.Strategy, p.cfg.Strategy)
		if err := p.ensureStream(ctx, stream, strategy); err != nil {
			return fmt.Errorf("provisioning stream %q: %w", stream.Name, err)
		}

		// Consumers always use CreateOrUpdateConsumer (atomic, idempotent)
		for _, consumer := range stream.Consumers {
			if err := p.ensureConsumer(ctx, stream.Name, consumer); err != nil {
				return fmt.Errorf("provisioning consumer %q on stream %q: %w", consumer.Name, stream.Name, err)
			}
		}
	}

	slog.Info("Provisioning complete")
	return nil
}

// buildStreamConfig converts a config.Stream to a jetstream.StreamConfig.
func buildStreamConfig(s config.Stream) (jetstream.StreamConfig, error) {
	cfg := jetstream.StreamConfig{
		Name:     s.Name,
		Subjects: s.Subjects,
	}

	if s.Description != "" {
		cfg.Description = s.Description
	}

	switch s.Retention {
	case "limits", "":
		cfg.Retention = jetstream.LimitsPolicy
	case "interest":
		cfg.Retention = jetstream.InterestPolicy
	case "workqueue":
		cfg.Retention = jetstream.WorkQueuePolicy
	}

	switch s.Storage {
	case "file", "":
		cfg.Storage = jetstream.FileStorage
	case "memory":
		cfg.Storage = jetstream.MemoryStorage
	}

	switch s.Discard {
	case "old", "":
		cfg.Discard = jetstream.DiscardOld
	case "new":
		cfg.Discard = jetstream.DiscardNew
	}

	if s.MaxMsgs != nil {
		cfg.MaxMsgs = *s.MaxMsgs
	}
	if s.MaxBytes != nil {
		cfg.MaxBytes = *s.MaxBytes
	}
	if s.MaxMsgSize != nil {
		cfg.MaxMsgSize = *s.MaxMsgSize
	}
	if s.NumReplicas != nil {
		cfg.Replicas = *s.NumReplicas
	}

	if s.MaxAge != "" {
		d, err := config.ParseDuration(s.MaxAge)
		if err != nil {
			return cfg, fmt.Errorf("parsing max_age: %w", err)
		}
		cfg.MaxAge = d
	}

	if s.DuplicateWindow != "" {
		d, err := config.ParseDuration(s.DuplicateWindow)
		if err != nil {
			return cfg, fmt.Errorf("parsing duplicate_window: %w", err)
		}
		cfg.Duplicates = d
	}

	if s.AllowRollup != nil {
		cfg.AllowRollup = *s.AllowRollup
	}
	if s.DenyDelete != nil {
		cfg.DenyDelete = *s.DenyDelete
	}
	if s.DenyPurge != nil {
		cfg.DenyPurge = *s.DenyPurge
	}

	return cfg, nil
}

// buildConsumerConfig converts a config.Consumer to a jetstream.ConsumerConfig.
func buildConsumerConfig(c config.Consumer) (jetstream.ConsumerConfig, error) {
	cfg := jetstream.ConsumerConfig{
		Name: c.Name,
	}

	if c.Description != "" {
		cfg.Description = c.Description
	}
	if c.FilterSubject != "" {
		cfg.FilterSubject = c.FilterSubject
	}

	switch c.AckPolicy {
	case "none":
		cfg.AckPolicy = jetstream.AckNonePolicy
	case "all":
		cfg.AckPolicy = jetstream.AckAllPolicy
	case "explicit", "":
		cfg.AckPolicy = jetstream.AckExplicitPolicy
	}

	switch c.DeliverPolicy {
	case "all", "":
		cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	case "last":
		cfg.DeliverPolicy = jetstream.DeliverLastPolicy
	case "new":
		cfg.DeliverPolicy = jetstream.DeliverNewPolicy
	case "by_start_sequence":
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		if c.OptStartSeq != nil {
			cfg.OptStartSeq = *c.OptStartSeq
		}
	case "by_start_time":
		cfg.DeliverPolicy = jetstream.DeliverByStartTimePolicy
		if c.OptStartTime != "" {
			t, err := time.Parse(time.RFC3339, c.OptStartTime)
			if err != nil {
				return cfg, fmt.Errorf("parsing opt_start_time: %w", err)
			}
			cfg.OptStartTime = &t
		}
	}

	switch c.ReplayPolicy {
	case "instant", "":
		cfg.ReplayPolicy = jetstream.ReplayInstantPolicy
	case "original":
		cfg.ReplayPolicy = jetstream.ReplayOriginalPolicy
	}

	if c.AckWait != "" {
		d, err := config.ParseDuration(c.AckWait)
		if err != nil {
			return cfg, fmt.Errorf("parsing ack_wait: %w", err)
		}
		cfg.AckWait = d
	}

	if c.MaxDeliver != nil {
		cfg.MaxDeliver = *c.MaxDeliver
	}
	if c.MaxAckPending != nil {
		cfg.MaxAckPending = *c.MaxAckPending
	}

	if c.InactiveThreshold != "" {
		d, err := config.ParseDuration(c.InactiveThreshold)
		if err != nil {
			return cfg, fmt.Errorf("parsing inactive_threshold: %w", err)
		}
		cfg.InactiveThreshold = d
	}

	return cfg, nil
}
