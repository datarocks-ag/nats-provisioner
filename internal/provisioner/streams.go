package provisioner

import (
	"context"
	"errors"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"

	"nats-provisioner/internal/config"
)

func (p *Provisioner) ensureStream(ctx context.Context, stream config.Stream, strategy string) error {
	desired, err := buildStreamConfig(stream)
	if err != nil {
		return err
	}

	existing, err := p.js.Stream(ctx, stream.Name)
	if err != nil {
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return err
		}

		slog.Info("Creating stream", "stream", stream.Name)
		_, err = p.js.CreateStream(ctx, desired)
		return err
	}

	if strategy == "create" {
		slog.Info("Skipping existing stream (strategy=create)", "stream", stream.Name)
		return nil
	}

	cfg := mergeStreamUpdate(existing.CachedInfo().Config, desired, stream)

	slog.Info("Updating stream", "stream", stream.Name)
	_, err = p.js.UpdateStream(ctx, cfg)
	return err
}

// mergeStreamUpdate returns the StreamConfig to send to UpdateStream.
//
// It starts from the broker's current state and overlays only fields the
// user explicitly set in YAML, so that omitted fields are not silently
// reset to their Go zero values. Immutable fields (storage, retention,
// deny_delete, deny_purge) are never overwritten — if the user explicitly
// requested a different value, a warning is logged.
func mergeStreamUpdate(existing, desired jetstream.StreamConfig, s config.Stream) jetstream.StreamConfig {
	cfg := existing
	cfg.Name = desired.Name
	cfg.Subjects = desired.Subjects

	if s.Description != "" {
		cfg.Description = desired.Description
	}

	if s.Storage != "" && existing.Storage != desired.Storage {
		slog.Warn("Stream storage type mismatch (immutable, cannot be changed)",
			"stream", s.Name,
			"current", existing.Storage,
			"desired", desired.Storage,
		)
	}
	if s.Retention != "" && existing.Retention != desired.Retention {
		slog.Warn("Stream retention policy mismatch (immutable, cannot be changed)",
			"stream", s.Name,
			"current", existing.Retention,
			"desired", desired.Retention,
		)
	}
	if s.DenyDelete != nil && existing.DenyDelete != desired.DenyDelete {
		slog.Warn("Stream deny_delete mismatch (immutable, cannot be changed)",
			"stream", s.Name,
			"current", existing.DenyDelete,
			"desired", desired.DenyDelete,
		)
	}
	if s.DenyPurge != nil && existing.DenyPurge != desired.DenyPurge {
		slog.Warn("Stream deny_purge mismatch (immutable, cannot be changed)",
			"stream", s.Name,
			"current", existing.DenyPurge,
			"desired", desired.DenyPurge,
		)
	}

	if s.Discard != "" {
		cfg.Discard = desired.Discard
	}
	if s.MaxMsgs != nil {
		cfg.MaxMsgs = desired.MaxMsgs
	}
	if s.MaxBytes != nil {
		cfg.MaxBytes = desired.MaxBytes
	}
	if s.MaxMsgSize != nil {
		cfg.MaxMsgSize = desired.MaxMsgSize
	}
	if s.NumReplicas != nil {
		cfg.Replicas = desired.Replicas
	}
	if s.MaxAge != "" {
		cfg.MaxAge = desired.MaxAge
	}
	if s.DuplicateWindow != "" {
		cfg.Duplicates = desired.Duplicates
	}
	if s.AllowRollup != nil {
		cfg.AllowRollup = desired.AllowRollup
	}

	return cfg
}
