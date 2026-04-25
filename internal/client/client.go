package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	maxAttempts  = 16
	initialDelay = 1 * time.Second
	maxDelay     = 30 * time.Second
	totalTimeout = 5 * time.Minute
)

// Connect establishes a NATS connection with exponential backoff retry
// and returns a JetStream context and the underlying connection.
func Connect(parentCtx context.Context, url, user, password, token string) (jetstream.JetStream, *nats.Conn, error) {
	ctx, cancel := context.WithTimeout(parentCtx, totalTimeout)
	defer cancel()

	var opts []nats.Option

	if user != "" && password != "" {
		opts = append(opts, nats.UserInfo(user, password))
	} else if token != "" {
		opts = append(opts, nats.Token(token))
	}

	contextErr := func() error {
		if err := parentCtx.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				return fmt.Errorf("connection canceled: %w", err)
			}
			return fmt.Errorf("connection deadline exceeded: %w", err)
		}
		return fmt.Errorf("connection timeout after %s: %w", totalTimeout, ctx.Err())
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		nc, err := nats.Connect(url, opts...)
		if err == nil {
			js, jsErr := jetstream.New(nc)
			if jsErr != nil {
				nc.Close()
				return nil, nil, fmt.Errorf("creating JetStream context: %w", jsErr)
			}

			slog.Info("Connected to NATS", "url", url)
			return js, nc, nil
		}
		lastErr = err

		if ctx.Err() != nil {
			return nil, nil, contextErr()
		}

		if attempt == maxAttempts {
			break
		}

		delay := time.Duration(float64(initialDelay) * math.Pow(2, float64(attempt-1)))
		if delay > maxDelay {
			delay = maxDelay
		}

		slog.Warn("NATS not ready, retrying",
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"delay", delay,
			"error", err,
		)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, nil, contextErr()
		}
	}

	return nil, nil, fmt.Errorf("failed to connect after %d attempts: %w", maxAttempts, lastErr)
}
