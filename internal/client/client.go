package client

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	maxRetries   = 15
	initialDelay = 1 * time.Second
	maxDelay     = 30 * time.Second
	totalTimeout = 5 * time.Minute
)

// Connect establishes a NATS connection with exponential backoff retry
// and returns a JetStream context and the underlying connection.
func Connect(ctx context.Context, url, user, password, token string) (jetstream.JetStream, *nats.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	var opts []nats.Option

	if user != "" && password != "" {
		opts = append(opts, nats.UserInfo(user, password))
	} else if token != "" {
		opts = append(opts, nats.Token(token))
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
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

		if ctx.Err() != nil {
			return nil, nil, fmt.Errorf("connection timeout after %s: %w", totalTimeout, ctx.Err())
		}

		delay := time.Duration(float64(initialDelay) * math.Pow(2, float64(attempt)))
		if delay > maxDelay {
			delay = maxDelay
		}

		slog.Warn("NATS not ready, retrying",
			"attempt", attempt+1,
			"max_retries", maxRetries,
			"delay", delay,
			"error", err,
		)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("connection timeout: %w", ctx.Err())
		}
	}

	return nil, nil, fmt.Errorf("failed to connect after %d retries", maxRetries+1)
}
