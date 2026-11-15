package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
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

// RedactURL returns a NATS URL (or comma-separated list of URLs) with any
// embedded userinfo (user:password@host) replaced by "redacted", so credentials
// are never written to logs. Everything between the scheme and the last '@' of
// each entry is masked, so unencoded '@', '/', '?' or '#' in a password cannot
// leak.
func RedactURL(raw string) string {
	parts := strings.Split(raw, ",")
	for i, p := range parts {
		parts[i] = redactSingleURL(strings.TrimSpace(p))
	}
	return strings.Join(parts, ",")
}

// redactSingleURL does not use url.Parse: it ends the userinfo at the first
// '/', '?' or '#', which leaves the rest of an unencoded password in the path,
// query or fragment. A NATS URL has no path, so its last '@' always ends the
// credentials.
func redactSingleURL(s string) string {
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return s
	}
	start := 0
	if i := strings.Index(s, "://"); i >= 0 && i < at && isScheme(s[:i]) {
		start = i + len("://")
	}
	return s[:start] + "redacted" + s[at:]
}

// isScheme reports whether s is a URL scheme as defined in RFC 3986, section 3.1.
func isScheme(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		case i > 0 && (r >= '0' && r <= '9' || r == '+' || r == '-' || r == '.'):
		default:
			return false
		}
	}
	return true
}

// redactError returns err's message with any occurrence of the connection URL(s)
// (including embedded credentials) or the raw password masked. NATS dial errors
// typically embed a single attempted server URL rather than the full
// comma-separated list, so each entry is redacted individually.
func redactError(err error, rawURL, password string) string {
	msg := err.Error()
	for _, entry := range strings.Split(rawURL, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		msg = strings.ReplaceAll(msg, entry, redactSingleURL(entry))
	}
	if password != "" {
		msg = strings.ReplaceAll(msg, password, "redacted")
	}
	return msg
}

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

			slog.Info("Connected to NATS", "url", RedactURL(url))
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
			"error", redactError(err, url, password),
		)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, nil, contextErr()
		}
	}

	return nil, nil, fmt.Errorf("failed to connect after %d attempts: %s", maxAttempts, redactError(lastErr, url, password))
}
