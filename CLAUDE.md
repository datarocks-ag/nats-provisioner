# CLAUDE.md

## Project Overview

A Go CLI tool that idempotently provisions NATS JetStream resources (streams, consumers) from a YAML config file. Designed as a Docker Compose init container.

## Build & Run

```bash
go build -o nats-provisioner ./cmd/nats-provisioner
make build          # same via Makefile
make test           # unit tests
make test-integration  # integration tests (requires Docker)
make lint           # golangci-lint
```

## Optional Environment Variables

- `NATS_URL` (default: `nats://localhost:4222`)
- `NATS_USER` — NATS username (optional)
- `NATS_PASSWORD` — NATS password (optional)
- `NATS_TOKEN` — NATS auth token (optional)
- `NATS_CONFIG_PATH` (default: `./config.yaml`)
- `LOG_LEVEL` (default: `info`)

## Architecture

```
cmd/nats-provisioner/main.go          # CLI entrypoint, env vars, slog setup
internal/
  config/config.go                    # YAML config structs, loader, env var expansion, validation
  config/config_test.go               # Unit tests for config parsing
  client/client.go                    # NATS connection with retry
  provisioner/
    provisioner.go                    # Orchestrator + JetStreamManager interface + config builders
    streams.go                        # Idempotent stream create/update
    consumers.go                      # Idempotent consumer create/update
    provisioner_test.go               # Unit tests with mock JetStreamManager
    integration_test.go               # Integration tests with testcontainers-nats
```

## Dependencies

Go 1.25 module using:
- `github.com/nats-io/nats.go` — NATS client + JetStream SDK
- `gopkg.in/yaml.v3` — YAML config parsing
- `github.com/testcontainers/testcontainers-go` — integration tests
- `github.com/testcontainers/testcontainers-go/modules/nats` — NATS testcontainer

## Key Design Decisions

- **Order**: Streams → Consumers
- **Idempotency**: `Stream()` → ErrStreamNotFound? `CreateStream` : `UpdateStream`; `CreateOrUpdateConsumer` (single atomic call)
- **Immutable fields**: Storage, Retention — logged as warnings if mismatch
- **JetStreamManager interface**: Narrow 4-method interface for clean mocking
- **YAML naming**: snake_case fields matching NATS server config conventions
- **Duration parsing**: Supports Go durations + "Nd" day notation (e.g., "7d")
- **Structured logging**: `log/slog` with JSON output
- **Connection retry**: Exponential backoff (1s–30s, 15 retries, 5min timeout)
