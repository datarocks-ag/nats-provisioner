# nats-provisioner

[![CI](https://github.com/datarocks-ag/nats-provisioner/actions/workflows/ci.yaml/badge.svg)](https://github.com/datarocks-ag/nats-provisioner/actions/workflows/ci.yaml)
![coverage](https://raw.githubusercontent.com/datarocks-ag/nats-provisioner/badges/.badges/develop/coverage.svg)

A Go CLI tool that idempotently provisions NATS JetStream resources from a YAML config file. Designed to run as a one-shot container — either as a Docker Compose init service (via `service_completed_successfully`) or as a Kubernetes `Job` / init container.

## Features

- Idempotent provisioning of JetStream streams and consumers
- YAML config with `${VAR}` environment variable expansion
- Configurable strategy: `update` (default) or `create` (skip existing)
- Exponential backoff retry for NATS connectivity
- Structured JSON logging via `log/slog`
- Duration support with "Nd" day notation (e.g., `7d`)
- Immutable field mismatch warnings (storage, retention, deny_delete, deny_purge)

## Quick Start

```bash
docker compose up
```

This starts NATS with JetStream and runs the provisioner with the example config.

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `NATS_URL` | no | `nats://localhost:4222` | NATS server URL |
| `NATS_USER` | no | — | Username for auth |
| `NATS_PASSWORD` | no | — | Password for auth |
| `NATS_TOKEN` | no | — | Token for auth |
| `NATS_CONFIG_PATH` | no | `./config.yaml` | Path to YAML config |
| `LOG_LEVEL` | no | `info` | Log level (debug/info/warn/error) |

All auth variables are optional (dev environments often have no auth).

## Strategy

Control whether existing streams are updated or skipped using the `strategy` field:

- `update` (default) — create streams if missing, update if they already exist
- `create` — create streams if missing, skip if they already exist

Strategy can be set globally or per stream. Per-stream strategy overrides the global setting.

```yaml
strategy: "create"              # global: skip existing streams

streams:
  - name: "ORDERS"
    strategy: "update"          # override: always reconcile this stream
    subjects: ["orders.>"]
```

**Note:** Consumers always use `CreateOrUpdateConsumer` which is an atomic, idempotent operation — strategy does not apply to consumers.

## Environment Variable Expansion

String values support `${VAR}` syntax. If the variable is set in the environment, it is replaced; if unset, the placeholder is preserved as-is.

```yaml
name: "${STREAM_NAME}"    # replaced with env var value at load time
```

Only the bare `${VAR}` form is recognized. Default-value syntax (`${VAR:-default}`) and dollar-escaping (`$$`) are not supported. Substitution applies to string fields only — numeric and boolean fields cannot be set via env vars.

## Provisioning Order

For each stream:

1. **Stream** — looked up by name; created if not found, updated if it exists
2. **Consumers** — `CreateOrUpdateConsumer` (atomic, idempotent)

## Duration Format

Duration fields (`max_age`, `ack_wait`, `duplicate_window`, `inactive_threshold`) support:

- Standard Go duration strings: `30s`, `5m`, `2h`, `1h30m`
- Day notation: `7d`, `30d` (converted to hours internally)

## Immutable Fields

Some NATS stream fields cannot be changed after creation. If a mismatch is detected, the provisioner logs a warning but does not fail:

- `storage` (file/memory)
- `retention` (limits/interest/workqueue)
- `deny_delete`
- `deny_purge`

## Config Example

See [config.example.yaml](config.example.yaml) for a full example.

```yaml
streams:
  - name: "ORDERS"
    subjects: ["orders.>"]
    retention: "limits"          # limits | interest | workqueue
    storage: "file"              # file | memory
    max_age: "7d"
    discard: "old"               # old | new
    consumers:
      - name: "order-processor"
        filter_subject: "orders.created"
        ack_policy: "explicit"   # none | all | explicit
        ack_wait: "30s"
        deliver_policy: "all"    # all | last | new | by_start_sequence | by_start_time
        replay_policy: "instant" # instant | original
        max_deliver: 5
```

## Connection Retry

On startup, the tool retries connecting to NATS with exponential backoff (1s initial, 30s cap, 15 retries, 5min total timeout). This handles Docker Compose startup ordering without requiring `wait-for-it` scripts.

## Development

```bash
make build            # Build binary
make test             # Run unit tests
make test-integration # Run integration tests (requires Docker)
make lint             # Run golangci-lint
make vet              # Run go vet
make docker           # Build Docker image
```

## Docker Compose Usage

```yaml
services:
  nats:
    image: nats:2.12-alpine
    command: ["--jetstream"]
    ports: ["4222:4222", "8222:8222"]
    volumes: [natsdata:/data]
    healthcheck:
      test: ["CMD-SHELL", "echo 'PING' | nc localhost 4222 | grep -q PONG"]
      interval: 2s
      timeout: 5s
      retries: 10

  nats-provisioner:
    image: ghcr.io/datarocks-ag/nats-provisioner:latest
    depends_on:
      nats: { condition: service_healthy }
    environment:
      NATS_URL: nats://nats:4222
      NATS_CONFIG_PATH: /config.yaml
    volumes:
      - ./config.example.yaml:/config.yaml:ro

  app:
    image: your-app
    depends_on:
      nats-provisioner:
        condition: service_completed_successfully

volumes:
  natsdata:
```

## Container Image

```bash
docker pull ghcr.io/datarocks-ag/nats-provisioner:latest
```
