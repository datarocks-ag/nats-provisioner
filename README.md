# nats-provisioner

A Go CLI tool that idempotently provisions NATS JetStream resources from a YAML config file. Designed as a Docker Compose init container.

## Features

- Idempotent provisioning of JetStream streams and consumers
- YAML config with `${VAR}` environment variable expansion
- Exponential backoff retry for NATS connectivity
- Structured JSON logging via `log/slog`
- Duration support with "Nd" day notation (e.g., "7d")

## Quick Start

```bash
docker compose up
```

This starts NATS with JetStream and runs the provisioner with the example config.

## Environment Variables

| Variable | Required | Default |
|---|---|---|
| `NATS_URL` | no | `nats://localhost:4222` |
| `NATS_USER` | no | — |
| `NATS_PASSWORD` | no | — |
| `NATS_TOKEN` | no | — |
| `NATS_CONFIG_PATH` | no | `./config.yaml` |
| `LOG_LEVEL` | no | `info` |

## Config Example

See [config.example.yaml](config.example.yaml) for a full example.

```yaml
streams:
  - name: "ORDERS"
    subjects: ["orders.>"]
    retention: "limits"
    storage: "file"
    max_age: "7d"
    consumers:
      - name: "order-processor"
        filter_subject: "orders.created"
        ack_policy: "explicit"
        ack_wait: "30s"
```

## Container Image

```bash
docker pull ghcr.io/datarocks-ag/nats-provisioner:latest
```
