# nats-provisioner

[![CI](https://github.com/datarocks-ag/nats-provisioner/actions/workflows/ci.yaml/badge.svg)](https://github.com/datarocks-ag/nats-provisioner/actions/workflows/ci.yaml)
![coverage](https://raw.githubusercontent.com/datarocks-ag/nats-provisioner/badges/.badges/develop/coverage.svg)

Declarative, idempotent provisioning for NATS JetStream. Describe the streams and consumers your
services need in one YAML file; the provisioner creates or updates them on the server every time
it runs, and then exits.

It is built to run **before your application starts**: as a Docker Compose service your app
`depends_on`, or as a Kubernetes Job or init container.

## Why

A service that uses JetStream needs its streams and consumers to exist before it publishes or
fetches. Usually that ends up in application startup code, in a script of `nats stream add`
commands, or in someone's shell history.

nats-provisioner replaces that with a file you can review in a pull request:

- **Idempotent**: run it on every deploy. Missing streams and consumers are created, existing
  ones are updated to match the file.
- **Additive**: it never deletes a stream or consumer that isn't in the config.
- **Careful with stream updates**: fields you leave out keep the server's value, and fields NATS
  can't change after creation are kept and reported with a warning instead of failing the run.
- **Environment-aware**: `${VAR}` references let one file serve several environments.
- **Small**: a single static binary in a `scratch` image, with structured JSON logs.

## Quick start

Two files next to each other. A config:

```yaml
# config.yaml
streams:
  - name: ORDERS
    subjects: ["orders.>"]
    max_age: 7d
```

And a Compose file that runs the provisioner once NATS is healthy:

```yaml
# docker-compose.yaml
services:
  nats:
    image: nats:2.12-alpine
    command: ["--jetstream"]
    healthcheck:
      test: ["CMD-SHELL", "echo 'PING' | nc localhost 4222 | grep -q PONG"]
      interval: 2s
      timeout: 5s
      retries: 10

  nats-provisioner:
    image: ghcr.io/datarocks-ag/nats-provisioner:1.1
    depends_on:
      nats:
        condition: service_healthy
    environment:
      NATS_URL: nats://nats:4222
      NATS_CONFIG_PATH: /config.yaml
    volumes:
      - ./config.yaml:/config.yaml:ro
```

```console
$ docker compose up nats-provisioner
...
{"level":"INFO","msg":"Connected to NATS","url":"nats://nats:4222"}
{"level":"INFO","msg":"Creating stream","stream":"ORDERS"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}
```

Run it again and the stream is updated to match the file instead of created. That's the whole
idea. The [getting started guide](docs/getting-started.md) takes this example further: a
consumer, publishing and reading a message, changing the stream, and starting your application
after the provisioner.

## Documentation

Read in this order if you're new; jump straight to a how-to if you know what you need.

| | |
|---|---|
| **[Getting started](docs/getting-started.md)** | A guided tour: from an empty NATS server to a stream with a consumer your application reads from. |
| **[How it works](docs/concepts.md)** | The model behind a run: the order of steps, how streams and consumers are updated, what is never deleted, `strategy`, durable consumers, connection retry. |
| **How-to guides** | Task-focused stories: |
| &nbsp;&nbsp;[Authenticate to a secured NATS server](docs/howto/authenticate-to-nats.md) | User and password, token or URL credentials, and which one wins. |
| &nbsp;&nbsp;[Run the provisioner as a Kubernetes Job](docs/howto/run-as-a-kubernetes-job.md) | A ConfigMap for the config, a Secret for credentials, and re-running on deploy. |
| &nbsp;&nbsp;[Use one config for several environments](docs/howto/use-one-config-for-several-environments.md) | `${VAR}` references and the traps of unset and empty variables. |
| &nbsp;&nbsp;[Limit how much a stream keeps](docs/howto/limit-stream-size-and-age.md) | `max_age`, `max_bytes`, `max_msgs`, `discard`, and removing a limit again. |
| &nbsp;&nbsp;[Protect a stream's messages from deletion and purge](docs/howto/protect-messages-from-deletion.md) | `deny_delete` and `deny_purge`, set when the stream is created. |
| &nbsp;&nbsp;[Deduplicate published messages](docs/howto/deduplicate-messages.md) | `duplicate_window` and the `Nats-Msg-Id` header. |
| &nbsp;&nbsp;[Replicate a stream across a cluster](docs/howto/replicate-a-stream.md) | `num_replicas` and listing several servers in `NATS_URL`. |
| &nbsp;&nbsp;[Set up a work queue](docs/howto/set-up-a-work-queue.md) | A `workqueue` stream and a pull consumer that several workers share. |
| &nbsp;&nbsp;[Replay messages from a sequence or a point in time](docs/howto/replay-messages-from-a-point.md) | `by_start_sequence`, `by_start_time` and `replay_policy`. |
| **[Deployment](docs/deployment.md)** | Image tags, Docker Compose, Kubernetes, binaries, environment variables, authentication, exit codes. |
| **[Configuration reference](docs/configuration.md)** | Every field of the YAML file, durations, `${VAR}` expansion, validation. |
| **[FAQ](docs/faq.md)** | Questions and errors people run into once the provisioner runs for real. |
| **[Changelog](CHANGELOG.md)** | What changed in each release. |

A complete example config lives in [config.example.yaml](config.example.yaml), and
[docker-compose.yaml](docker-compose.yaml) runs it against a local NATS server.

## Development

```bash
make build            # Build binary
make test             # Run unit tests
make test-integration # Run integration tests (requires Docker)
make lint             # Run golangci-lint
make vet              # Run go vet
make docker           # Build Docker image
```
