# Deployment

The provisioner is a short-lived process. It reads the config, connects to NATS, makes the
JetStream streams and consumers match the config, and exits: `0` on success, `1` on any error.
Run it wherever your application is deployed, as the step that has to succeed before the
application starts.

## Image tags

Images are published to `ghcr.io/datarocks-ag/nats-provisioner` for `linux/amd64` and
`linux/arm64`. They are built only when a release is tagged
([.github/workflows/release.yaml](../.github/workflows/release.yaml)):

| Tag | Points to | Use it for |
|---|---|---|
| `1.1.0` | exactly that release | production: reproducible |
| `1.1` | latest patch release of 1.1 | production: picks up fixes |
| `latest` | latest release | trying things out |
| `sha-<short-sha>` | the commit a release was built from | debugging |

Tags have no `v` prefix, and there is no major-only tag such as `1`.

Pin `<major>.<minor>` or an exact version rather than `latest`. The provisioner changes your
NATS server on every run, so a new release pulled in silently by `latest` can change what it does
to existing streams and consumers on the next deploy. With a pinned tag, you decide when to
upgrade and can read the [changelog](../CHANGELOG.md) first.

The documentation on the `develop` branch may describe changes that aren't released yet; the
changelog lists them under *Unreleased*. Two of them matter for deployment and are **not** in
1.1.0 (the current `latest` and `1.1`):

- Consumers are created as durable. 1.1.0 creates them without a durable name, so NATS removes
  them shortly after their last client disconnects. See
  [Durable consumers and inactivity](concepts.md#durable-consumers-and-inactivity).
- Credentials in `NATS_URL` are redacted from the logs. 1.1.0 logs them in plain text; see
  [Authentication](#authentication).

The image is built `FROM scratch` ([Dockerfile](../Dockerfile)): it contains the static binary
and CA certificates, nothing else, so there's no shell to `exec` into. It has no working
directory set, so the default config path `./config.yaml` resolves to `/config.yaml`. It sets no
user, so the container runs as root unless you choose another user.

## Docker Compose

The provisioner runs between NATS and the application:

```yaml
services:
  nats:
    image: nats:2.12-alpine
    command: ["--jetstream", "-sd", "/data"]   # -sd: store JetStream data in the volume
    volumes:
      - natsdata:/data
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
      - ./nats-config.yaml:/config.yaml:ro

  app:
    image: <your-app-image>
    depends_on:
      nats-provisioner:
        condition: service_completed_successfully

volumes:
  natsdata:
```

- `--jetstream` is required: without JetStream the provisioner connects but fails at the first
  stream.
- `-sd /data` points JetStream's storage at the volume, so streams and their messages survive
  `docker compose down` and container re-creation. Without it, NATS stores them inside the
  container. Checked with nats-server v2.12.15: `/data/jetstream` was created and the `ORDERS`
  stream was still there after a restart. `docker compose down -v` deletes the volume and with
  it all streams.
- `service_healthy` isn't strictly needed (the provisioner
  [retries the connection](concepts.md#connection-retry) for up to 5 minutes), but it keeps the
  logs free of retry warnings.
- `service_completed_successfully` keeps the application from starting when provisioning failed.
- Don't set `restart: always` on the provisioner. It is supposed to exit.

Running `docker compose up` with a `nats-config.yaml` holding one stream and one consumer
(`app` replaced by a container that prints `app started`) gives:

```console
$ docker compose ps -a
NAME                       IMAGE                                       SERVICE            STATUS
np-w2-app-1                alpine:3.20                                 app                Exited (0) 2 minutes ago
np-w2-nats-1               nats:2.12-alpine                            nats               Up 3 minutes (healthy)
np-w2-nats-provisioner-1   ghcr.io/datarocks-ag/nats-provisioner:1.1   nats-provisioner   Exited (0) 2 minutes ago
$ docker compose logs nats-provisioner
{"level":"INFO","msg":"Starting nats-provisioner","version":"v1.1.0"}
{"level":"INFO","msg":"Loading configuration","path":"/config.yaml"}
{"level":"INFO","msg":"Configuration loaded","streams":1}
{"level":"INFO","msg":"Connecting to NATS","url":"nats://nats:4222"}
{"level":"INFO","msg":"Connected to NATS","url":"nats://nats:4222"}
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Creating stream","stream":"ORDERS"}
{"level":"INFO","msg":"Ensuring consumer","stream":"ORDERS","consumer":"order-processor"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}
```

Columns and log lines are trimmed; real log lines also carry `time`, and `docker compose logs`
prefixes each line with the container name. For a guided walk-through, see
[Getting started](getting-started.md).

## Kubernetes

There are no manifests or Helm chart in the repository; the provisioner runs as a plain
container in one of two ways:

- **As a Job:** fits deploy pipelines and Helm hooks. Each deploy runs it once, and a failed run
  (exit code `1`) marks the Job as failed.
- **As an init container** of the application's Deployment: the application container only starts
  after the provisioner exits with `0`. Every replica runs it on every start; runs are
  idempotent, but a Job keeps the provisioning out of each pod start.

In both cases, ship the config as a ConfigMap mounted as a file and set `NATS_CONFIG_PATH` to
it, and pass credentials from a Secret as environment variables. The full manifest is in
[Run the provisioner as a Kubernetes Job](howto/run-as-a-kubernetes-job.md).

## Standalone binary

Every release has binaries for Linux and macOS (`amd64`, `arm64`) on the
[releases page](https://github.com/datarocks-ag/nats-provisioner/releases), built with GoReleaser
([.goreleaser.yaml](../.goreleaser.yaml)). The archives are named
`nats-provisioner_<version>_<os>_<arch>.tar.gz` (e.g. `nats-provisioner_1.1.0_linux_amd64.tar.gz`
for release v1.1.0), with SHA-256 checksums in `checksums.txt`:

```console
$ sha256sum --check --ignore-missing checksums.txt
nats-provisioner_1.1.0_linux_amd64.tar.gz: OK
$ tar xzf nats-provisioner_1.1.0_linux_amd64.tar.gz
$ export NATS_URL=nats://nats.internal:4222 NATS_CONFIG_PATH=./nats-config.yaml
$ ./nats-provisioner
```

The binary takes no command-line flags; everything is set through
[environment variables](#environment-variables). `go install` isn't supported: the module path
(`nats-provisioner`) is not a URL.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `NATS_URL` | `nats://localhost:4222` | Server URL, or a comma-separated list of URLs for a cluster (`nats://n1:4222,nats://n2:4222`). |
| `NATS_USER` | — | User name. Only used together with `NATS_PASSWORD`. |
| `NATS_PASSWORD` | — | Password. Only used together with `NATS_USER`. |
| `NATS_TOKEN` | — | Token. Ignored when `NATS_USER` and `NATS_PASSWORD` are both set. |
| `NATS_CONFIG_PATH` | `./config.yaml` | Path to the YAML config, relative to the working directory (`/` in the image). See [Configuration reference](configuration.md). |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`, lowercase only. |

- A variable set to an empty string counts as unset.
- `LOG_LEVEL` is case-sensitive: any other value, including `DEBUG` or `WARN`, silently means
  `info`. The provisioner writes no debug-level lines of its own, so `debug` shows the same as
  `info`. With `warn`, only warnings and errors are logged.
- Logs are JSON lines on standard output.
- Variables referenced in the config (`${ENV}`) are read from the same environment. See
  [Environment variable expansion](configuration.md#environment-variable-expansion).

Sources: [cmd/nats-provisioner/main.go](../cmd/nats-provisioner/main.go),
[internal/client/client.go](../internal/client/client.go).

## Authentication

The provisioner supports user and password, a token, or credentials embedded in the URL:

| Set | Used |
|---|---|
| `NATS_USER` and `NATS_PASSWORD` | user and password |
| `NATS_TOKEN` (without both of the above) | token |
| `nats://<user>:<password>@<host>:4222` in `NATS_URL` | the URL's credentials, which win over the variables above |
| only `NATS_USER`, or only `NATS_PASSWORD` | nothing: the variable is silently ignored |

Creds files, NKeys, JSON Web Tokens (JWT) and TLS client certificates are not supported; the tool
has no settings for them.

**Prefer `NATS_USER`/`NATS_PASSWORD` or `NATS_TOKEN` over credentials in the URL.** Release 1.1.0
writes `NATS_URL` to the `Connecting to NATS` and `Connected to NATS` log lines as it is,
credentials included. The next release replaces them with `redacted`
(`nats://redacted@host:4222`); see *Unreleased* in the [changelog](../CHANGELOG.md).

A wrong password is retried like any other connection error, so the run fails only after about
5 minutes. Each retry logs a warning with `nats: Authorization Violation`.

The full story, with examples for each method, is in
[Authenticate to a secured NATS server](howto/authenticate-to-nats.md).

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Every stream and consumer was provisioned. Warnings (e.g. about [fields that can't change after creation](concepts.md#fields-that-cant-change-after-creation)) don't change the exit code. |
| `1` | Any failure: the config couldn't be loaded or validated, the connection failed after retrying, or provisioning a stream or consumer failed. |

The `ERROR` log line tells which: `Failed to load configuration`, `Failed to connect to NATS` or
`Provisioning failed`, each with an `error` field. A provisioning failure stops the run at that
stream or consumer; what was provisioned before it stays, and a re-run continues from the start
([cmd/nats-provisioner/main.go](../cmd/nats-provisioner/main.go)).

`SIGINT` and `SIGTERM` cancel the run. A run cancelled while connecting exits with `1` and
`connection canceled: context canceled`.

## Next steps

- [Configuration reference](configuration.md) for every field.
- [Authenticate to a secured NATS server](howto/authenticate-to-nats.md) before going to production.
- [Run the provisioner as a Kubernetes Job](howto/run-as-a-kubernetes-job.md).
- [FAQ](faq.md) for operations questions.
