# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - Unreleased

Backwards-compatible bug fixes and a dependency refresh. The YAML schema is
unchanged.

### Behavior change worth calling out

- On the **stream update path**, fields the user omits in YAML are no longer
  silently reset to their Go zero values. The provisioner now starts from the
  broker's current `StreamConfig` and overlays only fields the user explicitly
  set. The most visible case is `discard`: a stream created with
  `discard: new` whose YAML later omits the field will now retain `new`
  instead of being silently flipped back to `old`. If you were relying on the
  previous behavior to "reset to defaults" by omitting a field, you must now
  set the field explicitly.

### Fixed

- Stream update silently regressed any mutable field the user omitted in YAML
  (most notably `discard`) to its Go zero value. Update path now merges onto
  the broker's existing config via a new internal `mergeStreamUpdate`
  helper.
- Immutable-field mismatch warnings (`storage`, `retention`, `deny_delete`,
  `deny_purge`) previously fired whenever the YAML omitted the field and the
  Go default differed from the broker's value. They now fire only when the
  user explicitly requested a different value.
- `client.Connect` reported `connection timeout after 5m` for every
  termination cause. It now distinguishes:
  - parent context canceled (e.g. SIGINT) → `connection canceled`
  - parent deadline exceeded → `connection deadline exceeded`
  - inner 5-minute total timeout → `connection timeout after 5m`
- Retry loop off-by-one: log field `max_attempts` now matches the actual
  loop bound, and the final retry-exhausted error wraps the last underlying
  connect error so the cause survives.

### Added

- Null-byte validation now also covers `max_age`, `duplicate_window`,
  `ack_wait`, `opt_start_time`, and `inactive_threshold`, matching the
  existing checks on names and subjects.
- `CHANGELOG.md` (this file).

### Changed

- Dependencies:
  - `github.com/nats-io/nats.go` `1.48.0` → `1.51.0`
  - `github.com/testcontainers/testcontainers-go` `0.40.0` → `0.42.0`
  - `github.com/testcontainers/testcontainers-go/modules/nats` `0.40.0` → `0.42.0`
  - Go toolchain `1.25` → `1.25.7`, plus refresh of indirect dependencies
    (including `go.opentelemetry.io/otel/sdk` `1.43.0`).
- CI / release workflow:
  - Release workflow now triggers on tag pushes only (`v*`); no longer
    triggers on push to `develop`.
  - Action version bumps: `codecov/codecov-action` v4 → v6,
    `actions/upload-artifact` v4 → v7, `docker/login-action` 3 → 4,
    `docker/metadata-action` 5 → 6, `docker/setup-buildx-action` 3 → 4,
    `docker/build-push-action` 6 → 7, `goreleaser/goreleaser-action` 6 → 7.
  - Added `.github/CODEOWNERS`.
- Documentation:
  - README broadened from "Designed as a Docker Compose init container" to
    also describe Kubernetes Job / init-container usage.
  - README documents the supported `${VAR}` expansion forms — `${VAR:-default}`
    and `$$` are not recognized; substitution applies to string fields only.
- Tooling: `make clean` now also removes `cover.out` and `coverage.out`.

## [1.0.0] - 2026-02-20

Initial release.

### Added

- Idempotent provisioning of NATS JetStream streams and consumers from a
  YAML config file.
- `${VAR}` environment variable expansion in string fields.
- Configurable strategy: `update` (default) or `create` (skip existing),
  global or per-stream.
- Exponential backoff retry for NATS connectivity (5-minute total budget).
- Structured JSON logging via `log/slog`.
- Duration support with `Nd` day notation (e.g. `7d`, `30d`).
- Immutable-field mismatch warnings for `storage`, `retention`,
  `deny_delete`, and `deny_purge`.
- Multi-stage Dockerfile producing a `scratch`-based image, plus a
  `docker-compose.yaml` example.

[1.1.0]: https://github.com/datarocks-ag/nats-provisioner/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/datarocks-ag/nats-provisioner/releases/tag/v1.0.0
