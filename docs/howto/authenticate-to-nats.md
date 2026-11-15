# Authenticate to a secured NATS server

After this, the provisioner logs in to a NATS server that requires a user and password or a
token, with the credentials kept out of your config file and out of the logs.

## When you need this

Your NATS server rejects anonymous clients, and the provisioner keeps logging
`NATS not ready, retrying` with `nats: Authorization Violation` until it gives up. Or you are
about to point the provisioner at a production server and want to know where its credentials
belong and what ends up in the logs.

## What you need to know first

- **NATS authentication:** a NATS server can require a username and password, or a single
  shared token, from every client. This guide uses a server started with
  `nats-server --jetstream --user <nats-user> --pass <nats-password>` or with
  `--auth <token>`. [Authentication](https://docs.nats.io/learn/security/authentication-basics)
  in the NATS documentation.
- **Three ways to pass credentials, one winner.** The provisioner reads `NATS_USER` +
  `NATS_PASSWORD` and `NATS_TOKEN` from its environment
  ([cmd/nats-provisioner/main.go](../../cmd/nats-provisioner/main.go)). Credentials can also be
  part of `NATS_URL` (`nats://<user>:<password>@<host>:4222`). Which one is used is decided as
  follows (verified against nats-server v2.12.15):

  | You set | Sent to the server |
  |---|---|
  | `NATS_USER` and `NATS_PASSWORD` | user and password |
  | only `NATS_USER` (or only `NATS_PASSWORD`) | **nothing**: both are ignored, without a warning |
  | `NATS_TOKEN` | the token |
  | `NATS_USER`, `NATS_PASSWORD` and `NATS_TOKEN` | user and password; the token is ignored |
  | credentials in `NATS_URL` | the URL's credentials, **whatever the variables say** |

  The first four rows come from
  [internal/client/client.go](../../internal/client/client.go) (`Connect`). The last row is
  behaviour of the NATS Go client library, captured live: with
  `NATS_URL=nats://u:p@…` and `NATS_PASSWORD=wrong` the run succeeds; with
  `NATS_URL=nats://u:wrong@…` and the correct `NATS_PASSWORD` it fails.
- **Only user/password and token are supported.** There are no settings for credentials
  files (`.creds`), NKeys, JSON Web Token (JWT) user credentials, TLS client certificates or a
  custom certificate authority (CA). If your server requires any of these, the provisioner
  can't log in to it. A `tls://` URL to a server whose certificate is signed by a public CA
  might work, because the image contains the usual CA certificates, but this is untested.
- **Credentials are read from environment variables only.** The provisioner has no
  `NATS_PASSWORD_FILE` or similar, so a secret mounted as a file can't be used directly; it
  has to become an environment variable.
- **A login failure is retried like a server that isn't up yet.** The provisioner can't tell
  the two apart, so a wrong password costs about five minutes before the run fails.
  [Connection retry](../concepts.md#connection-retry).

## Steps

### 1. Put the credentials in environment variables, not in the URL

Prefer `NATS_USER` + `NATS_PASSWORD` (or `NATS_TOKEN`) over credentials in `NATS_URL`, for
two reasons:

- **Release 1.1.0 logs the URL as it is.** With credentials in `NATS_URL`, the current image
  (`1.1`, `1.1.0`, `latest`) writes them in plain text to the `Connecting to NATS` and
  `Connected to NATS` lines. Captured with `ghcr.io/datarocks-ag/nats-provisioner:1.1`:

  ```
  {"level":"INFO","msg":"Starting nats-provisioner","version":"v1.1.0"}
  {"level":"INFO","msg":"Connecting to NATS","url":"nats://u:p@localhost:4222"}
  {"level":"INFO","msg":"Connected to NATS","url":"nats://u:p@localhost:4222"}
  ```

  The next release replaces the credentials with `redacted`
  ([Logs and credentials](../concepts.md#logs-and-credentials)). The variables never appear
  in the log in either version.
- **Special characters break the URL.** A password containing `@`, `/`, `:` or `%` must be
  percent-encoded in a URL (`p@ss/w:rd` becomes `p%40ss%2Fw%3Ard`). Unencoded, the
  provisioner doesn't connect. Release 1.1.0 logs the whole password, and the next release
  masks it (`"url":"nats://redacted@localhost:4222"`). In `NATS_PASSWORD` the same password
  works as it is.

Set both halves of a user/password pair. `NATS_USER` alone is ignored without a warning, and
the server sees an anonymous client.

### 2. Keep the values out of files you commit

With Docker Compose, put the variables in an env file that Git ignores and load it with
`env_file:`:

```console
$ cat nats-credentials.env
NATS_USER=<nats-user>
NATS_PASSWORD=<nats-password>
$ echo 'nats-credentials.env' >> .gitignore
```

```yaml
services:
  nats-provisioner:
    image: ghcr.io/datarocks-ag/nats-provisioner:1.1
    depends_on:
      nats: { condition: service_healthy }
    env_file: nats-credentials.env
    environment:
      NATS_URL: nats://nats:4222
      NATS_CONFIG_PATH: /config.yaml
    volumes:
      - ./config.yaml:/config.yaml:ro
```

For a token, the file holds `NATS_TOKEN=<token>` instead. Compose `secrets:` don't help here:
they arrive as files, and the provisioner reads only environment variables.

On Kubernetes, keep the variables in a Secret and load it with `envFrom` (`secretRef`). The
full manifest is in [Run the provisioner as a Kubernetes Job](run-as-a-kubernetes-job.md).

### 3. Run the provisioner

Run it as usual. Nothing in `config.yaml` changes. The examples below were captured against
nats-server v2.12.15 started with `--jetstream --user u --pass p`, using the standalone binary
from the next release (so URL credentials show as `redacted`). Log lines are trimmed; real
lines also carry `time`.

```console
$ NATS_URL=nats://localhost:4222 NATS_USER=u NATS_PASSWORD=p NATS_CONFIG_PATH=config.yaml nats-provisioner
{"level":"INFO","msg":"Connecting to NATS","url":"nats://localhost:4222"}
{"level":"INFO","msg":"Connected to NATS","url":"nats://localhost:4222"}
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Creating stream","stream":"ORDERS"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}
```

With a token server (`--auth <token>`) and `NATS_TOKEN=<token>`, the output is the same.

## Check that it works

The provisioner logged `Connected to NATS` and ended with
`nats-provisioner finished successfully`; its exit code is `0`.

Confirm with the [NATS command line tool](https://github.com/nats-io/natscli)
(`nats`), using the same credentials. This only reads, so it is safe against any environment:

```console
$ nats -s nats://localhost:4222 --user <nats-user> --password <nats-password> stream ls
╭─────────────────────────────────────────────────────────────────────────────╮
│                                   Streams                                   │
├────────┬─────────────┬─────────────────────┬──────────┬──────┬──────────────┤
│ Name   │ Description │ Created             │ Messages │ Size │ Last Message │
├────────┼─────────────┼─────────────────────┼──────────┼──────┼──────────────┤
│ ORDERS │             │ 2026-11-15 17:58:40 │ 0        │ 0 B  │ never        │
╰────────┴─────────────┴─────────────────────┴──────────┴──────┴──────────────╯
```

Without credentials the same command fails, which shows the server really requires them:

```console
$ nats -s nats://localhost:4222 stream ls
nats: error: setup failed: nats: Authorization Violation
```

## Pitfalls

- **A wrong password looks like a server that is down, for five minutes.** Every attempt is
  refused and retried with a growing delay (the `delay` field is in nanoseconds). The first
  ten seconds of a run with `NATS_PASSWORD=wrong`, stopped with `timeout 10`:

  ```console
  $ NATS_URL=nats://localhost:4222 NATS_USER=u NATS_PASSWORD=wrong NATS_CONFIG_PATH=config.yaml timeout 10 nats-provisioner
  {"level":"INFO","msg":"Connecting to NATS","url":"nats://localhost:4222"}
  {"level":"WARN","msg":"NATS not ready, retrying","attempt":1,"max_attempts":16,"delay":1000000000,"error":"nats: Authorization Violation"}
  {"level":"WARN","msg":"NATS not ready, retrying","attempt":2,"max_attempts":16,"delay":2000000000,"error":"nats: Authorization Violation"}
  {"level":"WARN","msg":"NATS not ready, retrying","attempt":3,"max_attempts":16,"delay":4000000000,"error":"nats: Authorization Violation"}
  {"level":"WARN","msg":"NATS not ready, retrying","attempt":4,"max_attempts":16,"delay":8000000000,"error":"nats: Authorization Violation"}
  {"level":"ERROR","msg":"Failed to connect to NATS","error":"connection canceled: context canceled"}
  ```

  Left alone, the run keeps retrying and fails after five minutes with
  `connection timeout after 5m0s: context deadline exceeded` and exit code 1 (from the code,
  not captured). `nats: Authorization Violation` in the `error` field means "wrong
  credentials", not "server not ready". The NATS server logs the rejected login too:
  `authentication error - User "u"`.
- **Credentials in `NATS_URL` silently override the variables.** If a URL with old
  credentials is left in a Compose file or a ConfigMap, updating `NATS_PASSWORD` changes
  nothing. With 1.1.0 you can see such credentials in the `Connecting to NATS` line; from the
  next release you see `redacted@` in the URL.
- **`NATS_USER` without `NATS_PASSWORD`** (or the other way round) sends no credentials at
  all, and the log shows only `Authorization Violation`. An empty variable counts as unset, so
  `NATS_PASSWORD=` from a missing secret has the same effect.
- **User/password set against a token server.** User/password wins over `NATS_TOKEN`; a
  leftover `NATS_USER`/`NATS_PASSWORD` pair makes a token login fail.
- **What the logs hide, and what they don't.** From the next release, URL credentials are
  replaced with `redacted`, and the retry warnings also mask the value of `NATS_PASSWORD`.
  The token is never written by the provisioner itself, but error texts are not scanned for
  it. Release 1.1.0 masks nothing.
  [Logs and credentials](../concepts.md#logs-and-credentials).

## Further reading

- [Authentication](../deployment.md#authentication) and
  [Environment variables](../deployment.md#environment-variables) in the deployment guide.
- [Connection retry](../concepts.md#connection-retry) for the retry schedule.
- [Run the provisioner as a Kubernetes Job](run-as-a-kubernetes-job.md) for the Secret.
- [Username/Password](https://docs.nats.io/learn/security/authentication-basics)
  and [Token](https://docs.nats.io/learn/security/authentication-basics)
  authentication in the NATS documentation.
- [internal/client/client.go](../../internal/client/client.go): `Connect`, `RedactURL`.
