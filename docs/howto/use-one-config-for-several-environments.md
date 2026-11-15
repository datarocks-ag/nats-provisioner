# Use one config for several environments

After this, one `config.yaml` provisions `staging` and `prod` (or any other environment) with
different stream names, subjects and limits, and a missing variable is caught before it creates
a stream with the wrong name.

## When you need this

Your streams look the same everywhere, but not quite: production keeps orders for 30 days,
staging for one; each environment publishes under its own subject prefix; or several
environments share one NATS server and need separate stream names. Copying `config.yaml` per
environment works until the copies drift apart. Instead, keep one file and put the parts that
differ into environment variables.

## What you need to know first

- **`${NAME}` expansion:** after the YAML file is parsed, every `${NAME}` inside a **string**
  field is replaced with the value of the environment variable `NAME`
  ([internal/config/config.go](../../internal/config/config.go), `expandEnvVars` and
  `expandConfig`). String fields are names, descriptions, subjects, `filter_subject`, the enum
  fields (`retention`, `storage`, `discard`, `ack_policy`, …), `strategy` and the duration
  fields (`max_age`, `duplicate_window`, `ack_wait`, `inactive_threshold`). The full list is in
  [Environment variable expansion](../configuration.md#environment-variable-expansion).
- **Expansion happens before validation.** The expanded values are checked like values typed
  into the file, and validation runs before the provisioner connects to NATS. A bad value stops
  the run with exit code 1 and nothing is changed on the server.
  [Validation](../configuration.md#validation).
- **It only knows `${NAME}`.** There is no default syntax such as `${NAME:-default}`, no `$NAME`
  form and no escape. A variable that is **not set** stays in the text as the literal
  `${NAME}`; a variable that is set to an **empty** value becomes an empty string. The
  [Pitfalls](#pitfalls) are all about these two cases.
- **Numbers and booleans can't take a variable.** `max_msgs`, `max_bytes`, `max_msg_size`,
  `num_replicas`, `max_deliver`, `max_ack_pending`, `opt_start_seq`, `allow_rollup`,
  `deny_delete` and `deny_purge` are parsed as numbers or booleans before any expansion
  happens.

## Steps

### 1. Put the per-environment parts into variables

Decide what differs between environments and give each difference a variable. Here the
environment name goes into the stream name, the description and the subjects, and the
retention and acknowledgement timeout differ per environment:

```yaml
streams:
  - name: "ORDERS_${ENVIRONMENT}"
    description: "Order events (${ENVIRONMENT})"
    subjects: ["${ENVIRONMENT}.orders.>"]
    max_age: "${ORDERS_MAX_AGE}"
    consumers:
      - name: "order-processor"
        filter_subject: "${ENVIRONMENT}.orders.created"
        ack_wait: "${ORDERS_ACK_WAIT}"
```

Quote every value that contains `${…}`. Inside a `[…]` list, YAML treats `{` and `}` as
structure, so an unquoted `subjects: [${ENVIRONMENT}.orders.>]` fails with
`parsing config YAML: yaml: line 2: did not find expected ',' or ']'` (the line number points
near, not exactly at, the problem). Quoting everywhere means you never have to think about
where it matters.

Keep numeric limits literal (see [What you need to know first](#what-you-need-to-know-first)).
If production needs a different `max_bytes` or `num_replicas` than staging, that part can't come
from a variable; use separate files for it or keep the larger value everywhere.

### 2. Set the variables in the provisioner's environment

The variables are read from the environment of the **provisioner process**, not from the NATS
server and not from your shell unless you pass them on.

With Docker Compose, pass them in `environment:`. Compose itself interpolates `${…}` in the
Compose file, so you can let Compose refuse to start when a variable is missing
(`${NAME:?message}`) and give defaults where one makes sense (`${NAME:-value}`). This is Compose
syntax, resolved by Compose before the container starts. The provisioner never sees it:

```yaml
services:
  nats-provisioner:
    image: ghcr.io/datarocks-ag/nats-provisioner:1.1
    environment:
      NATS_URL: nats://nats:4222
      NATS_CONFIG_PATH: /config.yaml
      ENVIRONMENT: ${ENVIRONMENT:?set ENVIRONMENT to the target environment}
      ORDERS_MAX_AGE: ${ORDERS_MAX_AGE:-7d}
      ORDERS_ACK_WAIT: ${ORDERS_ACK_WAIT:-30s}
    volumes:
      - ./config.yaml:/config.yaml:ro
```

Keep one env file per environment next to it, e.g. `prod.env`:

```
ENVIRONMENT=prod
ORDERS_MAX_AGE=30d
ORDERS_ACK_WAIT=2m
```

and select it when you run: `docker compose --env-file prod.env run --rm nats-provisioner`.
Without `ENVIRONMENT`, Compose stops before anything runs (Docker Compose 5.7.1):

```console
$ docker compose config
error while interpolating services.nats-provisioner.environment.ENVIRONMENT: required variable ENVIRONMENT is missing a value: set ENVIRONMENT to the target environment
```

On Kubernetes, set the variables in the Job's `env:` list, or keep them in one ConfigMap per
environment and load it with `envFrom: [{ configMapRef: { name: <configmap-name> } }]`. The
config file itself stays the same in every namespace. The Job is described in
[Run the provisioner as a Kubernetes Job](run-as-a-kubernetes-job.md).

### 3. Run it once per environment

Each run creates the streams for its own environment. Captured against a throwaway
nats-server v2.12.15, with the standalone binary (a build from the next release):

```console
$ ENVIRONMENT=staging ORDERS_MAX_AGE=1d ORDERS_ACK_WAIT=30s \
  NATS_URL=nats://localhost:4222 NATS_CONFIG_PATH=config.yaml nats-provisioner
{"level":"INFO","msg":"Configuration loaded","streams":1}
{"level":"INFO","msg":"Creating stream","stream":"ORDERS_staging"}
{"level":"INFO","msg":"Ensuring consumer","stream":"ORDERS_staging","consumer":"order-processor"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}
$ ENVIRONMENT=prod ORDERS_MAX_AGE=30d ORDERS_ACK_WAIT=2m \
  NATS_URL=nats://localhost:4222 NATS_CONFIG_PATH=config.yaml nats-provisioner
{"level":"INFO","msg":"Creating stream","stream":"ORDERS_prod"}
{"level":"INFO","msg":"Ensuring consumer","stream":"ORDERS_prod","consumer":"order-processor"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}
```

Log lines in this guide are trimmed to the relevant lines and fields; real lines also carry
`time`. The `Creating stream` (or `Updating stream`) line shows the stream name **after**
expansion, so it is the first place to look.

## Check that it works

List the streams with the [NATS command line tool](https://github.com/nats-io/natscli)
(`nats`). Every name should be fully expanded:

```console
$ nats -s nats://localhost:4222 stream ls
╭────────────────────────────────────────────────────────────────────────────────────────────────╮
│                                             Streams                                            │
├────────────────┬────────────────────────┬─────────────────────┬──────────┬──────┬──────────────┤
│ Name           │ Description            │ Created             │ Messages │ Size │ Last Message │
├────────────────┼────────────────────────┼─────────────────────┼──────────┼──────┼──────────────┤
│ ORDERS_prod    │ Order events (prod)    │ 2026-11-15 17:59:35 │ 0        │ 0 B  │ never        │
│ ORDERS_staging │ Order events (staging) │ 2026-11-15 17:59:35 │ 0        │ 0 B  │ never        │
╰────────────────┴────────────────────────┴─────────────────────┴──────────┴──────┴──────────────╯
```

Then check that the values reached the stream and the consumer (output trimmed):

```console
$ nats -s nats://localhost:4222 stream info ORDERS_prod
Information for Stream ORDERS_prod created 2026-11-15 17:59:35

                  Description: Order events (prod)
                     Subjects: prod.orders.>
…
                  Maximum Age: 30d0h0m0s
…
$ nats -s nats://localhost:4222 consumer info ORDERS_prod order-processor
…
          Filter Subject: prod.orders.created
…
                Ack Wait: 2m0s
…
```

This run used a build from the next release, where consumers are durable. With release 1.1.0
NATS deletes the consumer a few seconds after the run, so `consumer info` finds nothing
unless an application is connected
([Durable consumers and inactivity](../concepts.md#durable-consumers-and-inactivity)).

To find streams that were created from an unset variable, search the names for a leftover `${`:

```console
$ nats -s nats://localhost:4222 stream ls -n | grep -F '${'
ORDERS_${ENVIRONMENT}
```

No output means none exist. The commands above only read; they are safe to run against any
environment.

## Pitfalls

- **An unset variable creates a stream with `${…}` in its name.** `${ENVIRONMENT}` stays in
  the text when `ENVIRONMENT` is not set. `$`, `{` and `}` are allowed in stream names, so
  validation passes, the NATS server accepts it, and the run succeeds:

  ```console
  $ ORDERS_MAX_AGE=1d ORDERS_ACK_WAIT=30s nats-provisioner
  {"level":"INFO","msg":"Configuration loaded","streams":1}
  {"level":"INFO","msg":"Creating stream","stream":"ORDERS_${ENVIRONMENT}"}
  {"level":"INFO","msg":"Ensuring consumer","stream":"ORDERS_${ENVIRONMENT}","consumer":"order-processor"}
  {"level":"INFO","msg":"nats-provisioner finished successfully"}
  ```

  The stream also gets the literal subject `${ENVIRONMENT}.orders.>`, so no publisher ever
  reaches it. The provisioner never deletes anything, so the stray stream stays after you fix
  the variable. Find it as shown in [Check that it works](#check-that-it-works), check it is
  empty, and remove it by hand (quote the name so your shell doesn't expand it):
  `nats stream rm 'ORDERS_${ENVIRONMENT}' -f`. Deleting a stream deletes its messages; there is
  no undo. Guard against this with `${NAME:?…}` in Compose, as in step 2.
- **An unset variable in a duration field fails the run,** because `${ORDERS_MAX_AGE}` is not a
  duration. That is the friendly case, and nothing is changed on the server:

  ```
  {"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].max_age: invalid duration \"${ORDERS_MAX_AGE}\": time: invalid duration \"${ORDERS_MAX_AGE}\""}
  ```

  An unset variable in an enum field (`retention`, `ack_policy`, …) fails the same way with
  `invalid value "${…}"`.
- **A variable set to an empty value becomes `""`.** If it is the whole value of a required
  field, validation stops the run:

  ```
  {"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].name: is required"}
  {"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].subjects[0]: must not be empty"}
  ```

  If it is only part of a value, the result can still be valid: `ORDERS_${ENVIRONMENT}` with an
  empty `ENVIRONMENT` becomes `ORDERS_`. In the example above the subject `.orders.>` then fails
  on the server, after the configuration was loaded:

  ```
  {"level":"INFO","msg":"Creating stream","stream":"ORDERS_"}
  {"level":"ERROR","msg":"Provisioning failed","error":"provisioning stream \"ORDERS_\": nats: API error: code=500 err_code=10052 description=invalid subject"}
  ```

  In an optional field an empty value means "not set" and nobody complains: an empty
  `ORDERS_MAX_AGE` creates a stream without an age limit, an empty enum field falls back to its
  default. On an existing stream an omitted field keeps the server's current value
  ([How streams are updated](../concepts.md#how-streams-are-updated)).
- **A variable in a number or boolean field fails at parsing,** quoted or not:

  ```yaml
  num_replicas: ${ORDERS_REPLICAS}
  ```

  ```
  {"level":"ERROR","msg":"Failed to load configuration","error":"parsing config YAML: yaml: unmarshal errors:\n  line 4: cannot unmarshal !!str `${ORDER...` into int"}
  ```
- **`${NAME:-default}` is not a default.** The provisioner looks up a variable literally named
  `NAME:-default`, which never exists, so the text stays as it is. With
  `name: "${ORDERS_STREAM:-ORDERS}"` the run succeeds and creates a stream named
  `${ORDERS_STREAM:-ORDERS}`. Put defaults in the Compose file or the Kubernetes manifest
  instead, as in step 2.
- **The variable is set, but not for the provisioner.** Variables exported in your shell don't
  reach a container unless the Compose file or `docker run -e` passes them on, and a variable
  set on the NATS server's container is invisible to the provisioner. Look at the
  `Creating stream` / `Updating stream` lines: they show what the provisioner actually
  expanded.
- **Compose expands `${…}` in the Compose file, not in `config.yaml`.** A `config.yaml`
  mounted as a volume reaches the container unchanged; only the provisioner expands it. Don't
  write the provisioner's config inline in a Compose `configs:` block with `content:`, because
  Compose would then expand (and with `:-`/`:?` interpret) the `${…}` itself.

## Further reading

- [Environment variable expansion](../configuration.md#environment-variable-expansion) and
  [Validation](../configuration.md#validation) in the configuration reference.
- [Durations](../configuration.md#durations) for the values a duration variable may hold.
- [Nothing is deleted](../concepts.md#nothing-is-deleted): why a misnamed stream stays.
- [Run the provisioner as a Kubernetes Job](run-as-a-kubernetes-job.md).
- [Interpolation](https://docs.docker.com/reference/compose-file/interpolation/) in the Docker
  Compose documentation, for `${NAME:?…}` and `${NAME:-…}`.
- [internal/config/config.go](../../internal/config/config.go): `expandEnvVars`, `expandConfig`
  and `Load`.
