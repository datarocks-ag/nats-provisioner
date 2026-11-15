# Configuration reference

The complete reference for the YAML config. For a guided introduction, start with
[Getting started](getting-started.md). A commented example is in
[config.example.yaml](../config.example.yaml); a complete one with every supported field is in
[Streams and consumers together](#streams-and-consumers-together).

- [Top level](#top-level)
- [Streams](#streams)
- [Consumers](#consumers): [push consumers](#push-consumers-are-not-supported)
- [Durations](#durations)
- [Environment variable expansion](#environment-variable-expansion)
- [Validation](#validation)

The provisioner reads the file named by `NATS_CONFIG_PATH` (default `./config.yaml`, see
[Environment variables](deployment.md#environment-variables)). Loading happens in four steps,
before the provisioner connects to NATS: read the file, parse the YAML, expand `${VAR}`
references, validate. Any failure ends the run with exit code `1`
([internal/config/config.go](../internal/config/config.go), `Load`).

Values captured in this page come from nats-server v2.12.15. Log lines are trimmed; real lines
also carry `time`.

## Top level

```yaml
strategy: update     # optional: "update" (default) or "create"
streams: [...]       # optional: the streams to provision, in this order
```

| Field | Type | Allowed values | When omitted | Notes |
|---|---|---|---|---|
| `strategy` | string | `update`, `create` | `update` | Default for every stream; a stream's own `strategy` overrides it. Never applies to consumers. See [Strategy](concepts.md#strategy-update-or-create). |
| `streams` | list | [stream](#streams) entries | no streams | An empty file, or a file without `streams`, is valid and provisions nothing. |

## Streams

```yaml
streams:
  - name: ORDERS              # required
    subjects: ["orders.>"]    # required, at least one
    max_age: "7d"
    consumers: [...]          # optional, see Consumers
```

Streams are provisioned in file order. A stream that doesn't exist is created from the fields
you set, and NATS fills in its own defaults for the rest. For an existing stream with
`strategy: update`, the provisioner starts from the server's current stream config and
overlays only the fields you set, so **removing a line from the YAML does not reset the field**
on the server. See [How streams are updated](concepts.md#how-streams-are-updated).

"When omitted" below is what a new stream gets (server defaults, read with
`nats stream info --json` from a stream created with only `name` and `subjects`). On an
existing stream, an omitted field keeps its current value.

| Field | Type | Allowed values | When omitted | Notes |
|---|---|---|---|---|
| `name` | string | no whitespace, `.`, `*`, `>`, `/`, `\` or null byte | **required** | Unique across the file. Case-sensitive. |
| `subjects` | list of strings | non-empty strings | **required** | At least one entry. The tool doesn't check subject syntax; NATS does when the stream is created or updated. **Replaced wholesale on update**: the server gets exactly this list. |
| `description` | string | any | none | An empty or omitted value can't clear an existing description (verified: `description: ""` left `first` in place). |
| `strategy` | string | `update`, `create` | top-level `strategy` | `create` skips the stream if it exists; its consumers are still created or updated. See [Strategy](concepts.md#strategy-update-or-create). |
| `retention` | string | `limits`, `interest`, `workqueue` | `limits` | **Can't change after creation**, see below. |
| `storage` | string | `file`, `memory` | `file` | **Can't change after creation**, see below. |
| `max_msgs` | integer (64-bit) | `-1` or more | `-1` (unlimited) | Maximum number of messages. |
| `max_bytes` | integer (64-bit) | `-1` or more | `-1` (unlimited) | Maximum total size in bytes. |
| `max_age` | [duration](#durations) | `0s` or more | `0` (unlimited) | Messages older than this are removed. `"0s"` clears an existing limit (verified: `7d` → `0`). |
| `max_msg_size` | integer (32-bit) | `-1` or more | `-1` (unlimited) | Largest accepted message in bytes. |
| `num_replicas` | integer | `1` or more | `1` | Copies of the stream across a NATS cluster. Checked by NATS at provisioning time: a single server without clustering rejects anything above `1` (`replicas > 1 not supported in non-clustered mode`). See [Replicate a stream across a cluster](howto/replicate-a-stream.md). |
| `discard` | string | `old`, `new` | `old` | What happens when a limit is reached: drop the oldest messages, or reject new ones. |
| `duplicate_window` | [duration](#durations) | `0s` or more | `2m` | Window in which a repeated `Nats-Msg-Id` header is dropped. `"0s"` resets it to the server default `2m` (verified). See [Deduplicate published messages](howto/deduplicate-messages.md). |
| `allow_rollup` | boolean | `true`, `false` | `false` | Allows the `Nats-Rollup` header to replace messages. |
| `deny_delete` | boolean | `true`, `false` | `false` | Forbids deleting single messages. **Never changed on an existing stream** by the provisioner, see below. |
| `deny_purge` | boolean | `true`, `false` | `false` | Forbids purging the stream. **Never changed on an existing stream** by the provisioner, see below. |
| `consumers` | list | [consumer](#consumers) entries | no consumers | Provisioned after the stream, in file order. |

Sources: struct `Stream` in [internal/config/config.go](../internal/config/config.go);
`buildStreamConfig` in [internal/provisioner/provisioner.go](../internal/provisioner/provisioner.go);
`mergeStreamUpdate` in [internal/provisioner/streams.go](../internal/provisioner/streams.go).

### Fields fixed at creation

`retention`, `storage`, `deny_delete` and `deny_purge` are never changed on an existing stream:
the server's value is kept. If the YAML sets a different value, a warning is logged and the run
still exits `0`:

```json
{"level":"WARN","msg":"Stream storage type mismatch (immutable, cannot be changed)","stream":"UPD","current":"file","desired":"memory"}
{"level":"WARN","msg":"Stream retention policy mismatch (immutable, cannot be changed)","stream":"UPD","current":"limits","desired":"workqueue"}
```

No warning is logged when the field is omitted, or when the stream is skipped with
`strategy: create`. Background in
[Fields that can't change after creation](concepts.md#fields-that-cant-change-after-creation).

## Consumers

```yaml
streams:
  - name: ORDERS
    subjects: ["orders.>"]
    consumers:
      - name: order-processor          # required
        filter_subject: "orders.created"
        ack_wait: "30s"
        max_deliver: 5
```

All consumers are durable pull consumers: the consumer's name is also its durable name, so it
persists without connected clients. Every run sends the **full** consumer config to NATS
(`CreateOrUpdateConsumer`), without merging with the server's state, so **a field you remove
from the YAML goes back to its default** (verified: removing `max_deliver: 5` and
`ack_wait: "1m"` reset them to `-1` and `30s`). Strategy never applies to consumers. See
[How consumers are updated](concepts.md#how-consumers-are-updated).

"When omitted" below is what NATS stores for a consumer created with only `name`.

| Field | Type | Allowed values | When omitted | Notes |
|---|---|---|---|---|
| `name` | string | no whitespace, `.`, `*`, `>`, `/`, `\` or null byte | **required** | Unique within its stream. Also the durable name. |
| `description` | string | any | none | |
| `filter_subject` | string | a subject of the stream | all subjects | Only messages on matching subjects are delivered. Can be changed on an existing consumer. |
| `ack_policy` | string | `none`, `all`, `explicit` | `explicit` | The server refuses to change it on an existing consumer (see below). |
| `ack_wait` | [duration](#durations) | `0s` or more | `30s` | Time NATS waits for an acknowledgement before redelivering. |
| `max_deliver` | integer | `-1` or more | `-1` (unlimited) | Maximum delivery attempts per message. |
| `max_ack_pending` | integer | `-1` or more | `1000` | Maximum unacknowledged messages in flight. |
| `deliver_policy` | string | `all`, `last`, `new`, `by_start_sequence`, `by_start_time` | `all` | Where a new consumer starts reading. The server refuses to change it on an existing consumer. See [Replay messages from a sequence or a point in time](howto/replay-messages-from-a-point.md). |
| `opt_start_seq` | integer (unsigned 64-bit) | `0` or more | none | **Required** with `deliver_policy: by_start_sequence`; silently ignored with any other policy. |
| `opt_start_time` | string | RFC 3339 timestamp, e.g. `2026-11-01T00:00:00Z` | none | **Required** with `deliver_policy: by_start_time`; silently ignored with any other policy. |
| `replay_policy` | string | `instant`, `original` | `instant` | `original` replays at the rate messages were published. The server refuses to change it on an existing consumer. |
| `inactive_threshold` | [duration](#durations) | `0s` or more | none (kept forever) | NATS deletes the consumer, durable or not, after this long without activity. The next run recreates it, but its position is lost. See [Durable consumers and inactivity](concepts.md#durable-consumers-and-inactivity). |
| `deliver_subject` | string | — | none | **Rejected at provisioning time**, see [Push consumers are not supported](#push-consumers-are-not-supported). |
| `deliver_group` | string | — | none | **Rejected at provisioning time**, as `deliver_subject`. |

Sources: struct `Consumer` in [internal/config/config.go](../internal/config/config.go);
`buildConsumerConfig` in [internal/provisioner/provisioner.go](../internal/provisioner/provisioner.go).

> Consumers are created as durable from release 1.1.1 on (see the
> [changelog](../CHANGELOG.md)). Release 1.1.0 created them without a durable name, so NATS
> removed them shortly after their last client disconnected.

### Changes the server refuses

Some consumer fields can't change once the consumer exists. NATS rejects the update and the run
fails with exit code `1`. Captured for `ack_policy`, `deliver_policy` and `replay_policy`:

```json
{"level":"ERROR","msg":"Provisioning failed","error":"provisioning consumer \"c\" on stream \"UPD\": nats: API error: code=500 err_code=10012 description=deliver policy can not be updated"}
```

The other two read `ack policy can not be updated` and `replay policy can not be updated`.
Changing `filter_subject`, `max_ack_pending`, `description` and `inactive_threshold` worked.
To change a refused field, delete the consumer with the `nats` CLI (its position is lost) and
re-run.

### Push consumers are not supported

`deliver_subject` and `deliver_group` pass validation, but provisioning any consumer that sets
one of them fails. The stream itself has already been created or updated at that point; the run
stops there:

```json
{"level":"INFO","msg":"Creating stream","stream":"ORDERS"}
{"level":"ERROR","msg":"Provisioning failed","error":"provisioning consumer \"order-push\" on stream \"ORDERS\": deliver_subject is not supported (JetStream v2 API supports pull consumers only)"}
```

For `deliver_group` the message reads `deliver_group is not supported (JetStream v2 API supports
pull consumers only)`. Remove both fields; applications fetch messages from the pull consumer
instead.

### Streams and consumers together

A complete config with every supported field. It loads and provisions as shown (checked with
nats-server v2.12.15):

```yaml
strategy: update                     # default for every stream: update | create

streams:
  - name: ORDERS                     # required, unique
    description: "Order events"
    subjects: ["orders.>"]           # required, at least one
    strategy: update                 # overrides the top-level strategy
    retention: limits                # limits | interest | workqueue  (fixed after creation)
    storage: file                    # file | memory                  (fixed after creation)
    max_msgs: -1                     # -1 = unlimited
    max_bytes: 1073741824            # 1 GiB
    max_age: "7d"                    # Go duration or <n>d; "0s" = unlimited
    max_msg_size: 1048576            # 1 MiB
    num_replicas: 1                  # 1, or up to the number of servers in a cluster
    discard: old                     # old | new
    duplicate_window: "2m"
    allow_rollup: false
    deny_delete: false               # fixed after creation
    deny_purge: false                # fixed after creation
    consumers:
      - name: order-processor        # required, unique within the stream
        description: "Processes new orders"
        filter_subject: "orders.created"
        ack_policy: explicit         # none | all | explicit
        ack_wait: "30s"
        max_deliver: 5               # -1 = unlimited
        max_ack_pending: 1000        # -1 = unlimited
        deliver_policy: all          # all | last | new | by_start_sequence | by_start_time
        replay_policy: instant       # instant | original

      - name: order-audit
        deliver_policy: by_start_time
        opt_start_time: "2026-11-01T00:00:00Z"   # RFC 3339, required for by_start_time

      - name: order-backfill
        deliver_policy: by_start_sequence
        opt_start_seq: 1             # required for by_start_sequence
        inactive_threshold: "24h"    # NATS deletes the consumer after 24h without activity
```

## Durations

`max_age`, `duplicate_window`, `ack_wait` and `inactive_threshold` take a duration string:

| Form | Examples | Notes |
|---|---|---|
| Go duration | `30s`, `5m`, `2h`, `1h30m`, `500ms` | Units `ns`, `us`, `ms`, `s`, `m`, `h` ([Go `time.ParseDuration`](https://pkg.go.dev/time#ParseDuration)). |
| Whole days | `7d`, `30d` | An integer followed by a lowercase `d`, converted to `24h` per day. |

Not accepted: days combined with other units (`1d12h`, write `36h`), fractional days (`1.5d`),
an uppercase `D`, and negative durations. Quote durations in YAML so that a value is always read
as a string. An empty value counts as omitted.

```json
{"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].max_age: invalid duration \"1.5d\": time: unknown unit \"d\" in duration \"1.5d\""}
{"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].max_age: invalid duration \"1d12h\": time: unknown unit \"d\" in duration \"1d12h\""}
{"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].max_age: negative duration \"-1h\" is not allowed"}
```

Source: `ParseDuration` in [internal/config/config.go](../internal/config/config.go).

## Environment variable expansion

String values can reference environment variables as `${NAME}`. They are replaced after the YAML
is parsed and before validation:

```yaml
streams:
  - name: "${ENV}_ORDERS"
    subjects: ["${ENV}.orders.>"]
    max_age: "${ORDERS_MAX_AGE}"
    storage: "${ORDERS_STORAGE}"
```

With `ENV=staging ORDERS_MAX_AGE=3d ORDERS_STORAGE=memory`, this creates the stream
`staging_ORDERS` on `staging.orders.>`, with a 3-day `max_age` in memory.

| Situation | Result |
|---|---|
| Variable set | Replaced by its value. |
| Variable set to an empty string | Replaced by `""`, which counts as omitted (e.g. `storage` falls back to `file`). |
| Variable unset | The text `${NAME}` stays as it is, without an error. Where it ends up in a duration or enum field, validation rejects it (`invalid duration "${ORDERS_MAX_AGE}"`). In a name or subject it passes, so a stream could be created literally named `${ENV}_ORDERS`. |

- Only the `${NAME}` form is recognized. There is no default syntax (`${NAME:-default}`) and no
  escape (`$${`).
- Expansion applies to every string field: names, subjects (each entry), descriptions,
  `filter_subject`, enum values, `strategy`, durations, `opt_start_time`, `deliver_subject`,
  `deliver_group`.
- Numbers and booleans can't be expanded. The YAML parser rejects the string before expansion
  runs:

  ```json
  {"level":"ERROR","msg":"Failed to load configuration","error":"parsing config YAML: yaml: unmarshal errors:\n  line 4: cannot unmarshal !!str `${MAX}` into int64"}
  ```

See [Use one config for several environments](howto/use-one-config-for-several-environments.md).
Source: `expandEnvVars`, `expandConfig` in [internal/config/config.go](../internal/config/config.go).

## Validation

The config is validated completely before the provisioner connects to NATS, so you can check a
config without a server: the run stops with exit code `1` and an `ERROR` line
`Failed to load configuration`. Three things to know:

- **The first error wins.** Validation stops at the first problem, in file order, so fix it and
  run again to see the next one.
- **Unknown keys are silently ignored.** A misspelled field such as `max_agee: "7d"` is not an
  error; the field is not set (verified: the run succeeded and `max_age` stayed unset).
  Compare the result with `nats stream info <stream-name>` after the first run.
- **Some problems only show up at provisioning time**, after the connection: push consumer
  fields, subjects NATS rejects (`a..b` fails with `invalid subject`), `num_replicas` the
  server can't satisfy, and consumer changes the server refuses.

Validation errors start with `validating config: ` and name the field's path, e.g.
`streams[0].consumers[1].ack_wait`. Errors before validation start with
`reading config file: ` (file missing or unreadable) or `parsing config YAML: ` (invalid YAML, or
a value of the wrong type).

| Rejected | Error after `validating config: ` |
|---|---|
| Invalid top-level or stream `strategy` | `strategy: invalid strategy "<value>" (must be "create" or "update")` (path `streams[i].strategy` for a stream) |
| Missing name | `streams[i].name: is required` / `streams[i].consumers[j].name: is required` |
| Invalid characters in a name | `<path>.name: contains invalid characters (whitespace, '.', '*', '>', '/', '\')` |
| Duplicate stream name | `streams[i].name: duplicate stream name "<name>"` |
| Duplicate consumer name in a stream | `streams[i].consumers[j].name: duplicate consumer name "<name>"` |
| Null byte in a string field | `<path>: contains null byte` |
| No subjects | `streams[i].subjects: at least one subject is required` |
| Empty subject | `streams[i].subjects[k]: must not be empty` |
| Unknown `retention` | `streams[i].retention: invalid value "<value>" (must be limits, interest, or workqueue)` |
| Unknown `storage` | `streams[i].storage: invalid value "<value>" (must be file or memory)` |
| Unknown `discard` | `streams[i].discard: invalid value "<value>" (must be old or new)` |
| Unknown `ack_policy` | `<path>.ack_policy: invalid value "<value>" (must be none, all, or explicit)` |
| Unknown `deliver_policy` | `<path>.deliver_policy: invalid value "<value>" (must be all, last, new, by_start_sequence, or by_start_time)` |
| Unknown `replay_policy` | `<path>.replay_policy: invalid value "<value>" (must be instant or original)` |
| `max_msgs`, `max_bytes`, `max_msg_size`, `max_deliver`, `max_ack_pending` below `-1` | `<path>.<field>: must be >= -1, got <n>` |
| `num_replicas` below `1` | `streams[i].num_replicas: must be >= 1, got <n>` |
| Unparseable duration | `<path>.<field>: invalid duration "<value>": <reason>` |
| Negative duration | `<path>.<field>: negative duration "<value>" is not allowed` |
| `by_start_sequence` without `opt_start_seq` | `streams[i].consumers[j]: deliver_policy 'by_start_sequence' requires opt_start_seq` |
| `by_start_time` without `opt_start_time` | `streams[i].consumers[j]: deliver_policy 'by_start_time' requires opt_start_time` |
| `opt_start_time` not RFC 3339 | `streams[i].consumers[j].opt_start_time: invalid RFC3339 format "<value>": <reason>` |

Enum values are case-sensitive: `File` is not `file`. Captured output for common mistakes:

```json
{"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].name: contains invalid characters (whitespace, '.', '*', '>', '/', '\\')"}
{"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].subjects: at least one subject is required"}
{"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].storage: invalid value \"File\" (must be file or memory)"}
{"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].consumers[0]: deliver_policy 'by_start_time' requires opt_start_time"}
{"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].consumers[0].opt_start_time: invalid RFC3339 format \"2026-11-15\": parsing time \"2026-11-15\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"\" as \"T\""}
{"level":"ERROR","msg":"Failed to load configuration","error":"reading config file: open nope.yaml: no such file or directory"}
```

The `\\` and `\"` are JSON escaping in the log line; the error text itself has `\` and `"`.
The name error above came from `name: "orders.v1"`.

To check a config without touching a server, run the binary against it with an unreachable
`NATS_URL`: a valid config gets as far as `Configuration loaded` and then starts
[retrying the connection](concepts.md#connection-retry), so stop it with Ctrl+C.

Source: `validate`, `validateConsumers` in [internal/config/config.go](../internal/config/config.go).

## Next steps

- [Deployment](deployment.md): run the provisioner with Docker Compose, Kubernetes or as a binary.
- [How it works](concepts.md): what happens to existing streams and consumers on each run.
- [Limit how much a stream keeps](howto/limit-stream-size-and-age.md) and the other how-tos for
  common setups.
- [FAQ](faq.md) for questions and error messages.
