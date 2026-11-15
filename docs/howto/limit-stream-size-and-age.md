# Limit how much a stream keeps

After this, your stream keeps messages only up to the age, size and count you choose, and you
know what happens to a publish once a limit is reached.

## When you need this

A JetStream stream with no limits keeps every message forever. Sooner or later it fills the
disk of the NATS server, or holds data longer than you are allowed to keep it. You want the
stream to drop old messages on its own, or to refuse new ones when it is full, and you want
that set in the provisioner's config rather than by hand on each server.

## What you need to know first

- **Limits retention:** with the default `retention: limits`, a stream keeps messages until one
  of its limits is reached; then the server removes or refuses messages, whether or not anyone
  has read them. [Retention policies](https://docs.nats.io/learn/jetstream/retention-policies).
- **The limit fields:** `max_age` (how long a message is kept), `max_bytes` (total size of the
  stream), `max_msgs` (number of messages) and `max_msg_size` (size of one message). The count
  and size fields take `-1` for "unlimited"; `max_age` takes a duration such as `7d`.
  [Streams](../configuration.md#streams), [Durations](../configuration.md#durations).
- **Discard policy:** `discard` decides what happens when `max_msgs` or `max_bytes` is
  reached. `old` (the default) deletes the oldest messages to make room; `new` refuses the new
  message and the publisher gets an error.
- **Updates merge:** when the stream already exists, the provisioner starts from the server's
  current configuration and overlays only the fields you set in YAML. A field you leave out
  keeps its value on the server. [How streams are updated](../concepts.md#how-streams-are-updated).

## Steps

### 1. Choose the limits and the discard policy

Decide what should happen when the stream is full. For data that loses value with age, such as
sensor readings or logs, use `discard: old`: the stream behaves like a ring buffer. For data
you must not lose without a publisher noticing, such as orders, use `discard: new`: the stream
stops accepting messages and the publisher gets an error it can act on.

`max_age` applies with both policies: a message older than `max_age` is removed either way.

### 2. Set the limits on the stream

A stream for sensor readings that keeps at most 7 days, 1 GiB and 100 000 messages, and
refuses any single message above 64 KiB:

```yaml
streams:
  - name: SENSORS
    subjects: ["sensors.>"]
    max_age: 7d                  # days are allowed; 1d12h or 1.5d are not
    max_bytes: 1073741824        # 1 GiB
    max_msgs: 100000
    max_msg_size: 65536          # 64 KiB per message
    discard: old                 # the default; shown for clarity
```

`max_bytes` and `max_msg_size` are plain numbers of bytes; there is no `1GB` notation. Run
the provisioner. A new stream is created with these limits; an existing one is updated:

```text
{"level":"INFO","msg":"Creating stream","stream":"SENSORS"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}
```

Log lines are trimmed; real lines also carry `time`.

### 3. Change a limit later

To change one limit on an existing stream, change that one line and run the provisioner again.
Fields you don't mention stay as they are on the server. This config only raises `max_age` to
30 days; `max_msgs`, `max_bytes`, `max_msg_size` and `discard` keep the values from step 2:

```yaml
streams:
  - name: SENSORS
    subjects: ["sensors.>"]
    max_age: 30d
```

The flip side: **deleting a line from the YAML does not remove the limit.** The server keeps it.

Lowering a limit below what the stream holds right now takes effect at once on a
`discard: old` stream: on nats-server v2.12.15, lowering `max_msgs` from unlimited to `2` on a
stream with 8 messages left 2 messages. On a `discard: new` stream the server kept the existing
messages and refused new ones until the stream was under the limit again.

### 4. Remove a limit

To remove a limit, set it explicitly to "unlimited":

```yaml
streams:
  - name: SENSORS
    subjects: ["sensors.>"]
    max_age: "0s"                # 0 = keep forever
    max_bytes: -1
    max_msgs: -1
    max_msg_size: -1
```

`nats stream info` before and after this run (trimmed to the limits section):

```console
$ nats stream info SENSORS
…
Limits:

             Maximum Messages: 3
          Maximum Per Subject: unlimited
                Maximum Bytes: 1.0 GiB
                  Maximum Age: 30d0h0m0s
         Maximum Message Size: 64 KiB
            Maximum Consumers: unlimited
…
```

```console
$ nats stream info SENSORS
…
Limits:

             Maximum Messages: unlimited
          Maximum Per Subject: unlimited
                Maximum Bytes: unlimited
                  Maximum Age: unlimited
         Maximum Message Size: unlimited
            Maximum Consumers: unlimited
…
```

On nats-server v2.12.15, `0` for `max_msgs` and `max_bytes` also clears the limit (the server
stores it as `-1`), and `max_age: "0"` works as well as `"0s"`. Prefer `-1`, which says what
you mean.

## Check that it works

`nats stream info` shows the limits and the discard policy the server really applies (output
trimmed):

```console
$ nats stream info SENSORS
Information for Stream SENSORS created 2026-11-15 17:58:28
…
               Discard Policy: Old
…
Limits:

             Maximum Messages: 3
          Maximum Per Subject: unlimited
                Maximum Bytes: 1.0 GiB
                  Maximum Age: 7d0h0m0s
         Maximum Message Size: 64 KiB
            Maximum Consumers: unlimited

State:
…
                     Messages: 3
               First Sequence: 3 @ 2026-11-15 17:58:28
                Last Sequence: 5 @ 2026-11-15 17:58:28
```

This stream was created with `max_msgs: 3` and received 5 messages: the first two were
discarded, so the stream holds sequences 3 to 5. Safe to run anywhere; it only reads.

With `discard: new`, a publish past the limit fails. Try it in a test environment with
`nats pub --jetstream`, which waits for the server's acknowledgement. On a stream `ORDERS` with
`max_msgs: 3` that already holds 3 messages:

```console
$ nats pub --jetstream orders.new "order 4"
17:58:34 Published 7 bytes to "orders.new"
nats: error: maximum messages exceeded (10077)
```

A message above `max_msg_size` fails the same way, with
`nats: error: message size exceeds maximum allowed (10054)`.

## Pitfalls

- **`discard: new` drops core publishes silently.** A plain `nats pub` (or any core NATS
  publish without an acknowledgement) printed `Published …` and exited 0 while the stream was
  full, and the message was not stored. Publishers that must not lose data have to use
  JetStream publish and check the acknowledgement.
- **Removing a line keeps the limit.** Because updates merge, a limit or `discard: new` stays on
  the server after you delete it from the YAML. Set `-1` or `"0s"` instead, as in
  [step 4](#4-remove-a-limit).
- **Unsupported duration formats.** `max_age` takes Go durations (`30s`, `2h`, `1h30m`) or whole
  days (`7d`). `1d12h`, `1.5d` and `7D` fail validation before the provisioner connects; write
  `36h` instead of `1d12h`. [Durations](../configuration.md#durations).
- **`max_age` shorter than the duplicate window.** If you set a `duplicate_window` longer than
  `max_age`, the server refuses the stream and the run fails (see
  [Deduplicate published messages](deduplicate-messages.md)).

## Further reading

- [Streams](../configuration.md#streams) in the configuration reference, for every stream field.
- [How streams are updated](../concepts.md#how-streams-are-updated), for the merge rules.
- [Shaping the stream](https://docs.nats.io/learn/jetstream/shaping-the-stream), for
  the server's view of each limit and discard policy.
- [`internal/provisioner/streams.go`](../../internal/provisioner/streams.go), where the update
  merge happens.
