# Replay messages from a sequence or a point in time

After this, you can give an application a consumer that starts reading a stream at a chosen
message or at a chosen moment, instead of at the beginning.

## When you need this

A downstream service lost data, or a bug made it process messages wrongly, and you have fixed
it. Now it has to process everything again from a known point: from message 1,200,000, or from
2026-11-14 08:00 UTC, when the bug was deployed. Or a new service needs only today's messages,
not the stream's whole history.

## What you need to know first

- **Stream sequence:** every message in a stream has a sequence number, starting at 1 and
  counting up. `nats stream info <stream-name>` shows the first and last sequence, and
  `nats stream get <stream-name> <sequence>` shows one message with the time it was stored.
  [Streams](https://docs.nats.io/learn/jetstream/your-first-stream).
- **Deliver policy:** a consumer's `deliver_policy` decides where it starts reading. The server
  fixes the start position when it creates the consumer and keeps its own position from then
  on. [Consumers](../configuration.md#consumers).
- **The provisioner sends the full consumer config on every run.** For a deliver policy that
  means: the start position can't be edited later, a changed value makes the run fail. That is
  why a replay always gets a consumer of its own.
  [How consumers are updated](../concepts.md#how-consumers-are-updated).
- **A replay can only reach messages the stream still holds.** Messages removed by `max_age`,
  `max_msgs` or `max_bytes` are gone. See [Limit how much a stream keeps](limit-stream-size-and-age.md).

The deliver policies the provisioner accepts:

| `deliver_policy` | The consumer starts at |
|---|---|
| `all` (default) | the first message the stream holds |
| `last` | the last message in the stream |
| `new` | the next message published after the consumer was created |
| `by_start_sequence` | the message with sequence `opt_start_seq` |
| `by_start_time` | the first message stored at or after `opt_start_time` |

## Steps

### 1. Find the starting point

Look up the sequence or the time you want to start from. Here the stream `EVENTS` holds five
messages, published one second apart:

```console
$ nats stream get EVENTS 3
Item: EVENTS#3 received 2026-11-15 17:00:10.493717641 +0000 UTC (3.06s) on Subject events.order

event 3
```

### 2. Add a new consumer for the replay

Add a consumer with a name that says what it is for, next to the consumers that already exist.
Don't change the deliver policy of a consumer your application already uses: the server refuses
that (see [Pitfalls](#pitfalls)).

To start at a sequence, use `by_start_sequence` with `opt_start_seq`. To start at a time, use
`by_start_time` with `opt_start_time` in [RFC 3339](https://www.rfc-editor.org/info/rfc3339)
format, a date and time with a time zone, such as `2026-11-15T17:00:10Z` or
`2026-11-15T18:00:10+01:00`:

```yaml
streams:
  - name: EVENTS
    subjects:
      - "events.>"
    max_age: 7d
    consumers:
      - name: replay-from-3
        deliver_policy: by_start_sequence
        opt_start_seq: 3
      - name: replay-since-1700
        deliver_policy: by_start_time
        opt_start_time: "2026-11-15T17:00:10Z"
```

`opt_start_seq` and `opt_start_time` count only with their own deliver policy. With any other
policy the provisioner ignores them without a warning.

### 3. Decide how fast to replay

`replay_policy` controls the pace. `instant`, the default, delivers the messages as fast as the
application fetches them. `original` delivers them with the gaps they were published with, which
helps when you want to reproduce a load pattern:

```yaml
streams:
  - name: EVENTS
    subjects:
      - "events.>"
    max_age: 7d
    consumers:
      - name: replay-original
        deliver_policy: by_start_sequence
        opt_start_seq: 3
        replay_policy: original
```

Messages 3 to 5 of `EVENTS` were published one second apart, so fetching all three from this
consumer with `nats consumer next EVENTS replay-original --ack --count 3` took about two
seconds.

### 4. Provision and point the application at the new consumer

Run the provisioner. It creates the new consumers and leaves the others as they were (log lines
are trimmed; real lines also carry `time`):

```json
{"level":"INFO","msg":"Updating stream","stream":"EVENTS"}
{"level":"INFO","msg":"Ensuring consumer","stream":"EVENTS","consumer":"replay-from-3"}
{"level":"INFO","msg":"Ensuring consumer","stream":"EVENTS","consumer":"replay-since-1700"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}
```

Then configure the application to fetch from the new consumer's name.

### 5. Remove the replay consumer when it is done

The provisioner never deletes anything, so the consumer stays on the server after the replay,
holding its position. Remove it from the config first, then from the server:

```console
$ nats consumer rm EVENTS replay-from-3 -f
```

Keep this order. If the consumer is still in the config, the next provisioner run creates it
again, and it starts over at `opt_start_seq`.

## Check that it works

These checks ran against nats-server v2.12.15 with the
[NATS command line tool](https://github.com/nats-io/natscli) (`nats`). The
`--ack` fetches move the consumer forward, so run them on a test server, or on a replay
consumer the application doesn't use yet.

The consumer starts where you asked (output trimmed):

```console
$ nats consumer info EVENTS replay-from-3
…
                    Name: replay-from-3
               Pull Mode: true
          Deliver Policy: From Sequence 3
              Ack Policy: Explicit
…
$ nats consumer info EVENTS replay-since-1700
…
          Deliver Policy: Since 2026-11-15 17:00:10 +0000 UTC
…
```

The first message either consumer hands out is message 3:

```console
$ nats consumer next EVENTS replay-from-3 --ack
[18:00:19] subj: events.order / tries: 1 / cons seq: 1 / str seq: 3 / pending: 2

event 3

Acknowledged message

$ nats consumer next EVENTS replay-since-1700 --ack
[18:00:19] subj: events.order / tries: 1 / cons seq: 1 / str seq: 3 / pending: 2

event 3

Acknowledged message
```

`str seq: 3` is the stream sequence, `pending: 2` the messages left after it (4 and 5).

For comparison, a consumer with `deliver_policy: last` handed out message 5 and nothing after it,
and one with `deliver_policy: new` handed out nothing, because no message was published after it
was created.

## Pitfalls

- **Editing the start of an existing consumer fails the run.** The server refuses to change
  `deliver_policy`, `opt_start_seq`, `opt_start_time` or `replay_policy` of an existing consumer.
  The run stops at that consumer with `Provisioning failed` and exit code 1; streams and
  consumers before it in the file are already provisioned, the ones after it are not:

  ```json
  {"level":"ERROR","msg":"Provisioning failed","error":"provisioning consumer \"replay-from-3\" on stream \"EVENTS\": nats: API error: code=500 err_code=10012 description=start sequence can not be updated"}
  ```

  The other changes fail the same way with `description=deliver policy can not be updated`,
  `description=start time can not be updated` or `description=replay policy can not be updated`.
  Revert the change and add a new consumer instead. Deleting the lines doesn't help either: a
  consumer field you remove goes back to its default, which is also a change.
- **`by_start_time` without `opt_start_time`, or `by_start_sequence` without `opt_start_seq`.**
  Validation stops the run before it connects to NATS:

  ```json
  {"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].consumers[0]: deliver_policy 'by_start_time' requires opt_start_time"}
  ```

  For a missing `opt_start_seq` the text ends in
  `deliver_policy 'by_start_sequence' requires opt_start_seq`.
- **A time without a time zone, or with a space instead of `T`.** `opt_start_time` must be
  RFC 3339. `2026-11-15T17:00:10` (no zone) is rejected:

  ```json
  {"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].consumers[0].opt_start_time: invalid RFC3339 format \"2026-11-15T17:00:10\": parsing time \"2026-11-15T17:00:10\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"\" as \"Z07:00\""}
  ```

  `2026-11-15 17:00:10` fails with `cannot parse " 17:00:10" as "T"`. Add `Z` for UTC or an
  offset such as `+01:00`.
- **`inactive_threshold` on a replay consumer.** NATS deletes a consumer that nobody fetched
  from for that long, and the next provisioner run creates it again at the original start
  position, so the application replays the same messages a second time. Leave
  `inactive_threshold` out, or remove the consumer from the config as soon as the replay is done.
  See [Durable consumers and inactivity](../concepts.md#durable-consumers-and-inactivity).
- **Replaying a work queue.** A stream with `retention: workqueue` deletes messages once they are
  acknowledged, so there is nothing to replay, and it doesn't accept a second consumer for the
  same subjects. See [Set up a work queue](set-up-a-work-queue.md).

## Further reading

- [Consumers](../configuration.md#consumers) in the configuration reference, for
  `deliver_policy`, `opt_start_seq`, `opt_start_time` and `replay_policy`.
- [How consumers are updated](../concepts.md#how-consumers-are-updated) for why every run sends
  the full consumer config.
- NATS: [consumers, deliver policy and replay policy](https://docs.nats.io/learn/jetstream/pull-consumers).
- The code that builds the consumer: [internal/provisioner/provisioner.go](../../internal/provisioner/provisioner.go),
  and the validation: [internal/config/config.go](../../internal/config/config.go).
