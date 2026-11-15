# Deduplicate published messages

After this, a message that a publisher sends twice, for example after a timeout and a retry, is
stored in your stream only once.

## When you need this

A publisher sends a message, the acknowledgement gets lost, and the publisher retries. Now the
stream holds the message twice, and every consumer processes it twice: a payment booked twice,
an email sent twice. You want the stream to recognise the retry and drop it.

## What you need to know first

- **Message ID:** the publisher gives each message a unique ID in the `Nats-Msg-Id` header. The
  server remembers the IDs it has stored and drops a second message with the same ID.
  [Avoiding duplicate writes](https://docs.nats.io/learn/jetstream/publishing#avoiding-duplicate-writes).
- **Duplicate window:** the server remembers IDs only for a limited time, set per stream with
  `duplicate_window`. A duplicate that arrives after the window is stored as a new message. The
  field takes a duration such as `10m` or `1h`. [Durations](../configuration.md#durations).
- **Two halves:** the provisioner configures the window on the stream. Setting the header is up
  to your publishers; without it, nothing is deduplicated.

## Steps

### 1. Choose the window

The window must cover the longest time between a first attempt and its last retry. If your
publishers retry for up to 5 minutes, 10 minutes leaves room. A longer window costs server
memory, because the server keeps every ID it has seen within the window.

If you leave `duplicate_window` out, the server uses its default: on nats-server v2.12.15,
`nats stream info` shows `Duplicate Window: 2m0s` for a stream created without the field. When
the stream's `max_age` is shorter than that, the server shortens the default to `max_age`
(a stream with `max_age: 1m` showed `Duplicate Window: 1m0s`).

### 2. Set the window on the stream

```yaml
streams:
  - name: PAYMENTS
    subjects: ["payments.>"]
    duplicate_window: 10m
```

Run the provisioner. On an existing stream the window is updated in place; leaving the line out
later keeps the server's current window, because stream updates merge
([How streams are updated](../concepts.md#how-streams-are-updated)). To go back to the server's
default, set `duplicate_window: "0s"`: on nats-server v2.12.15 that resulted in `2m0s` again.

### 3. Set `Nats-Msg-Id` in your publishers

Each publisher sets the header to an ID that is the same for every retry of one logical message
and different for every other message, such as a payment ID or an idempotency key from the
request. A random ID generated per attempt defeats the purpose: each retry gets a new ID and is
stored again.

How you set the header depends on your client library; see
[Publish from a client library](https://docs.nats.io/learn/jetstream/publishing#publish-from-a-client-library).

## Check that it works

The window is on the stream (output trimmed):

```console
$ nats stream info PAYMENTS
…
             Duplicate Window: 10m0s
…
```

Then publish the same message twice with the same ID. Use `--jetstream`, so that the CLI shows
the server's acknowledgement:

```console
$ nats pub --jetstream payments.received --header Nats-Msg-Id:pay-1001 '{"amount":42}'
17:59:24 Published 13 bytes to "payments.received"
17:59:24 Stored in Stream: PAYMENTS Sequence: 1
$ nats pub --jetstream payments.received --header Nats-Msg-Id:pay-1001 '{"amount":42}'
17:59:24 Published 13 bytes to "payments.received"
17:59:24 Stored in Stream: PAYMENTS Sequence: 1 Duplicate: true
$ nats stream info PAYMENTS
…
                     Messages: 1
…
```

The second publish is acknowledged with the first message's sequence and `Duplicate: true`, and
the stream holds one message. Run this against a test stream: it stores a message.

## Pitfalls

- **Window longer than `max_age`.** The server refuses it, and the run fails with exit code 1 at
  that stream:

  ```text
  {"level":"ERROR","msg":"Provisioning failed","error":"provisioning stream \"SHORT\": nats: API error: code=500 err_code=10052 description=duplicates window can not be larger then max age"}
  ```

  Shorten `duplicate_window` or lengthen `max_age`.
  [Limit how much a stream keeps](limit-stream-size-and-age.md).
- **No header, no deduplication.** The window alone does nothing. Messages without `Nats-Msg-Id`
  are all stored.
- **A new ID per retry.** Clients that generate the ID inside the retry loop store every retry.
  Generate it once per logical message.
- **Retries after the window.** A retry that arrives after `duplicate_window` has passed is stored
  as a new message. Keep the publisher's retry period shorter than the window.

## Further reading

- [Streams](../configuration.md#streams) and [Durations](../configuration.md#durations) in the
  configuration reference.
- [Avoiding duplicate writes](https://docs.nats.io/learn/jetstream/publishing#avoiding-duplicate-writes)
  in the NATS docs.
- [`internal/provisioner/provisioner.go`](../../internal/provisioner/provisioner.go), where
  `duplicate_window` is turned into the stream's setting.
