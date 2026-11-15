# Protect a stream's messages from deletion and purge

After this, nobody can delete single messages from your stream or purge it through the NATS
API, and you know which protection this does not give you.

## When you need this

Some streams hold records that must stay until their limits expire them: an audit trail, a
ledger of payments, events other teams replay. Anyone with access to the NATS API can remove a
message with `nats stream rmm` or empty the whole stream with `nats stream purge`, on purpose
or by mistake. You want the server to refuse both.

## What you need to know first

- **`deny_delete` and `deny_purge`:** two stream settings. `deny_delete: true` makes the server
  refuse requests to delete individual messages; `deny_purge: true` makes it refuse purges of
  the stream or of a subject. Both default to `false`.
  [Altering stream state](https://docs.nats.io/learn/jetstream/altering-stream-state).
- **Set them at creation.** The provisioner sends `deny_delete` and `deny_purge` only when it
  creates a stream. On an existing stream it keeps the server's values and logs a warning if your
  YAML asks for something else.
  [Fields that can't change after creation](../concepts.md#fields-that-cant-change-after-creation).
- **Limits still apply.** The settings block delete and purge requests through the API. They
  don't stop the server from removing messages because of `max_age`, `max_msgs`, `max_bytes` or
  the retention policy. [Limit how much a stream keeps](limit-stream-size-and-age.md).

## Steps

### 1. Add the settings before the stream exists

Put both fields on the stream in the config that creates it:

```yaml
streams:
  - name: AUDIT
    subjects: ["audit.>"]
    max_age: 365d
    deny_delete: true
    deny_purge: true
```

Run the provisioner. The log must say `Creating stream`; only then are the settings sent:

```text
{"level":"INFO","msg":"Creating stream","stream":"AUDIT"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}
```

Log lines are trimmed; real lines also carry `time`.

### 2. If the stream already exists, read the warning

If the log says `Updating stream` instead, the stream existed already, and the settings were not
applied. The provisioner logs one warning per field and still exits 0:

```text
{"level":"WARN","msg":"Stream deny_delete mismatch (immutable, cannot be changed)","stream":"EVENTS","current":false,"desired":true}
{"level":"WARN","msg":"Stream deny_purge mismatch (immutable, cannot be changed)","stream":"EVENTS","current":false,"desired":true}
{"level":"INFO","msg":"Updating stream","stream":"EVENTS"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}
```

The warning calls the fields immutable, but on nats-server v2.12.15 the server itself does
accept turning protection **on** for an existing stream. It refuses turning it off. The
provisioner never sends the change either way. If you need the protection on an existing stream,
set it once by hand with the NATS command-line tool (CLI):

```console
$ nats stream edit EVENTS --deny-delete --deny-purge -f
```

This change can't be undone: a later `nats stream edit EVENTS --no-deny-delete -f` fails with
`stream configuration update can not cancel deny message deletes (10052)`. After the edit, the
YAML and the server agree, and the warnings stop.

## Check that it works

`nats stream info` shows the two settings as `Allows Msg Delete` and `Allows Purge` (output
trimmed):

```console
$ nats stream info AUDIT
…
            Allows Msg Delete: false
…
                 Allows Purge: false
…
```

Then try both operations against a test stream. On nats-server v2.12.15 they fail and the
message stays:

```console
$ nats stream rmm AUDIT 1 -f
nats: error: message delete not permitted (10057)
$ nats stream purge AUDIT -f
nats: error: could not purge Stream: stream purge not permitted (10110)
$ nats stream info AUDIT
…
                     Messages: 1
…
```

Run the `rmm` and `purge` commands only against a test stream: on an unprotected stream they do
delete data.

## Pitfalls

- **The stream itself can still be deleted.** `deny_delete` protects messages, not the stream.
  `nats stream rm AUDIT -f` removed a protected stream with all its messages on nats-server
  v2.12.15, without an error. Restrict who may call the stream delete API with NATS permissions
  if you need that. [Authorization](https://docs.nats.io/learn/security/authorization).
- **Protection is one way.** You can't turn it off again, neither with the provisioner (which
  keeps the server's value and logs `current: true, desired: false`) nor with `nats stream edit`.
  The only way back is deleting and recreating the stream, which loses its messages.
- **Removing the lines changes nothing.** Deleting `deny_delete` or `deny_purge` from the YAML
  leaves the server's value as it is, without a warning.
- **The run exits 0 on a mismatch.** A pipeline that only checks the exit code won't notice that
  the protection wasn't applied. Check the stream with `nats stream info` after the first run, or
  watch for `WARN` lines.
- **Messages still expire.** A protected stream with `max_msgs: 1` that received two messages held
  only the second one. Size the limits so records stay as long as you must keep them.

## Further reading

- [Fields that can't change after creation](../concepts.md#fields-that-cant-change-after-creation),
  for the other fields the provisioner never changes on an existing stream.
- [Streams](../configuration.md#streams) in the configuration reference.
- [Altering stream state](https://docs.nats.io/learn/jetstream/altering-stream-state) in the NATS docs, for deleting and purging messages.
- [`internal/provisioner/streams.go`](../../internal/provisioner/streams.go), where the warnings
  are logged.
