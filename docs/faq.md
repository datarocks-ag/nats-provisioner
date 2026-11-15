# FAQ

After reading this page you can answer the questions that come up once the provisioner runs
for real. The provisioner reads a YAML config and brings NATS JetStream streams and their
consumers in line with it on every run; it only creates and updates, it never deletes.
[How it works](concepts.md) explains the run in full, and the
[configuration reference](configuration.md) lists every field.

Log output on this page is trimmed to the relevant fields; real lines also carry `time`.
Outputs were captured with nats-server v2.12.15 (`nats:2.12-alpine`) and a pre-release build of 1.1.1.

- [Configuration errors](#configuration-errors)
  - [Why is my name rejected with `contains invalid characters`?](#why-is-my-name-rejected-with-contains-invalid-characters)
  - [Why do I get `invalid duration "1.5d"`?](#why-do-i-get-invalid-duration-15d)
  - [Why does `by_start_time` fail with `requires opt_start_time`?](#why-does-by_start_time-fail-with-requires-opt_start_time)
  - [Why does a consumer fail with `deliver_subject is not supported`?](#why-does-a-consumer-fail-with-deliver_subject-is-not-supported)
  - [I misspelled a field. Why is there no error?](#i-misspelled-a-field-why-is-there-no-error)
  - [Why does `max_msgs: ${LIMIT}` fail with `parsing config YAML`?](#why-does-max_msgs-limit-fail-with-parsing-config-yaml)
  - [Why is my stream literally named `${STREAM_NAME}`?](#why-is-my-stream-literally-named-stream_name)
- [Day-to-day behaviour](#day-to-day-behaviour)
  - [I removed a stream from the config. Why does it still exist?](#i-removed-a-stream-from-the-config-why-does-it-still-exist)
  - [I removed a field from a stream. Why didn't it reset?](#i-removed-a-field-from-a-stream-why-didnt-it-reset)
  - [And why did a consumer field reset?](#and-why-did-a-consumer-field-reset)
  - [Why does `strategy: create` still change my consumers?](#why-does-strategy-create-still-change-my-consumers)
  - [I changed `storage` and the run succeeded. Why is nothing different?](#i-changed-storage-and-the-run-succeeded-why-is-nothing-different)
  - [Is it safe to run on every deploy?](#is-it-safe-to-run-on-every-deploy)
  - [Can I preview changes?](#can-i-preview-changes)
  - [Why did my consumer disappear?](#why-did-my-consumer-disappear)
  - [Why is `LOG_LEVEL=DEBUG` not more verbose?](#why-is-log_leveldebug-not-more-verbose)
- [Connection and authentication](#connection-and-authentication)
  - [What happens if NATS isn't up yet?](#what-happens-if-nats-isnt-up-yet)
  - [Why does a wrong password take 5 minutes to fail?](#why-does-a-wrong-password-take-5-minutes-to-fail)
  - [I set `NATS_USER`. Why am I not authenticated?](#i-set-nats_user-why-am-i-not-authenticated)
  - [Which credentials win?](#which-credentials-win)
  - [Can I use a creds file, nkey or client certificate?](#can-i-use-a-creds-file-nkey-or-client-certificate)
  - [Can I list several servers?](#can-i-list-several-servers)
  - [Can credentials end up in logs?](#can-credentials-end-up-in-logs)
  - [Why does provisioning fail with `no responders available for request`?](#why-does-provisioning-fail-with-no-responders-available-for-request)
- [Operations](#operations)
  - [What do the exit codes mean?](#what-do-the-exit-codes-mean)
  - [Does a failed run leave partial state?](#does-a-failed-run-leave-partial-state)
  - [Which image tag should I use?](#which-image-tag-should-i-use)

## Configuration errors

All of these, except the `deliver_subject` error, come from loading the config, which
happens before the provisioner connects to NATS. You can reproduce them without a server,
and they are safe to try anywhere. Validation stops at the first error.

### Why is my name rejected with `contains invalid characters`?

Because stream and consumer names must not contain whitespace, `.`, `*`, `>`, `/` or `\`.

NATS uses `.` to separate subject tokens and `*` and `>` as wildcards, and stream and consumer
names become part of the subjects of the JetStream API, so these characters would break
them. Use `-` or `_` instead (`ORDERS_V1`, not `orders.v1`). The error names the entry by its
position in the file:

```yaml
streams:
  - name: orders.v1
    subjects: ["orders.>"]
```

```json
{"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].name: contains invalid characters (whitespace, '.', '*', '>', '/', '\\')"}
```

Subjects are a different matter: `orders.>` is a valid subject, and the provisioner doesn't
check subject syntax at all.

Further reading: [Validation](configuration.md#validation)

### Why do I get `invalid duration "1.5d"`?

Because days must be a whole number on their own: `7d` works, `1.5d`, `1d12h` and `7D` don't.

Durations (`max_age`, `duplicate_window`, `ack_wait`, `inactive_threshold`) accept Go
duration strings such as `30s`, `5m`, `36h` or `1h30m`, plus the extra form `<integer>d`.
Anything else goes to Go's parser, which knows no `d` unit:

```json
{"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].max_age: invalid duration \"1.5d\": time: unknown unit \"d\" in duration \"1.5d\""}
```

Write `36h` for one and a half days.

Further reading: [Durations](configuration.md#durations)

### Why does `by_start_time` fail with `requires opt_start_time`?

Because a consumer that starts at a point in time needs that point: set `opt_start_time` to an
RFC 3339 timestamp.

```json
{"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].consumers[0]: deliver_policy 'by_start_time' requires opt_start_time"}
```

The same holds for `by_start_sequence` and `opt_start_seq`. This config loads:

```yaml
streams:
  - name: ORDERS
    subjects: ["orders.>"]
    consumers:
      - name: replay
        deliver_policy: by_start_time
        opt_start_time: "2026-11-01T00:00:00Z"
```

Further reading: [Replay messages from a sequence or a point in time](howto/replay-messages-from-a-point.md)

### Why does a consumer fail with `deliver_subject is not supported`?

Because the provisioner only creates pull consumers; `deliver_subject` and `deliver_group`
(push consumers) are rejected.

Both fields pass validation, so the error only appears during the run, when the provisioner
reaches the consumer. Everything provisioned before it in that run stays in place:

```json
{"level":"INFO","msg":"Updating stream","stream":"ORDERS"}
{"level":"ERROR","msg":"Provisioning failed","error":"provisioning consumer \"notify\" on stream \"ORDERS\": deliver_subject is not supported (JetStream v2 API supports pull consumers only)"}
```

Remove the field and have your application fetch messages from the consumer (pull) instead.

Further reading: [Consumers](configuration.md#consumers),
[JetStream consumers](https://docs.nats.io/learn/jetstream/pull-consumers)

### I misspelled a field. Why is there no error?

Because unknown keys are ignored when the YAML is read, so a misspelled field is silently
dropped.

This config loads, and the stream is created without a message limit:

```yaml
streams:
  - name: ORDERS
    subjects: ["orders.>"]
    max_mesgs: 1000   # typo: should be max_msgs
```

```json
{"level":"INFO","msg":"Configuration loaded","streams":1}
```

After a change, check that the server has what you meant. This only reads, so it's safe
anywhere:

```console
$ nats stream info ORDERS
```

Further reading: [Configuration reference](configuration.md#streams)

### Why does `max_msgs: ${LIMIT}` fail with `parsing config YAML`?

Because `${VAR}` expansion only works in string fields; numbers and booleans can't come from
environment variables.

The YAML is parsed before any expansion, and `${LIMIT}` isn't a number:

```json
{"level":"ERROR","msg":"Failed to load configuration","error":"parsing config YAML: yaml: unmarshal errors:\n  line 4: cannot unmarshal !!str `${LIMIT}` into int64"}
```

Write the number into the config. If it differs per environment, keep one config file per
environment for that value.

Further reading: [Environment variable expansion](configuration.md#environment-variable-expansion),
[Use one config for several environments](howto/use-one-config-for-several-environments.md)

### Why is my stream literally named `${STREAM_NAME}`?

Because `STREAM_NAME` wasn't set when the provisioner ran; an unset variable stays in the
config as literal text, and that text is a valid name.

There is no error and no default syntax (`${VAR:-default}` isn't supported). A variable set to
an empty string becomes `""`, which then fails validation (`name: is required`). With
`STREAM_NAME` unset, this config creates a stream called `${STREAM_NAME}`:

```yaml
streams:
  - name: ${STREAM_NAME}
    subjects: ["${STREAM_NAME}.>"]
```

```json
{"level":"INFO","msg":"Creating stream","stream":"${STREAM_NAME}"}
```

Set the variable and re-run. The provisioner doesn't delete the wrongly named stream; remove it
with the NATS command-line tool (CLI). This deletes the stream and its messages and can't be
undone, so check the name first:

```console
$ nats stream rm '${STREAM_NAME}' -f
```

Further reading: [Environment variable expansion](configuration.md#environment-variable-expansion),
[Use one config for several environments](howto/use-one-config-for-several-environments.md)

## Day-to-day behaviour

### I removed a stream from the config. Why does it still exist?

Because the provisioner never deletes anything; you delete the stream yourself.

Removing a line from the config must never be able to throw away messages. The same holds for
consumers. After taking `AUDIT` out of the config, the next run touches only what is listed,
and the stream remains:

```console
$ nats stream ls -n
AUDIT
ORDERS
```

To delete it, first remove it from the config (or the next run creates it again), then run
`nats stream rm <stream-name>`, or `nats consumer rm <stream-name> <consumer-name>` for a
consumer. Both delete data and can't be undone.

Further reading: [Nothing is deleted](concepts.md#nothing-is-deleted)

### I removed a field from a stream. Why didn't it reset?

Because a stream update keeps the server's value for every field you leave out; to change a
field, set it explicitly.

The provisioner starts from the stream's current configuration on the server and overlays only
the fields present in the YAML. `subjects` is the exception: the server always gets exactly
the list in the config. A stream created with `discard: new` and `max_msgs: 1000` keeps both
after you delete the two lines:

```console
$ nats stream info ORDERS
…
               Discard Policy: New
             Maximum Messages: 1,000
…
```

(Output trimmed.) Write `discard: old` or `max_msgs: -1` to go back to the defaults.

Further reading: [How streams are updated](concepts.md#how-streams-are-updated),
[Limit how much a stream keeps](howto/limit-stream-size-and-age.md)

### And why did a consumer field reset?

Because consumers work the other way round: every run sends the consumer's full configuration,
and a field missing from the YAML gets its default.

After removing `max_deliver: 5` from the consumer `billing`, the next run sets it back to
unlimited (`-1`), and `nats consumer info ORDERS billing` no longer shows `Maximum Deliveries`.
For the same reason, a consumer setting changed by hand with the `nats` CLI is overwritten on
the next run. Keep every consumer setting you rely on in the config.

Further reading: [How consumers are updated](concepts.md#how-consumers-are-updated)

### Why does `strategy: create` still change my consumers?

Because `strategy` only applies to streams; consumers are created or updated on every run.

With `create`, an existing stream is skipped, but its consumers are still sent:

```json
{"level":"INFO","msg":"Skipping existing stream (strategy=create)","stream":"ORDERS"}
{"level":"INFO","msg":"Ensuring consumer","stream":"ORDERS","consumer":"billing"}
```

If a consumer should not be managed by the provisioner, leave it out of the config.

Further reading: [Strategy: `update` or `create`](concepts.md#strategy-update-or-create)

### I changed `storage` and the run succeeded. Why is nothing different?

Because NATS doesn't allow `storage` to change on an existing stream; the provisioner keeps the
server's value and logs a warning instead of failing.

The provisioner treats `retention`, `deny_delete` and `deny_purge` the same way. NATS itself
refuses `retention` changes too, and it allows turning `deny_delete` or `deny_purge` on later but
never off; the provisioner doesn't send either change. The run still exits with `0`, so look for
the warning:

```json
{"level":"WARN","msg":"Stream storage type mismatch (immutable, cannot be changed)","stream":"ORDERS","current":"file","desired":"memory"}
```

Changing `storage` or `retention`, or turning `deny_delete` or `deny_purge` off, means deleting
and re-creating the stream outside the provisioner, which loses its messages; this FAQ doesn't
describe that procedure.

Further reading: [Fields that can't change after creation](concepts.md#fields-that-cant-change-after-creation),
[JetStream streams](https://docs.nats.io/learn/jetstream/your-first-stream)

### Is it safe to run on every deploy?

Yes, that's what it's built for.

A second run on an unchanged config changes nothing: it updates each stream with the same
values and sends the same consumer configurations again. Nothing is ever deleted. Two things to
keep in mind: consumer settings changed by hand are reset to the config, and a change the
server refuses fails the run, for example a new `deliver_policy` on an existing consumer:

```json
{"level":"ERROR","msg":"Provisioning failed","error":"provisioning consumer \"billing\" on stream \"ORDERS\": nats: API error: code=500 err_code=10012 description=deliver policy can not be updated"}
```

Further reading: [A run, from start to finish](concepts.md#a-run-from-start-to-finish)

### Can I preview changes?

No, there is no dry-run.

You can check that a config is valid without touching a server: the config is loaded and
validated before the provisioner connects. Point `NATS_URL` at a port where nothing listens;
this is safe anywhere. An invalid config stops right away with exit code `1`; a valid one gets
as far as the first retry warning, then stop it with Ctrl-C:

```console
$ NATS_URL=nats://localhost:<unused-port> NATS_CONFIG_PATH=config.yaml ./nats-provisioner
```

```json
{"level":"INFO","msg":"Configuration loaded","streams":1}
{"level":"INFO","msg":"Connecting to NATS","url":"nats://localhost:<unused-port>"}
{"level":"WARN","msg":"NATS not ready, retrying","attempt":1,"max_attempts":16,"delay":1000000000,"error":"nats: no servers available for connection"}
```

To see what a run would change, compare the config with `nats stream info <stream-name>` and
`nats consumer info <stream-name> <consumer-name>`, or run it against a throwaway NATS server
first.

Further reading: [Validation](configuration.md#validation),
[Standalone binary](deployment.md#standalone-binary)

### Why did my consumer disappear?

Most likely because it has `inactive_threshold` set and no client used it for that long; NATS
then deletes it, even though it is durable.

The next run creates it again, but its position is gone: which messages were acknowledged is
lost, and it starts over according to its `deliver_policy`. Leave out `inactive_threshold` for
consumers that must survive quiet periods. With `inactive_threshold: 5s`, the consumer is gone
10 seconds after the run:

```console
$ nats consumer ls ORDERS -n
billing
report
$ sleep 10; nats consumer ls ORDERS -n
billing
```

Release 1.1.0 created all consumers as ephemeral, so NATS deleted them about 5 seconds after
the last client disconnected, with or without `inactive_threshold`. Release 1.1.1 fixes this
(see the changelog); its first run converts existing consumers to durable
ones and keeps their position.

Further reading: [Durable consumers and inactivity](concepts.md#durable-consumers-and-inactivity),
[CHANGELOG](../CHANGELOG.md)

### Why is `LOG_LEVEL=DEBUG` not more verbose?

Because the provisioner writes no debug messages, and because `LOG_LEVEL` is case-sensitive:
`DEBUG` isn't recognised and means `info`.

Valid values are `debug`, `info`, `warn` and `error`. `debug` and `info` print the same lines;
`warn` is the useful one, because it hides everything except warnings and errors:

```console
$ LOG_LEVEL=warn NATS_CONFIG_PATH=config.yaml ./nats-provisioner
```

```json
{"level":"WARN","msg":"Stream storage type mismatch (immutable, cannot be changed)","stream":"ORDERS","current":"file","desired":"memory"}
```

Further reading: [Environment variables](deployment.md#environment-variables)

## Connection and authentication

### What happens if NATS isn't up yet?

The provisioner retries with growing pauses for up to 5 minutes before it gives up.

The pause starts at 1 second and doubles up to 30 seconds. Each retry is logged as a warning;
`delay` is in nanoseconds:

```json
{"level":"WARN","msg":"NATS not ready, retrying","attempt":1,"max_attempts":16,"delay":1000000000,"error":"nats: no servers available for connection"}
{"level":"WARN","msg":"NATS not ready, retrying","attempt":2,"max_attempts":16,"delay":2000000000,"error":"nats: no servers available for connection"}
```

After 5 minutes the run ends with exit code `1` and
`connection timeout after 5m0s: context deadline exceeded` (written from the code, not
captured).

Further reading: [Connection retry](concepts.md#connection-retry),
[Docker Compose](deployment.md#docker-compose)

### Why does a wrong password take 5 minutes to fail?

Because the provisioner retries every connection error, and a rejected login is one of them.

It can't tell a server that isn't ready from one that refuses the credentials, so it keeps
trying for its 5-minute budget. The `error` field shows the real cause from the first attempt
on, so look there instead of waiting:

```json
{"level":"WARN","msg":"NATS not ready, retrying","attempt":1,"max_attempts":16,"delay":1000000000,"error":"nats: Authorization Violation"}
{"level":"WARN","msg":"NATS not ready, retrying","attempt":2,"max_attempts":16,"delay":2000000000,"error":"nats: Authorization Violation"}
{"level":"WARN","msg":"NATS not ready, retrying","attempt":3,"max_attempts":16,"delay":4000000000,"error":"nats: Authorization Violation"}
```

Further reading: [Connection retry](concepts.md#connection-retry),
[Authenticate to a secured NATS server](howto/authenticate-to-nats.md)

### I set `NATS_USER`. Why am I not authenticated?

Because user and password are only used when both `NATS_USER` and `NATS_PASSWORD` are set;
`NATS_USER` alone is ignored without a warning.

The provisioner then connects without credentials, and a secured server answers with
`nats: Authorization Violation` (see above). An empty variable counts as unset, so check that
the password really reaches the container, for example from your Secret.

Further reading: [Authentication](deployment.md#authentication),
[Authenticate to a secured NATS server](howto/authenticate-to-nats.md)

### Which credentials win?

Credentials in `NATS_URL` win, then `NATS_USER` with `NATS_PASSWORD`, then `NATS_TOKEN`.

The provisioner passes user and password if both are set, otherwise the token; it never sends
both. Credentials embedded in the URL (`nats://<user>:<password>@<host>:4222`) override either.
With a correct password in the URL and a wrong one in `NATS_PASSWORD`, the connection succeeds;
the other way round, it fails with `nats: Authorization Violation`:

```console
$ NATS_URL=nats://<user>:<password>@<host>:4222 NATS_USER=<user> NATS_PASSWORD=wrong ./nats-provisioner
```

```json
{"level":"INFO","msg":"Connected to NATS","url":"nats://redacted@<host>:4222"}
```

Set credentials in one place only, so you don't have to remember the order.

Further reading: [Authenticate to a secured NATS server](howto/authenticate-to-nats.md)

### Can I use a creds file, nkey or client certificate?

No. The provisioner supports user and password, a token, and credentials in the URL; nothing
else.

There are no settings for creds files (JWT), nkeys, a custom certificate authority or client
certificates. A `tls://` URL to a server whose certificate is signed by a public certificate
authority may work, because the image contains the usual CA certificates, but this isn't
tested.

Further reading: [Authenticate to a secured NATS server](howto/authenticate-to-nats.md),
[NATS authentication](https://docs.nats.io/learn/security/authentication-basics)

### Can I list several servers?

Yes: separate them with commas in `NATS_URL`; the provisioner connects to one that answers.

With the first server down and the second one up, the run connects right away:

```console
$ NATS_URL=nats://<host-1>:4222,nats://<host-2>:4222 ./nats-provisioner
```

```json
{"level":"INFO","msg":"Connected to NATS","url":"nats://<host-1>:4222,nats://<host-2>:4222"}
```

The log shows the whole list, not the server it picked.

Further reading: [Replicate a stream across a cluster](howto/replicate-a-stream.md),
[Environment variables](deployment.md#environment-variables)

### Can credentials end up in logs?

From release 1.1.1 on, the provisioner masks the credentials it knows about; release 1.1.0
logs credentials in `NATS_URL` in plain text.

In 1.1.1, user and password in `NATS_URL` are replaced by `redacted` in every
log line (`nats://redacted@<host>:4222`), and retry warnings also mask the value of
`NATS_PASSWORD`. The tool never logs `NATS_TOKEN` itself, but error messages aren't checked for
it. If you still run 1.1.0, prefer `NATS_USER` and `NATS_PASSWORD` over credentials in the
URL.

Further reading: [Logs and credentials](concepts.md#logs-and-credentials),
[CHANGELOG](../CHANGELOG.md)

### Why does provisioning fail with `no responders available for request`?

Because JetStream isn't enabled on the NATS server, so nothing answers the provisioner's
JetStream requests.

The connection succeeds, and the run fails at the first stream without retrying:

```json
{"level":"INFO","msg":"Connected to NATS","url":"nats://localhost:4222"}
{"level":"INFO","msg":"Starting provisioning"}
{"level":"ERROR","msg":"Provisioning failed","error":"provisioning stream \"ORDERS\": nats: no responders available for request"}
```

Start the server with JetStream, for example `nats-server --jetstream` or the `--jetstream`
argument in Docker Compose. This output was captured from a server started without
`--jetstream`; other causes of the same message weren't tested.

Further reading: [JetStream](https://docs.nats.io/concepts/jetstream),
[Docker Compose](deployment.md#docker-compose)

## Operations

### What do the exit codes mean?

`0`: everything was provisioned. `1`: something failed, whether config, connection or
provisioning.

The last log line says what (`Failed to load configuration`, `Failed to connect to NATS` or
`Provisioning failed`). A run stopped with SIGTERM or Ctrl-C also exits with `1`. Docker
Compose's `service_completed_successfully` and Kubernetes Jobs rely on this.

Further reading: [Exit codes](deployment.md#exit-codes)

### Does a failed run leave partial state?

Yes. The run stops at the first error and nothing is rolled back; fix the cause and run again.

Streams are handled in file order, each followed by its consumers. Whatever succeeded before
the error stays, and the next run continues from there. Here the stream was created before its
consumer failed:

```json
{"level":"INFO","msg":"Creating stream","stream":"SHIPMENTS"}
{"level":"ERROR","msg":"Provisioning failed","error":"provisioning consumer \"notify\" on stream \"SHIPMENTS\": deliver_subject is not supported (JetStream v2 API supports pull consumers only)"}
```

```console
$ nats stream ls -n
…
SHIPMENTS
```

Further reading: [The provisioning order](concepts.md#the-provisioning-order)

### Which image tag should I use?

A pinned release (`1.1.1`) or minor version (`1.1`) of `ghcr.io/datarocks-ag/nats-provisioner`.

Images are built only for releases. `latest` follows the newest release, and `sha-<short-sha>`
names the commit. Tags have no `v` prefix, and there is no major-only tag such as `1`.
Release 1.1.0 creates ephemeral consumers and logs credentials in `NATS_URL`; both are fixed
in 1.1.1 (see [Why did my consumer disappear?](#why-did-my-consumer-disappear)).

Further reading: [Image tags](deployment.md#image-tags)
