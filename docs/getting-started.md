# Getting started

This guide takes you from an empty NATS server to a JetStream stream with a consumer that an
application can rely on, all in Docker Compose. Along the way you'll see what a run logs, how
to check the result with the `nats` command-line tool, what happens when you change the config
and re-run, and how to keep your application from starting before its stream exists.

**The project:** you're building an order service. Orders are published on subjects like
`orders.created`, kept for a week in a stream called `ORDERS`, and processed by a worker that
reads them through a consumer called `order-processor`.

You'll need Docker with Compose v2 and the [`nats` CLI](https://github.com/nats-io/natscli).
Each step builds on the previous one, so keep the files you create.

> **Versions:** this guide uses the image tag `latest`. All tags are listed under
> [Deployment → Image tags](deployment.md#image-tags).
>
> Release 1.1.0 (the current `latest`) creates consumers as *ephemeral*: NATS deletes them about
> five seconds after the run, because no application is attached yet. With 1.1.0 the
> `nats consumer info` check in Step 1 and the read in Step 2 fail, because the consumer is
> already gone. From the next release on, consumers are durable and every step works as shown.
> [Durable consumers and inactivity](concepts.md#durable-consumers-and-inactivity) explains the
> difference.

## Step 1: A stream with one consumer

Start with the smallest useful config: one stream that stores every `orders.*` message for seven
days, and one consumer that hands the `orders.created` messages to your worker.

```yaml
# config.yaml
streams:
  - name: ORDERS
    subjects: ["orders.>"]
    max_age: 7d
    consumers:
      - name: order-processor
        filter_subject: orders.created
        ack_wait: 30s
        max_deliver: 5
```

A few things the config leaves to defaults: the stream keeps messages on disk (`storage: file`)
and drops the oldest ones when a limit is reached (`discard: old`); the consumer is a *pull*
consumer, which means your worker asks for messages instead of having them pushed, and it expects
every message to be acknowledged (`ack_policy: explicit`). A message that isn't acknowledged
within `ack_wait` is delivered again, at most `max_deliver` times. The
[Configuration reference](configuration.md) lists every field and its default.

Now the Compose file. NATS runs with JetStream enabled, which the provisioner needs. The
provisioner waits until NATS answers its health check, applies the config, and exits.

```yaml
# docker-compose.yaml
services:
  nats:
    image: nats:2.12-alpine
    command: ["--jetstream"]
    ports:
      - "4222:4222"
    healthcheck:
      test: ["CMD-SHELL", "echo 'PING' | nc localhost 4222 | grep -q PONG"]
      interval: 2s
      timeout: 5s
      retries: 10

  nats-provisioner:
    image: ghcr.io/datarocks-ag/nats-provisioner:latest
    depends_on:
      nats:
        condition: service_healthy
    environment:
      NATS_URL: nats://nats:4222
      NATS_CONFIG_PATH: /config.yaml
    volumes:
      - ./config.yaml:/config.yaml:ro
```

The health check sends `PING` to the NATS port and waits for `PONG`, so the provisioner starts
only once NATS accepts connections. The provisioner retries the connection for up to five minutes
anyway ([Connection retry](concepts.md#connection-retry)), so the health check mainly keeps the
log free of retry warnings.

Run it:

```console
$ docker compose run --rm nats-provisioner
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

(Log lines are JSON. The `time` field is left out here for readability. Compose's own progress
lines, such as `Container … Healthy`, are left out too.)

`docker compose run` starts NATS first, because the provisioner depends on it. Check what was
created. The `nats` CLI connects to `nats://127.0.0.1:4222` by default, which is the port the
Compose file publishes:

```console
$ nats stream info ORDERS
Information for Stream ORDERS created 2026-11-15 18:02:21

                     Subjects: orders.>
                     Replicas: 1
                      Storage: File
…
                    Retention: Limits
               Discard Policy: Old
…
                  Maximum Age: 7d0h0m0s
…
             Active Consumers: 1

$ nats consumer info ORDERS order-processor
Information for Consumer ORDERS > order-processor created 2026-11-15 18:02:21

Configuration:

                    Name: order-processor
               Pull Mode: true
          Filter Subject: orders.created
          Deliver Policy: All
              Ack Policy: Explicit
                Ack Wait: 30.00s
           Replay Policy: Instant
      Maximum Deliveries: 5
…
```

(`nats` output is trimmed with `…`; captured against nats-server v2.12.15.)

## Step 2: Send an order through it

A stream that exists isn't proof that it works. Publish an order and read it back through the
consumer, the way your worker will:

```console
$ nats pub orders.created '{"id":1001}'
18:02:32 Published 11 bytes to "orders.created"

$ nats consumer next ORDERS order-processor
[18:02:32] subj: orders.created / tries: 1 / cons seq: 1 / str seq: 1 / pending: 0

{"id":1001}

Acknowledged message
```

The stream stored the message, the consumer delivered it, and `nats consumer next` acknowledged
it. The consumer now remembers that position, so the message isn't delivered again:

```console
$ nats consumer info ORDERS order-processor
…
    Acknowledgment Floor: Consumer sequence: 1 Stream sequence: 1 Last Ack: 18ms ago
…
    Unprocessed Messages: 0
```

## Step 3: Change a setting and re-run

A week turns out to be too short; the support team wants two weeks of order history. Change
`max_age` in `config.yaml`:

```yaml
    max_age: 14d
```

…and run the provisioner again:

```console
$ docker compose run --rm nats-provisioner
…
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Updating stream","stream":"ORDERS"}
{"level":"INFO","msg":"Ensuring consumer","stream":"ORDERS","consumer":"order-processor"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}

$ nats stream info ORDERS | grep 'Maximum Age'
                  Maximum Age: 14d0h0m0s

$ nats consumer info ORDERS order-processor | grep 'Acknowledgment Floor'
    Acknowledgment Floor: Consumer sequence: 1 Stream sequence: 1 Last Ack: 5.29s ago
```

The stream wasn't recreated. It was *updated*: the new `max_age` was applied, and the message
you published in Step 2 is still there. The consumer was reconciled with the config too and kept
its position. This is the property everything else builds on: **you can run the provisioner on
every deploy**, and it brings NATS in line with the config instead of failing on things that
already exist.

Two limits to know about before you rely on it:

- A few stream settings can't change after creation (`storage`, `retention`, `deny_delete`,
  `deny_purge`). The provisioner keeps the server's value and logs a warning.
  [Fields that can't change after creation](concepts.md#fields-that-cant-change-after-creation)
  explains them.
- Nothing is ever deleted. Remove `ORDERS` from the config and the stream stays on the server.
  See [Nothing is deleted](concepts.md#nothing-is-deleted).

## Step 4: Start the application after the provisioner

The last piece is ordering. The order service must not start before its stream and consumer
exist. Compose expresses that with `service_completed_successfully`:

```yaml
  order-service:
    image: <your-registry>/order-service
    depends_on:
      nats-provisioner:
        condition: service_completed_successfully
    environment:
      NATS_URL: nats://nats:4222
```

`docker compose up -d` now starts NATS, waits for it to be healthy, runs the provisioner, and
starts `order-service` only if the provisioner exited with code `0`. If the config is broken,
the application doesn't start, and Compose tells you why. For example, with a typo in
`max_age` (`<project>` is the Compose project name, by default the directory name):

```console
$ docker compose up -d
…
 Container <project>-nats-provisioner-1 Error service "nats-provisioner" didn't complete successfully: exit 1
 Container <project>-order-service-1 not started: condition "service_completed_successfully" of service "nats-provisioner" not met
service "nats-provisioner" didn't complete successfully: exit 1

$ docker compose logs nats-provisioner
…
nats-provisioner-1  | {"level":"ERROR","msg":"Failed to load configuration","error":"validating config: streams[0].max_age: invalid duration \"14days\": time: unknown unit \"days\" in duration \"14days\""}
```

The config is validated before the provisioner connects to NATS, so a broken config never
leaves NATS half-provisioned. Fix the file and run `docker compose up -d` again.

## Where you are now

Your order stack has:

- a stream that keeps two weeks of order events,
- a durable pull consumer that delivers `orders.created` messages to your worker and remembers
  what was acknowledged,
- a provisioning step you can re-run on every deploy,
- an application that starts only after provisioning succeeded.

The final files:

<details>
<summary><code>config.yaml</code></summary>

```yaml
streams:
  - name: ORDERS
    subjects: ["orders.>"]
    max_age: 14d
    consumers:
      - name: order-processor
        filter_subject: orders.created
        ack_wait: 30s
        max_deliver: 5
```

</details>

<details>
<summary><code>docker-compose.yaml</code></summary>

```yaml
services:
  nats:
    image: nats:2.12-alpine
    command: ["--jetstream"]
    ports:
      - "4222:4222"
    healthcheck:
      test: ["CMD-SHELL", "echo 'PING' | nc localhost 4222 | grep -q PONG"]
      interval: 2s
      timeout: 5s
      retries: 10

  nats-provisioner:
    image: ghcr.io/datarocks-ag/nats-provisioner:latest
    depends_on:
      nats:
        condition: service_healthy
    environment:
      NATS_URL: nats://nats:4222
      NATS_CONFIG_PATH: /config.yaml
    volumes:
      - ./config.yaml:/config.yaml:ro

  order-service:
    image: <your-registry>/order-service
    depends_on:
      nats-provisioner:
        condition: service_completed_successfully
    environment:
      NATS_URL: nats://nats:4222
```

</details>

This NATS server keeps its data inside the container, so `docker compose down` loses the stream.
That's fine for trying things out; for a real deployment see [Deployment](deployment.md).

## Next steps

- **Understand what a run does.** [How it works](concepts.md) explains the order of steps, how
  streams and consumers are updated, what the provisioner never touches, and when to use
  `strategy: create`.
- **Look up a field.** The [Configuration reference](configuration.md) lists every stream and
  consumer field, duration formats and `${VAR}` expansion.
- **Run it for real.** [Deployment](deployment.md) covers image tags, Kubernetes, the standalone
  binary and authentication; [Run the provisioner as a Kubernetes Job](howto/run-as-a-kubernetes-job.md)
  walks through the Kubernetes setup, and
  [Authenticate to a secured NATS server](howto/authenticate-to-nats.md) adds credentials.
- **Tune the stream.** [Limit how much a stream keeps](howto/limit-stream-size-and-age.md) and
  [Set up a work queue](howto/set-up-a-work-queue.md) build on the stream from this guide.
- **Questions?** The [FAQ](faq.md) answers the ones that come up once it runs in production.
