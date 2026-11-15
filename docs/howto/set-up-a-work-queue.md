# Set up a work queue

After this, a stream holds jobs until a worker has processed them, any number of workers share
the jobs through one consumer, and a job a worker fails to acknowledge is handed out again.

## When you need this

Your application publishes jobs (resize an image, send an email) and a pool of workers
processes them. Each job must be done once, by one worker, and a job must not get lost when a
worker crashes halfway through. Once a job is done, there is no reason to keep it.

## What you need to know first

- **Work-queue retention:** a stream with `retention: workqueue` deletes a message as soon as a
  consumer has acknowledged it. The server allows only one consumer per subject on such a
  stream: either a single consumer without `filter_subject`, or several whose filters don't
  overlap. [Work-queue retention](https://docs.nats.io/learn/jetstream/retention-policies).
- **Pull consumer:** workers ask the consumer for the next messages (they "fetch"). Every worker
  that fetches from the same consumer gets different messages, so you scale by starting more
  workers, not by adding consumers. The provisioner supports only pull consumers.
  [Consumers](https://docs.nats.io/learn/jetstream/pull-consumers).
- **Acknowledgement (ack):** a worker tells the server it finished a message. With
  `ack_policy: explicit` every message needs its own ack, which a work-queue stream requires.
  `ack_wait` is how long the server waits for that ack before it hands the message out again;
  `max_deliver` caps how often it does that; `max_ack_pending` caps how many messages can be
  out unacknowledged at once, across all workers.
  [Consumers](../configuration.md#consumers).
- **Durable consumers:** the provisioner creates every consumer as durable, so it stays on the
  server while no worker is connected. This is a fix in release 1.1.1;
  release 1.1.0 created consumers that NATS deleted 5 seconds after the last client left.
  [Durable consumers and inactivity](../concepts.md#durable-consumers-and-inactivity).

## Steps

### 1. Declare the stream and its consumer

Put the stream and one consumer for all workers into your config:

```yaml
streams:
  - name: JOBS
    subjects:
      - "jobs.>"
    retention: workqueue
    consumers:
      - name: workers
        ack_policy: explicit     # the default, written out because the stream requires it
        ack_wait: 30s            # longer than your slowest job
        max_deliver: 5           # give up on a job after 5 attempts
        max_ack_pending: 100     # at most 100 jobs in progress at once
```

Set `ack_wait` longer than the slowest job takes. If a job runs longer, the server assumes the
worker died and hands the job to another worker while the first one is still on it, so the job
runs twice. Without `max_deliver`, a job that crashes every worker is handed out forever.

`retention` has to be right the first time: it can't change on an existing stream (see
[Pitfalls](#pitfalls)).

### 2. Provision it

Run the provisioner against the config. A first run creates the stream and the consumer
(log lines are trimmed; real lines also carry `time`):

```json
{"level":"INFO","msg":"Creating stream","stream":"JOBS"}
{"level":"INFO","msg":"Ensuring consumer","stream":"JOBS","consumer":"workers"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}
```

### 3. Point every worker at the same consumer

Every worker fetches from stream `JOBS`, consumer `workers`, and acknowledges each message
after it has finished the job, not before. A worker that acknowledges on receipt loses the job
when it crashes. How you fetch and acknowledge depends on your client library;
[Consumers](https://docs.nats.io/learn/jetstream/pull-consumers) links the client APIs.

To process different job types in separate pools, give each pool its own consumer with a
`filter_subject` that doesn't overlap with the others:

```yaml
streams:
  - name: TASKS
    subjects:
      - "tasks.>"
    retention: workqueue
    consumers:
      - name: resize-workers
        filter_subject: "tasks.resize"
      - name: email-workers
        filter_subject: "tasks.email"
```

## Check that it works

The checks below ran against nats-server v2.12.15 with the
[NATS command line tool](https://github.com/nats-io/natscli) (`nats`). Run
them on a test server: they publish and consume messages.

The consumer is a pull consumer with the settings from your config (output trimmed):

```console
$ nats consumer info JOBS workers
…
                    Name: workers
               Pull Mode: true
          Deliver Policy: All
              Ack Policy: Explicit
                Ack Wait: 30.00s
           Replay Policy: Instant
      Maximum Deliveries: 5
         Max Ack Pending: 100
…
```

Publish three jobs, then process two of them the way a worker would, fetching one message and
acknowledging it:

```console
$ nats pub jobs.resize "job 1"
$ nats pub jobs.resize "job 2"
$ nats pub jobs.resize "job 3"
$ nats consumer next JOBS workers --ack
[17:58:36] subj: jobs.resize / tries: 1 / cons seq: 1 / str seq: 1 / pending: 2

job 1

Acknowledged message

$ nats consumer next JOBS workers --ack
[17:58:36] subj: jobs.resize / tries: 1 / cons seq: 2 / str seq: 2 / pending: 1

job 2

Acknowledged message
```

The two acknowledged jobs are gone from the stream; only job 3 is left (output trimmed):

```console
$ nats stream info JOBS
…
                     Messages: 1
               First Sequence: 3 @ 2026-11-15 17:58:33
…
```

Two workers fetching at the same time get different jobs. Here two `nats consumer next
JOBS workers --ack --count 2` ran in parallel against four new jobs: one got jobs 1 and 3,
the other jobs 2 and 4, and the stream was empty afterwards.

### What happens to a job nobody acknowledges

To watch a redelivery without waiting 30 seconds, set `ack_wait: 2s` and `max_deliver: 2` on
the consumer in a test environment and run the provisioner again. Then fetch job 3 without
acknowledging it, as a crashing worker would:

```console
$ nats consumer next JOBS workers --no-ack
[17:58:42] subj: jobs.resize / tries: 1 / cons seq: 3 / str seq: 3 / pending: 0

job 3
$ sleep 3
$ nats consumer next JOBS workers --no-ack
[17:58:45] subj: jobs.resize / tries: 2 / cons seq: 4 / str seq: 3 / pending: 0

job 3
$ sleep 3
$ nats consumer next JOBS workers --no-ack --wait 2s
nats: error: no message received: nats: timeout
```

After `ack_wait` ran out, the server delivered job 3 again (`tries: 2`). After the second
attempt, `max_deliver: 2` was reached and the consumer stopped handing it out, so the third
fetch got nothing. (Depending on timing, the command line tool prints a `Status: 408`
Request Timeout reply instead of the error.) The job is **not deleted**: `nats stream info JOBS`
still shows `Messages: 1`.

The server announces the give-up on the subject
`$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>`. A subscription started in a
second terminal before the fetches received:

```console
$ nats sub '$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.JOBS.workers'
18:05:00 Subscribing on $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.JOBS.workers
[#1] Received on "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.JOBS.workers"
{"type":"io.nats.jetstream.advisory.v1.max_deliver","id":"SFnpQCKtPrOQdW0Ohr2Qnz","timestamp":"2026-11-15T17:05:06.652731876Z","stream":"JOBS","consumer":"workers","stream_seq":3,"deliveries":2}
```

Subscribe to it if you want to alert on jobs that failed for good; `stream_seq` tells you which
message to inspect with `nats stream get JOBS <sequence>`.

## Pitfalls

- **A second consumer for the same subjects is refused.** The server rejects a consumer whose
  filter overlaps with an existing one on a work-queue stream. The run stops there with exit
  code 1; the stream and every consumer before it are already provisioned:

  ```json
  {"level":"ERROR","msg":"Provisioning failed","error":"provisioning consumer \"resize-workers\" on stream \"JOBS\": nats: API error: code=400 err_code=10100 description=filtered consumer not unique on workqueue stream"}
  ```

  Two consumers without `filter_subject` fail with
  `description=multiple non-filtered consumers not allowed on workqueue stream` (err_code 10099).
  Start more workers on the one consumer instead, or split the subjects as in
  [step 3](#3-point-every-worker-at-the-same-consumer). Consumers that the config no longer
  lists stay on the server: the provisioner never deletes anything, so remove an old one with
  `nats consumer rm <stream-name> <consumer-name>` before you add a replacement.
- **`ack_policy: none` or `all` is refused.** A work-queue stream needs `explicit`. `none` fails
  with `description=consumer in pull mode requires explicit ack policy on workqueue stream`
  (err_code 10084), `all` with `description=workqueue stream requires explicit ack`
  (err_code 10098), both as `Provisioning failed`, exit code 1.
- **An existing stream doesn't become a work queue.** Adding `retention: workqueue` to a stream
  that was created with the default `limits` doesn't change it. The run logs a warning, keeps
  the server's value and still exits 0:

  ```json
  {"level":"WARN","msg":"Stream retention policy mismatch (immutable, cannot be changed)","stream":"ORDERS","current":"limits","desired":"workqueue"}
  ```

  Changing it means deleting and recreating the stream outside the provisioner, which deletes
  its messages. See [Fields that can't change after creation](../concepts.md#fields-that-cant-change-after-creation).
- **`inactive_threshold` deletes the consumer when no worker fetches.** Even a durable consumer
  is deleted by the server once no worker has fetched from it for that long and no message is
  waiting for an ack. From then on every worker's fetch fails until the next provisioner run
  creates the consumer again. The jobs themselves stay in the stream, and the new consumer hands
  them out with the delivery count starting at 1 again. Leave `inactive_threshold` out for a work
  queue whose workers can be down for a while, such as over a weekend.
- **Removing a line from the consumer resets the setting.** The provisioner sends the full
  consumer config on every run, so a field you delete goes back to the server's default. With
  `max_ack_pending` and `max_deliver` removed, the next run set `Max Ack Pending` back to 1,000
  and `max_deliver` to unlimited. See [How consumers are updated](../concepts.md#how-consumers-are-updated).
- **`strategy: create` doesn't protect consumers.** The strategy only decides whether an
  existing stream is updated; consumers are created or updated on every run regardless. See
  [Strategy: `update` or `create`](../concepts.md#strategy-update-or-create).

## Further reading

- [Consumers](../configuration.md#consumers) and [Streams](../configuration.md#streams) in the
  configuration reference, for every field used here.
- [Durable consumers and inactivity](../concepts.md#durable-consumers-and-inactivity) for what
  happens to a consumer between runs.
- [Replay messages from a sequence or a point in time](replay-messages-from-a-point.md), for a
  consumer that reads a stream from a chosen position instead of draining a queue.
- NATS: [work-queue retention](https://docs.nats.io/learn/jetstream/retention-policies),
  [consumers and acknowledgements](https://docs.nats.io/learn/jetstream/pull-consumers).
- The code that builds the consumer: [internal/provisioner/provisioner.go](../../internal/provisioner/provisioner.go).
