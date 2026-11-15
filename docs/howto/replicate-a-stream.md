# Replicate a stream across a cluster

After this, your stream is stored on several NATS servers, so it stays available and keeps its
messages when one server fails.

## When you need this

A stream with one replica lives on one server. When that server goes down, publishers and
consumers of the stream stop, and if its disk is lost, so are the messages. You run a NATS
cluster and want the provisioner to create the stream with copies on several servers, and to
reach the cluster even if one of the servers is down during the run.

## What you need to know first

- **Cluster:** several NATS servers connected to each other by routes. For replicated streams,
  every server needs JetStream enabled with its own storage directory, and all of them need the
  same cluster name and routes to each other.
  [Forming a cluster](https://docs.nats.io/learn/clustering/forming-a-cluster),
  [JetStream in a cluster](https://docs.nats.io/learn/topologies/jetstream-in-a-cluster).
- **Replicas:** `num_replicas` is the number of servers that hold a copy of the stream. One of
  them is the leader, the others follow it; a write is committed once a majority has it. With 3
  replicas, the stream survives the loss of one server. NATS allows at most 5.
  [Replication and R3](https://docs.nats.io/learn/clustering/replication-and-r3).
- **Several servers in `NATS_URL`:** `NATS_URL` takes a comma-separated list of server URLs.
  The provisioner hands the list to the NATS client, which connects to any server in it that
  answers. [Environment variables](../deployment.md#environment-variables).

## Steps

### 1. List every server in `NATS_URL`

Give the provisioner all servers of the cluster, not one, so that a single server being down
doesn't stop the run:

```yaml
environment:
  NATS_URL: nats://nats-1:4222,nats://nats-2:4222,nats://nats-3:4222
```

With one server of a three-node cluster stopped, a run with all three URLs connected and
finished without a retry. The log shows the URL list as given, with any credentials replaced by
`redacted` (other lines omitted):

```text
{"level":"INFO","msg":"Connecting to NATS","url":"nats://localhost:14441,nats://localhost:14442,nats://localhost:14443"}
{"level":"INFO","msg":"Connected to NATS","url":"nats://localhost:14441,nats://localhost:14442,nats://localhost:14443"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}
```

Log lines are trimmed; real lines also carry `time`.

### 2. Set `num_replicas` on the stream

Use an odd number no larger than the number of servers in the cluster. 3 is the usual choice:

```yaml
streams:
  - name: ORDERS
    subjects: ["orders.>"]
    num_replicas: 3
```

Run the provisioner. A new stream is created on three servers:

```text
{"level":"INFO","msg":"Creating stream","stream":"ORDERS"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}
```

Without `num_replicas`, a new stream gets 1 replica.

### 3. Change the replica count of an existing stream

The provisioner sends `num_replicas` on every update, so you can raise or lower it in the YAML
and run again. On nats-server v2.12.15, raising a stream with 3 messages from 1 to 3 replicas
was accepted; when checked two seconds later, the stream still held its 3 messages and both new replicas were
`current`.
Lowering it from 3 back to 1 was accepted as well, and the messages stayed.

Omitting `num_replicas` on an existing stream keeps its current count, because stream updates
merge. [How streams are updated](../concepts.md#how-streams-are-updated).

## Check that it works

`nats stream info` shows the replica count and, in the cluster section, the leader and the
other replicas (output trimmed):

```console
$ nats stream info ORDERS
Information for Stream ORDERS created 2026-11-15 17:59:50

                     Subjects: orders.>
                     Replicas: 3
                      Storage: File
…
Cluster Information:

                         Name: np
      Cluster Traffic Account: System Account
                Cluster Group: S-R3F-DgOUmuco
                       Leader: n3 (41ms)
                      Replica: n1, current, seen 41ms ago
                      Replica: n2, current, seen 41ms ago
…
```

Every replica should be `current`. The server names are the ones the NATS servers were started
with. Safe to run anywhere; it only reads.

## Pitfalls

- **More replicas than servers.** On a three-node cluster, `num_replicas: 5` fails the run with
  exit code 1, both for a new stream and when raising an existing one:

  ```text
  {"level":"ERROR","msg":"Provisioning failed","error":"provisioning stream \"BIG\": nats: API error: code=400 err_code=10005 description=no suitable peers for placement"}
  ```

  The provisioner stops at that stream; streams after it in the file are not provisioned. Fix the
  number and run again.
- **More than 5 replicas.** The server refuses it with
  `nats: API error: code=500 err_code=10052 description=maximum replicas is 5`.
- **A single server is not a cluster.** Against a NATS server without clustering, any
  `num_replicas` above 1 fails with
  `nats: API error: code=500 err_code=10074 description=replicas > 1 not supported in non-clustered mode`.
  If your development server is a single node, give it its own config file: `${VAR}` expansion
  works only on string fields, and `num_replicas` is a number
  ([Use one config for several environments](use-one-config-for-several-environments.md)).
- **Only one server in `NATS_URL`.** If that server is down, the provisioner retries for up to
  5 minutes and then fails, although the rest of the cluster is up.
  [Connection retry](../concepts.md#connection-retry).
- **Consumers get no replica setting.** The provisioner has no `num_replicas` field for
  consumers; how the server places them is up to NATS.

## Further reading

- [Streams](../configuration.md#streams) in the configuration reference.
- [Environment variables](../deployment.md#environment-variables), for `NATS_URL`.
- [JetStream in a cluster](https://docs.nats.io/learn/topologies/jetstream-in-a-cluster) and
  [Replication and R3](https://docs.nats.io/learn/clustering/replication-and-r3) in the NATS docs.
- [`internal/client/client.go`](../../internal/client/client.go), where `NATS_URL` is passed to the
  NATS client.
