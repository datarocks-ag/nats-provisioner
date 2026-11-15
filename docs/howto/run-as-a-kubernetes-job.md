# Run the provisioner as a Kubernetes Job

After this, a Kubernetes Job creates your streams and consumers from a ConfigMap, logs in with
credentials from a Secret, and tells you through its status and exit code whether it worked.

## When you need this

Your NATS server runs in Kubernetes, and the streams your applications need should be there
before the applications start, the same way in every cluster. The provisioner does one run
and exits, which is what a Kubernetes Job is for. The repository has no Helm chart or
manifests, so this guide gives you a complete set to adapt.

## What you need to know first

- **One run, then exit.** The provisioner reads its config, connects, creates or updates
  every stream and consumer, and exits with code `0` on success or `1` on any failure
  (config, connection or provisioning). It has no command-line flags; everything is set with
  environment variables. [A run, from start to finish](../concepts.md#a-run-from-start-to-finish),
  [Exit codes](../deployment.md#exit-codes).
- **Job:** a Kubernetes object that runs a Pod until it exits successfully, starting a new Pod
  after a failure until `backoffLimit` is used up. A Job's Pod template can't be changed after
  creation, so running it again means a new Job.
  [Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/).
- **ConfigMap and Secret:** the config file goes into a ConfigMap mounted as a file; the NATS
  credentials go into a Secret loaded as environment variables, because the provisioner reads
  credentials only from `NATS_USER`/`NATS_PASSWORD` or `NATS_TOKEN`, not from files.
  [Authenticate to a secured NATS server](authenticate-to-nats.md).
- **The image:** `ghcr.io/datarocks-ag/nats-provisioner`. Pin a release tag such as `1.1`
  rather than `latest`, so a new release doesn't change behaviour under you.
  [Image tags](../deployment.md#image-tags). The image contains only the binary and CA
  certificates: no shell, so `kubectl exec` into it is not possible.
- **Connection retry:** the provisioner waits up to five minutes for NATS to accept the
  connection, so the Job doesn't need to wait for the NATS server itself.
  [Connection retry](../concepts.md#connection-retry).

## Steps

### 1. Put the config into a ConfigMap

The config is the same YAML you would mount in Docker Compose, embedded under one key:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: nats-provisioner-config
data:
  config.yaml: |
    streams:
      - name: ORDERS
        description: "Order events"
        subjects: ["orders.>"]
        max_age: "7d"
        consumers:
          - name: order-processor
            filter_subject: "orders.created"
            ack_policy: explicit
            ack_wait: "30s"
            max_deliver: 5
```

Validation runs before the provisioner connects to NATS, so a broken config fails the Job in
the first second, with the reason in the log. To check it before deploying, run the
standalone binary or the image locally against a throwaway NATS server.

### 2. Put the credentials into a Secret

Name the keys exactly like the environment variables, so the Job can load the whole Secret
with `envFrom`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: nats-provisioner-credentials
type: Opaque
stringData:
  NATS_USER: <nats-user>
  NATS_PASSWORD: <nats-password>
```

For token authentication, use a single key `NATS_TOKEN: <token>` instead. Create the Secret
from your secret store (Sealed Secrets, External Secrets, `kubectl create secret generic …`)
rather than committing it. Skip this step if your NATS server needs no authentication.

Don't put credentials into `NATS_URL`: release 1.1.0 writes the URL to the log as it is,
including any password in it, and URL credentials override the Secret's variables.

### 3. Create the Job

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: nats-provisioner
spec:
  backoffLimit: 1                  # one more Pod after a failure, see below
  activeDeadlineSeconds: 900
  ttlSecondsAfterFinished: 86400   # keep the finished Job and its logs for a day
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsNonRoot: true
        runAsUser: 65534
        runAsGroup: 65534
      containers:
        - name: nats-provisioner
          image: ghcr.io/datarocks-ag/nats-provisioner:1.1
          env:
            - name: NATS_URL
              value: nats://nats:4222
            - name: NATS_CONFIG_PATH
              value: /config/config.yaml
          envFrom:
            - secretRef:
                name: nats-provisioner-credentials
          volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          resources:
            requests:
              cpu: 10m
              memory: 32Mi
            limits:
              memory: 64Mi
      volumes:
        - name: config
          configMap:
            name: nats-provisioner-config
```

Why these values:

- **`NATS_URL`** points at a Service named `nats` in the same namespace. For a server in
  another namespace use `nats://<service>.<namespace>:4222`; for a cluster, list several URLs
  separated by commas.
- **`NATS_CONFIG_PATH`**: the default is `./config.yaml`, relative to the working directory,
  which is `/` in this image. Mounting the ConfigMap as a directory and naming the file is
  clearer than mounting it onto `/config.yaml` with `subPath`, and both work.
- **`restartPolicy: Never`** keeps each failed Pod, so you can read its log. With `OnFailure`
  the container restarts inside the same Pod and the previous log is harder to get.
- **`backoffLimit: 1` and `activeDeadlineSeconds: 900`.** The provisioner already retries the
  connection for five minutes inside one Pod. A wrong password or an unreachable server
  therefore takes about five minutes per Pod to fail, and another Pod rarely helps. One extra
  Pod covers a lost node; the deadline (15 minutes) stops the Job even if something hangs.
- **`ttlSecondsAfterFinished`** deletes the Job and its Pods a day after they finish, which
  also makes room to create a Job with the same name again.
  [TTL after finished](https://kubernetes.io/docs/concepts/workloads/controllers/ttlafterfinished/).
- **The security settings** are optional. The image's default user is root, but the
  provisioner needs no privileges: it reads one file and opens a network connection. The
  `1.1` image ran with user 65534, a read-only root filesystem and all capabilities dropped
  (checked with `docker run`).

Apply the three manifests. These were validated with a server-side dry run against Kubernetes
v1.33.5 (k3s):

```console
$ kubectl apply -f nats-provisioner.yaml
configmap/nats-provisioner-config created
secret/nats-provisioner-credentials created
job.batch/nats-provisioner created
```

### 4. Run it again after a change

A Job runs once. After you change the ConfigMap, the Secret or the image tag, the existing Job
does nothing, and `kubectl apply` with a changed Pod template is refused (output trimmed):

```console
$ kubectl apply -f nats-provisioner.yaml
…
secret/nats-provisioner-credentials configured
The Job "nats-provisioner" is invalid: spec.template: Invalid value: core.PodTemplateSpec{…}: field is immutable
```

Running it again is safe: the provisioner creates what is missing, updates what exists and
deletes nothing. [Nothing is deleted](../concepts.md#nothing-is-deleted). Pick one of these:

- **Delete and recreate** the Job with every deployment:

  ```console
  $ kubectl delete job nats-provisioner --ignore-not-found
  $ kubectl apply -f nats-provisioner.yaml
  ```

- **A new name per deployment**, e.g. `nats-provisioner-<release>` or a name with a hash of
  the config. `ttlSecondsAfterFinished` cleans up the old ones.
- **Let your deployment tool do it** (pointers only, not tested with this provisioner): Helm
  runs a Job as a hook with the annotation `helm.sh/hook: pre-install,pre-upgrade` and
  `helm.sh/hook-delete-policy: before-hook-creation`; Argo CD runs it as a sync hook with
  `argocd.argoproj.io/hook: PreSync` and `argocd.argoproj.io/hook-delete-policy: BeforeHookCreation`.
  See the [Helm](https://helm.sh/docs/topics/charts_hooks/) and
  [Argo CD](https://argo-cd.readthedocs.io/en/stable/user-guide/resource_hooks/) documentation.

**Alternative: an init container.** Instead of a Job, add the same container as an
`initContainers` entry to the Deployment of the application that needs the streams.
Kubernetes then starts the application only after the provisioner exited with `0`. The
trade-off: it runs on every Pod start of every replica, and a NATS outage blocks your
application's restarts for up to five minutes per attempt. A Job runs once per deployment.
[Init containers](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/).

## Check that it works

The Job shows `Complete` and `1/1` (expected output, written from the Kubernetes Job API;
the test cluster could not run Pods, see the note at the end of this section):

```console
$ kubectl get job nats-provisioner
NAME               STATUS     COMPLETIONS   DURATION   AGE
nats-provisioner   Complete   1/1           4s         20s
```

`STATUS` is `Running` while it works and `Failed` once `backoffLimit` or
`activeDeadlineSeconds` is used up. To block a deployment script until it finishes:
`kubectl wait --for=condition=complete job/nats-provisioner --timeout=15m`. That command
waits until the timeout if the Job fails, so check the status afterwards.

The log ends with `nats-provisioner finished successfully`. These lines were captured from the
`1.1` image running with this guide's settings (non-root, read-only, config at
`/config/config.yaml`, user/password from the environment) against nats-server v2.12.15; they
are trimmed, real lines also carry `time`:

```console
$ kubectl logs job/nats-provisioner
{"level":"INFO","msg":"Starting nats-provisioner","version":"v1.1.0"}
{"level":"INFO","msg":"Loading configuration","path":"/config/config.yaml"}
{"level":"INFO","msg":"Configuration loaded","streams":1}
{"level":"INFO","msg":"Connecting to NATS","url":"nats://nats:4222"}
{"level":"INFO","msg":"Connected to NATS","url":"nats://nats:4222"}
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Creating stream","stream":"ORDERS"}
{"level":"INFO","msg":"Ensuring consumer","stream":"ORDERS","consumer":"order-processor"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"nats-provisioner finished successfully"}
```

(The host name in the URL is this guide's `nats`; the captured run used `localhost`.)

The exit code of each Pod (`0` success, `1` failure), useful after a failed Job with several
Pods:

```console
$ kubectl get pods -l batch.kubernetes.io/job-name=nats-provisioner \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.containerStatuses[0].state.terminated.exitCode}{"\n"}{end}'
nats-provisioner-<pod-suffix> 0
```

Finally, check the result on the NATS side with the
[NATS command line tool](https://github.com/nats-io/natscli), from wherever
you can reach the server (e.g. after `kubectl port-forward svc/nats 4222:4222`):

```console
$ nats -s nats://localhost:4222 --user <nats-user> --password <nats-password> stream info ORDERS
Information for Stream ORDERS created …

                  Description: Order events
                     Subjects: orders.>
…
```

All commands in this section only read; they are safe in any cluster.

Note: the manifests were validated (server-side dry run) and created on a throwaway k3s
v1.33.5 cluster, but its node could not schedule Pods, so the `kubectl get job` result and the
`kubectl logs` / exit-code commands above were not run against a real Job.

## Pitfalls

- **With the 1.1.0 image, consumers disappear right after the Job.** Release 1.1.0 creates
  consumers without a durable name, so NATS deletes each one about five seconds after the last
  client disconnects, which for a Job is right after it finishes, unless your application is
  already consuming. Run with the 1.1.0 image against nats-server v2.12.15, the consumer was
  gone seven seconds after a successful run (`nats consumer ls ORDERS` printed
  `No Consumers defined`). From 1.1.1 on, consumers are durable and stay.
  [Durable consumers and inactivity](../concepts.md#durable-consumers-and-inactivity).
- **The Job hangs in `Running` for five minutes, then fails.** The log shows repeated
  `NATS not ready, retrying` warnings. `nats: Authorization Violation` in the `error` field
  means wrong or missing credentials (check the Secret's key names: `NATS_USER` alone is
  ignored without `NATS_PASSWORD`); a dial or DNS error means `NATS_URL` points to the wrong
  Service or namespace.
- **The Job fails at once with `Failed to load configuration`.** The config is invalid, or the
  file isn't where `NATS_CONFIG_PATH` says. `reading config file: open /config/config.yaml: no
  such file or directory` usually means the ConfigMap key isn't `config.yaml`.
- **The ConfigMap changed, nothing happened.** The Job read the file when its Pod started.
  Run the Job again (step 4).
- **The Job is gone and so are its logs.** `ttlSecondsAfterFinished` deletes the Job and its
  Pods. Ship logs to your log system, or increase the value while you set things up.
- **`latest` in the image tag.** Without a pinned tag, nodes may run different versions
  depending on what they cached. Pin `1.1` or an exact version like `1.1.1`.
- **No `NATS_URL` set.** The default is `nats://localhost:4222`, which inside a Pod is the Pod
  itself; the Job retries for five minutes and fails.

## Further reading

- [Kubernetes](../deployment.md#kubernetes), [Environment variables](../deployment.md#environment-variables)
  and [Exit codes](../deployment.md#exit-codes) in the deployment guide.
- [Authenticate to a secured NATS server](authenticate-to-nats.md) for which credentials win.
- [Use one config for several environments](use-one-config-for-several-environments.md) to
  share one ConfigMap across namespaces.
- [Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/) and
  [Secrets](https://kubernetes.io/docs/concepts/configuration/secret/) in the Kubernetes
  documentation.
- [cmd/nats-provisioner/main.go](../../cmd/nats-provisioner/main.go) for the environment
  variables and exit codes.
