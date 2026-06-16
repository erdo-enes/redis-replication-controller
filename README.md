# redis-replication-controller

A Kubernetes-native controller that runs **plain Redis replication** (no Redis
Sentinel, no Redis Cluster) and gives application clients a single, stable write
endpoint:

```
Application
  └─▶ normal Redis client
        └─▶ redis-write Service (ClusterIP)
              └─▶ the current Redis master Pod
```

The controller watches the Redis Pods, keeps exactly one of them as master, and
moves a Kubernetes label so the `redis-write` Service always routes to that
master. When the master fails, it promotes the best replica and re-points the
Service.

---

## Table of contents

- [Problem statement](#problem-statement)
- [Why not Sentinel?](#why-not-sentinel)
- [Architecture](#architecture)
- [How routing works](#how-routing-works)
- [Controller responsibilities](#controller-responsibilities)
- [Reconciliation logic](#reconciliation-logic)
- [Failover flow](#failover-flow)
- [Split-brain protection](#split-brain-protection)
- [Important limitations (read this)](#important-limitations-read-this)
- [Environment variables](#environment-variables)
- [Build](#build)
- [Deploy](#deploy)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Production warnings](#production-warnings)
- [Repository layout](#repository-layout)

---

## Problem statement

Standard Redis high availability uses **Redis Sentinel**: clients connect to
Sentinel, ask "who is the master?", and reconnect when the master changes. This
requires a **Sentinel-aware client**.

Many applications use a **plain Redis client** that only knows how to connect to
a single host:port and does not speak the Sentinel protocol. For those clients
we need a fixed address that always points at the current master, and something
must keep that address correct as the master changes.

This controller provides that "something" using only:

- native Redis replication commands (`REPLICAOF`, `REPLICAOF NO ONE`, `ROLE`,
  `INFO replication`), and
- a Kubernetes `Service` whose label selector is updated to follow the master.

## Why not Sentinel?

| Concern | Sentinel | This controller |
| --- | --- | --- |
| Client requirement | Sentinel-aware client | **Any** plain Redis client |
| Master discovery | Client asks Sentinel | Kubernetes Service selector |
| Failover decision | Sentinel quorum | Controller reconcile loop + leader election |
| Topology config | Sentinel | Native `REPLICAOF` |

We are **not** using Sentinel because the application clients cannot use it.
Instead, application clients connect to a normal `Service` DNS name:

```
redis-write.<namespace>.svc.cluster.local:6379
```

> ⚠️ This is a deliberate trade-off. Sentinel and Redis Cluster are the standard,
> battle-tested options. See [Production warnings](#production-warnings).

## Architecture

```
                         ┌─────────────────────────────┐
   application  ───────▶ │ Service: redis-write         │
   (plain client)        │ selector:                    │
                         │   app=redis                  │
                         │   redis-current-role=master  │
                         └──────────────┬──────────────┘
                                        │ (selector matches exactly one Pod)
                                        ▼
   ┌───────────────┐   REPLICAOF   ┌───────────────┐   REPLICAOF   ┌───────────────┐
   │ redis-0       │◀──────────────│ redis-1       │               │ redis-2       │
   │ role: master  │               │ role: replica │               │ role: replica │
   │ label: master │               │ label: replica│               │ label: replica│
   └───────▲───────┘               └───────────────┘               └───────────────┘
           │  watch + PING + INFO replication + REPLICAOF + label patch
           │
   ┌───────┴────────────────────────────────────────────────────────────────────┐
   │ redis-replication-controller (Deployment, 2 replicas, 1 active via Lease)    │
   └──────────────────────────────────────────────────────────────────────────────┘
```

- **Redis Pods** run as a StatefulSet of ordinary `redis-server` processes
  (`redis-0`, `redis-1`, …). No Sentinel sidecars.
- **The controller** runs as a Deployment using in-cluster config and leader
  election, so multiple replicas are safe but only one performs failover.

## How routing works

The `redis-write` Service uses this selector:

```yaml
selector:
  app: redis
  redis-current-role: master
```

Kubernetes only adds **ready** Pods matching that selector to the Service's
EndpointSlice. The controller guarantees that **at most one** Pod ever carries
`redis-current-role=master`, so the Service resolves to exactly the current
master. All other Pods carry `redis-current-role=replica` (or no role label
yet).

A separate `redis-read` Service selects `app=redis` (all Pods) if you want to
send read-only traffic to replicas.

## Controller responsibilities

1. Discover Redis Pods via a label selector.
2. Health-check each Pod with `PING` and read its role via `INFO replication`.
3. Decide whether a single valid master exists.
4. Maintain the single-master invariant:
   - exactly one Pod labeled `redis-current-role=master`,
   - all other healthy Pods labeled `redis-current-role=replica` and following
     the master via `REPLICAOF <master-ip> <port>`.
5. Bootstrap an initial master when none exists.
6. Detect master failure, wait a configurable threshold, then promote the best
   replica with `REPLICAOF NO ONE` and verify it via `ROLE`.
7. Reconfigure a recovered old master as a replica.
8. Update the Kubernetes label so `redis-write` follows the new master.
9. Fail safe: never promote during a Kubernetes API outage, never label a Pod
   master whose role it cannot verify, and refuse to act on an ambiguous
   split-brain.

## Reconciliation logic

Each pass (`internal/controller/controller.go`) first groups the discovered Pods
into independent replication sets (by `REDIS_SET_LABEL_KEY`) and then runs the
logic below **per set**, against that set's own state. It is **idempotent** and
decides based on two facts per Pod: the **Kubernetes label** and the **observed
Redis role**.

```
list Pods ──(API error)──▶ return error, DO NOT touch Redis      (fail safe)
   │
   ├─ probe each Pod: PING + INFO replication  (unverifiable ⇒ treated unhealthy)
   │
   ├─ authoritative master = the single labeled Pod that is also a healthy
   │  Redis master
   │
   ├─ if authoritative exists       ──▶ converge to it
   │     (also safely demotes any extra masters — see split-brain)
   ├─ else if exactly 1 real master ──▶ adopt it (fix missing/stale label)
   ├─ else if 0 real masters        ──▶ bootstrap (fresh) OR failover (known master)
   └─ else (≥2 masters, none authoritative)
         ├─ no prior master recorded ──▶ bootstrap one (fresh standalone Pods)
         └─ otherwise                ──▶ split-brain: log critical, DO NOTHING
```

**Convergence** to a chosen master `M`:

1. Remove the master label from every other Pod (so the Service never has two
   master endpoints at once).
2. Ensure `M` is labeled `redis-current-role=master`.
3. For every other healthy Pod: label it `replica` and, **only if it is not
   already following `M`**, send `REPLICAOF <M-ip> <port>`.

Because steps only act when state is wrong, repeated reconciles are no-ops once
the topology is correct (`"no topology change required"`).

## Failover flow

```
master Pod stops answering PING / INFO
        │
        ▼
controller observes 0 healthy masters but a master was previously known
        │
        ├─ start/continue a failure timer
        ├─ elapsed < MASTER_FAILURE_THRESHOLD_SECONDS  ──▶ wait (no promotion)
        ▼
elapsed ≥ threshold
        │
        ├─ pick best replica:
        │     healthy ▸ role=replica ▸ highest INFO replication offset ▸ lowest ordinal
        ├─ REPLICAOF NO ONE on the chosen replica
        ├─ verify with ROLE that it now reports master   ──(fail)──▶ abort, no label change
        ├─ label new master / strip old master label
        └─ REPLICAOF the remaining healthy Pods to the new master
                │
                ▼
        redis-write now routes new connections to the new master
```

When the **old master returns**, it usually comes back still believing it is a
master. The controller sees two masters, treats the labeled one as
authoritative, and demotes the returning Pod with `REPLICAOF <new-master>` plus
a `replica` label.

## Split-brain protection

More than one Pod reporting `role=master` is treated as a split-brain risk:

- The controller **logs a critical error** (`"multiple masters detected"`).
- It resolves the situation **only** when exactly one of those masters is the
  Kubernetes-authoritative master (carries the `redis-current-role=master`
  label). It then demotes the others.
- If authority is ambiguous (zero or several labeled masters), it makes **no
  destructive change** and waits for an operator. The single exception is a
  brand-new cluster with no recorded master anywhere, which is safely
  bootstrapped.

## Important limitations (read this)

1. **Existing TCP connections are not migrated.** After a failover, clients
   holding a connection to the old master will see connection resets, write
   errors, or `READONLY` errors. **Clients must reconnect** (use a client with
   reconnect + retry). The `redis-write` Service only changes where *new*
   connections land.
2. **Redis replication is asynchronous.** A write acknowledged by the old master
   may not have reached the promoted replica. Such writes can be **lost** during
   failover. Do not use this for data that cannot tolerate small windows of loss.
3. **This controller replaces responsibilities normally handled by Sentinel.**
   It is therefore conservative about split-brain, stale state, and unsafe
   promotion, and it will refuse to act rather than risk data corruption.
4. **Accept the operational risk.** Custom failover logic is only appropriate if
   your team owns and understands it.
5. **Prefer the standard tools when you can.** For stronger, standard Redis HA,
   use **Redis Sentinel** or **Redis Cluster**.

## Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `REDIS_NAMESPACE` | `default` | Fallback namespace (used for the Lease and when `REDIS_NAMESPACES` is unset). |
| `REDIS_NAMESPACES` | `=REDIS_NAMESPACE` | Comma-separated list of namespaces to manage. Needs a Pods Role in each. |
| `REDIS_POD_LABEL_SELECTOR` | `app=redis` | Broad selector matching every managed Redis Pod across all sets. |
| `REDIS_SET_LABEL_KEY` | `redis-set` | Pod label whose value groups Pods into independent replication sets. |
| `DEFAULT_SET_NAME` | `default` | Set name for Pods missing `REDIS_SET_LABEL_KEY` (keeps a single unlabeled topology working). |
| `PROBE_CONCURRENCY` | `16` | Max Redis Pods probed in parallel per reconcile. |
| `REDIS_PORT` | `6379` | Redis port on each Pod. |
| `REDIS_WRITE_SERVICE_NAME` | `redis-write` | Service validated to point at the master. |
| `RECONCILE_INTERVAL_SECONDS` | `10` | Time between reconcile passes. |
| `MASTER_FAILURE_THRESHOLD_SECONDS` | `15` | How long the master must be unhealthy before failover. |
| `REDIS_CONNECT_TIMEOUT_SECONDS` | `2` | TCP connect timeout per Redis call. |
| `REDIS_COMMAND_TIMEOUT_SECONDS` | `2` | Command read/write timeout. |
| `CONTROLLER_ID` | hostname | Identity used for leader election. |
| `ENABLE_LEADER_ELECTION` | `true` | Enable Lease-based leader election. |
| `LEASE_NAME` | `redis-replication-controller` | Lease object name. |
| `LEASE_NAMESPACE` | `=REDIS_NAMESPACE` | Namespace for the Lease. |
| `INITIAL_MASTER_STRATEGY` | `lowest-pod-ordinal` | `first-healthy`, `lowest-pod-ordinal`, or `annotation-preferred`. |
| `ENABLE_CONFIG_REWRITE` | `false` | Run `CONFIG REWRITE` after role changes (see below). |
| `HEALTH_PROBE_ADDR` | `:8081` | Address for `/healthz` and `/readyz`. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error`. |

**`INITIAL_MASTER_STRATEGY` values**

- `first-healthy` — first healthy Pod in discovery (ordinal) order.
- `lowest-pod-ordinal` — Pod with the lowest StatefulSet ordinal (default).
- `annotation-preferred` — a Pod annotated
  `redis-controller/preferred-master: "true"`, falling back to lowest ordinal.

**`CONFIG REWRITE`** is optional and disabled by default. When enabled, the
controller persists the `replicaof` directive into each Pod's config file so the
role survives a Redis restart. This requires Redis to have been started with a
writable config file; it is best-effort and failures are logged, not fatal.
Leaving it disabled is also fine — the controller will simply re-establish the
correct topology after a restart.

## Managing multiple replication sets

One controller manages any number of independent replication sets. It discovers
every Pod under `REDIS_POD_LABEL_SELECTOR`, then **groups them by the value of
the `REDIS_SET_LABEL_KEY` label** and reconciles each group on its own:

```
app=redis , redis-set=cache    → master: cache-0     → redis-write-cache
app=redis , redis-set=sessions → master: sessions-1  → redis-write-sessions
```

Each set keeps **its own** master-failure timer and failover epoch, so a healthy
set can never delay or mask a failover in another set. Every `REPLICAOF` and
label change is scoped to the set it belongs to — a replica in one set is never
pointed at another set's master.

To add a set:

1. Run Redis Pods labelled `redis-set: <name>` (plus `app=redis`). They boot as
   plain standalone masters — no `--replicaof`.
2. Create a `redis-write-<name>` Service whose selector is
   `{ app: redis, redis-set: <name>, redis-current-role: master }`.

Pods without the set label fold into `DEFAULT_SET_NAME`, so an existing
single-topology deployment keeps working with no changes. See
[`manifests/03-redis-cache-statefulset.yaml`](manifests/03-redis-cache-statefulset.yaml)
(set `cache`) and
[`manifests/06-redis-sessions.yaml`](manifests/06-redis-sessions.yaml)
(set `sessions`) for a two-set example driven by a single controller.

### Sets in different namespaces

Sets do not have to share a namespace. List every namespace to manage in
`REDIS_NAMESPACES` (comma-separated); the controller searches each one and keeps
them independent. A set's identity is **(namespace, `redis-set` value)**, so two
namespaces that reuse the same set name never merge, and the controller never
issues `REPLICAOF` across a namespace boundary.

Access is **least-privilege, opt-in**: there is no `ClusterRole`. For each
additional namespace you create a Pods-only `Role` + `RoleBinding` to the
controller's ServiceAccount (which lives in the controller's own namespace). The
Lease for leader election stays in the controller's namespace only. The
`sessions` example above lives in its own `redis-sessions` namespace and ships
exactly that `Role`/`RoleBinding`. Leaving `REDIS_NAMESPACES` unset preserves the
original single-namespace behaviour.

**Availability.** Each set keeps serving down to its last surviving Pod: a lone
surviving master is kept, and a lone surviving replica is promoted after
`MASTER_FAILURE_THRESHOLD_SECONDS`. During that promotion window the set's
`redis-write-<name>` Service has no endpoint, so writes briefly fail until the
new master is labelled and `Ready`. Probing is parallelised
(`PROBE_CONCURRENCY`) so one set's dead Pods do not stall failover elsewhere.
Failover promotes the reached replica with the highest replication offset (and a
healthy master link where known) to minimise lost writes.

## Build

Requires Go 1.22+ (the module is verified with the standard toolchain).

```bash
# Compile, vet and test
go vet ./...
go test ./...
go build -o bin/controller ./cmd

# Or via the Makefile (which can run Go inside a container if you have no local Go):
make check          # tidy + vet + test + build
make GO=go check    # use your local Go instead of the golang container
```

Build the container image (requires a running Docker daemon):

```bash
make docker-build               # builds redis-replication-controller:latest
# or
docker build -t redis-replication-controller:latest .
```

The image is a multi-stage build producing a static binary on top of
`gcr.io/distroless/static:nonroot` (runs as UID 65532, read-only root FS).

## Deploy

```bash
# 1. Make the image available to your cluster
#    (push to a registry, or `kind load docker-image redis-replication-controller:latest`)

# 2. Apply manifests. They are numbered (00-,01-,...) so a single directory
#    apply runs them in dependency order: namespaces -> ServiceAccount -> RBAC
#    -> Redis sets -> controller. No conflicts, no manual ordering.
kubectl apply -f manifests/

# or simply:
make deploy
```

This applies both example sets — `cache` in namespace `redis` and `sessions` in
namespace `redis-sessions` — plus the external LoadBalancer. To deploy only the
basics, apply the lower-numbered files individually (e.g. `00-` through `04-`
and `07-controller-deployment.yaml`).

Then verify:

```bash
kubectl -n redis get pods -L redis-current-role
kubectl -n redis run rc --rm -it --image=redis:7-alpine --restart=Never -- \
  redis-cli -h redis-write -p 6379 SET test-key initial
kubectl -n redis run rc --rm -it --image=redis:7-alpine --restart=Never -- \
  redis-cli -h redis-write -p 6379 GET test-key   # -> "initial"
```

## Testing

### Unit tests

```bash
go test ./...
go test -race ./...
```

Coverage highlights:

- **`internal/redis`** — RESP encode/decode; `ROLE` and `INFO replication`
  parsing (master, replica, unexpected, malformed); `PING` success/error/
  timeout/connection-refused against a real in-process TCP server; verifies the
  exact bytes sent for `REPLICAOF` / `REPLICAOF NO ONE`.
- **`internal/kubernetes`** — Pod discovery + ordinal sorting, find-by-IP, label
  set/remove, annotation patch, and API-failure handling (via the fake
  clientset).
- **`internal/controller`** — single healthy master (no action), bootstrap of
  fresh standalone masters, label/role mismatch correction, below/above failure
  threshold, best-replica selection by offset, no-healthy-replica, failed
  promotion verification, old-master demotion, split-brain (resolvable and
  ambiguous), and idempotency (no repeated patches or `REPLICAOF`).
- **`internal/config`** — defaults, overrides, and validation errors.

### Kubernetes integration test

A scripted end-to-end test lives in [`tests/integration`](tests/integration).
It deploys the example StatefulSet + controller, verifies bootstrap, writes
through `redis-write`, kills the master, and confirms failover. It requires a
**throwaway** cluster (kind/minikube) — never run it against production.

```bash
make integration-test           # uses NAMESPACE=redis by default
```

## Troubleshooting

```bash
# Who is currently the master?
kubectl -n redis get pods -L redis-current-role

# Does redis-write resolve to exactly one (the master) endpoint?
kubectl -n redis get endpointslice -l kubernetes.io/service-name=redis-write -o wide

# Controller logs (structured JSON)
kubectl -n redis logs deploy/redis-replication-controller -f

# Which replica leads? (leader election Lease)
kubectl -n redis get lease redis-replication-controller -o yaml

# Inspect a Pod's real role directly
kubectl -n redis exec redis-0 -- redis-cli ROLE
kubectl -n redis exec redis-0 -- redis-cli INFO replication
```

Common issues:

- **`redis-write` has no endpoints** — no Pod is labeled master yet (check
  controller logs for bootstrap), or the master Pod is not `Ready`.
- **Controller can't reach Redis** — ensure `--protected-mode no` (or a shared
  password) so a different Pod can connect; check `NetworkPolicy`.
- **Two masters reported** — look for `"multiple masters detected"`; the
  controller will not auto-resolve an ambiguous split-brain. Decide the true
  master, label exactly one Pod `redis-current-role=master`, and it will
  converge.

## Production warnings

- Existing connections are **not** migrated; clients **must** reconnect.
- Asynchronous replication means **recent writes can be lost** on failover.
- This is **custom** HA logic replacing Sentinel — accept the risk or prefer
  **Redis Sentinel** / **Redis Cluster** for standard guarantees.
- Enable persistence (AOF/RDB + PVCs) on the Redis StatefulSet for real data;
  the example uses `emptyDir` for portability only.

## Repository layout

```
.
├── cmd/main.go                     # entrypoint: config, in-cluster client, leader election
├── internal/
│   ├── config/                     # env-var configuration + validation
│   ├── redis/                      # minimal RESP client (PING/ROLE/INFO/REPLICAOF)
│   ├── kubernetes/                 # Pod discovery, label/annotation patching, endpoints
│   ├── controller/                 # reconcile loop, failover, selection, split-brain
│   └── leader/                     # Lease-based leader election
├── manifests/                      # namespace, SA, RBAC, Deployment, Services, StatefulSet
├── tests/integration/              # end-to-end script for a throwaway cluster
├── Dockerfile                      # multi-stage, distroless non-root runtime
└── Makefile
```
