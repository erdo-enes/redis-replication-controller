# failover-probe

A tiny, standalone Redis write client (its own module/image) that shows what an
**active connection** experiences during a controller failover. You trigger the
failover; the probe measures.

It opens **one** long-lived connection to a `redis-write-<set>` Service and runs
`INCR` on a loop, like a plain application. It reconnects only when the
connection breaks or the server replies `READONLY` — exactly the behaviour a
client must have for the label-flip failover to be transparent.

## Build & push

```bash
cd tools/failover-probe
docker build -t harbor-enes-k8s.local/test/failover-probe:v1 .
docker push harbor-enes-k8s.local/test/failover-probe:v1
```

## Run in-cluster

```bash
kubectl apply -f deploy.yaml
kubectl -n redis logs -f deploy/failover-probe
```

Point `TARGET_ADDR` at any set, including one in another namespace, e.g.
`redis-write-sessions.redis-sessions.svc.cluster.local:6379`. No RBAC needed —
it is just a Redis client.

## What you will see

Steady state:

```
[CONNECTED] run_id=9f3a1c2b... role=master
[OK] counter=520 rtt=1ms run_id=9f3a1c2b... | ok=520 readonly=0 disconnect=0 dial-err=0
```

When you kill the master, one of two things happens to the live connection:

- **Master pod died** → the connection breaks:
  ```
  [DISCONNECT] write failed: ... ; will reconnect
  [DIAL-ERR] ... (Service may have no master endpoint during failover)   <- the outage window
  ...
  [ENDPOINT-CHANGED] now run_id=7c2d... role=master (previously 9f3a...)
  [RECOVERED] write outage = 18.4s; writable again on run_id=7c2d... (counter=521)
  ```
- **Master demoted but still alive** (convergence/split-brain) → the connection
  stays open but turns read-only:
  ```
  [READONLY] write rejected: server is now a read-only replica; dropping connection to re-resolve via the Service
  [ENDPOINT-CHANGED] now run_id=7c2d... role=master (previously 9f3a...)
  [RECOVERED] write outage = 0.3s; ...
  ```

The `run_id` change proves the Service endpoint actually moved to a different
pod. The `[RECOVERED] write outage = …` line is the number you care about.

## "How does it know the endpoints change in 15s, or 5s?"

It does **not** know or guess the threshold. It just writes continuously and
times the gap. The connection only recovers because the probe **drops it and
redials** — a client that never reconnects would stay stuck (broken socket, or
forever `READONLY` on a demoted replica).

The measured write outage is roughly:

```
outage ≈ detection (≤ RECONCILE_INTERVAL_SECONDS, the controller only notices on a tick)
        + MASTER_FAILURE_THRESHOLD_SECONDS
        + promotion round-trip + EndpointSlice propagation + new master readiness
```

So the threshold is the dominant, tunable term:

| `MASTER_FAILURE_THRESHOLD_SECONDS` | Typical observed outage* |
| --- | --- |
| `15` (default) | ~15–30 s of failing writes |
| `5` | ~5–18 s of failing writes |

\*with `RECONCILE_INTERVAL_SECONDS=10`; lower the interval too to tighten the
range. During the whole window `redis-write-<set>` has no endpoint, so the probe
logs `[DIAL-ERR]`/`[DISCONNECT]` and writes fail; then `[RECOVERED]` prints the
actual elapsed time, which tracks the threshold you set.

**The trade-off:** a shorter threshold means a shorter outage but a higher
chance of an *unnecessary* failover — a brief network blip or GC pause that
makes the master miss a couple of probes can trip a promotion you did not want.
15 s is conservative; 5 s is more aggressive. The probe lets you pick a value
and see the resulting outage directly.
