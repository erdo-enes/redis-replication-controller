# Integration test

End-to-end test of the controller against a **real, throwaway** Kubernetes
cluster (kind or minikube). It deploys everything in `manifests/`, then checks:

1. **Bootstrap** — the controller elects exactly one master and labels the rest
   as replicas.
2. **Routing** — `redis-write` resolves to the master; `SET`/`GET` succeed.
3. **Replication** — data written via `redis-write` appears on a replica.
4. **Failover** — deleting the master Pod causes promotion of a replica, the
   master label moves, and new writes through `redis-write` succeed.
5. **Endpoints** — the `redis-write` EndpointSlice points only at the master.

## Prerequisites

- A throwaway cluster and a `kubectl` context pointing at it.
- The controller image available to the cluster, e.g. with kind:

  ```bash
  docker build -t redis-replication-controller:latest .
  kind load docker-image redis-replication-controller:latest
  ```

## Run

The script refuses to run unless `ALLOW_INTEGRATION=1` is set, because it
**deletes the master Pod** and mutates cluster state.

```bash
ALLOW_INTEGRATION=1 NAMESPACE=redis ./tests/integration/run-integration-test.sh
```

> ⚠️ Never run this against a shared or production cluster. Double-check
> `kubectl config current-context` first.

## Manual checks

```bash
# Initial bootstrap
kubectl -n redis get pods -L redis-current-role
kubectl -n redis exec redis-client -- redis-cli -h redis-write SET test-key initial
kubectl -n redis exec redis-client -- redis-cli -h redis-write GET test-key   # -> initial

# Failover
kubectl -n redis delete pod "$(kubectl -n redis get pod -l app=redis,redis-current-role=master -o jsonpath='{.items[0].metadata.name}')"
kubectl -n redis get pods -L redis-current-role -w
kubectl -n redis exec redis-client -- redis-cli -h redis-write SET failover-test ok
kubectl -n redis exec redis-client -- redis-cli -h redis-write GET failover-test  # -> ok

# Wrong-label and no-label recovery
kubectl -n redis label pod redis-1 redis-current-role=master --overwrite   # create a 2nd master label
kubectl -n redis get pods -L redis-current-role                            # controller corrects it
kubectl -n redis label pod --all redis-current-role-                       # remove all role labels
kubectl -n redis get pods -L redis-current-role                            # controller restores them

# Endpoints should list only the current master IP
kubectl -n redis get endpointslice -l kubernetes.io/service-name=redis-write -o wide
```
