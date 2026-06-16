#!/usr/bin/env bash
#
# End-to-end integration test for redis-replication-controller.
#
# It deploys the example StatefulSet + controller, verifies bootstrap and
# routing, writes through redis-write, then kills the master and verifies
# failover.
#
# SAFETY: this script CREATES, MODIFIES and DELETES objects (including deleting
# the master Pod). It must only be run against a throwaway cluster
# (kind/minikube). It refuses to run unless ALLOW_INTEGRATION=1 is set.
#
# Usage:
#   ALLOW_INTEGRATION=1 NAMESPACE=redis ./tests/integration/run-integration-test.sh
#
set -euo pipefail

NAMESPACE="${NAMESPACE:-redis}"
MANIFESTS="$(cd "$(dirname "$0")/../../manifests" && pwd)"
CLIENT_POD="redis-client"
KEY_INITIAL="test-key"
KEY_FAILOVER="failover-test"

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
info()  { printf '\033[36m==> %s\033[0m\n' "$*"; }
fail()  { red "FAIL: $*"; exit 1; }

if [[ "${ALLOW_INTEGRATION:-}" != "1" ]]; then
  red "Refusing to run: this test mutates the cluster and deletes Pods."
  red "Current kube-context: $(kubectl config current-context 2>/dev/null || echo unknown)"
  red "Set ALLOW_INTEGRATION=1 ONLY for a throwaway cluster (kind/minikube), then re-run."
  exit 2
fi

kc() { kubectl -n "$NAMESPACE" "$@"; }

# cli runs redis-cli inside the helper client Pod against the given host.
cli() { kc exec "$CLIENT_POD" -- redis-cli -h "$1" -p 6379 "${@:2}"; }

# count_masters prints the number of Pods labeled redis-current-role=master.
count_masters() {
  kc get pods -l 'app=redis,redis-current-role=master' \
    -o jsonpath='{.items[*].metadata.name}' | wc -w | tr -d ' '
}

master_pod() {
  kc get pods -l 'app=redis,redis-current-role=master' \
    -o jsonpath='{.items[0].metadata.name}'
}

# retry <attempts> <sleep-seconds> <cmd...>
retry() {
  local attempts=$1 sleep_s=$2; shift 2
  local i
  for ((i = 1; i <= attempts; i++)); do
    if "$@"; then return 0; fi
    sleep "$sleep_s"
  done
  return 1
}

one_master() { [[ "$(count_masters)" == "1" ]]; }

cleanup() {
  info "Cleaning up helper client Pod"
  kc delete pod "$CLIENT_POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- deploy -------------------------------------------------------------------
info "Applying manifests into namespace '$NAMESPACE'"
# Minimal single-namespace ("cache") deployment: skip the LoadBalancer (05) and
# the second-namespace example (06) so this runs on a plain kind cluster.
kubectl apply -f "$MANIFESTS/00-namespace.yaml"
kubectl apply -f "$MANIFESTS/01-serviceaccount.yaml"
kubectl apply -f "$MANIFESTS/02-rbac.yaml"
kubectl apply -f "$MANIFESTS/03-redis-cache-statefulset.yaml"
kubectl apply -f "$MANIFESTS/04-redis-cache-write-service.yaml"
kubectl apply -f "$MANIFESTS/07-controller-deployment.yaml"

info "Waiting for Redis StatefulSet to be ready"
kc rollout status statefulset/redis --timeout=180s

info "Waiting for controller Deployment to be available"
kc rollout status deploy/redis-replication-controller --timeout=120s

info "Starting helper client Pod"
kc run "$CLIENT_POD" --image=redis:7-alpine --restart=Never --command -- sleep 3600 >/dev/null
kc wait --for=condition=Ready "pod/$CLIENT_POD" --timeout=60s

# --- bootstrap ----------------------------------------------------------------
info "Waiting for the controller to elect exactly one master"
retry 30 2 one_master || fail "expected exactly one master, found $(count_masters)"
MASTER="$(master_pod)"
green "Initial master is $MASTER"

info "Verifying replicas"
REPLICAS=$(kc get pods -l 'app=redis,redis-current-role=replica' -o jsonpath='{.items[*].metadata.name}')
green "Replicas: ${REPLICAS:-<none>}"

# --- write/read through redis-write ------------------------------------------
info "Writing through redis-write"
cli redis-write SET "$KEY_INITIAL" initial >/dev/null
GOT="$(cli redis-write GET "$KEY_INITIAL")"
[[ "$GOT" == "initial" ]] || fail "GET $KEY_INITIAL via redis-write = '$GOT', want 'initial'"
green "redis-write SET/GET works (got '$GOT')"

# --- replication --------------------------------------------------------------
if [[ -n "${REPLICAS:-}" ]]; then
  REPLICA=$(echo "$REPLICAS" | awk '{print $1}')
  REPLICA_IP=$(kc get pod "$REPLICA" -o jsonpath='{.status.podIP}')
  info "Verifying replication to $REPLICA ($REPLICA_IP)"
  retry 15 2 bash -c "[[ \"\$(kubectl -n $NAMESPACE exec $CLIENT_POD -- redis-cli -h $REPLICA_IP -p 6379 GET $KEY_INITIAL)\" == 'initial' ]]" \
    && green "Data replicated to $REPLICA" \
    || fail "data did not replicate to $REPLICA"
fi

# --- failover -----------------------------------------------------------------
info "Simulating master failure: deleting Pod $MASTER"
kc delete pod "$MASTER" --now >/dev/null

info "Waiting for failover to a new master"
new_master_elected() { local m; m="$(master_pod || true)"; [[ -n "$m" && "$m" != "$MASTER" ]]; }
retry 60 2 new_master_elected || fail "no new master elected after deleting $MASTER"
NEW_MASTER="$(master_pod)"
green "Failover complete: new master is $NEW_MASTER"

retry 30 2 one_master || fail "expected exactly one master after failover, found $(count_masters)"

info "Verifying writes through redis-write after failover"
retry 15 2 bash -c "kubectl -n $NAMESPACE exec $CLIENT_POD -- redis-cli -h redis-write -p 6379 SET $KEY_FAILOVER ok >/dev/null" \
  || fail "could not write through redis-write after failover"
GOT="$(cli redis-write GET "$KEY_FAILOVER")"
[[ "$GOT" == "ok" ]] || fail "GET $KEY_FAILOVER via redis-write = '$GOT', want 'ok'"
green "redis-write SET/GET works after failover (got '$GOT')"

# --- endpoint check -----------------------------------------------------------
info "redis-write EndpointSlice addresses (should be only the new master IP):"
kc get endpointslice -l "kubernetes.io/service-name=redis-write" -o wide || true

green "INTEGRATION TEST PASSED"
