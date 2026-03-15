#!/bin/bash
set -euo pipefail

CLUSTER_NAME="walrus-test"
NAMESPACE="walrus"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cleanup() {
    echo "=== Cleaning up ==="
    kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for a workload to have at least 1 ready replica, tolerating CrashLoopBackOff.
# Usage: wait_for_ready <resource> <timeout_seconds>
wait_for_ready() {
    local resource="$1"
    local timeout="$2"
    local elapsed=0
    while [ $elapsed -lt "$timeout" ]; do
        local ready
        ready=$(kubectl -n "$NAMESPACE" get "$resource" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
        ready="${ready:-0}"
        if [ "$ready" -ge 1 ] 2>/dev/null; then
            echo "  $resource is ready"
            return 0
        fi
        local phase
        phase=$(kubectl -n "$NAMESPACE" get pods -l "app.kubernetes.io/name=$(echo "$resource" | sed 's|.*/||')" -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "unknown")
        local restarts
        restarts=$(kubectl -n "$NAMESPACE" get pods -l "app.kubernetes.io/name=$(echo "$resource" | sed 's|.*/||')" -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' 2>/dev/null || echo "?")
        echo "  Waiting for $resource... (phase=$phase, restarts=$restarts, ${elapsed}s/${timeout}s)"
        sleep 10
        elapsed=$((elapsed + 10))
    done
    echo "  TIMEOUT waiting for $resource after ${timeout}s"
    echo "--- $resource logs ---"
    kubectl -n "$NAMESPACE" logs "$resource" --tail=50 2>&1 || true
    return 1
}

echo "=== Creating kind cluster ==="
kind create cluster --name "$CLUSTER_NAME" --wait 60s

echo "=== Loading images into kind ==="
kind load docker-image walrus/wal-capture:test --name "$CLUSTER_NAME"
kind load docker-image walrus/iceberg-writer:test --name "$CLUSTER_NAME"

echo "=== Deploying test Postgres ==="
kubectl create namespace "$NAMESPACE"
kubectl apply -f "$REPO_ROOT/deploy/helm/test-postgres.yaml"
kubectl -n "$NAMESPACE" rollout status statefulset/postgres --timeout=120s
# Give Postgres time to finish init SQL
sleep 10

echo "=== Installing Walrus via Helm ==="
helm install walrus "$REPO_ROOT/deploy/helm/walrus/" \
    --namespace "$NAMESPACE" \
    --set config.source.host=postgres \
    --set config.source.tlsMode=disable \
    --set secret.pgPassword=testpass \
    --set walCapture.image.repository=walrus/wal-capture \
    --set walCapture.image.tag=test \
    --set icebergWriter.image.repository=walrus/iceberg-writer \
    --set icebergWriter.image.tag=test \
    --set persistence.staging.accessModes[0]=ReadWriteOnce \
    --set persistence.staging.size=1Gi \
    --set persistence.warehouse.accessModes[0]=ReadWriteOnce \
    --set persistence.warehouse.size=1Gi

echo "=== Waiting for WAL Capture to be ready ==="
wait_for_ready statefulset/walrus-wal-capture 300

echo "=== Waiting for Iceberg Writer to be ready ==="
wait_for_ready deployment/walrus-iceberg-writer 300

echo "=== Verifying health endpoints ==="

# Port-forward WAL Capture health endpoint
kubectl -n "$NAMESPACE" port-forward statefulset/walrus-wal-capture 18081:8081 &
PF_WAL_PID=$!
sleep 2

WAL_HEALTH=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:18081/healthz || echo "000")
kill $PF_WAL_PID 2>/dev/null || true
wait $PF_WAL_PID 2>/dev/null || true

if [ "$WAL_HEALTH" = "200" ]; then
    echo "  PASS: WAL Capture /healthz returned 200"
else
    echo "  FAIL: WAL Capture /healthz returned $WAL_HEALTH"
    echo "--- WAL Capture logs ---"
    kubectl -n "$NAMESPACE" logs statefulset/walrus-wal-capture --tail=50
    exit 1
fi

# Port-forward Iceberg Writer health endpoint
kubectl -n "$NAMESPACE" port-forward deployment/walrus-iceberg-writer 18082:8082 &
PF_ICE_PID=$!
sleep 2

ICE_HEALTH=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:18082/healthz || echo "000")
kill $PF_ICE_PID 2>/dev/null || true
wait $PF_ICE_PID 2>/dev/null || true

if [ "$ICE_HEALTH" = "200" ]; then
    echo "  PASS: Iceberg Writer /healthz returned 200"
else
    echo "  FAIL: Iceberg Writer /healthz returned $ICE_HEALTH"
    echo "--- Iceberg Writer logs ---"
    kubectl -n "$NAMESPACE" logs deployment/walrus-iceberg-writer --tail=50
    exit 1
fi

echo "=== Smoke test: verify replication state ==="

RETRIES=0
MAX_RETRIES=30
while [ $RETRIES -lt $MAX_RETRIES ]; do
    STATE=$(kubectl -n "$NAMESPACE" exec statefulset/postgres -- \
        psql -U replicator -d sourcedb -tAc \
        "SELECT COUNT(*) FROM _pgiceberg.replication_state" 2>/dev/null || echo "0")
    STATE=$(echo "$STATE" | tr -d '[:space:]')
    if [ "$STATE" != "0" ] && [ -n "$STATE" ]; then
        echo "  PASS: Replication state found ($STATE row(s))"
        break
    fi
    RETRIES=$((RETRIES + 1))
    echo "  Waiting for replication state... ($RETRIES/$MAX_RETRIES)"
    sleep 10
done

if [ $RETRIES -eq $MAX_RETRIES ]; then
    echo "  FAIL: Replication state not created after ${MAX_RETRIES} attempts"
    echo "--- WAL Capture logs ---"
    kubectl -n "$NAMESPACE" logs statefulset/walrus-wal-capture --tail=100
    exit 1
fi

echo ""
echo "=== All K8s integration tests passed! ==="
