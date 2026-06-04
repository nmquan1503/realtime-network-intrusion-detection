#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
NAMESPACE="${NAMESPACE:-bigdata}"

kubectl delete job/stream-producer -n "$NAMESPACE" --ignore-not-found=true
kubectl wait --for=delete job/stream-producer -n "$NAMESPACE" --timeout=60s >/dev/null 2>&1 || true
kubectl apply -f k8s/streaming/stream_producer.yaml -n "$NAMESPACE"
kubectl wait --for=condition=Ready pod -l job-name=stream-producer -n "$NAMESPACE" --timeout=120s

echo "==> Streaming demo started"
echo "Dashboard: http://localhost:15000"
