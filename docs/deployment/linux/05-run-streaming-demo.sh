#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
NAMESPACE="${NAMESPACE:-bigdata}"

kubectl rollout restart deployment/stream-producer -n "$NAMESPACE"
kubectl rollout status deployment/stream-producer -n "$NAMESPACE" --timeout=180s

echo "==> Streaming demo started"
echo "Dashboard: http://localhost:15000"
