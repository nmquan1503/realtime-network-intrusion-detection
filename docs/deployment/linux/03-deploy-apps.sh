#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
NAMESPACE="${NAMESPACE:-bigdata}"

kubectl apply -f k8s/dashboard/dashboard.yaml -n "$NAMESPACE"
kubectl apply -f k8s/streaming/streaming_predictor.yaml -n "$NAMESPACE"
kubectl rollout status deployment/dashboard -n "$NAMESPACE" --timeout=180s
kubectl rollout status deployment/streaming-predictor -n "$NAMESPACE" --timeout=300s

echo "==> Apps deployed. Use 04/05 scripts to run producer Jobs."
