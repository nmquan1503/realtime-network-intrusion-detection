#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
NAMESPACE="${NAMESPACE:-bigdata}"

kubectl apply -f k8s/streaming/streaming_predictor.yaml -n "$NAMESPACE"
kubectl apply -f k8s/dashboard/dashboard.yaml -n "$NAMESPACE"
kubectl apply -f k8s/streaming/stream_producer.yaml -n "$NAMESPACE"
kubectl rollout status deployment/streaming-predictor -n "$NAMESPACE" --timeout=300s
kubectl rollout status deployment/dashboard -n "$NAMESPACE" --timeout=180s

kubectl rollout restart deployment/stream-producer -n "$NAMESPACE"
kubectl rollout status deployment/stream-producer -n "$NAMESPACE" --timeout=180s

echo "==> Streaming demo started"
echo "Dashboard: http://localhost:15000"
