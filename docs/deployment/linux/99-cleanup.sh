#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
NAMESPACE="${NAMESPACE:-bigdata}"

if [[ "${1:-}" == "--delete-namespace" ]]; then
  kubectl delete namespace "$NAMESPACE" --ignore-not-found
  exit 0
fi

kubectl delete -f k8s/streaming/stream_producer.yaml -n "$NAMESPACE" --ignore-not-found
kubectl delete -f k8s/streaming/streaming_predictor.yaml -n "$NAMESPACE" --ignore-not-found
kubectl delete -f k8s/dashboard/dashboard.yaml -n "$NAMESPACE" --ignore-not-found
kubectl delete -f k8s/simulator/batch_producer.yaml -n "$NAMESPACE" --ignore-not-found
kubectl delete -f k8s/airflow/airflow-deployments.yaml -n "$NAMESPACE" --ignore-not-found
kubectl delete job/airflow-init -n "$NAMESPACE" --ignore-not-found

echo "==> Cleanup completed. PVCs and infra are kept. Use --delete-namespace for full reset."
