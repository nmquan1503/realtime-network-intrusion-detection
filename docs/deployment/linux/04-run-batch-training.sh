#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
NAMESPACE="${NAMESPACE:-bigdata}"
DAG_ID="${DAG_ID:-batch_training_pipeline_k8s}"

kubectl delete job/batch-producer -n "$NAMESPACE" --ignore-not-found=true
kubectl wait --for=delete job/batch-producer -n "$NAMESPACE" --timeout=60s >/dev/null 2>&1 || true
kubectl apply -f k8s/simulator/batch_producer.yaml -n "$NAMESPACE"
kubectl wait --for=condition=complete job/batch-producer -n "$NAMESPACE" --timeout=900s

sleep 10

kubectl exec deployment/airflow-webserver -n "$NAMESPACE" -- airflow dags unpause "$DAG_ID"
RUN_ID="manual__$(date +%Y%m%d_%H%M%S)"
kubectl exec deployment/airflow-webserver -n "$NAMESPACE" -- airflow dags trigger "$DAG_ID" --run-id "$RUN_ID"

echo "==> Triggered DAG run: ${RUN_ID}"
echo "Open Airflow: http://localhost:18080"
