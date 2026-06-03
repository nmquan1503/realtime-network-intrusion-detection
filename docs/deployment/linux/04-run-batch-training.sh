#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
NAMESPACE="${NAMESPACE:-bigdata}"
DAG_ID="${DAG_ID:-batch_training_pipeline_k8s}"

kubectl apply -f k8s/simulator/batch_producer.yaml -n "$NAMESPACE"
kubectl rollout restart deployment/batch-producer -n "$NAMESPACE"
kubectl rollout status deployment/batch-producer -n "$NAMESPACE" --timeout=180s

sleep 30

kubectl exec deployment/airflow-webserver -n "$NAMESPACE" -- airflow dags unpause "$DAG_ID"
RUN_ID="manual__$(date +%Y%m%d_%H%M%S)"
kubectl exec deployment/airflow-webserver -n "$NAMESPACE" -- airflow dags trigger "$DAG_ID" --run-id "$RUN_ID"

echo "==> Triggered DAG run: ${RUN_ID}"
echo "Open Airflow: http://localhost:18080"
