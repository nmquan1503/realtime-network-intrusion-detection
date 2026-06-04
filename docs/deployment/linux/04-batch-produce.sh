#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
NAMESPACE="${NAMESPACE:-bigdata}"
DAG_ID="${DAG_ID:-batch_training_pipeline_k8s}"

kubectl apply -f k8s/simulator/batch_producer.yaml -n "$NAMESPACE"