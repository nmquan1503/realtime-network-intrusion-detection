#!/usr/bin/env bash
set -euo pipefail
NAMESPACE="${NAMESPACE:-bigdata}"

kubectl get pods,svc,deploy,jobs -n "$NAMESPACE"
echo
kubectl get kafkatopic -n "$NAMESPACE"
echo
kubectl logs deployment/streaming-predictor -n "$NAMESPACE" --tail=80 || true
echo
kubectl logs deployment/dashboard -n "$NAMESPACE" --tail=40 || true
