#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
NAMESPACE="${NAMESPACE:-bigdata}"

kubectl apply -f k8s/streaming/stream_producer.yaml -n "$NAMESPACE"