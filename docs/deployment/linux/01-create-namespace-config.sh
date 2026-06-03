#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
NAMESPACE="${NAMESPACE:-bigdata}"

kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"
kubectl apply -f "https://strimzi.io/install/latest?namespace=${NAMESPACE}" -n "$NAMESPACE"

kubectl create configmap config -n "$NAMESPACE" \
  --from-literal=KAFKA_BOOTSTRAP_SERVERS=kafka-cluster-kafka-bootstrap.bigdata.svc.cluster.local:9092 \
  --from-literal=KAFKA_CONSUMER_POLL_TIMEOUT=1000 \
  --from-literal=BATCH_KAFKA_TOPIC=openmeteo-data \
  --from-literal=BATCH_KAFKA_GROUP_ID=batch-consumer \
  --from-literal=BATCH_FLUSH_SIZE=5000 \
  --from-literal=BATCH_MAX_SILENCE_TIME=30 \
  --from-literal=BATCH_PRODUCE_BATCH_SIZE=1048576 \
  --from-literal=MINIO_ENDPOINT=http://minio.bigdata.svc.cluster.local:9000 \
  --from-literal=MINIO_ACCESS_KEY=minioadmin \
  --from-literal=MINIO_SECRET_KEY=minioadmin123 \
  --from-literal=MINIO_BUCKET=data-lake \
  --from-literal=MINIO_RETRY_COUNT=5 \
  --from-literal=MINIO_RETRY_DELAY=3 \
  --from-literal=SPARK_MASTER_URL=spark://spark-master-svc.bigdata.svc.cluster.local:7077 \
  --from-literal=STREAMING_KAFKA_TOPIC=stream-topic \
  --from-literal=PREDICTION_KAFKA_TOPIC=prediction-topic \
  --from-literal=STREAM_SEND_INTERVAL_MS=50 \
  --from-literal=STREAM_LOOP_FILES=false \
  --from-literal=STREAM_PRODUCE_CHUNK_SIZE=1000 \
  --from-literal=MODEL_RELOAD_INTERVAL_SEC=60 \
  --from-literal=PREDICT_BATCH_SIZE=200 \
  --from-literal=DASHBOARD_MAX_ITEMS=200 \
  --from-literal=DASHBOARD_THROUGHPUT_WINDOW_SEC=10 \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> Namespace/config ready"
