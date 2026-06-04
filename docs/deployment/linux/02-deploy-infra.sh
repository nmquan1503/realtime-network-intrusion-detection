#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
NAMESPACE="${NAMESPACE:-bigdata}"

kubectl apply -f k8s/kafka/kafka_cluster.yaml -n "$NAMESPACE"
kubectl wait kafka/kafka-cluster --for=condition=Ready -n "$NAMESPACE" --timeout=600s
kubectl apply -f k8s/kafka/kafka_topic.yaml -n "$NAMESPACE"
kubectl apply -f k8s/kafka/kafka_ui.yaml -n "$NAMESPACE"

kubectl apply -f k8s/minio/minio_headless_service.yaml -n "$NAMESPACE"
kubectl apply -f k8s/minio/minio_external_service.yaml -n "$NAMESPACE"
kubectl apply -f k8s/minio/minio_statefulset.yaml -n "$NAMESPACE"
kubectl rollout status statefulset/minio -n "$NAMESPACE" --timeout=300s
kubectl delete job/minio-init-buckets -n "$NAMESPACE" --ignore-not-found
kubectl apply -f k8s/minio/minio_init.yaml -n "$NAMESPACE"
kubectl wait --for=condition=complete job/minio-init-buckets -n "$NAMESPACE" --timeout=180s

kubectl apply -f k8s/spark/spark_cluster.yaml -n "$NAMESPACE"
kubectl apply -f k8s/spark/spark_service.yaml -n "$NAMESPACE"
kubectl rollout status deployment/spark-master -n "$NAMESPACE" --timeout=300s
kubectl rollout status deployment/spark-worker -n "$NAMESPACE" --timeout=300s

kubectl apply -f k8s/airflow/rbac.yaml -n "$NAMESPACE"
kubectl apply -f k8s/airflow/airflow-common.yaml -n "$NAMESPACE"
kubectl apply -f k8s/airflow/postgres.yaml -n "$NAMESPACE"
kubectl rollout status deployment/airflow-postgres -n "$NAMESPACE" --timeout=300s
kubectl delete job/airflow-init -n "$NAMESPACE" --ignore-not-found
kubectl apply -f k8s/airflow/airflow-init-job.yaml -n "$NAMESPACE"
kubectl wait --for=condition=complete job/airflow-init -n "$NAMESPACE" --timeout=300s
kubectl apply -f k8s/airflow/airflow-deployments.yaml -n "$NAMESPACE"
kubectl rollout status deployment/airflow-scheduler -n "$NAMESPACE" --timeout=300s
kubectl rollout status deployment/airflow-webserver -n "$NAMESPACE" --timeout=420s

echo "==> Infra ready"
