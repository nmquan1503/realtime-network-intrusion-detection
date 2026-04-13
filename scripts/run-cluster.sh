#!/bin/bash
set -e

# --------------------------------------------------
# Apply Kafka cluster and topic
# --------------------------------------------------
kubectl apply -f k8s/kafka/kafka_cluster.yaml -n bigdata
kubectl apply -f k8s/kafka/kafka_topic.yaml -n bigdata
kubectl apply -f k8s/kafka/kafka_ui.yaml -n bigdata



# # --------------------------------------------------
# # Install or upgrade RustFS via Helm
# # --------------------------------------------------
# helm upgrade --install rustfs rustfs/rustfs \
#   -n bigdata \
#   --create-namespace \
#   -f k8s/rustfs/rustfs_values.yaml



kubectl apply -f k8s/spark/spark_cluster.yaml -n bigdata
kubectl apply -f k8s/spark/spark_service.yaml -n bigdata



kubectl apply -f k8s/minio/minio_statefulset.yaml -n bigdata
kubectl apply -f k8s/minio/minio_headless_service.yaml -n bigdata
kubectl apply -f k8s/minio/minio_external_service.yaml -n bigdata
kubectl apply -f k8s/minio/minio_init.yaml -n bigdata