param(
    [string]$Namespace = "bigdata"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
Set-Location $RepoRoot

Write-Host "==> Deploying Kafka" -ForegroundColor Cyan
kubectl apply -f k8s/kafka/kafka_cluster.yaml -n $Namespace
kubectl wait kafka/kafka-cluster --for=condition=Ready -n $Namespace --timeout=600s
kubectl apply -f k8s/kafka/kafka_topic.yaml -n $Namespace
kubectl apply -f k8s/kafka/kafka_ui.yaml -n $Namespace

Write-Host "==> Deploying MinIO" -ForegroundColor Cyan
kubectl apply -f k8s/minio/minio_headless_service.yaml -n $Namespace
kubectl apply -f k8s/minio/minio_external_service.yaml -n $Namespace
kubectl apply -f k8s/minio/minio_statefulset.yaml -n $Namespace
kubectl rollout status statefulset/minio -n $Namespace --timeout=300s
kubectl delete job/minio-init-buckets -n $Namespace --ignore-not-found
kubectl apply -f k8s/minio/minio_init.yaml -n $Namespace
kubectl wait --for=condition=complete job/minio-init-buckets -n $Namespace --timeout=180s

Write-Host "==> Deploying Spark Standalone" -ForegroundColor Cyan
kubectl apply -f k8s/spark/spark_cluster.yaml -n $Namespace
kubectl apply -f k8s/spark/spark_service.yaml -n $Namespace
kubectl rollout status deployment/spark-master -n $Namespace --timeout=300s
kubectl rollout status deployment/spark-worker -n $Namespace --timeout=300s

Write-Host "==> Deploying Airflow" -ForegroundColor Cyan
kubectl apply -f k8s/airflow/rbac.yaml -n $Namespace
kubectl apply -f k8s/airflow/postgres.yaml -n $Namespace
kubectl rollout status deployment/airflow-postgres -n $Namespace --timeout=300s
kubectl apply -f k8s/airflow/airflow-common.yaml -n $Namespace
kubectl delete job/airflow-init -n $Namespace --ignore-not-found
kubectl apply -f k8s/airflow/airflow-init-job.yaml -n $Namespace
kubectl wait --for=condition=complete job/airflow-init -n $Namespace --timeout=300s
kubectl apply -f k8s/airflow/airflow-deployments.yaml -n $Namespace
kubectl rollout status deployment/airflow-scheduler -n $Namespace --timeout=300s
kubectl rollout status deployment/airflow-webserver -n $Namespace --timeout=420s

Write-Host "==> Infra ready" -ForegroundColor Green
