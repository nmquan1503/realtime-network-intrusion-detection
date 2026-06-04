param(
    [string]$Namespace = "bigdata",
    [switch]$DeleteNamespace
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
Set-Location $RepoRoot

if ($DeleteNamespace) {
    Write-Host "==> Deleting namespace $Namespace" -ForegroundColor Yellow
    kubectl delete namespace $Namespace --ignore-not-found
    exit 0
}

Write-Host "==> Deleting app workloads" -ForegroundColor Yellow
kubectl delete -f k8s/streaming/stream_producer.yaml -n $Namespace --ignore-not-found
kubectl delete -f k8s/streaming/streaming_predictor.yaml -n $Namespace --ignore-not-found
kubectl delete -f k8s/dashboard/dashboard.yaml -n $Namespace --ignore-not-found
kubectl delete -f k8s/simulator/batch_producer.yaml -n $Namespace --ignore-not-found

Write-Host "==> Deleting Airflow" -ForegroundColor Yellow
kubectl delete -f k8s/airflow/airflow-deployments.yaml -n $Namespace --ignore-not-found
kubectl delete job/airflow-init -n $Namespace --ignore-not-found

Write-Host "==> Cleanup completed. PVCs and infra are kept. Use -DeleteNamespace for full reset." -ForegroundColor Green
