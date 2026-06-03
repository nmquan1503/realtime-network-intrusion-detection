param(
    [string]$Namespace = "bigdata",
    [string]$DagId = "batch_training_pipeline_k8s"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
Set-Location $RepoRoot

Write-Host "==> Ensuring batch producer deployment exists" -ForegroundColor Cyan
kubectl apply -f k8s/simulator/batch_producer.yaml -n $Namespace
kubectl rollout restart deployment/batch-producer -n $Namespace
kubectl rollout status deployment/batch-producer -n $Namespace --timeout=180s

Write-Host "==> Waiting briefly for batch data to enter Kafka" -ForegroundColor Cyan
Start-Sleep -Seconds 30

Write-Host "==> Triggering Airflow DAG $DagId" -ForegroundColor Cyan
kubectl exec deployment/airflow-webserver -n $Namespace -- airflow dags unpause $DagId
$runId = "manual__" + (Get-Date -Format "yyyyMMdd_HHmmss")
kubectl exec deployment/airflow-webserver -n $Namespace -- airflow dags trigger $DagId --run-id $runId

Write-Host "==> Triggered DAG run: $runId" -ForegroundColor Green
Write-Host "Open Airflow: http://localhost:18080"
Write-Host "Logs: kubectl logs deployment/airflow-scheduler -n $Namespace --tail=100"
