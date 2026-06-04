param(
    [string]$Namespace = "bigdata",
    [string]$DagId = "batch_training_pipeline_k8s"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
Set-Location $RepoRoot

Write-Host "==> Running batch producer Job once" -ForegroundColor Cyan
kubectl delete job/batch-producer -n $Namespace --ignore-not-found=true
kubectl wait --for=delete job/batch-producer -n $Namespace --timeout=60s *> $null
kubectl apply -f k8s/simulator/batch_producer.yaml -n $Namespace
kubectl wait --for=condition=complete job/batch-producer -n $Namespace --timeout=3600s
if ($LASTEXITCODE -ne 0) {
    throw "batch-producer Job did not complete successfully."
}

Write-Host "==> Waiting briefly for batch data to enter Kafka" -ForegroundColor Cyan
Start-Sleep -Seconds 10

Write-Host "==> Triggering Airflow DAG $DagId" -ForegroundColor Cyan
kubectl exec deployment/airflow-webserver -n $Namespace -- airflow dags unpause $DagId
$runId = "manual__" + (Get-Date -Format "yyyyMMdd_HHmmss")
kubectl exec deployment/airflow-webserver -n $Namespace -- airflow dags trigger $DagId --run-id $runId

Write-Host "==> Triggered DAG run: $runId" -ForegroundColor Green
Write-Host "Open Airflow: http://localhost:18080"
Write-Host "Logs: kubectl logs deployment/airflow-scheduler -n $Namespace --tail=100"
