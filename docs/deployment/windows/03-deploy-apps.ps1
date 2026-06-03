param(
    [string]$Namespace = "bigdata"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
Set-Location $RepoRoot

Write-Host "==> Deploying dashboard and streaming predictor" -ForegroundColor Cyan
kubectl apply -f k8s/dashboard/dashboard.yaml -n $Namespace
kubectl apply -f k8s/streaming/streaming_predictor.yaml -n $Namespace
kubectl rollout status deployment/dashboard -n $Namespace --timeout=180s
kubectl rollout status deployment/streaming-predictor -n $Namespace --timeout=300s

Write-Host "==> Applying producer deployments without forcing replay" -ForegroundColor Cyan
kubectl apply -f k8s/simulator/batch_producer.yaml -n $Namespace
kubectl apply -f k8s/streaming/stream_producer.yaml -n $Namespace

Write-Host "==> Apps deployed. Use 04/05 scripts to restart producers intentionally." -ForegroundColor Green
