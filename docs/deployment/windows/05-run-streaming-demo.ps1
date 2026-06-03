param(
    [string]$Namespace = "bigdata"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
Set-Location $RepoRoot

Write-Host "==> Ensuring streaming services exist" -ForegroundColor Cyan
kubectl apply -f k8s/streaming/streaming_predictor.yaml -n $Namespace
kubectl apply -f k8s/dashboard/dashboard.yaml -n $Namespace
kubectl apply -f k8s/streaming/stream_producer.yaml -n $Namespace
kubectl rollout status deployment/streaming-predictor -n $Namespace --timeout=300s
kubectl rollout status deployment/dashboard -n $Namespace --timeout=180s

Write-Host "==> Restarting stream producer to send streaming CSV once" -ForegroundColor Cyan
kubectl rollout restart deployment/stream-producer -n $Namespace
kubectl rollout status deployment/stream-producer -n $Namespace --timeout=180s

Write-Host "==> Streaming demo started" -ForegroundColor Green
Write-Host "Dashboard: http://localhost:15000"
Write-Host "Predictor logs: kubectl logs deployment/streaming-predictor -n $Namespace --tail=100"
