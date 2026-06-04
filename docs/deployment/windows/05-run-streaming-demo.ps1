param(
    [string]$Namespace = "bigdata"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
Set-Location $RepoRoot

Write-Host "==> Running stream producer Job once" -ForegroundColor Cyan
kubectl delete job/stream-producer -n $Namespace --ignore-not-found=true
kubectl wait --for=delete job/stream-producer -n $Namespace --timeout=60s *> $null
kubectl apply -f k8s/streaming/stream_producer.yaml -n $Namespace
kubectl wait --for=condition=Ready pod -l job-name=stream-producer -n $Namespace --timeout=120s
if ($LASTEXITCODE -ne 0) {
    throw "stream-producer Job pod did not become ready."
}

Write-Host "==> Streaming demo started" -ForegroundColor Green
Write-Host "Dashboard: http://localhost:15000"
Write-Host "Predictor logs: kubectl logs deployment/streaming-predictor -n $Namespace --tail=100"
