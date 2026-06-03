param(
    [string]$Namespace = "bigdata"
)

$ErrorActionPreference = "Stop"

Write-Host "==> Kubernetes resources" -ForegroundColor Cyan
kubectl get pods,svc,deploy,jobs -n $Namespace

Write-Host "`n==> Kafka topics" -ForegroundColor Cyan
kubectl get kafkatopic -n $Namespace

Write-Host "`n==> Streaming predictor logs" -ForegroundColor Cyan
kubectl logs deployment/streaming-predictor -n $Namespace --tail=80

Write-Host "`n==> Dashboard logs" -ForegroundColor Cyan
kubectl logs deployment/dashboard -n $Namespace --tail=40
