param(
    [string]$Namespace = "bigdata"
)

$ErrorActionPreference = "Stop"

function Start-PortForward($Name, $Service, $Mapping) {
    Write-Host "==> Port-forward ${Name}: $Mapping" -ForegroundColor Cyan
    Start-Process -WindowStyle Hidden kubectl -ArgumentList @("port-forward", $Service, $Mapping, "-n", $Namespace)
}

Start-PortForward "Airflow" "svc/airflow-webserver" "18080:8080"
Start-PortForward "Dashboard" "svc/dashboard" "15000:5000"
Start-PortForward "Kafka UI" "svc/kafka-ui-external" "18081:8080"
Start-PortForward "Spark Master UI" "svc/spark-master-ui-external" "18082:8082"
Start-PortForward "MinIO Console" "svc/minio-external" "19001:9001"

Start-Sleep -Seconds 3

$urls = @(
    "http://localhost:18080",
    "http://localhost:15000",
    "http://localhost:18081",
    "http://localhost:18082",
    "http://localhost:19001"
)

foreach ($url in $urls) {
    Start-Process $url
}

Write-Host "Airflow login: admin / admin"
Write-Host "MinIO login: minioadmin / minioadmin123"
