#!/usr/bin/env bash
set -euo pipefail
NAMESPACE="${NAMESPACE:-bigdata}"

start_pf() {
  local name="$1"
  local service="$2"
  local mapping="$3"
  local log="/tmp/${name}-port-forward.log"
  echo "==> Port-forward ${name}: ${mapping}"
  nohup kubectl port-forward "$service" "$mapping" -n "$NAMESPACE" >"$log" 2>&1 &
}

start_pf airflow svc/airflow-webserver 18080:8080
start_pf dashboard svc/dashboard 15000:5000
start_pf kafka-ui svc/kafka-ui-external 18081:8080
start_pf spark-master-ui svc/spark-master-ui-external 18082:8082
start_pf minio-console svc/minio-external 19001:9001

sleep 3

URLS=(
  http://localhost:18080
  http://localhost:15000
  http://localhost:18081
  http://localhost:18082
  http://localhost:19001
)

if command -v xdg-open >/dev/null 2>&1; then
  for url in "${URLS[@]}"; do xdg-open "$url" >/dev/null 2>&1 || true; done
else
  printf '%s\n' "${URLS[@]}"
fi

echo "Airflow login: admin / admin"
echo "MinIO login: minioadmin / minioadmin123"
