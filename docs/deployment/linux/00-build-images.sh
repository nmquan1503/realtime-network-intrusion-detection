#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."

PUSH="false"
NO_CACHE="false"
for arg in "$@"; do
  case "$arg" in
    --push) PUSH="true" ;;
    --no-cache) NO_CACHE="true" ;;
  esac
done

IMAGES=(
  "docker/Dockerfile.batch mquan1503/bigdata-batch:latest"
  "docker/Dockerfile.simulator mquan1503/bigdata-simulator:latest"
  "docker/Dockerfile.streaming mquan1503/bigdata-streaming:latest"
  "docker/Dockerfile.dashboard mquan1503/bigdata-dashboard:latest"
  "docker/Dockerfile.airflow mquan1503/bigdata-airflow:latest"
  "docker/Dockerfile.spark mquan1503/bigdata-spark:latest"
)

for item in "${IMAGES[@]}"; do
  dockerfile="${item% *}"
  tag="${item#* }"
  echo "==> Building ${tag}"
  if [[ "$NO_CACHE" == "true" ]]; then
    docker build --no-cache -f "$dockerfile" -t "$tag" .
  else
    docker build -f "$dockerfile" -t "$tag" .
  fi
  if [[ "$PUSH" == "true" ]]; then
    echo "==> Pushing ${tag}"
    docker push "$tag"
  fi
done
