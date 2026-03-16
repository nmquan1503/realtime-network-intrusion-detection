#!/bin/bash

set -e

echo "Starting cluster..."

cd infrastructure/dev

docker compose up -d --build

echo ""
echo "Cluster started."
echo ""
echo "Services:"
echo "Spark UI      -> http://localhost:8081"
echo "HDFS UI       -> http://localhost:9870"
echo "Airflow       -> http://localhost:8080"
echo "Dashboard     -> http://localhost:5000"