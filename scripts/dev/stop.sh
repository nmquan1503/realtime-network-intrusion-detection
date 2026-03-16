#!/bin/bash

set -e

echo "Stopping cluster..."

cd infrastructure/dev

docker compose down

echo "Cluster stopped."