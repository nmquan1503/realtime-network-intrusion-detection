# Batch Airflow Orchestration on Kubernetes

## Overview

This project uses Apache Airflow to orchestrate the batch training pipeline for the realtime network intrusion detection system.

The Airflow DAG is:

`producer -> consumer -> preprocessing -> feature_engineering -> trainer`

Each step runs in its own Kubernetes pod through `KubernetesPodOperator`.

## DAG

- DAG id: `batch_training_pipeline_k8s`
- File: `orchestration/airflow/dags/batch_dag.py`
- Namespace: `bigdata`
- Schedule: daily at `01:00`
- Catchup: `False`

Task order:

1. `produce_batch_data`
2. `consume_kafka_to_bronze`
3. `preprocess_bronze_to_silver`
4. `feature_engineering`
5. `train_model`

`PROCESS_DATE={{ ds }}` is passed into the batch tasks so bronze, silver, and gold partitions stay aligned by day.

The batch producer keeps its original behavior and pushes all CSV files packaged in `/data/csv` into Kafka for each batch run.

## Images

Build and push the images before deploying Airflow:

```bash
./scripts/build-images.sh
```

The batch orchestration uses:

- `mquan1503/bigdata-simulator:latest`
- `mquan1503/bigdata-batch:latest`
- `mquan1503/bigdata-airflow:latest`

## Deploy Airflow

Apply the Airflow manifests in this order:

```bash
kubectl apply -f k8s/airflow/rbac.yaml -n bigdata
kubectl apply -f k8s/airflow/airflow-common.yaml -n bigdata
kubectl apply -f k8s/airflow/postgres.yaml -n bigdata
kubectl apply -f k8s/airflow/airflow-init-job.yaml -n bigdata
kubectl wait --for=condition=complete job/airflow-init -n bigdata --timeout=300s
kubectl apply -f k8s/airflow/airflow-deployments.yaml -n bigdata
```

Port-forward the webserver:

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 -n bigdata
```

Open Airflow at `http://localhost:8080`.

Default demo credentials from the manifest:

- Username: `admin`
- Password: `admin`

## Enable the DAG

After the webserver and scheduler are running:

1. Open the Airflow UI.
2. Find `batch_training_pipeline_k8s`.
3. Unpause the DAG.
4. Trigger a manual run and choose the logical date you want downstream bronze, silver, and gold partitions to use.

## Task Logs

You can inspect logs from the Airflow UI or directly from Kubernetes:

```bash
kubectl get pods -n bigdata -l pipeline=batch-training
kubectl logs -n bigdata <pod-name>
```

Scheduler and webserver logs:

```bash
kubectl logs deploy/airflow-scheduler -n bigdata
kubectl logs deploy/airflow-webserver -n bigdata
```

## Data Checks in MinIO

After each step, verify the expected prefixes in MinIO:

- Bronze: `s3://$MINIO_BUCKET/bronze/year=YYYY/month=MM/day=DD/`
- Silver: `s3://$MINIO_BUCKET/silver/year=YYYY/month=MM/day=DD/`
- Gold: `s3://$MINIO_BUCKET/gold/year=YYYY/month=MM/day=DD/`
- Model: `s3://$MINIO_BUCKET/model/`

The consumer writes bronze data using `PROCESS_DATE`, so the bronze partition date stays consistent with preprocessing and feature engineering.

## Notes

- This task only wires the batch training orchestration.
- Streaming inference and dashboard logic are intentionally left untouched.
- The simulator image now contains the demo batch CSV files at `/data/csv`, so the producer pod can run without a hostPath mount.
