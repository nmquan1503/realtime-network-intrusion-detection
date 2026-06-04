# Airflow demo worklog

Last updated: 2026-06-04

## Cap nhat 2026-06-04

Da tiep tuc mo rong project tu demo Airflow batch sang demo realtime streaming prediction + dashboard.

### Tong ket viec da hoan thanh

- Da xay luong streaming rieng cho demo realtime:
  - `data/streaming/*.csv`
  - Kafka topic `stream-topic`
  - `streaming-predictor`
  - Kafka topic `prediction-topic`
  - Dashboard realtime
- Da them `core/simulator/stream_producer.py` de doc file CSV streaming va day tung row vao Kafka.
- Da giu producer o che do khong loop file:
  - Batch producer gui het file batch mot lan roi ket thuc Job.
  - Stream producer gui het file streaming mot lan roi ket thuc Job.
  - Muon demo lai thi xoa Job cu va apply lai manifest bang script `04` hoac `05`.
- Da chinh batch producer va stream producer sang Kubernetes Job de tiet kiem tai nguyen sau khi gui het file.
- Da chinh Airflow DAG `batch_training_pipeline_k8s`:
  - Bo task `produce_batch_data` khoi DAG.
  - Flow moi:

```text
start
-> consume_kafka_to_bronze
-> preprocess_bronze_to_silver
-> feature_engineering
-> train_model
-> wait_streaming_model_reload
-> end
```

- Da them task `wait_streaming_model_reload` de Airflow UI the hien giai doan cho streaming predictor reload model moi.
- Da xay streaming predictor:
  - Consume Kafka theo micro-batch.
  - Tim model moi nhat trong MinIO duoi path `model/year=YYYY/month=MM/day=DD/HH-MM-SS/model/`.
  - Load Spark `PipelineModel`.
  - Lay feature order tu `VectorAssembler`.
  - Build dung raw feature va derived feature nhu batch:
    - `is_well_known_port`
    - `is_ssh_port`
    - `is_web_port`
    - `pkt_ratio`
    - `byte_ratio`
    - `bytes_per_pkt`
  - Validate missing feature, NaN, inf.
  - Row loi duoc gui ra dashboard voi `status=invalid`, day khong phai nhan train cua model.
  - Tu reload model moi nhat theo `MODEL_RELOAD_INTERVAL_SEC`.
- Da xay dashboard consume `prediction-topic`:
  - Luu recent prediction trong memory.
  - Hien thi Total, Benign, Attack, Invalid, Predict / Sec, Avg Latency.
  - Hien thi Label Distribution va Recent Predictions.
  - Tieu de UI da doi thanh `REALTIME NETWORK INTRUSION DETECTION`.
  - Subtitle da doi thanh `Prediction based on latest model.`
- Da them Kubernetes manifests:
  - `k8s/streaming/stream_producer.yaml`
  - `k8s/streaming/streaming_predictor.yaml`
  - `k8s/dashboard/dashboard.yaml`
- Da them Kafka topics:
  - `stream-topic`
  - `prediction-topic`
- Da cap nhat Docker/requirements cho simulator, streaming predictor va dashboard.
- Da tao bo tai lieu va script deployment moi, khong phu thuoc folder `scripts/` cu:
  - `docs/deployment/project_runbook.md`
  - `docs/deployment/windows/*.ps1`
  - `docs/deployment/linux/*.sh`
- Da commit va push len remote branch `VanDaiAiflow`:
  - `3997504 Add Streaming-Predict flow, Dashboard and project describe, guild to deployment`
  - `02ab111 Update build images script`

### Trang thai demo da dat duoc

- Airflow UI da tung mo duoc bang port-forward.
- Kafka UI, Spark Master UI va MinIO Console da co lenh port-forward trong deployment scripts.
- Streaming predictor da tung load duoc model:

```text
year=2026/month=06/day=02/20-17-49
```

- Dashboard da hien thi du lieu prediction realtime va co metric `Predict / Sec`.

### Luu y ky thuat da thong nhat

- `invalid` khong phai nhan trong model training.
- `invalid` chi la status cua streaming predictor khi row realtime co gia tri khong hop le nhu missing, NaN hoac inf.
- Cac gia tri `inf` trong du lieu CIC/IDS thuong xuat hien o cac cot rate nhu `Flow Pkts/s`, `Flow Byts/s`, nhieu truong hop do `Flow Duration = 0`.
- Batch pipeline hien tai da drop/clean cac gia tri khong hop le truoc khi train, nen model khong hoc nhan `invalid`.
- Stream producer mac dinh gui 1 row moi `50ms`, tuong duong khoang 20 rows/second neu Kafka va pod on dinh.
- Streaming predictor gom toi da `PREDICT_BATCH_SIZE=200` rows moi micro-batch, nhung batch thuc te phu thuoc toc do producer va Kafka.

### Lenh UI chinh

```text
Airflow: http://localhost:18080
Dashboard: http://localhost:15000
Kafka UI: http://localhost:18081
Spark Master UI: http://localhost:18082
MinIO Console: http://localhost:19001
```

Chay tren Windows:

```powershell
powershell -ExecutionPolicy Bypass -File docs/deployment/windows/06-open-ui.ps1
```

### Trang thai git cuoi cung

- Remote branch da push: `VanDaiAiflow`.
- Source code chinh da commit.
- Con lai cac folder tam `__pycache__` dang untracked, khong can commit.

## Muc tieu

Trien khai demo Airflow tren Docker Desktop Kubernetes cho project realtime network intrusion detection.
Airflow dung de dieu phoi batch training pipeline tren Kubernetes.

## Nhung viec da lam

- Da tang tai nguyen WSL2/Docker trong `C:\Users\ADMIN\.wslconfig`.
- Cau hinh hien tai:

```ini
[wsl2]
memory=10GB
processors=4
swap=4GB
localhostForwarding=true
```

- Da deploy namespace `bigdata`.
- Da deploy Airflow:
  - Postgres metadata DB.
  - Airflow init job.
  - Airflow webserver.
  - Airflow scheduler.
  - RBAC cho KubernetesPodOperator.
- Da deploy Kafka/Strimzi:
  - Kafka cluster `kafka-cluster`.
  - Topic `openmeteo-data`.
  - Kafka UI.
- Da deploy MinIO:
  - StatefulSet 2 replicas.
  - Service external/nodeport.
- Da deploy Spark:
  - Spark master.
  - Spark worker.
- Da build local image:
  - `mquan1503/bigdata-airflow:latest`
  - `mquan1503/bigdata-simulator:latest`
  - `mquan1503/bigdata-spark:latest`
- Image con thieu cho demo full DAG:
  - `mquan1503/bigdata-batch:latest`

## Airflow URL

Khong dung `localhost:8080` cho Airflow vi Kafka UI dang chiem cong nay.

Dung port-forward rieng:

```powershell
kubectl port-forward svc/airflow-webserver 18080:8080 -n bigdata
```

Sau do mo:

```text
http://localhost:18080
```

Login:

```text
admin / admin
```

Neu `18080` bi ket do port-forward cu, co the dung cong khac:

```powershell
kubectl port-forward svc/airflow-webserver 18081:8080 -n bigdata
```

## Trang thai cuoi cung truoc khi dung

Nguoi dung yeu cau dung trien khai.

Truoc do:

- Airflow webserver da tung vao duoc UI qua `localhost:18080`.
- Log webserver cho thay request UI va login thanh cong.
- Sau do Airflow webserver bi `WORKER TIMEOUT`.
- Kubernetes readiness probe cho webserver bi timeout.
- Da sua manifest Airflow de on dinh hon:
  - `AIRFLOW__WEBSERVER__WORKERS: "2"`
  - `AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT: "600"`
  - `AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT: "600"`
  - Deployment strategy doi sang `Recreate`.
  - Webserver limit tang len `cpu: "2"`, `memory: "3Gi"`.
- Da apply lai Airflow manifest.
- Da restart rollout webserver/scheduler.
- Rollout bi ket vi pod cu dang `Terminating`.
- Da scale Airflow webserver/scheduler xuong 0.
- Da force delete 2 pod Airflow dang ket:
  - `airflow-scheduler-5cfd55dd87-4ph9p`
  - `airflow-webserver-b7b744b67-pnptt`
- Lenh scale Airflow len lai 1 replica bi `TLS handshake timeout`.
- Khong tiep tuc thao tac nua theo yeu cau nguoi dung.

Luu y quan trong:

- File manifest `k8s/airflow/airflow-deployments.yaml` van khai bao `replicas: 1`.
- Neu live deployment dang bi scale ve 0, chi can apply lai manifest hoac scale len lai 1.
- Docker Desktop/Kubernetes API co luc bi cham/timeout, nen can cho Docker on dinh truoc khi deploy tiep.

## Cac file Airflow da tao/sua

- `k8s/airflow/rbac.yaml`
- `k8s/airflow/postgres.yaml`
- `k8s/airflow/airflow-common.yaml`
- `k8s/airflow/airflow-init-job.yaml`
- `k8s/airflow/airflow-deployments.yaml`
- `orchestration/airflow/dags/batch_dag.py`
- `docker/Dockerfile.airflow`

## DAG hien tai

DAG:

```text
batch_training_pipeline_k8s
```

Pipeline:

```text
produce_batch_data
-> consume_kafka_to_bronze
-> preprocess_bronze_to_silver
-> feature_engineering
-> train_model
```

DAG dung KubernetesPodOperator de tao pod job trong namespace `bigdata`.

## Viec can lam khi trien khai lai

1. Kiem tra Docker/Kubernetes:

```powershell
docker version
kubectl cluster-info
kubectl get pods -n bigdata
```

2. Neu Airflow dang scale 0, bat lai:

```powershell
kubectl apply -f k8s/airflow/airflow-common.yaml -f k8s/airflow/airflow-deployments.yaml -n bigdata
kubectl scale deployment/airflow-webserver deployment/airflow-scheduler -n bigdata --replicas=1
```

3. Cho Airflow san sang:

```powershell
kubectl rollout status deployment/airflow-webserver -n bigdata --timeout=240s
kubectl rollout status deployment/airflow-scheduler -n bigdata --timeout=240s
kubectl get pods -n bigdata | findstr airflow
```

4. Mo UI Airflow:

```powershell
kubectl port-forward svc/airflow-webserver 18080:8080 -n bigdata
```

5. Test health:

```powershell
Invoke-WebRequest -UseBasicParsing http://localhost:18080/health
```

6. Neu can chay full DAG, build image batch truoc:

```powershell
docker build -f docker/Dockerfile.batch -t mquan1503/bigdata-batch:latest .
```

7. Tao/init bucket MinIO neu chua co:

```powershell
kubectl apply -f k8s/minio/minio_init.yaml -n bigdata
kubectl wait --for=condition=complete job/minio-init-buckets -n bigdata --timeout=180s
```

8. Trigger DAG bang UI hoac CLI:

```powershell
kubectl exec -n bigdata deployment/airflow-scheduler -- airflow dags trigger batch_training_pipeline_k8s
```

## Loi/van de da gap

- Docker Desktop tung crash voi loi lien quan `com.docker.build`.
- Docker/Kubernetes tung bi `TLS handshake timeout`.
- `docker stats` va `docker version` co luc timeout khi Docker Desktop qua tai.
- Airflow webserver bi Gunicorn `WORKER TIMEOUT`.
- Simple Browser trong VS Code co the tai UI cham hon browser ngoai.
- `localhost:8080` trung voi Kafka UI, nen Airflow can dung `18080`.

## Goi y khi tiep tuc

- Khong nen trigger DAG khi Docker/Kubernetes dang timeout.
- Nen chi mo cac thanh phan can demo, tranh chay qua nhieu stack cung luc.
- Neu Docker tiep tuc ket, restart Docker Desktop sau do cho pod warm-up 2-5 phut.
- Neu Kubernetes API on dinh, tiep tuc tu buoc scale/apply Airflow len lai.
