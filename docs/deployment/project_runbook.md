# Realtime Network Intrusion Detection - Project Runbook

Tài liệu này mô tả tổng quan project, bố cục thư mục, công cụ sử dụng, luồng hoạt động và hướng dẫn chạy hệ thống từ đầu. Bộ script deployment mới nằm trong `docs/deployment` và không phụ thuộc vào folder `scripts` cũ.

## 1. Tổng Quan

Project xây dựng hệ thống phát hiện tấn công mạng theo thời gian thực. Hệ thống có hai luồng chính:

```text
Batch producer -> Kafka openmeteo-data -> Airflow batch DAG -> MinIO bronze/silver/gold/model
Stream producer -> Kafka stream-topic -> Streaming predictor -> Kafka prediction-topic -> Dashboard
```

Luồng batch train model định kỳ bằng Airflow. Luồng streaming nhận dữ liệu realtime, load model mới nhất từ MinIO, predict bằng Spark model và gửi kết quả lên dashboard.

## 2. Bố Cục Folder

```text
core/batch
```

Code xử lý batch training:

- `consumer.py`: consume Kafka topic batch và ghi raw data vào MinIO bronze.
- `preprocessing.py`: đọc bronze, cast schema, drop null/NaN/inf, ghi silver.
- `feature_engineering.py`: tạo feature bổ sung và chuẩn hóa label, ghi gold.
- `trainer.py`: train Spark/XGBoost pipeline model và ghi model/metrics vào MinIO.
- `spark_session.py`: tạo SparkSession dùng Spark Standalone.

```text
core/simulator
```

Producer dữ liệu demo:

- `batch_producer.py`: đọc `data/batch/*.csv`, gửi từng dòng vào Kafka `openmeteo-data`.
- `stream_producer.py`: đọc `data/streaming/*.csv`, gửi từng dòng vào Kafka `stream-topic`.
- `config.py`: cấu hình Kafka, tốc độ gửi, loop/idle behavior.

```text
core/streaming
```

Luồng inference realtime:

- `streaming_job.py`: service chạy nền consume `stream-topic`, predict, produce `prediction-topic`.
- `model_loader.py`: tìm model mới nhất trong MinIO theo path `model/year=*/month=*/day=*/HH-MM-SS/model/`.
- `feature_builder.py`: dựng đúng feature order từ model assembler, validate input, tạo derived features.

```text
core/services/dashboard
```

Dashboard Flask:

- `app.py`: consume `prediction-topic`, giữ recent predictions trong memory, expose `/data`.
- `templates/index.html`: UI realtime hiển thị model version, counts, throughput, latency, recent predictions.

```text
orchestration/airflow/dags
```

Airflow DAG:

- `batch_dag.py`: DAG `batch_training_pipeline_k8s` chạy consumer -> preprocessing -> feature engineering -> trainer -> wait streaming reload.

```text
k8s
```

Kubernetes manifests cho Kafka, MinIO, Spark, Airflow, batch jobs, simulator, streaming, dashboard.

```text
docker
```

Dockerfiles cho batch, Spark cluster, simulator, streaming predictor, dashboard, Airflow.

```text
data/batch
data/streaming
```

CSV demo cho batch training và streaming realtime.

```text
docs/deployment
```

Runbook và script deployment mới cho Windows/Linux.

## 3. Công Cụ Sử Dụng

- Docker: build image cho từng service.
- Kubernetes: chạy toàn bộ hệ thống bằng namespace `bigdata`.
- Strimzi Kafka Operator: triển khai Kafka KRaft cluster và KafkaTopic.
- Kafka: message bus cho batch data, stream data, prediction output.
- Spark Standalone: Spark master/worker cho batch preprocessing/feature/training và streaming inference.
- MinIO: object storage S3-compatible lưu bronze/silver/gold/model.
- Airflow: điều phối batch training theo lịch hoặc manual trigger.
- Flask: dashboard web UI.
- PySpark/XGBoost: train và inference model ML.
- `kafka-python`, `pandas`, `boto3`, `pyarrow`: producer, consumer, MinIO/parquet utilities.

## 4. Kafka Topics

```text
openmeteo-data
```

Batch producer gửi dữ liệu lịch sử vào topic này. Batch consumer đọc topic này để ghi bronze.

```text
stream-topic
```

Stream producer gửi dữ liệu realtime demo vào topic này. Streaming predictor đọc topic này.

```text
prediction-topic
```

Streaming predictor ghi prediction output vào topic này. Dashboard consume topic này để hiển thị.

## 5. MinIO Layout

Bucket chính:

```text
data-lake
```

Các path logic:

```text
bronze/year=YYYY/month=MM/day=DD/*.parquet
silver/year=YYYY/month=MM/day=DD/*.parquet
gold/year=YYYY/month=MM/day=DD/*.parquet
model/year=YYYY/month=MM/day=DD/HH-MM-SS/model/
model/year=YYYY/month=MM/day=DD/HH-MM-SS/metrics/
```

Trainer ghi model theo timestamp hiện tại. Streaming predictor tự tìm version mới nhất theo path `model/year=*/month=*/day=*/HH-MM-SS/model/` và reload mỗi 60 giây.

## 6. Batch Flow

1. `batch_producer.py` đọc toàn bộ CSV trong `/data/csv` và gửi từng row vào Kafka `openmeteo-data`.
2. Airflow DAG `batch_training_pipeline_k8s` chạy `consume_kafka_to_bronze`.
3. Consumer đọc Kafka, gom batch, ghi parquet vào MinIO bronze.
4. Preprocessing đọc bronze, cast schema, drop null/NaN/inf, ghi silver.
5. Feature engineering tạo feature: `is_well_known_port`, `is_ssh_port`, `is_web_port`, `pkt_ratio`, `byte_ratio`, `bytes_per_pkt`.
6. Trainer đọc gold, train Spark XGBoost pipeline, ghi model và metrics vào MinIO.
7. DAG chờ 75 giây để streaming predictor có thời gian reload model mới.

DAG schedule hiện tại:

```python
schedule="0 1 * * *"
```

Nghĩa là DAG chạy vào 01:00 hằng ngày nếu DAG đang unpause. DAG không tự pause sau khi chạy xong.

## 7. Streaming Flow

1. `stream_producer.py` đọc CSV trong `/data/streaming`.
2. Producer gửi từng row vào Kafka `stream-topic`.
3. `streaming_job.py` chạy nền, dùng SparkSession dài hạn.
4. Predictor tìm model mới nhất trong MinIO.
5. Predictor lấy feature order từ `VectorAssembler.getInputCols()` trong model.
6. Predictor build raw + derived features giống batch.
7. Dòng thiếu feature/null/inf được gửi ra dashboard với `status=invalid`, không coi là label model.
8. Dòng hợp lệ được predict và gửi vào `prediction-topic`.
9. Dashboard consume `prediction-topic` và update UI.

## 8. Dashboard Metrics

Dashboard hiển thị:

```text
Total
Benign
Attack
Invalid
Predict / Sec
Avg Latency
Label Distribution
Recent Predictions
Model Version
```

`Actual` là nhãn thật từ cột `Label` trong CSV demo. Realtime ngoài đời thường không có actual label ngay lập tức.

`Invalid` là trạng thái dòng input không hợp lệ để predict, không phải class label của model.

`Predict / Sec` là số prediction dashboard nhận được mỗi giây, tính trên cửa sổ rolling 10 giây.

## 9. Yêu Cầu Trước Khi Chạy

Windows:

- Docker Desktop đang bật.
- Kubernetes trong Docker Desktop đang enabled và Running.
- `kubectl` trỏ đúng context Docker Desktop.
- PowerShell 5+ hoặc PowerShell 7.

Linux:

- Docker đã cài.
- Kubernetes cluster đã chạy.
- `kubectl` đã cấu hình đúng context.
- Cluster có storage class tương thích với manifest MinIO. Nếu dùng kubeadm từ máy trắng, cần cài CNI và storage class trước.

Kiểm tra nhanh:

```powershell
docker version
kubectl cluster-info
kubectl get nodes
```

## 10. Chạy Từ Đầu Trên Windows

Mở PowerShell tại root repo:

```powershell
cd F:\BTL_BigData\realtime-network-intrusion-detection
```

Nếu script bị chặn execution policy:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

Build toàn bộ image local:

```powershell
.\docs\deployment\windows\00-build-images.ps1
```

Tạo namespace, cài Strimzi operator, tạo ConfigMap `config`:

```powershell
.\docs\deployment\windows\01-create-namespace-config.ps1
```

Deploy infrastructure:

```powershell
.\docs\deployment\windows\02-deploy-infra.ps1
```

Deploy app services:

```powershell
.\docs\deployment\windows\03-deploy-apps.ps1
```

Lệnh này deploy dashboard và streaming predictor. Producer được chạy theo Job ở bước `04` và `05`.

Chạy batch training qua Airflow:

```powershell
.\docs\deployment\windows\04-run-batch-training.ps1
```

Chạy streaming demo:

```powershell
.\docs\deployment\windows\05-run-streaming-demo.ps1
```

Bật UI:

```powershell
.\docs\deployment\windows\06-open-ui.ps1
```

Xem trạng thái:

```powershell
.\docs\deployment\windows\07-status.ps1
```

Cleanup app/infrastructure nếu cần:

```powershell
.\docs\deployment\windows\99-cleanup.ps1
```

Xóa toàn bộ namespace nếu muốn reset sạch:

```powershell
.\docs\deployment\windows\99-cleanup.ps1 -DeleteNamespace
```

## 11. Chạy Từ Đầu Trên Linux

Mở terminal tại root repo:

```bash
cd /path/to/realtime-network-intrusion-detection
```

Cấp quyền chạy script:

```bash
chmod +x docs/deployment/linux/*.sh
```

Build toàn bộ image local:

```bash
./docs/deployment/linux/00-build-images.sh
```

Tạo namespace/config:

```bash
./docs/deployment/linux/01-create-namespace-config.sh
```

Deploy infrastructure:

```bash
./docs/deployment/linux/02-deploy-infra.sh
```

Deploy apps:

```bash
./docs/deployment/linux/03-deploy-apps.sh
```

Lệnh này deploy dashboard và streaming predictor. Producer được chạy theo Job ở bước `04` và `05`.

Chạy batch training:

```bash
./docs/deployment/linux/04-run-batch-training.sh
```

Chạy streaming demo:

```bash
./docs/deployment/linux/05-run-streaming-demo.sh
```

Bật UI:

```bash
./docs/deployment/linux/06-open-ui.sh
```

Xem status:

```bash
./docs/deployment/linux/07-status.sh
```

Cleanup:

```bash
./docs/deployment/linux/99-cleanup.sh
```

Reset namespace:

```bash
./docs/deployment/linux/99-cleanup.sh --delete-namespace
```

## 12. UI URLs

Sau khi chạy script `06-open-ui`:

```text
Airflow: http://localhost:18080
Dashboard: http://localhost:15000
Kafka UI: http://localhost:18081
Spark Master UI: http://localhost:18082
MinIO Console: http://localhost:19001
```

Airflow login:

```text
username: admin
password: admin
```

MinIO login:

```text
username: minioadmin
password: minioadmin123
```

## 13. Lệnh Kiểm Tra Thủ Công

Pods/services/jobs:

```bash
kubectl get pods,svc,jobs -n bigdata
```

Kafka topics:

```bash
kubectl get kafkatopic -n bigdata
```

Streaming predictor logs:

```bash
kubectl logs deployment/streaming-predictor -n bigdata --tail=100
```

Dashboard data API:

```bash
curl http://localhost:15000/data
```

Airflow DAG trigger thủ công:

```bash
kubectl exec deployment/airflow-webserver -n bigdata -- airflow dags trigger batch_training_pipeline_k8s
```

Xem model trong MinIO:

```text
data-lake/model/year=YYYY/month=MM/day=DD/HH-MM-SS/model/
```

## 14. Troubleshooting

Docker Desktop bị thiếu RAM:

- Tăng memory WSL/Docker Desktop lên tối thiểu 8GB nếu có thể.
- Spark + Airflow + Kafka + MinIO chạy cùng lúc khá nặng.

Airflow không mở được:

```bash
kubectl get pods -n bigdata | grep airflow
kubectl logs deployment/airflow-webserver -n bigdata --tail=100
kubectl port-forward svc/airflow-webserver 18080:8080 -n bigdata
```

Kafka topic chưa ready:

```bash
kubectl get kafka,kafkatopic -n bigdata
kubectl logs deployment/strimzi-cluster-operator -n bigdata --tail=100
```

Streaming predictor chưa predict:

- Kiểm tra đã có model trong MinIO chưa.
- Kiểm tra logs:

```bash
kubectl logs deployment/streaming-predictor -n bigdata --tail=200
```

Dashboard không tăng số liệu:

- Kiểm tra stream producer đã chạy chưa.
- Kiểm tra prediction topic có message trong Kafka UI.
- Kiểm tra dashboard logs:

```bash
kubectl logs deployment/dashboard -n bigdata --tail=100
```

Build image streaming quá lâu:

- Image streaming tải PySpark/XGBoost/scikit-learn và Hadoop AWS jars nên build lần đầu có thể mất nhiều phút.

Cluster khác Docker Desktop không thấy local image:

- Dùng `docker push` lên registry hoặc load image vào node runtime.
- Mặc định scripts hiện tại không push image.

## 15. Ghi Chú Vận Hành Demo

Producer batch và streaming hiện chạy bằng Kubernetes Job và không loop file. Khi gửi hết CSV, Job sẽ `Completed` để không giữ tài nguyên chạy nền.

Muốn chạy lại producer bằng script:

```bash
bash docs/deployment/linux/04-run-batch-training.sh
bash docs/deployment/linux/05-run-streaming-demo.sh
```

Muốn chạy lại thủ công thì xóa Job cũ rồi apply lại manifest:

```bash
kubectl delete job/batch-producer -n bigdata --ignore-not-found=true
kubectl apply -f k8s/simulator/batch_producer.yaml -n bigdata

kubectl delete job/stream-producer -n bigdata --ignore-not-found=true
kubectl apply -f k8s/streaming/stream_producer.yaml -n bigdata
```

Streaming predictor tự reload model mới nhất mỗi 60 giây. Sau khi trainer ghi model mới, dashboard sẽ chuyển sang model version mới sau khoảng 60-75 giây.
