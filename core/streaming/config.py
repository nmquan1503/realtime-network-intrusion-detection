import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-cluster-kafka-bootstrap.bigdata.svc.cluster.local:9092",
)
STREAMING_KAFKA_TOPIC = os.getenv("STREAMING_KAFKA_TOPIC", "stream-topic")
PREDICTION_KAFKA_TOPIC = os.getenv("PREDICTION_KAFKA_TOPIC", "prediction-topic")
STREAMING_KAFKA_GROUP_ID = os.getenv("STREAMING_KAFKA_GROUP_ID", "streaming-predictor")
STREAMING_AUTO_OFFSET_RESET = os.getenv("STREAMING_AUTO_OFFSET_RESET", "latest")

PREDICT_BATCH_SIZE = int(os.getenv("PREDICT_BATCH_SIZE", "200"))
PREDICT_POLL_TIMEOUT_MS = int(os.getenv("PREDICT_POLL_TIMEOUT_MS", "1000"))
MODEL_RELOAD_INTERVAL_SEC = int(os.getenv("MODEL_RELOAD_INTERVAL_SEC", "60"))
MODEL_NOT_FOUND_SLEEP_SEC = int(os.getenv("MODEL_NOT_FOUND_SLEEP_SEC", "10"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.bigdata.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "data-lake")
MODEL_PREFIX = os.getenv("MODEL_PREFIX", "model/")

SPARK_MASTER_URL = os.getenv(
    "SPARK_MASTER_URL",
    "spark://spark-master-svc.bigdata.svc.cluster.local:7077",
)
