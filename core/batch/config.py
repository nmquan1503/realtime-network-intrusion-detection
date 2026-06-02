import os
from datetime import datetime

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("BATCH_KAFKA_TOPIC")
KAFKA_GROUP_ID = os.getenv("BATCH_KAFKA_GROUP_ID")
KAFKA_CONSUMER_POLL_TIMEOUT = os.getenv("KAFKA_CONSUMER_POLL_TIMEOUT")
FLUSH_SIZE = int(os.getenv("BATCH_FLUSH_SIZE"))
MAX_SILENCE_TIME = int(os.getenv("BATCH_MAX_SILENCE_TIME"))
KAFKA_BATCH_SIZE = int(os.getenv("BATCH_PRODUCE_BATCH_SIZE"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_RETRY_COUNT = int(os.getenv("MINIO_RETRY_COUNT"))
MINIO_RETRY_DELAY = int(os.getenv("MINIO_RETRY_DELAY"))

SPARK_MASTER_URL = os.getenv(
    "SPARK_MASTER_URL",
    "spark://spark-master-svc.bigdata.svc.cluster.local:7077"
)

PROCESS_DATE = os.getenv(
    "PROCESS_DATE",
    datetime.now().strftime("%Y-%m-%d")
)
