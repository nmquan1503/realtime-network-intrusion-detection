import os
from datetime import datetime

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

BATCH_KAFKA_TOPIC = os.getenv("BATCH_KAFKA_TOPIC")
BATCH_PRODUCE_BATCH_SIZE = int(os.getenv("BATCH_PRODUCE_BATCH_SIZE"))

STREAMING_KAFKA_TOPIC = os.getenv("STREAMING_KAFKA_TOPIC")

PROCESS_DATE = os.getenv(
    "PROCESS_DATE",
    datetime.now().strftime("%Y-%m-%d")
)
