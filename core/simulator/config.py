import os
from datetime import datetime

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

BATCH_KAFKA_TOPIC = os.getenv("BATCH_KAFKA_TOPIC")
BATCH_PRODUCE_BATCH_SIZE = int(os.getenv("BATCH_PRODUCE_BATCH_SIZE"))
BATCH_LOOP_FILES = os.getenv("BATCH_LOOP_FILES", "false").lower() == "true"
BATCH_LOOP_SLEEP_SEC = int(os.getenv("BATCH_LOOP_SLEEP_SEC", "300"))
PRODUCER_IDLE_AFTER_COMPLETE = os.getenv("PRODUCER_IDLE_AFTER_COMPLETE", "false").lower() == "true"
PRODUCER_IDLE_SLEEP_SEC = int(os.getenv("PRODUCER_IDLE_SLEEP_SEC", "3600"))

STREAMING_KAFKA_TOPIC = os.getenv("STREAMING_KAFKA_TOPIC")
STREAM_SEND_INTERVAL_MS = int(os.getenv("STREAM_SEND_INTERVAL_MS", "50"))
STREAM_LOOP_FILES = os.getenv("STREAM_LOOP_FILES", "false").lower() == "true"
STREAM_PRODUCE_CHUNK_SIZE = int(os.getenv("STREAM_PRODUCE_CHUNK_SIZE", "1000"))

PROCESS_DATE = os.getenv(
    "PROCESS_DATE",
    datetime.now().strftime("%Y-%m-%d")
)
