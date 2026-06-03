import glob
import json
import math
import os
import time
from typing import Any

import pandas as pd
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
STREAMING_KAFKA_TOPIC = os.getenv("STREAMING_KAFKA_TOPIC", "stream-topic")
STREAM_SEND_INTERVAL_MS = int(os.getenv("STREAM_SEND_INTERVAL_MS", "50"))
STREAM_LOOP_FILES = os.getenv("STREAM_LOOP_FILES", "false").lower() == "true"
STREAMING_DATA_DIR = os.getenv("STREAMING_DATA_DIR", "data/streaming")


def clean_value(value: Any):
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def create_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value, allow_nan=False).encode("utf-8"),
                acks="all",
                retries=5,
            )
        except Exception as exc:
            print(f"[StreamSimulator] Kafka not available, retrying in 5s: {exc}", flush=True)
            time.sleep(5)


def stream_simulator():
    producer = create_producer()

    while True:
        filepaths = sorted(glob.glob(f"{STREAMING_DATA_DIR}/*.csv"))
        if not filepaths:
            print(f"[StreamSimulator] No CSV files found in {STREAMING_DATA_DIR}", flush=True)
            time.sleep(10)
            continue

        for filepath in filepaths:
            print(f"[StreamSimulator] Streaming {filepath}", flush=True)
            for chunk in pd.read_csv(filepath, chunksize=1000):
                for _, row in chunk.iterrows():
                    data = {column: clean_value(value) for column, value in row.items()}
                    producer.send(STREAMING_KAFKA_TOPIC, data)
                    if STREAM_SEND_INTERVAL_MS > 0:
                        time.sleep(STREAM_SEND_INTERVAL_MS / 1000)
                producer.flush()

        if not STREAM_LOOP_FILES:
            break


if __name__ == "__main__":
    stream_simulator()
