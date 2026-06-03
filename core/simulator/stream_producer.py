import glob
import json
import math
import time
from typing import Any

import pandas as pd
from kafka import KafkaProducer

import config


def idle_after_complete():
    if not config.PRODUCER_IDLE_AFTER_COMPLETE:
        return

    print("[StreamProducer] Producer completed; staying idle to avoid Deployment restart loop", flush=True)
    while True:
        time.sleep(config.PRODUCER_IDLE_SLEEP_SEC)


def clean_value(value: Any):
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def clean_row(row):
    return {column: clean_value(value) for column, value in row.items()}


def create_producer():
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value, allow_nan=False).encode("utf-8"),
                acks="all",
                retries=5,
                enable_idempotence=True,
                compression_type="lz4",
                batch_size=config.BATCH_PRODUCE_BATCH_SIZE,
                linger_ms=20,
                buffer_memory=67108864,
            )
        except Exception as exc:
            print(f"[StreamProducer] Kafka not available, retrying in 5s: {exc}", flush=True)
            time.sleep(5)

    print("[StreamProducer] Producer created", flush=True)
    return producer


def produce_file(producer, filepath):
    sent = 0
    print(f"[StreamProducer] Streaming file: {filepath}", flush=True)

    for chunk in pd.read_csv(filepath, chunksize=config.STREAM_PRODUCE_CHUNK_SIZE):
        for _, row in chunk.iterrows():
            producer.send(config.STREAMING_KAFKA_TOPIC, clean_row(row))
            sent += 1

            if config.STREAM_SEND_INTERVAL_MS > 0:
                time.sleep(config.STREAM_SEND_INTERVAL_MS / 1000)

            if sent % 1000 == 0:
                producer.flush()
                print(f"[StreamProducer] Sent {sent} rows from {filepath}", flush=True)

    producer.flush()
    print(f"[StreamProducer] Completed {filepath}, total rows={sent}", flush=True)


def stream():
    producer = create_producer()
    data_dir = "/data/streaming"

    while True:
        filepaths = sorted(glob.glob(f"{data_dir}/*.csv"))
        if not filepaths:
            print(f"[StreamProducer] No CSV files found in {data_dir}", flush=True)
            if not config.STREAM_LOOP_FILES:
                idle_after_complete()
                break
            time.sleep(10)
            continue

        for filepath in filepaths:
            produce_file(producer, filepath)

        if not config.STREAM_LOOP_FILES:
            idle_after_complete()
            break

        print("[StreamProducer] Looping streaming files", flush=True)


if __name__ == "__main__":
    stream()
