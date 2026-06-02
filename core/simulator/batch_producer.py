import time
import config
import pandas as pd
from kafka import KafkaProducer
import json
import glob

def create_producer():
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
                enable_idempotence=True,
                compression_type="lz4",
                batch_size=config.BATCH_PRODUCE_BATCH_SIZE,
                linger_ms=50,
                buffer_memory=67108864
            )
        except Exception as e:
            print(f"[Kafka] Not available yet, retrying in 5s... Error: {e}")
            time.sleep(5)
    print("[Kafka] Producer created!")
    return producer

def produce():
    producer = create_producer()
    data_dir = "/data/csv"
    print(f"[Simulator] Reading CSV files from: {data_dir}")
    filepaths = glob.glob(f"{data_dir}/*.csv")

    for file in filepaths:
        print(f"[Simulator] Processing {file}")
        for chunk in pd.read_csv(file, chunksize=100000):
            for _, row in chunk.iterrows():
                producer.send(config.BATCH_KAFKA_TOPIC, row.to_dict())
            print(f"[Simulator] Sent batch of {len(chunk)} rows from {file}")

    producer.flush()
    print("[Simulator] All CSV files sent!")

if __name__ == "__main__":
    produce()
