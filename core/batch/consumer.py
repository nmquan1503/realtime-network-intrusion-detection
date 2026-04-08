import pandas as pd
from kafka import KafkaConsumer
import config
import json
import time
from hdfs import InsecureClient
from datetime import datetime

def create_consumer():
    consumer = None
    while not consumer:
        try:
            consumer = KafkaConsumer(
                config.KAFKA_TOPIC,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                group_id=config.KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except Exception as e:
            print(f"[Kafka] Not available yet, retrying in 5s... Error: {e}")
            time.sleep(5)
    print("[Kafka] Consumer created!")
    return consumer

def save_to_hdfs(batch):
    if not batch:
        return
    df = pd.DataFrame(batch)
    client = InsecureClient(config.HDFS_URL, user=config.HDFS_USER)
    now = datetime.now()
    hdfs_dir = f"{config.HDFS_BASE_PATH}/year={now.year}/month={now.month}/day={now.day:02d}"
    attempts = 0
    while attempts < config.HDFS_RETRY_COUNT:
        try:
            client.makedirs(hdfs_dir)
            filepath = f"{hdfs_dir}/batch_{now.strftime("%H%M%S")}.parquet"
            localpath = "/tmp/batch.parquet"
            df.to_parquet(localpath, index=False)
            client.upload(filepath, localpath, overwrite=True)
            print(f"[HDFS] Saved {len(df)} records to {filepath}")
            break
        except Exception as e:
            attempts += 1
            print(f"[HDFS] Write attempt {attempts} failed: {e}")
            if attempts < config.HDFS_RETRY_COUNT:
                print(f"[HDFS] Retrying in {config.HDFS_RETRY_DELAY}s...")
                time.sleep(config.HDFS_RETRY_DELAY)
            else:
                print(f"[HDFS] Failed to write batch after {config.HDFS_RETRY_COUNT} attempts. Skipping.")

def consume():
    consumer = create_consumer()
    batch = []
    for message in consumer:
        batch.append(message.value)
        if len(batch) >= config.FLUSH_SIZE:
            save_to_hdfs(batch)
            consumer.commit()
            batch = []

if __name__ == "__main__":
    consume()