import pandas as pd
from kafka import KafkaConsumer
import config
import json
import time
from datetime import datetime
import boto3
import io


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
                max_partition_fetch_bytes=config.KAFKA_BATCH_SIZE,
                fetch_max_bytes=config.KAFKA_BATCH_SIZE,
            )
        except Exception as e:
            print(f"[Kafka] Not available yet, retrying in 5s... Error: {e}")
            time.sleep(5)

    print("[Kafka] Consumer created!")
    return consumer


def save_to_minio(batch):
    if not batch:
        return False

    df = pd.DataFrame(batch)

    s3 = boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    now = datetime.now()
    filename = f"batch_{now.strftime('%H%M%S_%f')}.parquet"

    # bucket = data-lake
    # key = bronze/...
    object_key = (
        f"bronze/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{filename}"
    )

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    attempts = 0

    while attempts < config.MINIO_RETRY_COUNT:
        try:
            s3.put_object(
                Bucket=config.MINIO_BUCKET,
                Key=object_key,
                Body=buffer.getvalue(),
            )

            print(
                f"[MinIO] Saved {len(df)} records "
                f"to s3://{config.MINIO_BUCKET}/{object_key}"
            )
            return True

        except Exception as e:
            attempts += 1
            print(f"[MinIO] Write attempt {attempts} failed: {e}")
            time.sleep(config.MINIO_RETRY_DELAY)

    return False


def consume():
    consumer = create_consumer()

    batch = []
    last_flush = time.time()
    idle_count = 0

    while True:
        records = consumer.poll(timeout_ms=1000)
        has_message = False

        for tp, messages in records.items():
            if messages:
                has_message = True

            for msg in messages:
                batch.append(msg.value)

                if len(batch) >= config.FLUSH_SIZE:
                    success = save_to_minio(batch)

                    if success:
                        consumer.commit()
                        batch = []
                        last_flush = time.time()

        if not has_message:
            idle_count += 1
            print(f"[IDLE] {idle_count}/10")

            if idle_count >= 10:
                break
        else:
            idle_count = 0

        if time.time() - last_flush >= config.MAX_SILENCE_TIME:
            print("[EXIT] batch window finished (no flush activity)")
            break

    # final flush
    success = save_to_minio(batch)
    if success:
        consumer.commit()


if __name__ == "__main__":
    consume()