import pandas as pd
from kafka import KafkaConsumer
import config
import json
import time
from datetime import datetime
import boto3
import io
from logger import Logger


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
            print(e)
            time.sleep(5)

    return consumer


def normalize_for_parquet(df):
    for column in df.columns:
        df[column] = df[column].apply(
            lambda value: None if pd.isna(value) else str(value)
        )
    return df


def save_to_minio(batch):
    if not batch:
        return False

    df = normalize_for_parquet(pd.DataFrame(batch))

    s3 = boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    process_dt = datetime.strptime(config.PROCESS_DATE, "%Y-%m-%d")
    now = datetime.now()
    filename = f"batch_{now.strftime('%H%M%S_%f')}.parquet"

    # bucket = data-lake
    # key = bronze/...
    object_key = (
        f"bronze/"
        f"year={process_dt.year}/month={process_dt.month:02d}/day={process_dt.day:02d}/"
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
            return True

        except Exception as e:
            attempts += 1
            time.sleep(config.MINIO_RETRY_DELAY)

    return False


def consume():
    log = Logger("BATCH-CONSUMER")
    log.start()

    consumer = create_consumer()
    log.log("KAFKA")

    batch = []
    last_flush = time.time()
    idle_count = 0

    while True:
        records = consumer.poll(timeout_ms=1000)
        log.log("POLL")
        
        has_message = False
        for tp, messages in records.items():
            if messages:
                has_message = True

            for msg in messages:
                batch.append(msg.value)

                if len(batch) >= config.FLUSH_SIZE:
                    success = save_to_minio(batch)
                    log.log("WRITE")

                    if success:
                        consumer.commit()
                        log.log("COMMIT")
                        batch = []
                        last_flush = time.time()
        

        if not has_message:
            idle_count += 1
            if idle_count >= 10:
                break
        else:
            idle_count = 0

        if time.time() - last_flush >= config.MAX_SILENCE_TIME:
            break

    # final flush
    success = save_to_minio(batch)
    log.log("FINAL_WRITE")

    if success:
        consumer.commit()
        log.log("FINAL_COMMIT")
    
    log.end()


if __name__ == "__main__":
    consume()
