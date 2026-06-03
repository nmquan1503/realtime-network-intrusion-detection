import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

import config
from feature_builder import build_feature_record
from model_loader import LoadedModel, find_latest_model_version, load_pipeline_model


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def create_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("realtime_intrusion_predictor")
        .master(config.SPARK_MASTER_URL)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.pyspark.python", "python3")
        .config("spark.pyspark.driver.python", "python3")
        .config("spark.executor.instances", "1")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
    )

    import os

    pod_ip = os.getenv("POD_IP")
    if pod_ip:
        builder = builder.config("spark.driver.host", pod_ip)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(
        "[Streaming] Spark ready: "
        f"master={spark.sparkContext.master}, appId={spark.sparkContext.applicationId}",
        flush=True,
    )
    return spark


def create_consumer() -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                config.STREAMING_KAFKA_TOPIC,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                group_id=config.STREAMING_KAFKA_GROUP_ID,
                auto_offset_reset=config.STREAMING_AUTO_OFFSET_RESET,
                enable_auto_commit=False,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                max_poll_records=config.PREDICT_BATCH_SIZE,
            )
            print(f"[Streaming] Consuming topic {config.STREAMING_KAFKA_TOPIC}", flush=True)
            return consumer
        except Exception as exc:
            print(f"[Streaming] Kafka consumer unavailable: {exc}", flush=True)
            time.sleep(5)


def create_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value, allow_nan=False).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=20,
            )
            print(f"[Streaming] Producing topic {config.PREDICTION_KAFKA_TOPIC}", flush=True)
            return producer
        except Exception as exc:
            print(f"[Streaming] Kafka producer unavailable: {exc}", flush=True)
            time.sleep(5)


def maybe_reload_model(
    spark: SparkSession,
    current_model: Optional[LoadedModel],
) -> Optional[LoadedModel]:
    latest = find_latest_model_version()
    if not latest:
        if current_model is None:
            print("[Streaming] No trained model found in MinIO yet", flush=True)
        return current_model

    version, model_path = latest
    if current_model and current_model.version == version:
        return current_model

    print(f"[Streaming] Loading model version={version} path={model_path}", flush=True)
    loaded = load_pipeline_model(spark, version, model_path)
    print(
        "[Streaming] Model loaded: "
        f"version={loaded.version}, features={len(loaded.feature_columns)}, labels={loaded.labels}",
        flush=True,
    )
    return loaded


def invalid_message(
    error: Dict[str, Any],
    metadata: Dict[str, Any],
    model: Optional[LoadedModel],
    started_at: float,
) -> Dict[str, Any]:
    return {
        "status": "invalid",
        "error": error.get("error", "invalid_row"),
        "missing_features": error.get("missing_features", []),
        "event_time": metadata.get("event_time"),
        "prediction_time": utc_now(),
        "model_version": model.version if model else None,
        "latency_ms": int((time.time() - started_at) * 1000),
        "actual_label": metadata.get("actual_label"),
        "source": metadata.get("source", {}),
    }


def probability_confidence(probability: Any, prediction_index: int) -> Optional[float]:
    if probability is None:
        return None
    values = probability.toArray().tolist() if hasattr(probability, "toArray") else list(probability)
    if prediction_index < 0 or prediction_index >= len(values):
        return max(values) if values else None
    return float(values[prediction_index])


def predict_records(
    spark: SparkSession,
    model: LoadedModel,
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    feature_rows = []
    metadata_by_index = {}
    outputs = []

    for row in rows:
        started_at = time.time()
        feature_record, metadata, error = build_feature_record(row, model.feature_columns)
        if error:
            outputs.append(invalid_message(error, metadata, model, started_at))
            continue

        meta_index = len(feature_rows)
        feature_record["_meta_index"] = meta_index
        feature_rows.append(feature_record)
        metadata_by_index[meta_index] = {
            **metadata,
            "started_at": started_at,
        }

    if not feature_rows:
        return outputs

    schema = StructType(
        [StructField("_meta_index", IntegerType(), False)]
        + [StructField(column, DoubleType(), True) for column in model.feature_columns]
    )
    df = spark.createDataFrame(feature_rows, schema=schema)
    assembled = model.assembler.transform(df)
    prediction_df = model.classifier.transform(assembled)

    selected = prediction_df.select("_meta_index", "prediction", "probability").collect()
    for record in selected:
        meta = metadata_by_index[int(record["_meta_index"])]
        prediction_index = int(record["prediction"])
        predicted_label = (
            model.labels[prediction_index]
            if 0 <= prediction_index < len(model.labels)
            else "Unknown"
        )
        confidence = probability_confidence(record["probability"], prediction_index)
        outputs.append({
            "status": "ok",
            "event_time": meta.get("event_time"),
            "prediction_time": utc_now(),
            "predicted_label": predicted_label,
            "prediction_index": float(prediction_index),
            "confidence": confidence,
            "model_version": model.version,
            "latency_ms": int((time.time() - meta["started_at"]) * 1000),
            "actual_label": meta.get("actual_label"),
            "source": meta.get("source", {}),
        })

    return outputs


def records_from_poll(polled_records) -> List[Dict[str, Any]]:
    rows = []
    for messages in polled_records.values():
        for message in messages:
            if isinstance(message.value, dict):
                rows.append(message.value)
    return rows


def streaming_process():
    spark = create_spark()
    consumer = create_consumer()
    producer = create_producer()
    model: Optional[LoadedModel] = None
    last_model_check = 0.0

    while True:
        now = time.time()
        if model is None or now - last_model_check >= config.MODEL_RELOAD_INTERVAL_SEC:
            try:
                model = maybe_reload_model(spark, model)
            except Exception as exc:
                print(f"[Streaming] Model reload failed: {exc}", flush=True)
            last_model_check = now

        if model is None:
            time.sleep(config.MODEL_NOT_FOUND_SLEEP_SEC)
            continue

        polled = consumer.poll(
            timeout_ms=config.PREDICT_POLL_TIMEOUT_MS,
            max_records=config.PREDICT_BATCH_SIZE,
        )
        rows = records_from_poll(polled)
        if not rows:
            continue

        try:
            predictions = predict_records(spark, model, rows)
            for prediction in predictions:
                producer.send(config.PREDICTION_KAFKA_TOPIC, prediction)
            producer.flush()
            consumer.commit()
            ok_count = sum(1 for prediction in predictions if prediction["status"] == "ok")
            invalid_count = len(predictions) - ok_count
            print(
                "[Streaming] Predicted batch: "
                f"input={len(rows)}, ok={ok_count}, invalid={invalid_count}, model={model.version}",
                flush=True,
            )
        except Exception as exc:
            print(f"[Streaming] Predict batch failed: {exc}", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    streaming_process()
