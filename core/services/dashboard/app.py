import json
import os
import threading
import time
from collections import Counter, deque

from flask import Flask, jsonify, render_template
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-cluster-kafka-bootstrap.bigdata.svc.cluster.local:9092",
)
PREDICTION_KAFKA_TOPIC = os.getenv("PREDICTION_KAFKA_TOPIC", "prediction-topic")
DASHBOARD_KAFKA_GROUP_ID = os.getenv("DASHBOARD_KAFKA_GROUP_ID", "dashboard")
DASHBOARD_MAX_ITEMS = int(os.getenv("DASHBOARD_MAX_ITEMS", "200"))
DASHBOARD_ATTACK_HISTORY_MAX = int(os.getenv("DASHBOARD_ATTACK_HISTORY_MAX", "300"))
THROUGHPUT_WINDOW_SEC = int(os.getenv("DASHBOARD_THROUGHPUT_WINDOW_SEC", "10"))

app = Flask(__name__)

state_lock = threading.Lock()
recent_items = deque(maxlen=DASHBOARD_MAX_ITEMS)
attack_items = deque(maxlen=DASHBOARD_ATTACK_HISTORY_MAX)
prediction_timestamps = deque()
label_counts = Counter()
total_count = 0
invalid_count = 0
latency_sum = 0.0
latency_count = 0
latest_model_version = None


def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                PREDICTION_KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=DASHBOARD_KAFKA_GROUP_ID,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
            print(f"[Dashboard] Consuming topic {PREDICTION_KAFKA_TOPIC}", flush=True)
            return consumer
        except Exception as exc:
            print(f"[Dashboard] Kafka not available, retrying in 5s: {exc}", flush=True)
            time.sleep(5)


def consume_predictions():
    global total_count, invalid_count, latency_sum, latency_count, latest_model_version

    consumer = create_consumer()
    for message in consumer:
        item = message.value
        status = item.get("status", "unknown")
        predicted_label = item.get("predicted_label")
        latency_ms = item.get("latency_ms")
        model_version = item.get("model_version")

        with state_lock:
            now = time.time()
            total_count += 1
            prediction_timestamps.append(now)
            cutoff = now - THROUGHPUT_WINDOW_SEC
            while prediction_timestamps and prediction_timestamps[0] < cutoff:
                prediction_timestamps.popleft()

            if status == "invalid":
                invalid_count += 1
            if status == "ok" and predicted_label:
                label_counts[predicted_label] += 1
            if isinstance(latency_ms, (int, float)):
                latency_sum += float(latency_ms)
                latency_count += 1
            if model_version:
                latest_model_version = model_version

            dashboard_item = {
                "status": status,
                "event_time": item.get("event_time"),
                "prediction_time": item.get("prediction_time"),
                "predicted_label": predicted_label,
                "confidence": item.get("confidence"),
                "actual_label": item.get("actual_label"),
                "latency_ms": latency_ms,
                "model_version": model_version,
                "source": item.get("source", {}),
                "error": item.get("error"),
            }
            recent_items.appendleft(dashboard_item)

            if status == "ok" and predicted_label and predicted_label != "Benign":
                attack_items.appendleft(dashboard_item)


def start_consumer_thread():
    thread = threading.Thread(target=consume_predictions, daemon=True)
    thread.start()


@app.route("/data")
def get_data():
    with state_lock:
        benign_count = label_counts.get("Benign", 0)
        attack_count = sum(count for label, count in label_counts.items() if label != "Benign")
        avg_latency = round(latency_sum / latency_count, 2) if latency_count else 0
        predictions_per_sec = round(
            len(prediction_timestamps) / THROUGHPUT_WINDOW_SEC,
            2,
        )

        return jsonify({
            "model_version": latest_model_version,
            "total": total_count,
            "attack_count": attack_count,
            "benign_count": benign_count,
            "invalid_count": invalid_count,
            "avg_latency_ms": avg_latency,
            "predictions_per_sec": predictions_per_sec,
            "throughput_window_sec": THROUGHPUT_WINDOW_SEC,
            "label_counts": dict(label_counts),
            "items": list(recent_items),
            "attack_items": list(attack_items),
        })


@app.route("/")
def index():
    return render_template("index.html")


start_consumer_thread()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
