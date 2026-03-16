import time
import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)

def stream_simulator():
    while True:
        data = {
            "timestamp": int(time.time()),
            "close": random.randint(0, 100)
        }

        producer.send("stream_topic", data)
        print("sent:", data)

        time.sleep(2)


if __name__ == "__main__":
    stream_simulator()