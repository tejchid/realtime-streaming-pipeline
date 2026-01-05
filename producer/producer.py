import json
import time
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks=1,
    linger_ms=5,
)

EVENT_TYPES = ["click", "view", "purchase"]

def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "payload": {
            "user_id": random.randint(1, 1_000_000),
            "value": random.random()
        },
        "created_at": datetime.utcnow().isoformat()
    }

RATE_PER_SEC = 100

if __name__ == "__main__":
    interval = 1.0 / RATE_PER_SEC
    sent = 0

    while True:
        event = generate_event()
        producer.send("events.raw", event)
        sent += 1

        if sent % 1000 == 0:
            print(f"Sent {sent} events")

        time.sleep(interval)

