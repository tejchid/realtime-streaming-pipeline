import json
import time
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
from prometheus_client import start_http_server, Counter, Histogram

# ---------- METRICS ----------
EVENTS_PROCESSED = Counter(
    "events_processed_total",
    "Total number of events processed"
)

EVENTS_FAILED = Counter(
    "events_failed_total",
    "Total number of events sent to DLQ"
)

BATCH_LATENCY = Histogram(
    "batch_insert_latency_seconds",
    "Latency of batch inserts into Postgres"
)

# ---------- DB ----------
DB_CONN = psycopg2.connect(
    dbname="events_db",
    user="events_user",
    password="events_pass",
    host="localhost",
    port=5432,
)
DB_CONN.autocommit = False
CURSOR = DB_CONN.cursor()

# ---------- Kafka ----------
consumer = KafkaConsumer(
    "events.raw",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: v.decode("utf-8"),
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    group_id="events-consumer-group",
)

dlq_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

BATCH_SIZE = 100

def send_to_dlq(event, reason):
    EVENTS_FAILED.inc()
    dlq_event = {
        "original_event": event,
        "error_reason": reason,
        "failed_at": datetime.utcnow().isoformat(),
    }
    dlq_producer.send("events.dlq", dlq_event)

def insert_batch(events):
    start = time.time()
    query = """
        INSERT INTO events (event_id, event_type, payload, created_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """
    values = [
        (
            e["event_id"],
            e["event_type"],
            json.dumps(e["payload"]),
            datetime.fromisoformat(e["created_at"]),
        )
        for e in events
    ]
    CURSOR.executemany(query, values)
    DB_CONN.commit()
    BATCH_LATENCY.observe(time.time() - start)

batch = []

print("Consumer with metrics started...")

# 🔥 Start metrics server
start_http_server(8000, addr="0.0.0.0")


for message in consumer:
    try:
        event = json.loads(message.value)

        for field in ["event_id", "event_type", "payload", "created_at"]:
            if field not in event:
                raise ValueError(f"Missing field: {field}")

        batch.append(event)
        EVENTS_PROCESSED.inc()

        if len(batch) >= BATCH_SIZE:
            insert_batch(batch)
            consumer.commit()
            print(f"Inserted {len(batch)} events")
            batch.clear()

    except Exception as e:
        send_to_dlq(message.value, str(e))
        consumer.commit()
        print("Sent event to DLQ:", e)
