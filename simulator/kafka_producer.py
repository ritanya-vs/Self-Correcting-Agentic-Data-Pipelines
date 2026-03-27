import json
import time
import argparse
import sys
import os
import threading

sys.path.insert(0, os.path.dirname(__file__))

from confluent_kafka import Producer
from patient_generator import generate_patient_event
from database import insert_ehr_event, insert_iot_event

KAFKA_BOOTSTRAP = "localhost:9092"

def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Kafka delivery failed: {err}")

def _db_write_async(event, iot_payload):
    """Writes to Databricks in a background thread so it doesn't block Kafka streaming."""
    try:
        insert_ehr_event(event)
        insert_iot_event(iot_payload)
    except Exception as e:
        print(f"[DB-ERROR] {e}")

def stream_events(duration_seconds=60, interval=1.0):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    print(f"[INFO] Streaming for {duration_seconds}s (interval={interval}s)...")
    start = time.time()
    count = 0

    while time.time() - start < duration_seconds:
        event = generate_patient_event()

        # 1. Push full event to Kafka ehr-stream topic
        producer.produce(
            "ehr-stream",
            key=event["patient_id"],
            value=json.dumps(event),
            callback=delivery_report
        )

        # 2. Push sensor-only payload to Kafka iot-vitals topic
        iot_payload = {
            "patient_id": event["patient_id"],
            "heart_rate": event["heart_rate"],
            "spo2":       event["spo2"],
            "timestamp":  event["timestamp"],
        }
        producer.produce(
            "iot-vitals",
            key=event["patient_id"],
            value=json.dumps(iot_payload),
            callback=delivery_report
        )
        producer.poll(0)

        # 3. Write to Databricks in background (non-blocking)
        t = threading.Thread(
            target=_db_write_async,
            args=(event, iot_payload),
            daemon=True
        )
        t.start()

        count += 1
        if count % 5 == 0:
            elapsed = round(time.time() - start, 1)
            print(f"[INFO] {count} events → Kafka + Databricks ({elapsed}s elapsed)...")

        time.sleep(interval)

    producer.flush()
    print(f"[DONE] Streamed {count} events to Kafka and Databricks.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()
    stream_events(args.duration, args.interval)