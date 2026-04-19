import json
import time
import os
import argparse
import numpy as np
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

BASELINE_DIR = "baselines"
BASELINE_FILE = os.path.join(BASELINE_DIR, "ehr_baseline.json")

NUMERIC_FIELDS = [
    "heart_rate", "bp_systolic", "bp_diastolic",
    "spo2", "temperature_c", "respiratory_rate"
]

def capture_baseline(duration_minutes=5):
    os.makedirs(BASELINE_DIR, exist_ok=True)

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id":          "baseline-capture-group",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe(["ehr-stream"])

    print(f"[BASELINE] Listening on ehr-stream for {duration_minutes} minute(s)...")
    print(f"[BASELINE] Run kafka_producer.py in another terminal if not already running!")

    samples = defaultdict(list)
    start   = time.time()
    count   = 0

    while time.time() - start < duration_minutes * 60:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[ERROR] {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))
        for field in NUMERIC_FIELDS:
            if field in event:
                samples[field].append(event[field])
        count += 1

        if count % 30 == 0:
            elapsed = round((time.time() - start) / 60, 2)
            print(f"[BASELINE] {count} events captured ({elapsed}/{duration_minutes} min)...")

    consumer.close()

    baseline = {
        "captured_at":  datetime.utcnow().isoformat(),
        "event_count":  count,
        "duration_min": duration_minutes,
        "fields":       {}
    }

    for field, values in samples.items():
        arr = np.array(values)
        baseline["fields"][field] = {
            "mean":    round(float(np.mean(arr)), 4),
            "std":     round(float(np.std(arr)), 4),
            "min":     round(float(np.min(arr)), 4),
            "max":     round(float(np.max(arr)), 4),
            "p25":     round(float(np.percentile(arr, 25)), 4),
            "p75":     round(float(np.percentile(arr, 75)), 4),
            "samples": values       # raw list — KS-test uses this in Sprint 2
        }

    with open(BASELINE_FILE, "w") as f:
        json.dump(baseline, f, indent=2)

    print(f"\n[BASELINE] Saved to: {BASELINE_FILE}")
    print(f"[BASELINE] Summary:")
    for field, s in baseline["fields"].items():
        print(f"  {field:22s}  mean={s['mean']:7.2f}  std={s['std']:5.2f}  n={len(s['samples'])}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=int, default=5)
    args = parser.parse_args()
    capture_baseline(args.minutes)