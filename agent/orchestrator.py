import json
import sys
import os
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from confluent_kafka import Consumer, KafkaError
from crisis_packet import CrisisPacketBuilder
from react_agent import LARFReActAgent

from detectors.zscore_detector import check_batch as zscore_batch
from detectors.ks_test         import run_ks_test
from detectors.schema_entropy  import check_batch as schema_batch

KAFKA_BOOTSTRAP = "localhost:9092"
WINDOW_SIZE     = 50   # how many events to read before running detectors

def consume_events(topic="ehr-stream", n=50, timeout_seconds=30) -> list:
    """
    Reads the last N events from Kafka.
    Returns them as a list of dicts.
    """
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id":          f"larf-orchestrator-{int(time.time())}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])

    print(f"[ORCHESTRATOR] Reading {n} events from '{topic}'...")
    events  = []
    start   = time.time()

    while len(events) < n and (time.time() - start) < timeout_seconds:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[ERROR] {msg.error()}")
            continue
        try:
            event = json.loads(msg.value().decode("utf-8"))
            events.append(event)
        except Exception as e:
            print(f"[WARN] Could not parse event: {e}")

    consumer.close()
    print(f"[ORCHESTRATOR] Collected {len(events)} events in "
          f"{round(time.time()-start, 1)}s")
    return events

def run_detectors(events: list) -> list:
    """
    Runs all 3 detectors on the event batch.
    Returns a list of alert dicts for any faults found.
    """
    alerts = []

    if not events:
        alerts.append({
            "detector":   "stall",
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "is_anomaly": True,
            "message":    "No events received — pipeline may be stalled"
        })
        return alerts

    # 1. Z-Score detector
    zscore_result = zscore_batch(events)
    if zscore_result["fault_detected"]:
        print(f"[DETECT] ⚠️  Z-Score anomaly — "
              f"{zscore_result['flagged_events']}/{zscore_result['total_events']} events flagged")
        alerts.append({
            "detector":       "zscore",
            "timestamp":      datetime.now(timezone.utc).isoformat(),
            "flagged_events": zscore_result["flagged_events"],
            "anomalous_fields": zscore_result["anomalous_fields"],
        })

    # 2. KS-Test detector
    ks_result = run_ks_test(events)
    if ks_result["drift_detected"]:
        drifted = [f for f, r in ks_result["fields"].items() if r["drifted"]]
        print(f"[DETECT] ⚠️  KS-Test drift detected in fields: {drifted}")
        alerts.append({
            "detector":      "ks_test",
            "timestamp":     datetime.now(timezone.utc).isoformat(),
            "drifted_fields": drifted,
            "fields":        ks_result["fields"],
        })

    # 3. Schema entropy detector
    schema_result = schema_batch(events)
    if schema_result["fault_detected"]:
        print(f"[DETECT] ⚠️  Schema anomaly — "
              f"{schema_result['flagged_events']} malformed events | "
              f"missing={schema_result['missing_fields']} | "
              f"extra={schema_result['extra_fields']}")
        alerts.append({
            "detector":       "schema_entropy",
            "timestamp":      datetime.now(timezone.utc).isoformat(),
            "flagged_events": schema_result["flagged_events"],
            "missing_fields": schema_result["missing_fields"],
            "extra_fields":   schema_result["extra_fields"],
        })

    if not alerts:
        print("[DETECT] ✅ All detectors clear — pipeline is healthy")

    return alerts

def run_ooda_cycle():
    """
    One full Observe → Orient → Decide → Act cycle.
    """
    print("\n" + "="*55)
    print("🌟 LARF OODA CYCLE STARTED")
    print("="*55 + "\n")

    # ── OBSERVE ──────────────────────────────────────────
    print("[OBSERVE] Reading live events from Kafka...")
    events = consume_events(topic="ehr-stream", n=WINDOW_SIZE)

    # ── ORIENT ───────────────────────────────────────────
    print("\n[ORIENT] Running all detectors...")
    alerts = run_detectors(events)

    if not alerts:
        print("\n✅ No faults detected. Pipeline is healthy. Exiting.")
        return

    # ── DECIDE ───────────────────────────────────────────
    print(f"\n[DECIDE] {len(alerts)} fault signal(s) detected. Building crisis packet...")
    builder = CrisisPacketBuilder()
    for alert in alerts:
        builder.add_alert(alert)
    crisis_packet = builder.build()

    print(f"Crisis ID : {crisis_packet['crisis_id']}")
    print(f"Severity  : {crisis_packet['severity']}")
    print(f"Components: {crisis_packet['affected_components']}")

    print("\n[DECIDE] Booting LLM agent...")
    agent = LARFReActAgent()

    # ── ACT ───────────────────────────────────────────────
    print("\n[ACT] Agent taking control...\n")
    result = agent.resolve_crisis(crisis_packet)

    print("\n" + "="*55)
    print("✅ OODA CYCLE COMPLETE")
    print("="*55)

    if result:
        print(f"\nAgent Report:\n{result.get('output', 'No output')}")

def run_continuous(interval_seconds=30):
    """
    Runs OODA cycles continuously — one every N seconds.
    This is the real production mode.
    """
    print(f"[LARF] Starting continuous monitoring (cycle every {interval_seconds}s)")
    print("[LARF] Press Ctrl+C to stop\n")

    while True:
        try:
            run_ooda_cycle()
            print(f"\n[LARF] Sleeping {interval_seconds}s before next cycle...\n")
            time.sleep(interval_seconds)
        except KeyboardInterrupt:
            print("\n[LARF] Stopped by user.")
            break

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "continuous"],
                        default="once",
                        help="Run one cycle or continuously")
    parser.add_argument("--interval", type=int, default=30,
                        help="Seconds between cycles in continuous mode")
    args = parser.parse_args()

    if args.mode == "continuous":
        run_continuous(args.interval)
    else:
        run_ooda_cycle()