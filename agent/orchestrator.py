import json
import sys
import os
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from confluent_kafka import Consumer, KafkaError
from crisis_packet import CrisisPacketBuilder
from react_agent import LARFReActAgent

from detectors.zscore_detector import (
    check_batch          as zscore_batch,
    check_db_latency,
    check_security_pattern,
    check_connector_health,
)
from detectors.ks_test        import run_ks_test
from detectors.schema_entropy import check_batch as schema_batch

KAFKA_BOOTSTRAP = "localhost:9092"
WINDOW_SIZE     = 50

def consume_events(topic="ehr-stream", n=50, timeout_seconds=30) -> list:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id":          f"larf-orchestrator-{int(time.time())}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])
    print(f"[ORCHESTRATOR] Reading {n} events from '{topic}'...")
    events = []
    start  = time.time()

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
    alerts = []

    # ── Stall check (no events at all) ───────────────────────────
    if not events:
        alerts.append({
            "detector":   "stall",
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "fault_type": "stall",
            "message":    "No events received — pipeline may be stalled",
        })
        return alerts

    # ── 1. Z-Score (data quality) ─────────────────────────────────
    zscore_result = zscore_batch(events)
    if zscore_result["fault_detected"]:
        print(f"[DETECT] ⚠️  Z-Score anomaly — "
              f"{zscore_result['flagged_events']}/"
              f"{zscore_result['total_events']} events flagged")
        alerts.append({
            "detector":         "zscore",
            "timestamp":        datetime.now(timezone.utc).isoformat(),
            "fault_type":       "data_quality",
            "flagged_events":   zscore_result["flagged_events"],
            "anomalous_fields": zscore_result["anomalous_fields"],
        })

    # ── 2. KS-Test (distribution drift) ──────────────────────────
    ks_result = run_ks_test(events)
    if ks_result["drift_detected"]:
        drifted = [f for f, r in ks_result["fields"].items() if r["drifted"]]
        print(f"[DETECT] ⚠️  KS-Test drift in fields: {drifted}")
        alerts.append({
            "detector":       "ks_test",
            "timestamp":      datetime.now(timezone.utc).isoformat(),
            "fault_type":     "data_quality",
            "drifted_fields": drifted,
            "fields":         ks_result["fields"],
        })

    # ── 3. Schema entropy (schema fault) ─────────────────────────
    schema_result = schema_batch(events)
    if schema_result["fault_detected"]:
        print(f"[DETECT] ⚠️  Schema anomaly — "
              f"{schema_result['flagged_events']} malformed events | "
              f"missing={schema_result['missing_fields']} | "
              f"extra={schema_result['extra_fields']}")
        alerts.append({
            "detector":       "schema_entropy",
            "timestamp":      datetime.now(timezone.utc).isoformat(),
            "fault_type":     "schema",
            "flagged_events": schema_result["flagged_events"],
            "missing_fields": schema_result["missing_fields"],
            "extra_fields":   schema_result["extra_fields"],
        })

    # ── 4. DB latency (performance fault) ────────────────────────
    latency_result = check_db_latency()
    if latency_result["fault_detected"]:
        print(f"[DETECT] ⚠️  Performance fault — DB latency: "
              f"{latency_result['latency_seconds']}s "
              f"(threshold: {latency_result['threshold']}s)")
        alerts.append({
            "detector":        "zscore_latency",
            "timestamp":       datetime.now(timezone.utc).isoformat(),
            "fault_type":      "performance",
            "latency_seconds": latency_result["latency_seconds"],
            "threshold":       latency_result["threshold"],
        })

    # ── 5. Security pattern (brute-force fault) ───────────────────
    security_result = check_security_pattern(events)
    if security_result["fault_detected"]:
        rogue = security_result["rogue_patient_id"]
        pct   = security_result["percentage"]
        print(f"[DETECT] 🛡️  Security fault — '{rogue}' "
              f"is {pct}% of all events")
        alerts.append({
            "detector":         "zscore_security",
            "timestamp":        datetime.now(timezone.utc).isoformat(),
            "fault_type":       "security",
            "rogue_patient_id": rogue,
            "percentage":       pct,
        })

    # ── 6. Connector health (stall fault) ────────────────────────
    connector_result = check_connector_health()
    if connector_result["fault_detected"]:
        state = connector_result.get("connector_state", "UNKNOWN")
        print(f"[DETECT] ⚠️  Stall fault — connector state: {state}")
        alerts.append({
            "detector":        "connector_health",
            "timestamp":       datetime.now(timezone.utc).isoformat(),
            "fault_type":      "stall",
            "connector_state": state,
            "task_states":     connector_result.get("task_states", []),
        })

    if not alerts:
        print("[DETECT] ✅ All detectors clear — pipeline is healthy")

    return alerts

def run_ooda_cycle():
    print("\n" + "="*55)
    print("🌟 LARF OODA CYCLE STARTED")
    print("="*55 + "\n")

    # OBSERVE
    print("[OBSERVE] Reading live events from Kafka...")
    events = consume_events(topic="ehr-stream", n=WINDOW_SIZE)

    # ORIENT
    print("\n[ORIENT] Running all detectors...")
    alerts = run_detectors(events)

    if not alerts:
        print("\n✅ No faults detected. Pipeline is healthy. Exiting.")
        return

    # DECIDE
    print(f"\n[DECIDE] {len(alerts)} fault signal(s) detected. "
          f"Building crisis packet...")
    builder = CrisisPacketBuilder()
    for alert in alerts:
        builder.add_alert(alert)
    crisis_packet = builder.build()

    print(f"Crisis ID : {crisis_packet['crisis_id']}")
    print(f"Severity  : {crisis_packet['severity']}")
    print(f"Components: {crisis_packet['affected_components']}")

    print("\n[DECIDE] Booting LLM agent...")
    agent = LARFReActAgent()

    # ACT
    print("\n[ACT] Agent taking control...\n")
    result = agent.resolve_crisis(crisis_packet)

    print("\n" + "="*55)
    print("✅ OODA CYCLE COMPLETE")
    print("="*55)

    if result:
        print(f"\nAgent Report:\n{result.get('output', 'No output')}")

def run_continuous(interval_seconds=30):
    print(f"[LARF] Continuous monitoring — cycle every {interval_seconds}s")
    print("[LARF] Press Ctrl+C to stop\n")
    while True:
        try:
            run_ooda_cycle()
            print(f"\n[LARF] Sleeping {interval_seconds}s...\n")
            time.sleep(interval_seconds)
        except KeyboardInterrupt:
            print("\n[LARF] Stopped.")
            break

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "continuous"],
                        default="once")
    parser.add_argument("--interval", type=int, default=30)
    args = parser.parse_args()

    if args.mode == "continuous":
        run_continuous(args.interval)
    else:
        run_ooda_cycle()