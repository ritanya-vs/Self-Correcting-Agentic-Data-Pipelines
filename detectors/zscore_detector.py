import json
import os
import sys
import time
import requests
from collections import Counter
from datetime import datetime, timezone

BASELINE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "simulator", "baselines", "ehr_baseline.json"
)

ZSCORE_THRESHOLD = 3.0

def load_baseline() -> dict:
    with open(BASELINE_FILE, "r") as f:
        return json.load(f)

def compute_zscore(value: float, mean: float, std: float) -> float:
    if std == 0:
        return 0.0
    return abs((value - mean) / std)

def check_event(event: dict) -> dict:
    baseline  = load_baseline()
    anomalies = {}
    for field, stats in baseline["fields"].items():
        if field not in event:
            continue
        value = event[field]
        mean  = stats["mean"]
        std   = stats["std"]
        z     = compute_zscore(value, mean, std)
        if z > ZSCORE_THRESHOLD:
            anomalies[field] = {
                "value":     value,
                "mean":      mean,
                "std":       std,
                "zscore":    round(z, 2),
                "threshold": ZSCORE_THRESHOLD,
            }
    return {
        "detector":        "zscore",
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "patient_id":      event.get("patient_id"),
        "anomalies_found": len(anomalies) > 0,
        "anomalies":       anomalies,
        "fault_type":      "data_quality" if anomalies else None,
    }

def check_batch(events: list) -> dict:
    results            = [check_event(e) for e in events]
    flagged            = [r for r in results if r["anomalies_found"]]
    all_anomaly_fields = {}
    for r in flagged:
        for field, info in r["anomalies"].items():
            if field not in all_anomaly_fields:
                all_anomaly_fields[field] = []
            all_anomaly_fields[field].append(info["zscore"])
    return {
        "detector":        "zscore",
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "total_events":    len(events),
        "flagged_events":  len(flagged),
        "flag_rate":       round(len(flagged) / len(events), 3) if events else 0,
        "anomalous_fields": {
            field: {
                "count":      len(scores),
                "max_zscore": round(max(scores), 2),
                "avg_zscore": round(sum(scores)/len(scores), 2),
            }
            for field, scores in all_anomaly_fields.items()
        },
        "fault_detected": len(flagged) > 0,
    }

# ── NEW: Fault 3 — Performance detector ──────────────────────────
def check_db_latency() -> dict:
    """
    Measures how long a simple Databricks query takes.
    If it takes more than 3 seconds, flags a performance fault.
    """
    LATENCY_THRESHOLD = 3.0
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "simulator"))
    from database import get_connection

    try:
        start  = time.time()
        con    = get_connection()
        cursor = con.cursor()
        cursor.execute("SELECT COUNT(*) FROM healthcare_db.ehr_stream")
        cursor.fetchone()
        cursor.close()
        con.close()
        latency = round(time.time() - start, 2)
        fault   = latency > LATENCY_THRESHOLD

        print(f"[ZSCORE] DB latency: {latency}s "
              f"{'⚠️  SLOW' if fault else '✅ OK'}")
        return {
            "detector":        "zscore_latency",
            "latency_seconds": latency,
            "threshold":       LATENCY_THRESHOLD,
            "fault_detected":  fault,
            "fault_type":      "performance" if fault else None,
        }
    except Exception as e:
        print(f"[ZSCORE] DB latency check error: {e}")
        return {
            "detector":        "zscore_latency",
            "latency_seconds": -1,
            "fault_detected":  True,
            "fault_type":      "performance",
            "error":           str(e),
        }

# ── NEW: Fault 4 — Security detector ─────────────────────────────
def check_security_pattern(events: list) -> dict:
    """
    Detects brute-force pattern.
    If any single patient_id appears in more than 20% of events → security fault.
    """
    THRESHOLD_PCT = 0.20

    if not events:
        return {"detector": "zscore_security", "fault_detected": False}

    patient_counts = Counter(e.get("patient_id") for e in events)
    total          = len(events)

    for patient_id, count in patient_counts.most_common(1):
        pct = count / total
        if pct > THRESHOLD_PCT:
            print(f"[ZSCORE] 🛡️  Security pattern: '{patient_id}' "
                  f"= {count}/{total} events ({round(pct*100,1)}%)")
            return {
                "detector":         "zscore_security",
                "fault_detected":   True,
                "fault_type":       "security",
                "rogue_patient_id": patient_id,
                "event_count":      count,
                "percentage":       round(pct * 100, 1),
            }

    return {"detector": "zscore_security", "fault_detected": False}

# ── NEW: Fault 5 — Stall / connector health detector ─────────────
def check_connector_health() -> dict:
    """
    Calls Kafka Connect REST API to check connector status.
    If connector is PAUSED or FAILED → stall fault.
    """
    CONNECT_URL    = "http://localhost:8083"
    CONNECTOR_NAME = "databricks-sink"

    try:
        resp  = requests.get(
            f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status",
            timeout=3
        )
        data        = resp.json()
        state       = data.get("connector", {}).get("state", "UNKNOWN")
        tasks       = data.get("tasks", [])
        task_states = [t.get("state") for t in tasks]
        all_running = state == "RUNNING" and all(
            s == "RUNNING" for s in task_states
        )
        fault = not all_running

        print(f"[ZSCORE] Connector '{CONNECTOR_NAME}': state={state} "
              f"tasks={task_states} "
              f"{'⚠️  NOT RUNNING' if fault else '✅ OK'}")
        return {
            "detector":        "connector_health",
            "connector_state": state,
            "task_states":     task_states,
            "fault_detected":  fault,
            "fault_type":      "stall" if fault else None,
        }

    except requests.exceptions.ConnectionError:
        # Kafka Connect not set up — don't raise a false alarm
        print("[ZSCORE] Kafka Connect REST API not reachable — skipping stall check")
        return {"detector": "connector_health", "fault_detected": False}
    except Exception as e:
        print(f"[ZSCORE] Connector health check error: {e}")
        return {"detector": "connector_health", "fault_detected": False}


if __name__ == "__main__":
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "simulator"))
    from patient_generator import generate_patient_event
    import numpy as np

    print("=== Testing Z-Score Detector ===\n")

    print("-- Normal event --")
    normal = generate_patient_event(anomalous=False)
    result = check_event(normal)
    print(f"Anomalies found: {result['anomalies_found']}")
    print(f"Anomalies: {result['anomalies']}\n")

    print("-- Anomalous event --")
    bad = generate_patient_event(anomalous=False)
    bad["heart_rate"]  = 280.0
    bad["spo2"]        = 35.0
    bad["bp_systolic"] = 260.0
    result = check_event(bad)
    print(f"Anomalies found: {result['anomalies_found']}")
    for field, info in result["anomalies"].items():
        print(f"  {field}: value={info['value']}  z-score={info['zscore']}")

    print("\n-- Security pattern test --")
    sec_events = [generate_patient_event() for _ in range(40)]
    for e in sec_events[:15]:   # 15/40 = 37.5% → above 20% threshold
        e["patient_id"] = "PT-ATTACKER-0000"
    result = check_security_pattern(sec_events)
    print(f"Security fault: {result['fault_detected']}")
    if result["fault_detected"]:
        print(f"Rogue ID: {result['rogue_patient_id']} "
              f"({result['percentage']}% of events)")