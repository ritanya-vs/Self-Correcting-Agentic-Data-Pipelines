import sys
import os
import json
import time
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "agent"))

from simulator.patient_generator import generate_patient_event
from simulator.database          import get_connection
from detectors.zscore_detector   import (
    check_batch as zscore_batch,
    check_security_pattern,
    check_db_latency,
    check_connector_health,
)
from detectors.ks_test           import run_ks_test
from detectors.schema_entropy    import check_batch as schema_batch

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {
        "message": "LARF Monitoring API is LIVE",
        "endpoints": ["/api/pipeline-status", "/api/health"]
    }

def fetch_db_events(limit=50):
    try:
        con    = get_connection()
        cursor = con.cursor()
        cursor.execute(f"""
            SELECT patient_id, heart_rate, spo2, bp_systolic,
                   bp_diastolic, temperature_c, respiratory_rate,
                   ward, timestamp
            FROM   healthcare_db.ehr_stream
            ORDER  BY timestamp DESC
            LIMIT  {limit}
        """)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        cursor.close()
        con.close()
        return [dict(zip(cols, row)) for row in rows]
    except Exception as e:
        return []

def generate_mock_events(n=50, fault_type=None):
    """Fallback when Databricks is not available."""
    events = []
    for _ in range(n):
        e = generate_patient_event(anomalous=False)
        if fault_type == "data_quality":
            import random
            e["heart_rate"]  = round(random.uniform(220, 300), 1)
            e["spo2"]        = round(random.uniform(30, 60), 1)
        elif fault_type == "schema":
            e.pop("spo2", None)
            e["diagnosis_code"] = "ICD-9999"
        elif fault_type == "security":
            e["patient_id"] = "PT-ATTACKER-0000"
        events.append(e)
    return events

@app.get("/api/pipeline-status")
def get_pipeline_status():
    """Main endpoint — returns full pipeline health."""
    events = fetch_db_events(50)
    if not events:
        events = generate_mock_events(50)
        source = "mock"
    else:
        source = "databricks"

    # Run all detectors
    zscore_r   = zscore_batch(events)
    ks_r       = run_ks_test(events)
    schema_r   = schema_batch(events)
    security_r = check_security_pattern(events)
    latency_r  = check_db_latency()
    connector_r= check_connector_health()

    # Collect faults
    faults = []
    if zscore_r["fault_detected"]:
        faults.append({
            "type":     "Data Quality",
            "detector": "Z-Score",
            "severity": "HIGH",
            "details":  f"{zscore_r['flagged_events']} events with impossible vitals",
            "fields":   list(zscore_r["anomalous_fields"].keys()),
        })
    if ks_r["drift_detected"]:
        drifted = [f for f, r in ks_r["fields"].items() if r["drifted"]]
        faults.append({
            "type":     "Distribution Drift",
            "detector": "KS-Test",
            "severity": "MEDIUM",
            "details":  f"Statistical drift in: {drifted}",
            "fields":   drifted,
        })
    if schema_r["fault_detected"]:
        faults.append({
            "type":     "Schema Fault",
            "detector": "Schema Entropy",
            "severity": "CRITICAL",
            "details":  f"Missing: {list(schema_r['missing_fields'].keys())} | Extra: {list(schema_r['extra_fields'].keys())}",
            "fields":   list(schema_r["missing_fields"].keys()),
        })
    if security_r["fault_detected"]:
        faults.append({
            "type":     "Security Breach",
            "detector": "Z-Score Security",
            "severity": "CRITICAL",
            "details":  f"Brute force from {security_r['rogue_patient_id']} ({security_r['percentage']}% of events)",
            "fields":   ["patient_id"],
        })
    if latency_r["fault_detected"]:
        faults.append({
            "type":     "Performance",
            "detector": "Latency Monitor",
            "severity": "HIGH",
            "details":  f"DB latency {latency_r['latency_seconds']}s > {latency_r['threshold']}s threshold",
            "fields":   [],
        })
    if connector_r["fault_detected"]:
        faults.append({
            "type":     "Pipeline Stall",
            "detector": "Connector Health",
            "severity": "CRITICAL",
            "details":  f"Connector state: {connector_r.get('connector_state')}",
            "fields":   [],
        })

    # Recent events for table (latest 10)
    recent = []
    for e in events[:10]:
        row = {k: (str(v)[:20] if v is not None else None) for k, v in e.items()}
        row["_anomalous"] = (
            e.get("heart_rate", 0) > 200 or
            e.get("spo2", 100) < 70 or
            "spo2" not in e or
            "diagnosis_code" in e
        )
        recent.append(row)

    return {
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "source":       source,
        "total_events": len(events),
        "status":       "FAULT" if faults else "HEALTHY",
        "faults":       faults,
        "recent_events": recent,
        "metrics": {
            "total_events":   len(events),
            "flagged_events": zscore_r["flagged_events"],
            "healthy_events": len(events) - zscore_r["flagged_events"],
            "db_latency":     latency_r.get("latency_seconds", 0),
            "fault_count":    len(faults),
        },
        "detectors": {
            "zscore":    {"status": "FAULT" if zscore_r["fault_detected"] else "CLEAR",
                         "flagged": zscore_r["flagged_events"]},
            "ks_test":   {"status": "FAULT" if ks_r["drift_detected"] else "CLEAR",
                         "drifted": [f for f, r in ks_r["fields"].items() if r["drifted"]]},
            "schema":    {"status": "FAULT" if schema_r["fault_detected"] else "CLEAR",
                         "missing": list(schema_r["missing_fields"].keys())},
            "security":  {"status": "FAULT" if security_r["fault_detected"] else "CLEAR"},
            "latency":   {"status": "FAULT" if latency_r["fault_detected"] else "CLEAR",
                         "value": latency_r.get("latency_seconds", 0)},
            "connector": {"status": "FAULT" if connector_r["fault_detected"] else "CLEAR"},
        }
    }

@app.get("/api/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}