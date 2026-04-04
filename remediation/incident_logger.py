import json
import os
from datetime import datetime, timezone

LOG_DIR  = os.path.join(os.path.dirname(__file__), "..", "logs")
LOG_FILE = os.path.join(LOG_DIR, "incident_log.jsonl")

def _ensure_log_dir():
    os.makedirs(LOG_DIR, exist_ok=True)

def log_incident(
    crisis_packet:    dict,
    agent_diagnosis:  str,
    actions_taken:    list,
    validation_result: dict,
) -> dict:
    """
    Writes one complete incident record to the append-only log.
    Called after every OODA cycle completes.
    """
    _ensure_log_dir()

    record = {
        "log_id":            f"INC-{int(datetime.now(timezone.utc).timestamp())}",
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "crisis_id":         crisis_packet.get("crisis_id"),
        "severity":          crisis_packet.get("severity"),
        "affected_components": crisis_packet.get("affected_components", []),
        "fault_signals":     crisis_packet.get("fault_signals", []),
        "agent_diagnosis":   agent_diagnosis,
        "actions_taken":     actions_taken,
        "validation_status": validation_result.get("status"),
        "validation_failures": validation_result.get("failures", []),
        "mttr_seconds":      None,   # filled in by orchestrator
    }

    # Append-only — never overwrite
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")

    print(f"[LOGGER] ✅ Incident {record['log_id']} logged to {LOG_FILE}")
    return record

def log_mttr(log_id: str, fault_start_time: datetime, fix_end_time: datetime):
    """
    Updates MTTR for a specific incident.
    Reads the whole log, updates the record, rewrites.
    """
    _ensure_log_dir()
    if not os.path.exists(LOG_FILE):
        return

    mttr = round((fix_end_time - fault_start_time).total_seconds(), 2)

    records = []
    with open(LOG_FILE, "r") as f:
        for line in f:
            record = json.loads(line.strip())
            if record["log_id"] == log_id:
                record["mttr_seconds"] = mttr
            records.append(record)

    with open(LOG_FILE, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    print(f"[LOGGER] MTTR for {log_id}: {mttr}s")

def get_all_incidents() -> list:
    """Returns all logged incidents as a list of dicts."""
    _ensure_log_dir()
    if not os.path.exists(LOG_FILE):
        return []
    records = []
    with open(LOG_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records

def print_summary():
    """Prints a summary table of all incidents."""
    incidents = get_all_incidents()
    if not incidents:
        print("[LOGGER] No incidents logged yet.")
        return

    print(f"\n{'='*70}")
    print(f"  LARF Incident Log — {len(incidents)} total incidents")
    print(f"{'='*70}")
    print(f"{'ID':<20} {'Severity':<10} {'Status':<18} {'MTTR':>8}")
    print(f"{'-'*70}")

    for inc in incidents:
        mttr = f"{inc['mttr_seconds']}s" if inc['mttr_seconds'] else "N/A"
        print(f"{inc['log_id']:<20} {inc['severity']:<10} {inc['validation_status']:<18} {mttr:>8}")

    # Stats
    resolved = [i for i in incidents if i["validation_status"] == "STEADY_STATE"]
    mttrs    = [i["mttr_seconds"] for i in incidents if i["mttr_seconds"]]

    print(f"\nResolved: {len(resolved)}/{len(incidents)}")
    if mttrs:
        print(f"Avg MTTR: {round(sum(mttrs)/len(mttrs), 1)}s")
        print(f"Max MTTR: {max(mttrs)}s")
        print(f"Min MTTR: {min(mttrs)}s")


if __name__ == "__main__":
    print("=== Incident Logger Test ===\n")

    # Simulate a full incident
    test_packet = {
        "crisis_id":           "CRISIS-TEST-001",
        "severity":            "CRITICAL",
        "affected_components": ["Kafka_Topic_ehr-stream", "Databricks_Warehouse"],
        "fault_signals": [
            {"detector": "schema_entropy", "missing_fields": ["spo2"]}
        ]
    }

    test_validation = {
        "status":   "STEADY_STATE",
        "failures": []
    }

    record = log_incident(
        crisis_packet     = test_packet,
        agent_diagnosis   = "Schema evolution fault — spo2 field missing from producer",
        actions_taken     = ["ALTER TABLE healthcare_db.ehr_stream ADD COLUMN spo2 DOUBLE"],
        validation_result = test_validation,
    )

    # Simulate MTTR
    from datetime import timedelta
    fault_start = datetime.now(timezone.utc) - timedelta(minutes=3, seconds=42)
    fix_end     = datetime.now(timezone.utc)
    log_mttr(record["log_id"], fault_start, fix_end)

    print_summary()