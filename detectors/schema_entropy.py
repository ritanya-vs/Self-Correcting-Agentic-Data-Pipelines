import json
import os
from datetime import datetime, timezone

SCHEMA_V1 = os.path.join(
    os.path.dirname(__file__), "..", "simulator", "schemas", "ehr_v1.json"
)
SCHEMA_V2 = os.path.join(
    os.path.dirname(__file__), "..", "simulator", "schemas", "ehr_v2.json"
)

ENTROPY_THRESHOLD = 0.15   # anything above this = schema anomaly

def load_schema(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)

def get_expected_fields(schema: dict) -> set:
    return set(schema.get("properties", {}).keys())

def compute_entropy_distance(event: dict, schema: dict) -> dict:
    """
    Compares the fields in an incoming event against a schema.

    Distance score:
      0.0 = perfect match
      1.0 = completely different
    """
    expected_fields = get_expected_fields(schema)
    incoming_fields = set(event.keys())

    # Fields that are missing from the event
    missing_fields = expected_fields - incoming_fields

    # Fields in the event that aren't in the schema
    extra_fields = incoming_fields - expected_fields

    # Fields that match
    matching_fields = expected_fields & incoming_fields

    total_expected = len(expected_fields)
    if total_expected == 0:
        return {"distance": 0.0, "missing": [], "extra": [], "matching": []}

    # Distance = proportion of expected fields that are wrong
    distance = (len(missing_fields) + len(extra_fields)) / (total_expected + len(extra_fields))

    return {
        "distance":       round(distance, 4),
        "missing_fields": list(missing_fields),
        "extra_fields":   list(extra_fields),
        "matching_fields": list(matching_fields),
        "match_rate":     round(len(matching_fields) / total_expected, 4),
    }

def check_event(event: dict) -> dict:
    """
    Check a single event against both v1 and v2 schemas.
    Returns which schema it matches better and whether it's anomalous.
    """
    schema_v1 = load_schema(SCHEMA_V1)
    schema_v2 = load_schema(SCHEMA_V2)

    v1_result = compute_entropy_distance(event, schema_v1)
    v2_result = compute_entropy_distance(event, schema_v2)

    # Use the better matching schema as reference
    best_distance = min(v1_result["distance"], v2_result["distance"])
    best_schema   = "v1" if v1_result["distance"] <= v2_result["distance"] else "v2"
    best_result   = v1_result if best_schema == "v1" else v2_result

    anomaly_detected = bool(best_distance > ENTROPY_THRESHOLD)

    return {
        "detector":         "schema_entropy",
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "patient_id":       event.get("patient_id"),
        "best_schema":      best_schema,
        "distance":         best_distance,
        "threshold":        ENTROPY_THRESHOLD,
        "anomaly_detected": anomaly_detected,
        "missing_fields":   best_result["missing_fields"],
        "extra_fields":     best_result["extra_fields"],
        "fault_type":       "schema" if anomaly_detected else None,
    }

def check_batch(events: list) -> dict:
    """Check a batch of events and return overall schema health."""
    results  = [check_event(e) for e in events]
    flagged  = [r for r in results if r["anomaly_detected"]]

    all_missing = {}
    all_extra   = {}
    for r in flagged:
        for f in r["missing_fields"]:
            all_missing[f] = all_missing.get(f, 0) + 1
        for f in r["extra_fields"]:
            all_extra[f] = all_extra.get(f, 0) + 1

    return {
        "detector":         "schema_entropy",
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "total_events":     len(events),
        "flagged_events":   len(flagged),
        "flag_rate":        round(len(flagged) / len(events), 3) if events else 0,
        "missing_fields":   all_missing,
        "extra_fields":     all_extra,
        "fault_detected":   bool(len(flagged) > 0),
        "fault_type":       "schema" if flagged else None,
    }

if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "simulator"))
    from patient_generator import generate_patient_event

    print("=== Testing Schema Entropy Detector ===\n")

    print("-- Normal event (should NOT flag) --")
    normal = generate_patient_event()
    result = check_event(normal)
    print(f"Anomaly detected: {result['anomaly_detected']}")
    print(f"Distance: {result['distance']}  Best schema: {result['best_schema']}\n")

    print("-- Schema fault event (missing spo2, extra diagnosis_code) --")
    fault_event = generate_patient_event()
    fault_event.pop("spo2")
    fault_event["diagnosis_code"] = "ICD-1234"
    result = check_event(fault_event)
    print(f"Anomaly detected: {result['anomaly_detected']}")
    print(f"Distance: {result['distance']}")
    print(f"Missing fields: {result['missing_fields']}")
    print(f"Extra fields:   {result['extra_fields']}")