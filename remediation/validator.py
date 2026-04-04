import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from detectors.zscore_detector   import check_batch as zscore_batch
from detectors.ks_test           import run_ks_test
from detectors.schema_entropy    import check_batch as schema_batch

STEADY_STATE    = "STEADY_STATE"
STILL_ANOMALOUS = "STILL_ANOMALOUS"

def validate_pipeline(recent_events: list) -> dict:
    """
    Re-runs all Sprint 2 detectors on a fresh batch of events.
    Returns STEADY_STATE if everything is healthy, STILL_ANOMALOUS if not.

    Call this after every fix the agent applies.
    """
    if not recent_events:
        return {
            "status":    STILL_ANOMALOUS,
            "reason":    "No events received — pipeline may be stalled",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "checks":    {}
        }

    print("[VALIDATOR] Re-running all detectors on fresh events...")

    # Run all 3 detectors
    zscore_result = zscore_batch(recent_events)
    ks_result     = run_ks_test(recent_events)
    schema_result = schema_batch(recent_events)

    checks = {
        "zscore":         zscore_result,
        "ks_test":        ks_result,
        "schema_entropy": schema_result,
    }

    # Collect any failures
    failures = []
    if zscore_result.get("fault_detected"):
        failures.append(f"Z-Score: {zscore_result['flagged_events']} anomalous events")
    if ks_result.get("drift_detected"):
        drifted = [f for f, r in ks_result["fields"].items() if r["drifted"]]
        failures.append(f"KS-Test drift in: {drifted}")
    if schema_result.get("fault_detected"):
        failures.append(f"Schema entropy: {schema_result['flagged_events']} malformed events")

    status = STEADY_STATE if not failures else STILL_ANOMALOUS

    result = {
        "status":         status,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "events_checked": len(recent_events),
        "failures":       failures,
        "checks":         checks,
    }

    if status == STEADY_STATE:
        print(f"[VALIDATOR] ✅ STEADY STATE — pipeline is healthy ({len(recent_events)} events checked)")
    else:
        print(f"[VALIDATOR] ❌ STILL ANOMALOUS — {len(failures)} issue(s) remain:")
        for f in failures:
            print(f"  - {f}")

    return result


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "simulator"))
    from patient_generator import generate_patient_event
    import numpy as np

    print("=== Validator Tests ===\n")

    print("-- Test 1: Clean events (should return STEADY_STATE) --")
    clean = [generate_patient_event(anomalous=False) for _ in range(50)]
    result = validate_pipeline(clean)
    print(f"Status: {result['status']}\n")

    print("-- Test 2: Anomalous events (should return STILL_ANOMALOUS) --")
    bad = []
    for _ in range(50):
        e = generate_patient_event(anomalous=False)
        e["heart_rate"]  = round(np.random.uniform(220, 300), 1)
        e["spo2"]        = round(np.random.uniform(30,  60),  1)
        bad.append(e)
    result = validate_pipeline(bad)
    print(f"Status: {result['status']}")
    print(f"Failures: {result['failures']}\n")

    print("-- Test 3: Empty events (should return STILL_ANOMALOUS) --")
    result = validate_pipeline([])
    print(f"Status: {result['status']}")
    print(f"Reason: {result['reason']}")