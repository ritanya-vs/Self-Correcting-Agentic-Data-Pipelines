import json
import os
import numpy as np
from scipy import stats
from datetime import datetime, timezone

BASELINE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "simulator", "baselines", "ehr_baseline.json"
)

PVALUE_THRESHOLD = 0.0001  

def load_baseline() -> dict:
    with open(BASELINE_FILE, "r") as f:
        return json.load(f)

def run_ks_test(incoming_events: list) -> dict:
    """
    Compare a batch of incoming events against the gold baseline
    using the Kolmogorov-Smirnov test.

    KS test returns a p-value. If p < 0.05 the distributions are
    significantly different — meaning data has drifted from normal.
    """
    baseline = load_baseline()
    results  = {}
    drift_detected = False

    for field, base_stats in baseline["fields"].items():
        # Get values from incoming batch
        incoming_values = [
            e[field] for e in incoming_events
            if field in e and e[field] is not None
        ]

        if len(incoming_values) < 10:
            # Not enough data to run the test
            continue

        baseline_samples = base_stats["samples"]

        # Run the KS test
        ks_stat, p_value = stats.ks_2samp(baseline_samples, incoming_values)

        drifted = bool(p_value < PVALUE_THRESHOLD)

        if drifted:
            drift_detected = bool(True)

        results[field] = {
            "ks_statistic":  round(float(ks_stat), 4),
            "p_value":       round(float(p_value), 6),
            "drifted":       drifted,
            "threshold":     PVALUE_THRESHOLD,
            "incoming_mean": round(float(np.mean(incoming_values)), 2),
            "baseline_mean": round(base_stats["mean"], 2),
            "mean_shift":    round(float(np.mean(incoming_values)) - base_stats["mean"], 2),
        }

    return {
        "detector":       "ks_test",
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "events_tested":  len(incoming_events),
        "drift_detected": bool(drift_detected),
        "fields":         results,
        "fault_type":     "data_quality" if drift_detected else None,
    }

if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "simulator"))
    from patient_generator import generate_patient_event

    print("=== Testing KS Detector ===\n")

    print("-- 50 normal events (should NOT detect drift) --")
    normal_batch = [generate_patient_event(anomalous=False) for _ in range(50)]
    result = run_ks_test(normal_batch)
    print(f"Drift detected: {result['drift_detected']}")
    for field, r in result["fields"].items():
        if r["drifted"]:
            print(f"  DRIFT: {field} p={r['p_value']} shift={r['mean_shift']}")

    print("\n-- 50 anomalous events (SHOULD detect drift) --")
    bad_batch = []
    for _ in range(50):
        e = generate_patient_event(anomalous=False)
        e["heart_rate"]  = round(np.random.uniform(220, 300), 1)
        e["spo2"]        = round(np.random.uniform(30, 60), 1)
        e["bp_systolic"] = round(np.random.uniform(200, 280), 1)
        bad_batch.append(e)

    result = run_ks_test(bad_batch)
    print(f"Drift detected: {result['drift_detected']}")
    for field, r in result["fields"].items():
        if r["drifted"]:
            print(f"  DRIFT: {field}  p={r['p_value']}  "
                  f"baseline_mean={r['baseline_mean']}  "
                  f"incoming_mean={r['incoming_mean']}  "
                  f"shift={r['mean_shift']}")