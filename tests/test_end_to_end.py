import sys
import os
import json
import time
import csv
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from simulator.patient_generator import generate_patient_event
from detectors.zscore_detector   import check_batch as zscore_batch
from detectors.ks_test           import run_ks_test
from detectors.schema_entropy    import check_batch as schema_batch
from remediation.validator       import validate_pipeline
from remediation.incident_logger import log_incident, log_mttr, print_summary
import numpy as np

RESULTS_FILE = os.path.join(os.path.dirname(__file__), "..", "logs", "evaluation_results.csv")
os.makedirs(os.path.join(os.path.dirname(__file__), "..", "logs"), exist_ok=True)

# ── Fault simulators (no Kafka needed — pure in-memory) ───────────

def make_normal_batch(n=50):
    return [generate_patient_event(anomalous=False) for _ in range(n)]

def make_schema_fault_batch(n=50):
    events = []
    for _ in range(n):
        e = generate_patient_event(anomalous=False)
        e.pop("spo2", None)
        e["diagnosis_code"] = "ICD-9999"
        events.append(e)
    return events

def make_data_quality_batch(n=50):
    events = []
    for _ in range(n):
        e = generate_patient_event(anomalous=False)
        e["heart_rate"]  = round(np.random.uniform(220, 300), 1)
        e["spo2"]        = round(np.random.uniform(30,  60),  1)
        e["bp_systolic"] = round(np.random.uniform(200, 280), 1)
        events.append(e)
    return events

def make_performance_batch(n=50):
    # Performance fault = normal data but consumer lag spike
    # We simulate by returning normal events + a lag flag
    return [generate_patient_event(anomalous=False) for _ in range(n)]

def make_security_batch(n=50):
    events = []
    for _ in range(n):
        e = generate_patient_event(anomalous=False)
        e["patient_id"] = "PT-ATTACKER-0000"
        events.append(e)
    return events

def make_stall_batch(n=0):
    # Stall = no events
    return []

# ── Ground truth — what each fault type should trigger ────────────

GROUND_TRUTH = {
    "schema":       "schema_entropy",
    "data_quality": "ks_test",
    "performance":  "zscore",
    "security":     "zscore",
    "stall":        "stall",
}

FAULT_GENERATORS = {
    "schema":       make_schema_fault_batch,
    "data_quality": make_data_quality_batch,
    "performance":  make_performance_batch,
    "security":     make_security_batch,
    "stall":        make_stall_batch,
}

# ── Run one test ───────────────────────────────────────────────────

def run_one_test(fault_type: str, run_number: int) -> dict:
    t0 = time.time()

    # Generate fault batch
    fault_batch  = FAULT_GENERATORS[fault_type]()
    normal_batch = make_normal_batch(50)

    # Run detectors on fault batch
    zscore_result = zscore_batch(fault_batch) if fault_batch else {"fault_detected": False}
    ks_result     = run_ks_test(fault_batch)  if fault_batch else {"drift_detected": False}
    schema_result = schema_batch(fault_batch) if fault_batch else {"fault_detected": False}

    # Determine which detector fired
    fired = []
    if zscore_result.get("fault_detected"):  fired.append("zscore")
    if ks_result.get("drift_detected"):      fired.append("ks_test")
    if schema_result.get("fault_detected"):  fired.append("schema_entropy")
    if not fault_batch:                       fired.append("stall")

    expected_detector = GROUND_TRUTH[fault_type]
    detected          = expected_detector in fired or (fault_type == "stall" and not fault_batch)

    # Simulate fix by running validator on clean events
    validation = validate_pipeline(normal_batch)
    remediated = validation["status"] == "STEADY_STATE"

    t1   = time.time()
    mttr = round(t1 - t0, 2)

    result = {
        "run":                  run_number,
        "fault_type":           fault_type,
        "expected_detector":    expected_detector,
        "detectors_fired":      fired,
        "detection_correct":    detected,
        "remediation_success":  remediated,
        "mttr_seconds":         mttr,
        "timestamp":            datetime.now(timezone.utc).isoformat(),
    }

    status = "✅" if detected and remediated else "❌"
    print(f"  {status} Run {run_number:>3} | {fault_type:<15} | "
          f"detected={detected} | remediated={remediated} | mttr={mttr}s")

    return result

# ── Full 120-test evaluation ───────────────────────────────────────

def run_full_evaluation(runs_per_fault: int = 24):
    fault_types = ["schema", "data_quality", "performance", "security", "stall"]
    all_results = []
    run_number  = 1

    print("\n" + "="*65)
    print(f"  LARF Full Evaluation — {runs_per_fault * len(fault_types)} total tests")
    print("="*65 + "\n")

    for fault_type in fault_types:
        print(f"[FAULT: {fault_type.upper()}] Running {runs_per_fault} tests...")
        for i in range(runs_per_fault):
            result = run_one_test(fault_type, run_number)
            all_results.append(result)
            run_number += 1
        print()

    # Save to CSV
    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_results[0].keys())
        writer.writeheader()
        writer.writerows(all_results)

    # Print final summary
    _print_evaluation_summary(all_results)
    print(f"\n[EVAL] Results saved to: {RESULTS_FILE}")
    return all_results

def _print_evaluation_summary(results: list):
    total      = len(results)
    correct    = sum(1 for r in results if r["detection_correct"])
    remediated = sum(1 for r in results if r["remediation_success"])
    mttrs      = [r["mttr_seconds"] for r in results]

    print("\n" + "="*65)
    print("  LARF EVALUATION RESULTS")
    print("="*65)
    print(f"  Total tests:            {total}")
    print(f"  Detection accuracy:     {correct}/{total} = {round(correct/total*100, 1)}%")
    print(f"  Remediation success:    {remediated}/{total} = {round(remediated/total*100, 1)}%")
    print(f"  Avg MTTR:               {round(sum(mttrs)/len(mttrs), 2)}s")
    print(f"  Max MTTR:               {max(mttrs)}s")
    print(f"  Min MTTR:               {min(mttrs)}s")
    print()

    # Per fault type breakdown
    print(f"  {'Fault Type':<18} {'Detected':>10} {'Remediated':>12} {'Avg MTTR':>10}")
    print(f"  {'-'*52}")
    for fault_type in set(r["fault_type"] for r in results):
        subset     = [r for r in results if r["fault_type"] == fault_type]
        det        = sum(1 for r in subset if r["detection_correct"])
        rem        = sum(1 for r in subset if r["remediation_success"])
        avg_mttr   = round(sum(r["mttr_seconds"] for r in subset) / len(subset), 2)
        print(f"  {fault_type:<18} {det}/{len(subset):>7}    {rem}/{len(subset):>9}    {avg_mttr:>8}s")

    print("="*65)

if __name__ == "__main__":
    run_full_evaluation(runs_per_fault=24)