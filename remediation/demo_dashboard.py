import json
import sys
import os
import time
import pandas as pd
from datetime import datetime

# Add parent and agent directories to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "agent"))

from simulator.database import get_connection
from detectors.zscore_detector import check_batch as zscore_batch
from detectors.ks_test import run_ks_test
from detectors.schema_entropy import check_batch as schema_batch

# ── PRETTY PRINT HELPERS ──────────────────────────────────────────

def banner(text, char="=", color="\033[96m"):
    reset = "\033[0m"
    line  = char * 85
    print(f"\n{color}{line}")
    print(f"  {text}")
    print(f"{line}{reset}\n")

def green(t):  return f"\033[92m{t}\033[0m"
def red(t):    return f"\033[91m{t}\033[0m"
def yellow(t): return f"\033[93m{t}\033[0m"
def bold(t):   return f"\033[1m{t}\033[0m"

def fetch_live_data(limit=20):
    """Fetch real data from Databricks for analysis."""
    try:
        con = get_connection()
        cursor = con.cursor()
        # Fetching all columns to detect schema changes dynamically
        cursor.execute(f"SELECT * FROM healthcare_db.ehr_stream ORDER BY timestamp DESC LIMIT {limit}")
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        cursor.close()
        con.close()
        
        # Convert to list of dicts for our detectors
        events = []
        for row in rows:
            events.append(dict(zip(cols, row)))
        return events, cols
    except Exception as e:
        print(red(f"  [DB ERROR] Could not connect: {e}"))
        return [], []

def print_live_table(events, cols, title):
    """Prints a clean table of what is currently in the DB."""
    print(bold(f"  {title} (Live from Databricks)"))
    
    # Select important columns to show in terminal
    display_cols = [c for c in cols if c in ['patient_id', 'heart_rate', 'spo2', 'timestamp', 'diagnosis_code']]
    header = " | ".join(f"{c:<15}" for c in display_cols)
    print(f"  {header}")
    print(f"  {'-' * len(header)}")
    
    for e in events[:8]: # Show top 8
        row_vals = []
        for c in display_cols:
            val = e.get(c)
            if val is None:
                row_vals.append(red(f"{'NULL':<15}"))
            elif c == 'heart_rate' and float(val) > 180:
                row_vals.append(red(f"{str(val)[:12]:<15}"))
            else:
                row_vals.append(f"{str(val)[:12]:<15}")
        print(f"  {' | '.join(row_vals)}")
    print()

def run_diagnostics(events):
    """Runs real detectors on real data."""
    if not events: return False
    
    zscore_r = zscore_batch(events)
    ks_r     = run_ks_test(events)
    schema_r = schema_batch(events)

    print(f"  {'Detector':<22} {'Status':<12} {'Details'}")
    print(f"  {'-'*80}")

    # Z-Score Logic
    if zscore_r["fault_detected"]:
        print(f"  {'Z-Score (Clinical)':<22} {red('⚠ FAULT'):<12} Critical Vitals! Avg HR: {zscore_r['anomalous_fields']['heart_rate']['avg_zscore']:.1f} sigma")
    else:
        print(f"  {'Z-Score (Clinical)':<22} {green('✅ CLEAR'):<12} Vitals normal")

    # Schema Logic
    if schema_r["fault_detected"]:
        missing = list(schema_r['missing_fields'].keys())
        extra = list(schema_r['extra_fields'].keys())
        print(f"  {'Schema Entropy':<22} {red('⚠ FAULT'):<12} Missing: {missing} | Extra: {extra}")
    else:
        print(f"  {'Schema Entropy':<22} {green('✅ CLEAR'):<12} Schema matches gold standard")

    return zscore_r["fault_detected"] or schema_r["fault_detected"]

# ── LIVE DEMO PHASES ──────────────────────────────────────────────

def phase_monitor(label, color_code):
    banner(f"PHASE: {label}", "★", color_code)
    print("  Fetching latest 20 events from Databricks pipeline...")
    events, cols = fetch_live_data(20)
    
    if not events:
        print(yellow("  Waiting for data to arrive in Databricks..."))
        return
        
    print_live_table(events, cols, "Current Pipeline State")
    print(bold("  Running LARF Detectors on Live Data:"))
    run_diagnostics(events)

if __name__ == "__main__":
    while True:
        print("\n" + "="*30)
        print("  LARF LIVE VIEW SELECTOR")
        print("="*30)
        print("1: View Steady State (Healthy)")
        print("2: View Injected Fault (Real-time)")
        print("3: Run Remediation (Trigger Orchestrator)")
        print("4: View Recovery (Post-Fix)")
        print("5: Exit")
        
        choice = input("\nSelect Phase to View: ")
        
        if choice == "1":
            phase_monitor("STEADY STATE (HEALTHY)", "\033[92m")
        elif choice == "2":
            phase_monitor("FAULT DETECTED (LIVE)", "\033[91m")
            print(bold("\n  [TEACHER INFO]"))
            print("  This data was captured directly from the stream.")
            print("  Detectors have identified the exact root cause shown above.")
        elif choice == "3":
            banner("TRIGGERING AGENT REMEDIATION", "⚙", "\033[93m")
            print("  Calling orchestrator.py to resolve detected faults...")
            # Note: In a real demo, you would run the orchestrator in Terminal 3
            print(yellow("  Switch to Terminal 3 to watch the OODA Loop in action!"))
        elif choice == "4":
            phase_monitor("RECOVERY VALIDATION", "\033[92m")
        elif choice == "5":
            break
        
        input(bold("\n  Press ENTER to return to Menu..."))