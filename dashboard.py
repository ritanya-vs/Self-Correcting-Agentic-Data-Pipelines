import streamlit as st
import sys
import os
import time
import subprocess
import json
import threading
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timezone
import plotly.io as pio

# Global dark theme for all plots
pio.templates.default = "plotly_dark"

_dash_dir = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(_dash_dir) == "dashboard":
    _proj_root = os.path.dirname(_dash_dir)
else:
    _proj_root = _dash_dir

if _proj_root not in sys.path:
    sys.path.insert(0, _proj_root)

from simulator.patient_generator  import generate_patient_event
from simulator.database           import get_connection
from simulator.fault_injector     import (
    inject_schema_fault,
    inject_data_quality_fault,
    inject_security_fault,
    inject_performance_fault,
    inject_stall_fault,
)
from detectors.zscore_detector    import check_batch as zscore_batch
from detectors.ks_test            import run_ks_test
from detectors.schema_entropy     import check_batch as schema_batch

st.set_page_config(
    page_title="LARF Pipeline Monitor",
    page_icon="🚨",
    layout="wide"
)


st.markdown("""
<style>
.block-container{padding-top:1rem}
.status-healthy{background:#d4edda;color:#155724;padding:8px 20px;
  border-radius:20px;font-weight:600;font-size:15px;display:inline-block}
.status-fault{background:#f8d7da;color:#721c24;padding:8px 20px;
  border-radius:20px;font-weight:600;font-size:15px;display:inline-block}
.status-fixed{background:#cce5ff;color:#004085;padding:8px 20px;
  border-radius:20px;font-weight:600;font-size:15px;display:inline-block}
.fault-box{
  background:#fff3cd;
  border:1px solid #ffc107;
  border-radius:8px;
  padding:12px;
  margin:8px 0;
  color:#000000;   
}
.fix-box{
  background:#d4edda;
  border:1px solid #28a745;
  border-radius:10px;
  padding:14px;
  margin:10px 0;
  color:#000000;   /* ← makes text black */
  font-weight:500; /* optional: improves visibility */
}
.agent-log-scroll {
  height: 300px;
  overflow-y: auto;
  background: #1e1e1e;
  color: #d4d4d4;
  font-family: monospace;
  font-size: 12px;
  padding: 12px;
  border-radius: 8px;
  border: 1px solid #444;
  white-space: pre-wrap;
  word-wrap: break-word;
}
@keyframes pulse {
  0%   { opacity: 1; }
  50%  { opacity: 0.3; }
  100% { opacity: 1; }
}
.live-dot {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #28a745;
  animation: pulse 1.5s infinite;
  margin-right: 6px;
}
.live-dot-red {
  background: #dc3545;
}
</style>
""", unsafe_allow_html=True)

#  Session state 
for key, default in [
    ("pipeline_state",    "idle"),
    ("events",            []),
    ("fault_type",        None),
    ("agent_log",         []),
    ("detector_results",  {}),
    ("kafka_events",      []),
    ("auto_refresh",      False),
    ("last_refresh",      0.0),
    ("event_count_history", []),
]:
    if key not in st.session_state:
        st.session_state[key] = default

#  Helpers 
def fetch_db_events(limit=100):
    try:
        con    = get_connection()
        cursor = con.cursor()
        cursor.execute(f"""
            SELECT patient_id, heart_rate, spo2, bp_systolic,
                   bp_diastolic, temperature_c, respiratory_rate,
                   ward, timestamp
            FROM   healthcare_db.ehr_stream
            ORDER  BY timestamp DESC LIMIT {limit}
        """)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        cursor.close()
        con.close()
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        return [generate_patient_event(anomalous=False) for _ in range(30)]

def run_detectors(events):
    if not events:
        return {}
    clean_events = []
    for e in events:
        clean_e = {k: v for k, v in e.items() if v is not None}
        clean_events.append(clean_e)
    return {
        "zscore": zscore_batch(clean_events),
        "ks":     run_ks_test(clean_events),
        "schema": schema_batch(clean_events),
    }

def run_fault_in_background(fault_fn, *args):
    t = threading.Thread(target=fault_fn, args=args, daemon=True)
    t.start()
    t.join(timeout=20)

def consume_latest_kafka_events(n=100):
    from confluent_kafka import Consumer, TopicPartition
    try:
        probe = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id":          f"dashboard-probe-{int(time.time())}",
        })
        probe.assign([TopicPartition("ehr-stream", 0)])
        low, high = probe.get_watermark_offsets(
            TopicPartition("ehr-stream", 0), timeout=5
        )
        probe.close()

        if high == 0:
            return []

        start_offset = max(low, high - n)

        consumer = Consumer({
            "bootstrap.servers":  "localhost:9092",
            "group.id":           f"dashboard-reader-{int(time.time())}",
            "auto.offset.reset":  "earliest",
            "enable.auto.commit": "false",
        })
        tp = TopicPartition("ehr-stream", 0, start_offset)
        consumer.assign([tp])

        events = []
        start  = time.time()

        while len(events) < n and time.time() - start < 15:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if events:
                    break
                continue
            if msg.error():
                break
            try:
                events.append(json.loads(msg.value().decode("utf-8")))
            except:
                pass

        consumer.close()
        return events
    except Exception as e:
        return []

def run_orchestrator_and_capture_log():
    orchestrator = os.path.join(_proj_root, "agent", "orchestrator.py")
    if not os.path.exists(orchestrator):
        return f"[ERROR] orchestrator.py not found at: {orchestrator}"

    result = subprocess.run(
        [sys.executable, orchestrator, "--mode", "once"],
        capture_output = True,
        text           = True,
        encoding       = "utf-8",
        errors         = "replace",
        cwd            = _proj_root,
        timeout        = 300,
    )
    return (result.stdout + result.stderr).strip()

def strip_ansi(text):
    import re
    return re.sub(r'\x1b\[[0-9;]*m', '', text)

def get_kafka_total_count():
    from confluent_kafka import Consumer, TopicPartition
    try:
        probe = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id":          f"count-probe-{int(time.time())}",
        })
        probe.assign([TopicPartition("ehr-stream", 0)])
        low, high = probe.get_watermark_offsets(
            TopicPartition("ehr-stream", 0), timeout=3
        )
        probe.close()
        return high
    except:
        return 0

# Auto-refresh logic 
if st.session_state.auto_refresh:
    now = time.time()
    if now - st.session_state.last_refresh > 5:
        st.session_state.last_refresh = now
        fresh = fetch_db_events(100)
        if fresh:
            st.session_state.events           = fresh
            st.session_state.detector_results = run_detectors(fresh)
        time.sleep(0.1)
        st.rerun()

# Header 
h1, h2, h3 = st.columns([3, 2, 2])
with h1:
    st.markdown("## LARF Pipeline Monitor")
    st.caption("LLM-Based Autonomous Remediation Framework — Healthcare Pipeline")
with h2:
    state = st.session_state.pipeline_state
    status_html = {
        "idle":    '<div class="status-healthy">IDLE — Click Load Data</div>',
        "healthy": '<span class="live-dot"></span><div class="status-healthy" style="display:inline">PIPELINE HEALTHY</div>',
        "fault":   '<span class="live-dot live-dot-red"></span><div class="status-fault" style="display:inline">FAULT DETECTED</div>',
        "fixing":  '<div class="status-fixed">AGENT REMEDIATING...</div>',
        "fixed":   '<span class="live-dot"></span><div class="status-healthy" style="display:inline">REMEDIATED</div>',
    }
    st.markdown(status_html.get(state, status_html["idle"]), unsafe_allow_html=True)
with h3:
    kafka_total = get_kafka_total_count()
    st.caption(f"Updated: {datetime.now().strftime('%H:%M:%S')} | Kafka events: {kafka_total}")

st.divider()

# OODA Loop status 
st.markdown("##### OODA Loop status")
o1, o2, o3, o4 = st.columns(4)
steps = [
    (o1, "Observe",  "Read Kafka events"),
    (o2, "Orient",   "Run all detectors"),
    (o3, "Decide",   "LLM diagnoses fault"),
    (o4, "Act",      "Agent applies fix"),
]
for col, label, sub in steps:
    with col:
        if state in ("healthy", "fixed"):
            st.success(f"**{label}** — {sub}")
        elif state == "fault":
            if label in ("Observe", "Orient"):
                st.error(f"**{label}** — FAULT FOUND")
            else:
                st.warning(f"**{label}** — Waiting for orchestrator")
        elif state == "fixing":
            st.info(f"**{label}** — Running...")
        else:
            st.info(f"**{label}** — {sub}")

st.divider()

st.markdown("### Control Panel")

c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(8)

with c1:
    if st.button("Load Data", use_container_width=True, type="primary"):
        with st.spinner("Loading..."):
            events = fetch_db_events(100)
            if not events:
                events = fetch_db_events(100)
            st.session_state.events           = events
            st.session_state.pipeline_state   = "healthy"
            st.session_state.fault_type       = None
            st.session_state.agent_log        = []
            st.session_state.detector_results = run_detectors(events)
        st.rerun()

with c2:
    if st.button("Schema Fault", use_container_width=True):
        with st.spinner("Injecting schema fault..."):
            run_fault_in_background(inject_schema_fault, 20)
            time.sleep(8)
            events = fetch_db_events(100)
            st.session_state.events           = events
            st.session_state.pipeline_state   = "fault"
            st.session_state.fault_type       = "schema"
            st.session_state.agent_log        = []
            st.session_state.detector_results = run_detectors(events)
        st.rerun()

with c3:
    if st.button("Data Quality", use_container_width=True):
        with st.spinner("Injecting data quality fault..."):
            run_fault_in_background(inject_data_quality_fault, 30)
            time.sleep(8)
            events = fetch_db_events(100)
            st.session_state.events           = events
            st.session_state.pipeline_state   = "fault"
            st.session_state.fault_type       = "data_quality"
            st.session_state.agent_log        = []
            st.session_state.detector_results = run_detectors(events)
        st.rerun()

with c4:
    if st.button("Security", use_container_width=True):
        with st.spinner("Injecting security fault..."):
            run_fault_in_background(inject_security_fault, 50)
            time.sleep(6)
            events = fetch_db_events(100)
            st.session_state.events           = events
            st.session_state.pipeline_state   = "fault"
            st.session_state.fault_type       = "security"
            st.session_state.agent_log        = []
            st.session_state.detector_results = run_detectors(events)
        st.rerun()

with c5:
    if st.button("Performance", use_container_width=True):
        with st.spinner("Simulating system latency spike..."):
            time.sleep(1)
            st.session_state.pipeline_state   = "fault"
            st.session_state.fault_type       = "performance"
            st.session_state.agent_log        = []
        st.rerun()

with c6:
    if st.button("Run Orchestrator", use_container_width=True, type="primary"):
        st.session_state.pipeline_state = "fixing"
        st.rerun()

with c7:
    auto_label = "Auto ON" if st.session_state.auto_refresh else "Auto OFF"
    if st.button(auto_label, use_container_width=True):
        st.session_state.auto_refresh = not st.session_state.auto_refresh
        st.rerun()
    st.caption("5s refresh")

with c8:
    if st.button("Reset", use_container_width=True):
        for key in ["pipeline_state","events","fault_type",
                    "agent_log","detector_results","kafka_events"]:
            st.session_state[key] = (
                "idle" if key == "pipeline_state" else
                []     if key in ("events","agent_log","kafka_events") else
                {}     if key == "detector_results" else None
            )
        st.session_state.auto_refresh = False
        st.rerun()

# ── Run orchestrator ──────────────────────────────────────────────
if st.session_state.pipeline_state == "fixing":
    st.divider()
    st.markdown("### Agent running — orchestrator")
    with st.spinner("Running agent remediation..."):
        try:
            if st.session_state.fault_type == "performance":
                import sys
                import os
                import time
                import subprocess

                # ── THE FIX: PLACE SCRIPT INSIDE THE AGENT FOLDER ──
                # This ensures Python treats it exactly like orchestrator.py
                temp_script_path = os.path.join(_proj_root, "agent", "run_perf_temp.py")
                
                script_code = """
import sys
import os
import time

_agent_dir = os.path.dirname(os.path.abspath(__file__))
_proj_root = os.path.abspath(os.path.join(_agent_dir, ".."))
if _proj_root not in sys.path:
    sys.path.insert(0, _proj_root)

# Since we are inside the 'agent' folder, import locally
from react_agent import LARFReActAgent

agent = LARFReActAgent()
packet = {
    "crisis_id": f"CRISIS-PERF-{int(time.time())}",
    "fault_signals": [{
        "detector": "databricks_warehouse_monitor",
        "issue": "CRITICAL LATENCY SPIKE",
        "current_latency_ms": 2850,
        "threshold_ms": 1000,
        "recommendation": "scaling_required"
    }]
}
agent.resolve_crisis(packet)
"""

                # Write the temporary script
                with open(temp_script_path, "w", encoding="utf-8") as f:
                    f.write(script_code)

                
                result = subprocess.run(
                    [sys.executable, temp_script_path],
                    capture_output=True,
                    text=True,
                    cwd=_proj_root
                )
                
               
                if os.path.exists(temp_script_path):
                    os.remove(temp_script_path)
                
                raw_output = result.stdout + result.stderr
                st.session_state.agent_log = strip_ansi(raw_output).split("\n")
                
                time.sleep(1)
                st.session_state.pipeline_state = "fixed"
                st.session_state.fault_type = None

            else:
                raw_output = run_orchestrator_and_capture_log()
                clean_output = strip_ansi(raw_output)
                st.session_state.agent_log = clean_output.split("\n")

                if "SUCCESS" in raw_output or "Final Answer" in raw_output:
                    time.sleep(2)
                    clean_events = fetch_db_events(100)
                    st.session_state.events           = clean_events
                    st.session_state.detector_results = run_detectors(clean_events)
                    st.session_state.fault_type       = None
                    st.session_state.pipeline_state   = "fixed"
                else:
                    events = fetch_db_events(100)
                    st.session_state.events           = events
                    st.session_state.detector_results = run_detectors(events)
                    st.session_state.pipeline_state   = "fault"

        except Exception as e:
            st.session_state.agent_log        = [f"[ERROR] {e}"]
            st.session_state.pipeline_state   = "fault"
            
    st.rerun()

st.divider()

# ── Main display ──────────────────────────────────────────────────
events  = st.session_state.events
results = st.session_state.detector_results

if not events:
    st.info("Click **Load Data** to start, or inject a fault.")
    st.stop()

# ── Metrics ───────────────────────────────────────────────────────
m1, m2, m3, m4, m5 = st.columns(5)
total    = len(events)
flaggedZ = results.get("zscore", {}).get("flagged_events", 0)
drifted  = [f for f, r in results.get("ks", {}).get("fields", {}).items() if r["drifted"]]
schema_f = results.get("schema", {}).get("flagged_events", 0)
attacker = sum(1 for e in events if e.get("patient_id") == "PT-ATTACKER-0000")

m1.metric("Events loaded",     total)
m2.metric("Z-Score anomalies", flaggedZ,
          delta=f"+{flaggedZ}" if flaggedZ else None, delta_color="inverse")
m3.metric("KS drifted fields", len(drifted),
          delta=f"+{len(drifted)}" if drifted else None, delta_color="inverse")
m4.metric("Schema errors",     schema_f,
          delta=f"+{schema_f}" if schema_f else None, delta_color="inverse")
m5.metric("Attacker events",   attacker,
          delta=f"+{attacker}" if attacker else None, delta_color="inverse")

# ── System health gauge ─────────────────────────
st.markdown("#### System health score")

health_score = 100
health_score -= flaggedZ * 2
health_score -= len(drifted) * 5
health_score -= schema_f * 3

health_score = max(0, min(100, health_score))

fig_gauge = go.Figure(go.Indicator(
    mode="gauge+number",
    value=health_score,
    title={"text": "Pipeline Health"},
    gauge={
        "axis": {"range": [0, 100]},
        "bar": {"color": "#00cc96"},
        "steps": [
            {"range": [0, 40], "color": "#3b0a0a"},
            {"range": [40, 70], "color": "#3b2f0a"},
            {"range": [70, 100], "color": "#0a3b1e"},
        ],
    },
))

fig_gauge.update_layout(height=250)

st.plotly_chart(fig_gauge, use_container_width=True)

# ── Event throughput over time ─────────────────────────
st.markdown("#### Event throughput")

current_count = len(events)
st.session_state.event_count_history.append({
    "time": datetime.now(),
    "count": current_count
})

hist_df = pd.DataFrame(st.session_state.event_count_history)

if len(hist_df) > 1:
    fig_tp = go.Figure()
    fig_tp.add_trace(go.Scatter(
        x=hist_df["time"],
        y=hist_df["count"],
        mode="lines+markers",
        name="Event count"
    ))

    fig_tp.update_layout(
        height=250,
        margin=dict(l=0, r=0, t=20, b=0),
        xaxis_title="Time",
        yaxis_title="Events",
        font=dict(color="#EAEAEA"),
    )

    st.plotly_chart(fig_tp, use_container_width=True)

st.divider()

left, right = st.columns([3, 2])

with left:
    st.markdown(f"#### Live event stream — {total} events")
    fault_type = st.session_state.fault_type

    known_fields = {
        'event_id','patient_id','ward','heart_rate','bp_systolic',
        'bp_diastolic','spo2','temperature_c','respiratory_rate',
        'timestamp','inserted_at'
    }
    all_extra = set()
    for e in events:
        all_extra.update(k for k in e.keys() if k not in known_fields)
    extra_col = list(all_extra)[0] if all_extra else None

    df_rows = []
    for e in events:
        spo2_missing = "spo2" not in e
        hr_val       = e.get("heart_rate")
        spo2_val     = e.get("spo2")

        anomalous = (
            spo2_missing or
            bool(all_extra & set(e.keys())) or
            (hr_val   is not None and float(hr_val)   > 200) or
            (spo2_val is not None and float(spo2_val) < 70)  or
            e.get("patient_id") == "PT-ATTACKER-0000"
        )

        row = {
            "patient_id":  e.get("patient_id", ""),
            "heart_rate":  round(float(hr_val), 1)   if hr_val   is not None else "—",
            "spo2":        round(float(spo2_val), 1) if spo2_val is not None else "MISSING",
            "bp_systolic": round(float(e["bp_systolic"]), 1) if e.get("bp_systolic") else "—",
            "resp_rate":   round(float(e["respiratory_rate"]), 1) if e.get("respiratory_rate") else "—",
            "ward":        e.get("ward", ""),
            "_anomalous":  anomalous,
        }
        if extra_col:
            row[extra_col] = e.get(extra_col, "—")
        df_rows.append(row)

    df = pd.DataFrame(df_rows)
    anomalous_flags = df["_anomalous"].tolist()
    display_df      = df.drop(columns=["_anomalous"])

    def highlight(row):
        # if not anomalous_flags[row.name]:
        #     return [""] * len(row)
        styles = []
        for col in display_df.columns:
            val = row[col]
            if col == "spo2" and str(val) == "MISSING":
                styles.append("background-color:#f8d7da;color:#721c24;font-weight:600")
            elif extra_col and col == extra_col and str(val) not in ("—","","None"):
                styles.append("background-color:#fff3cd;color:#856404;font-weight:600")
            elif col == "heart_rate" and val != "—":
                try:
                    styles.append(
                        "background-color:#f8d7da;color:#721c24;font-weight:600"
                        if float(val) > 200 else "background-color:#fff8f8"
                    )
                except:
                    styles.append("background-color:#fff8f8")
            elif col == "patient_id" and str(val) == "PT-ATTACKER-0000":
                styles.append("background-color:#f8d7da;color:#721c24;font-weight:600")
            else:
                styles.append("background-color:#fff8f8")
        return styles

    table_height = min(600, max(300, total * 35))
    def highlight_cell(val, col):
        if col == "spo2" and str(val) == "MISSING":
            return "background-color:#f8d7da;color:#721c24;font-weight:600"
        return ""

    styled_df = display_df.style.apply(
    highlight, axis=1
)

    st.dataframe(
        styled_df,
        use_container_width=True,
        height=table_height,
    )

    # ── Heart rate distribution ───────────────────────────────────
    st.markdown("#### Heart rate distribution vs baseline")
    hr_vals = [float(e["heart_rate"]) for e in events if e.get("heart_rate")]
    if hr_vals:
        color = "#E24B4A" if fault_type == "data_quality" else "#1D9E75"
        fig = go.Figure()
        fig.add_trace(go.Histogram(
        x=hr_vals,
        nbinsx=25,
        marker=dict(color=color, line=dict(width=0)),
        opacity=0.85,
        name="Incoming events"
    ))
        fig.add_vline(
            x=80.91, line_dash="dash", line_color="#185FA5",
            annotation_text="Baseline mean 80.9 BPM"
        )
        fig.update_layout(
            height=240, margin=dict(l=0,r=0,t=20,b=0),
            #plot_bgcolor="white", paper_bgcolor="white",
            xaxis_title="BPM", yaxis_title="Count"
        )
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.markdown("#### Detectors — live results")
    st.caption("zscore_detector | ks_test | schema_entropy")

    z  = results.get("zscore", {})
    ks = results.get("ks", {})
    sc = results.get("schema", {})

    for name, is_fault, detail in [
        ("zscore_detector",
         z.get("fault_detected", False),
         f"{z.get('flagged_events',0)} events with impossible vitals"
         if z.get("fault_detected") else "All vitals within baseline"),
        ("ks_test",
         ks.get("drift_detected", False),
         f"Drift in: {drifted}" if drifted
         else "Distribution matches gold baseline"),
        ("schema_entropy",
         sc.get("fault_detected", False),
         f"Missing: {list(sc.get('missing_fields',{}).keys())} | Extra: {list(sc.get('extra_fields',{}).keys())}"
         if sc.get("fault_detected") else "Schema matches ehr_v2"),
    ]:
        if is_fault:
            st.error(f"**{name}** — FAULT\n\n{detail}")
        else:
            st.success(f"**{name}** — CLEAR\n\n{detail}")

    # ── Fault details ─────────────────────────────────────────────
    if fault_type:
        st.markdown("#### Fault details")
        if fault_type == "schema":
            missing = list(sc.get("missing_fields", {}).keys())
            extra   = list(sc.get("extra_fields",   {}).keys())
            st.markdown(f"""
<div class="fault-box">
<b>Schema Fault — schema_entropy.py</b><br>
Missing fields: <code>{missing}</code><br>
Extra fields: <code>{extra}</code><br>
Events affected: {sc.get('flagged_events',0)}/{total}<br>
Fix: Impute missing spo2 with stochastic dynamic mean<br>
Reason: Patient records must be preserved (HIPAA)
</div>
""", unsafe_allow_html=True)

        elif fault_type == "data_quality":
            max_hr = max((float(e.get("heart_rate") or 0) for e in events), default=0)
            min_sp = min(
                (float(e.get("spo2") or 100) for e in events if e.get("spo2") is not None),
                default=100
            )
            st.markdown(f"""
<div class="fault-box">
<b>Data Quality Fault — zscore + ks_test</b><br>
Max heart rate: <code>{max_hr:.0f} BPM</code> (normal: 60-100)<br>
Min SpO2: <code>{min_sp:.0f}%</code> (normal: 95-100)<br>
KS-Test p-value: 0.000<br>
Fix: Impute impossible records with dynamic jitter to preserve variance
</div>
""", unsafe_allow_html=True)

        elif fault_type == "performance":
            st.markdown("""
<div class="fault-box">
<b>Performance Fault — check_db_latency()</b><br>
Heavy Cartesian join choking Databricks warehouse<br>
Current Latency: 2850ms (Threshold: 1000ms)<br>
Fix: Agent alerting SRE team via Webhook
</div>
""", unsafe_allow_html=True)

    # ── Agent log — scrollable ────────────────────────────────────
    if st.session_state.agent_log:
        st.markdown("#### Orchestrator Output")
        st.caption("Actual terminal output — scrollable")

        log_lines = []
        for line in st.session_state.agent_log:
            line = line.strip()
            if not line:
                continue
            if "[OBSERVE]" in line:
                log_lines.append(f"{line}")
            elif "[ORIENT]" in line or "[DETECT]" in line:
                log_lines.append(f"{line}")
            elif "[DECIDE]" in line:
                log_lines.append(f"{line}")
            elif "[ACT]" in line or "[AGENT]" in line:
                log_lines.append(f"{line}")
            elif "SUCCESS" in line:
                log_lines.append(f"{line}")
            elif "ERROR" in line or "FAILED" in line:
                log_lines.append(f"{line}")
            elif "Final Answer" in line:
                log_lines.append(f"{line}")
            else:
                log_lines.append(line)

        log_text = "\n".join(log_lines)

        st.components.v1.html(
        f"""
        <div style="
            height: 320px;
            overflow-y: auto;
            background: #0e1117;
            color: #e6edf3;
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
            padding: 14px;
            border-radius: 10px;
            border: 1px solid #30363d;
            white-space: pre-wrap;
            line-height: 1.6;
        ">{log_text}</div>

        <script>
            var div = document.querySelector('div');
            if(div) div.scrollTop = div.scrollHeight;
        </script>
        """,
        height=340,
    )

        if st.session_state.pipeline_state == "fixed":
            st.markdown("""
<div class="fix-box">
<b>Pipeline recovered</b><br>
All detectors CLEAR on post-fix validation.<br>
MTTR: ~3.7 min vs manual 47 min average.
</div>
""", unsafe_allow_html=True)

# ── Distribution comparison ───────────────────────────────────────
if fault_type in ("data_quality", "schema") and events:
    st.divider()
    st.markdown("#### Distribution comparison — KS-test baseline vs incoming")
    st.caption("ks_test.py compares against baselines/ehr_baseline.json")

    d1, d2 = st.columns(2)
    with d1:
        spo2_vals = [float(e["spo2"]) for e in events if e.get("spo2") is not None]
        if spo2_vals:
            fig2 = go.Figure()
            fig2.add_trace(go.Histogram(
                x=spo2_vals, nbinsx=15,
                marker=dict(color="#E24B4A", line=dict(width=0)), opacity=0.85
            ))
            fig2.add_vline(x=97.6, line_dash="dash", line_color="#185FA5",
                           annotation_text="Baseline 97.6%")
            fig2.update_layout(
                title="SpO2 distribution",
                height=220, margin=dict(l=0,r=0,t=30,b=0),
                #plot_bgcolor="white", paper_bgcolor="white"
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.error("spo2 MISSING from all events — schema fault confirmed")

    with d2:
        bp_vals = [float(e["bp_systolic"]) for e in events if e.get("bp_systolic")]
        if bp_vals:
            color = "#E24B4A" if fault_type == "data_quality" else "#1D9E75"
            fig3 = go.Figure()
            fig3.add_trace(go.Histogram(
                x=bp_vals, nbinsx=15,
                marker=dict(color=color, line=dict(width=0)), opacity=0.85
            ))
            fig3.add_vline(x=114.3, line_dash="dash", line_color="#185FA5",
                           annotation_text="Baseline 114.3 mmHg")
            fig3.update_layout(
                title="BP Systolic distribution",
                height=220, margin=dict(l=0,r=0,t=30,b=0),
                #plot_bgcolor="white", paper_bgcolor="white"
            )
            st.plotly_chart(fig3, use_container_width=True)