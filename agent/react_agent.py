import os
import json
import sys
from dotenv import load_dotenv

_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.abspath(os.path.join(_current_dir, ".."))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from langchain_ollama import ChatOllama
from langchain_classic.agents import AgentExecutor, create_react_agent
from langchain_community.vectorstores import Chroma
from langchain_huggingface import HuggingFaceEmbeddings

from tools.schema_tool import execute_sql_ddl
from tools.infra_tool import execute_bash_command
from tools.sql_tool import execute_sql_dml
from tools.security_tool import quarantine_patient
from prompts import get_react_prompt

load_dotenv()

# ── Ollama config ─────────────────────────────────────────────────
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "https://iva-unministrative-unhesitatively.ngrok-free.dev")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL",    "qwen2.5")

class LARFReActAgent:
    def __init__(self):
        print(f"[AGENT] Connecting to Ollama at {OLLAMA_BASE_URL}")
        print(f"[AGENT] Using model: {OLLAMA_MODEL}")

        # 1. Initialize Ollama LLM via ngrok tunnel
        self.llm = ChatOllama(
            base_url    = OLLAMA_BASE_URL,
            model       = OLLAMA_MODEL,
            temperature = 0.1,
        )

        # 2. Load tools
        self.tools = [
            execute_sql_ddl,
            execute_bash_command,
            execute_sql_dml,
            quarantine_patient
        ]

        # 3. Setup ChromaDB memory
        self.retriever = self._setup_chromadb()

        # 4. Bind the ReAct prompt
        self.prompt = get_react_prompt()

        # 5. Create agent
        self.agent = create_react_agent(self.llm, self.tools, self.prompt)

        self.agent_executor = AgentExecutor(
            agent              = self.agent,
            tools              = self.tools,
            verbose            = True,
            handle_parsing_errors = True,
            max_iterations     = 8
        )

    def _setup_chromadb(self):
        print("[INFO] Initializing ChromaDB Runbook Retriever...")
        embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

        runbooks = [
            "RUNBOOK A: If Kafka consumer lag is high and pipeline is stalled, "
            "it may be a connector lock. Action: Use execute_bash_command "
            "to check system logs or restart the service.",

            "RUNBOOK B: [SCHEMA FAULT] If 'spo2' is missing or unauthorized columns "
            "like 'diagnosis_code' appear. Action: Use execute_sql_dml to "
            "impute missing values: UPDATE healthcare_db.ehr_stream SET spo2 = 97.6 WHERE spo2 IS NULL. "
            "Rule: Do NOT add new columns (Strict Schema Enforcement).",

            "RUNBOOK C: [RESOURCE LIMIT] If warehouse latency is high or scaling is required. "
            "Action: Use execute_bash_command to send a Discord Webhook alert "
            "notifying the SRE team that manual intervention is required.",

            "RUNBOOK D: [DATA QUALITY - VITALS] If zscore detects impossible vitals "
            "(heart_rate > 200 OR bp_systolic > 180), Action: Use execute_sql_dml to impute "
            "with safe baseline values: "
            "UPDATE healthcare_db.ehr_stream SET "
            "heart_rate = 80.0, "
            "bp_systolic = 120.0 "
            "WHERE heart_rate > 200 OR bp_systolic > 180.",

            "RUNBOOK E: [SECURITY] If rapid events arrive from PT-ATTACKER-0000. "
            "Action: Use quarantine_patient to purge the attacker records immediately.",

            "RUNBOOK F: [STALL] If the stream is idle for 30s. Action: Use "
            "execute_bash_command to ping the connector status API."
        ]

        vectorstore = Chroma.from_texts(texts=runbooks, embedding=embeddings)
        # Increasing k to 3 so the AI sees more context for complex multi-faults
        return vectorstore.as_retriever(search_kwargs={"k": 3})

    def resolve_crisis(self, crisis_packet):
        print(f"\n[AGENT] Initiating ReAct Loop for "
              f"{crisis_packet.get('crisis_id')}...")

        # RAG — retrieve relevant runbooks
        print("[AGENT] Searching runbooks for similar past incidents...")
        # We pass the fault signals to the retriever to find the best-matching runbooks
        context_docs = self.retriever.invoke(json.dumps(crisis_packet['fault_signals'], indent=2))
        runbook_context = "\n".join([doc.page_content for doc in context_docs])
        print(f"[AGENT] Found Runbook:\n{runbook_context}\n")

        # ─── DYNAMIC TERMINATION LOGIC ───
        # We look at exactly what triggered the alarm and set a strict completion goal
        goals = []
        for signal in crisis_packet.get('fault_signals', []):
            det = signal.get("detector", "")
            if "schema_entropy" in det:
                goals.append("spo2 NULLs are fixed")
            elif "zscore" in det or "ks_test" in det:
                goals.append("heart_rate/BP outliers are imputed")
            elif "warehouse_monitor" in det or "latency" in det.lower():
                goals.append("the Discord webhook is successfully fired")
            elif "security" in det:
                goals.append("attacker records are completely purged")
        
        # Fallback just in case
        if not goals:
            goals.append("all signals are resolved")
            
        dynamic_stop_condition = " AND ".join(goals)

        # ─── UPDATED PROMPT CONSTRUCTION ───
        # We wrap the JSON in a "Checklist" instruction so the AI knows it's a multi-step job
        packet_str = (
            f"=== MANDATORY SRE CHECKLIST ===\n"
            f"1. ANALYZE DETECTIONS: {json.dumps(crisis_packet.get('fault_signals'), indent=2)}\n"
            f"2. REFERENCE RUNBOOKS:\n{runbook_context}\n\n"
            f"INSTRUCTION: You must address EVERY signal found in Step 1.\n"
            f"If a SECURITY fault is present (PT-ATTACKER-0000), you MUST call quarantine_patient FIRST.\n"
            f"Once a tool returns SUCCESS, re-check remaining faults.\n"
            f"Do not stop until {dynamic_stop_condition}.\n"
            f"- heart_rate > 200 fixed\n"
            f"- bp_systolic > 180 fixed\n"
            f"- spo2 < 70 fixed\n"
            f"- spo2 NULL values fixed\n"
            f"- PT-ATTACKER-0000 removed if present\n"        )

        # Run the ReAct loop
        try:
            # We send the formatted 'packet_str' into the {crisis_packet} variable in our prompt
            response = self.agent_executor.invoke({"crisis_packet": packet_str})
            return response
        except Exception as e:
            print(f"[AGENT FATAL ERROR] {e}")
            return None


if __name__ == "__main__":
    test_packet = {
        "crisis_id": "CRISIS-TEST-001",
        "fault_signals": [
            {"detector": "schema_entropy", "missing_fields": ["spo2"],
             "extra_fields": ["diagnosis_code"]},
             {"detector": "latency_monitor", "latency_ms": 2500}
        ],
    }
    agent = LARFReActAgent()
    agent.resolve_crisis(test_packet)