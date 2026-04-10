import os
import json
from dotenv import load_dotenv
from langchain_ollama import ChatOllama
from langchain_classic.agents import AgentExecutor, create_react_agent
from langchain_community.vectorstores import Chroma
from langchain_huggingface import HuggingFaceEmbeddings

from tools.schema_tool   import execute_sql_ddl
from tools.infra_tool    import execute_bash_command
from tools.sql_tool      import execute_sql_dml
from tools.security_tool import quarantine_patient
from prompts             import get_react_prompt

load_dotenv()

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL",
    "https://iva-unministrative-unhesitatively.ngrok-free.dev")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL", "qwen2.5")

class LARFReActAgent:
    def __init__(self):
        print(f"[AGENT] Connecting to Ollama at {OLLAMA_BASE_URL}")
        print(f"[AGENT] Using model: {OLLAMA_MODEL}")

        self.llm = ChatOllama(
            base_url    = OLLAMA_BASE_URL,
            model       = OLLAMA_MODEL,
            temperature = 0.1,
        )

        self.tools = [
            execute_sql_ddl,
            execute_bash_command,
            execute_sql_dml,
            quarantine_patient,
        ]

        self.retriever = self._setup_chromadb()
        self.prompt    = get_react_prompt()
        self.agent     = create_react_agent(self.llm, self.tools, self.prompt)

        self.agent_executor = AgentExecutor(
            agent                 = self.agent,
            tools                 = self.tools,
            verbose               = True,
            handle_parsing_errors = True,
            max_iterations        = 8,
        )

    def _setup_chromadb(self):
        print("[INFO] Initializing ChromaDB Runbook Retriever...")
        embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

        runbooks = [
            # ── Fault 1: Schema ───────────────────────────────────
            "RUNBOOK A: If schema_entropy detector flags missing fields like "
            "'spo2' or extra unknown fields like 'diagnosis_code', the upstream "
            "producer changed its schema without notice. "
            "Action: Use execute_sql_ddl with: "
            "ALTER TABLE healthcare_db.ehr_stream ADD COLUMN spo2 DOUBLE",

            # ── Fault 2: Data Quality ─────────────────────────────
            "RUNBOOK B: If zscore or ks_test detects impossible vital signs "
            "such as heart_rate above 200 or spo2 below 70, it is a data "
            "quality fault from a malfunctioning sensor. "
            "Look for the rogue patient_id in the anomalous_fields section. "
            "Action: Use quarantine_patient with the rogue patient_id, "
            "then use execute_sql_dml with: "
            "DELETE FROM healthcare_db.ehr_stream WHERE heart_rate > 200",

            # ── Fault 3: Performance ──────────────────────────────
            "RUNBOOK C: If zscore_latency detector flags DB latency above 3 "
            "seconds, a heavy runaway query is choking the Databricks warehouse. "
            "Action: Use execute_bash_command with: "
            "curl -X GET http://localhost:8083/connectors to check status, "
            "then scale up the warehouse or kill the runaway query.",

            # ── Fault 4: Security ─────────────────────────────────
            "RUNBOOK D: If zscore_security detector flags a single patient_id "
            "appearing in more than 20 percent of recent events, it is a "
            "brute-force data exfiltration attempt. "
            "The rogue_patient_id is explicitly in the fault_signals. "
            "Action: Use quarantine_patient tool with exactly that "
            "rogue_patient_id string value from the crisis packet.",

            # ── Fault 5: Stall ────────────────────────────────────
            "RUNBOOK E: If connector_health detector shows connector state "
            "is PAUSED or FAILED, the Kafka Connect sink is down and events "
            "are building up in the topic unprocessed. "
            "Action: Use execute_bash_command with: "
            "curl -X PUT http://localhost:8083/connectors/databricks-sink/resume",

            # ── Fallback ──────────────────────────────────────────
            "RUNBOOK F: If multiple detectors fire simultaneously (CRITICAL "
            "severity), address schema faults first, then data quality, "
            "then infrastructure. Always call quarantine_patient before "
            "DELETE queries when a rogue_patient_id is present.",
        ]

        vectorstore = Chroma.from_texts(texts=runbooks, embedding=embeddings)
        return vectorstore.as_retriever(search_kwargs={"k": 2})

    def resolve_crisis(self, crisis_packet):
        print(f"\n[AGENT] Initiating ReAct Loop for "
              f"{crisis_packet.get('crisis_id')}...")

        print("[AGENT] Searching runbooks for similar past incidents...")
        context_docs    = self.retriever.invoke(
            str(crisis_packet['fault_signals'])
        )
        runbook_context = "\n".join([d.page_content for d in context_docs])
        print(f"[AGENT] Found Runbook:\n{runbook_context}\n")

        packet_str = (
            json.dumps(crisis_packet, indent=2)
            + f"\n\nHistorical Runbook Context:\n{runbook_context}"
        )

        try:
            response = self.agent_executor.invoke(
                {"crisis_packet": packet_str}
            )
            return response
        except Exception as e:
            print(f"[AGENT FATAL ERROR] {e}")
            return None


if __name__ == "__main__":
    test_packet = {
        "crisis_id": "CRISIS-TEST-001",
        "fault_signals": [
            {"detector": "schema_entropy",
             "missing_fields": ["spo2"],
             "extra_fields":   ["diagnosis_code"]}
        ],
    }
    agent = LARFReActAgent()
    agent.resolve_crisis(test_packet)