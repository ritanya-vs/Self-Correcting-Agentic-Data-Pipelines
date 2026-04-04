import os
import json
from dotenv import load_dotenv
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
            "RUNBOOK A: If Kafka consumer lag > 100 and CPU is normal, "
            "the downstream DB is choking. Action: Use execute_bash_command "
            "to throttle the producer rate.",

            "RUNBOOK B: If schema entropy triggers missing 'spo2' field or "
            "unknown field 'diagnosis_code' appears, the producer updated "
            "schemas without notice. Action: Use execute_sql_ddl to run "
            "ALTER TABLE healthcare_db.ehr_stream ADD COLUMN spo2 DOUBLE.",

            "RUNBOOK C: If memory > 80% and DB latency > 2s, Databricks "
            "warehouse needs scaling. Action: Use execute_bash_command to "
            "scale the warehouse. Do not drop tables.",

            "RUNBOOK D: If zscore detects impossible heart rate (>200) or "
            "spo2 (<70) for a specific patient_id, it is a data quality fault. "
            "Action: Use quarantine_patient tool with the rogue patient_id.",

            "RUNBOOK E: If 50+ events arrive from the same patient_id within "
            "seconds, it is a security/brute-force fault. "
            "Action: Use quarantine_patient to purge those records.",

            "RUNBOOK F: If no events are received for 30+ seconds, "
            "the pipeline is stalled. Action: Use execute_bash_command "
            "to restart the Kafka Connect task.",
        ]

        vectorstore = Chroma.from_texts(texts=runbooks, embedding=embeddings)
        return vectorstore.as_retriever(search_kwargs={"k": 2})

    def resolve_crisis(self, crisis_packet):
        print(f"\n[AGENT] Initiating ReAct Loop for "
              f"{crisis_packet.get('crisis_id')}...")

        # RAG — retrieve relevant runbooks
        print("[AGENT] Searching runbooks for similar past incidents...")
        context_docs    = self.retriever.invoke(str(crisis_packet['fault_signals']))
        runbook_context = "\n".join([doc.page_content for doc in context_docs])
        print(f"[AGENT] Found Runbook:\n{runbook_context}\n")

        # Build prompt input
        packet_str = (
            json.dumps(crisis_packet, indent=2)
            + f"\n\nHistorical Runbook Context:\n{runbook_context}"
        )

        # Run the ReAct loop
        try:
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
             "extra_fields": ["diagnosis_code"]}
        ],
    }
    agent = LARFReActAgent()
    agent.resolve_crisis(test_packet)