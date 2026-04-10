from langchain_core.prompts import PromptTemplate

# 1. System Topology & Context
PIPELINE_CONTEXT = """
SYSTEM TOPOLOGY:
- Source: Python IoT/EHR Producer
- Message Broker: Kafka (Topic: ehr-stream)
- Sink: Kafka Connect (databricks-sink)
- Target: Databricks SQL Warehouse (Table: healthcare_db.ehr_stream)

NORMAL BASELINES:
- Expected Schema: patient_id, heart_rate, bp_systolic, bp_diastolic, spo2, temperature_c, ward, timestamp
- Clinical Normal: Heart Rate (60-100), SpO2 (95-100)
"""

# 2. The ReAct Template
# CRITICAL: ReAct agents REQUIRE {tools}, {tool_names}, and {agent_scratchpad}.
# Your custom variable {crisis_packet} is included here as an input.
# We remove semicolons, backticks, and markdown to prevent SQL parsing errors.
REACT_COT_TEMPLATE = """
You are an Autonomous Site Reliability Engineer and Clinical Data Guardian.
You must diagnose and fix EVERY fault signal described in the Crisis Packet below.

=== MANDATORY REMEDIATION RULES ===
1. ADDRESS ALL FAULTS: If the packet contains multiple signals (e.g., Schema AND Z-Score), you MUST call tools to fix BOTH before finishing.
2. SCHEMA FAULTS: Use 'execute_sql_ddl' to fix structure or 'execute_sql_dml' to clean NULLs/bad records.
3. CLINICAL FAULTS (Z-Score): If 'zscore' detects impossible vitals, you MUST call 'quarantine_patient' for the affected patient IDs.
4. FORMAT: Only call ONE tool per Action step. Read the Observation before taking the next step.
5. SQL: Plain text only. NO backticks, NO markdown, NO semicolons.
===========================================

You have access to the following tools:
{tools}

Use the following format:

Thought: Do I need to use a tool? Yes
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Context:
{pipeline_context}

Crisis Packet:
{crisis_packet}

Thought: {agent_scratchpad}
"""

def get_react_prompt():
    # We explicitly define the input_variables that create_react_agent expects.
    # crisis_packet is included here so it can be passed during agent.invoke().
    return PromptTemplate(
        template          = REACT_COT_TEMPLATE,
        input_variables   = ["tools", "tool_names", "agent_scratchpad", "crisis_packet"],
        partial_variables = {"pipeline_context": PIPELINE_CONTEXT}
    )