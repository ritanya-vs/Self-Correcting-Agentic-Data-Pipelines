import sys
import os
import re
from langchain.tools import tool

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from simulator.database import get_connection

def _clean_sql(sql: str) -> str:
    """Remove markdown backticks and code fences the LLM adds."""
    sql = re.sub(r"```sql\s*", "", sql)
    sql = re.sub(r"```\s*",    "", sql)
    sql = sql.replace("`",     "")
    return sql.strip().rstrip(";")

@tool
def execute_sql_ddl(command: str) -> str:
    """
    Use this tool to execute SQL DDL commands (like ALTER TABLE, CREATE, DROP)
    to fix database schema drift issues in Databricks.
    Input must be plain SQL only. No backticks. No markdown. No code fences.
    Example: ALTER TABLE healthcare_db.ehr_stream ADD COLUMN spo2 DOUBLE
    """
    command = _clean_sql(command)
    print(f"\n[🔧 SCHEMA TOOL] Executing SQL...")
    print(f"> {command}")

    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(command)
        conn.commit()
        cursor.close()
        conn.close()
        return "SUCCESS: SQL command executed successfully. The schema has been updated."
    except Exception as e:
        error_msg = f"FAILED: Database error occurred - {str(e)}"
        print(f"[❌ ERROR] {error_msg}")
        return error_msg