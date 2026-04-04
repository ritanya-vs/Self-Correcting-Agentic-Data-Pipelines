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
def execute_sql_dml(query: str) -> str:
    """
    Use this tool to execute SQL DML commands (UPDATE, DELETE, INSERT).
    Useful for fixing data quality issues or dropping corrupted rows.
    Input must be plain SQL only. No backticks. No markdown. No code fences.
    Example: DELETE FROM healthcare_db.ehr_stream WHERE spo2 IS NULL
    """
    query = _clean_sql(query)
    print(f"\n[🛠️ SQL TOOL] Executing DML Query...")
    print(f"> {query}")

    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        rows = cursor.rowcount
        cursor.close()
        conn.close()
        return f"SUCCESS: Query executed. {rows} rows affected."
    except Exception as e:
        error_msg = f"FAILED: Database error occurred - {str(e)}"
        print(f"[❌ ERROR] {error_msg}")
        return error_msg