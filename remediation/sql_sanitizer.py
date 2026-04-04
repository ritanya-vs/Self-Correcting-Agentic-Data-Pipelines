import re
from datetime import datetime, timezone

# Commands that are absolutely blocked — no exceptions
BLOCKED_PATTERNS = [
    (r"\bDROP\s+TABLE\b",        "DROP TABLE is permanently blocked"),
    (r"\bDROP\s+DATABASE\b",     "DROP DATABASE is permanently blocked"),
    (r"\bTRUNCATE\b",            "TRUNCATE is permanently blocked"),
    (r"\bDROP\s+SCHEMA\b",       "DROP SCHEMA is permanently blocked"),
    (r"\bDELETE\b(?!.*\bWHERE\b)","DELETE without WHERE clause is blocked"),
    (r"\bALTER\s+USER\b",        "ALTER USER is blocked"),
    (r"\bGRANT\b",               "GRANT is blocked"),
    (r"\bREVOKE\b",              "REVOKE is blocked"),
    (r"\bCREATE\s+USER\b",       "CREATE USER is blocked"),
    (r"\bEXEC\b",                "EXEC is blocked"),
    (r"\bXP_\w+",                "Extended procedures are blocked"),
]

# Commands that are allowed
ALLOWED_PREFIXES = [
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",   # only with WHERE — checked above
    "ALTER TABLE",
    "CREATE TABLE",
    "DESCRIBE",
    "SHOW",
]

def sanitize(sql: str) -> str:
    """
    Validates a SQL string against the security whitelist.
    Returns the SQL if safe, raises ValueError if blocked.
    """
    if not sql or not sql.strip():
        raise ValueError("Empty SQL command rejected")

    upper = sql.upper().strip()

    # Check every blocked pattern
    for pattern, reason in BLOCKED_PATTERNS:
        if re.search(pattern, upper, re.IGNORECASE):
            raise ValueError(f"BLOCKED: {reason}\nSQL: {sql}")

    return sql.strip()

def is_safe(sql: str) -> bool:
    """Returns True if SQL is safe, False if blocked. Never raises."""
    try:
        sanitize(sql)
        return True
    except ValueError:
        return False

def audit_sql(sql: str, agent_id: str = "LARF-AGENT") -> dict:
    """
    Sanitizes SQL and returns a full audit record.
    Use this instead of sanitize() when you need a log entry.
    """
    result = {
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "agent_id":   agent_id,
        "sql":        sql,
        "approved":   False,
        "reason":     None,
    }

    try:
        sanitize(sql)
        result["approved"] = True
        result["reason"]   = "Passed all security checks"
    except ValueError as e:
        result["approved"] = False
        result["reason"]   = str(e)

    return result


if __name__ == "__main__":
    print("=== SQL Sanitizer Tests ===\n")

    test_cases = [
        # (sql, should_pass)
        ("SELECT * FROM healthcare_db.ehr_stream WHERE patient_id = 'PT-001'", True),
        ("ALTER TABLE healthcare_db.ehr_stream ADD COLUMN respiratory_rate DOUBLE", True),
        ("DELETE FROM healthcare_db.ehr_stream WHERE patient_id = 'PT-ATTACKER-0000'", True),
        ("DROP TABLE healthcare_db.ehr_stream", False),
        ("TRUNCATE healthcare_db.ehr_stream", False),
        ("DELETE FROM healthcare_db.ehr_stream", False),   # no WHERE clause
        ("ALTER USER admin IDENTIFIED BY 'hacked'", False),
        ("GRANT ALL PRIVILEGES TO hacker", False),
    ]

    passed = 0
    for sql, should_pass in test_cases:
        safe = is_safe(sql)
        status = "✅" if safe == should_pass else "❌"
        label  = "ALLOWED" if safe else "BLOCKED"
        if safe == should_pass:
            passed += 1
        print(f"{status} {label}: {sql[:65]}...")

    print(f"\n{passed}/{len(test_cases)} tests passed")