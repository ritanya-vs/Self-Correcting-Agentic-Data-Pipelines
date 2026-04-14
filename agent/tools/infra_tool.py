import subprocess
import re
from langchain.tools import tool

def _clean_cmd(command: str) -> str:
    """Remove markdown backticks and code fences the LLM adds."""
    command = re.sub(r"```bash\s*", "", command)
    command = re.sub(r"```\s*",     "", command)
    command = command.replace("`",  "")
    return command.strip()

@tool
def execute_bash_command(command: str) -> str:
    """
    Use this tool to execute system-level bash commands.
    Useful for restarting Kafka connectors or throttling producers.
    Input must be a plain bash command string. No backticks. No markdown.
    Example: curl -X POST http://localhost:8083/connectors/ehr-sink/tasks/0/restart
    """
    command = _clean_cmd(command)
    print(f"\n[INFRA TOOL] Executing Bash Command...")
    print(f"> {command}")

    try:
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True
        )
        if result.returncode == 0:
            output = result.stdout.strip()
            return f"SUCCESS: Command executed."
        else:
            error_msg = (f"FAILED: Exit code {result.returncode}. "
                        f"Error: {result.stderr.strip()}")
            print(f"[ERROR] {error_msg}")
            return error_msg
    except Exception as e:
        error_msg = f"FAILED: System execution error - {str(e)}"
        print(f"[ERROR] {error_msg}")
        return error_msg