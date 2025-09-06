# Airflow AI Alert – Minimal Demo (Single File)

This demo script fetches failed DAG runs from an Airflow 2.2.0 instance and asks an Agno agent (backed by your external LLM API) to propose concise fixes. No database, Redis, webhooks, or UI — just console output.

## What it does
- Calls Airflow REST API (basic auth)
- Lists recent DAG runs and filters the failed ones
- Optionally pulls short log snippets for failed tasks
- Sends a compact summary to an Agno agent (your external LLM via `LLM_BASE_URL`)
- Prints the agent’s root cause + fix steps to the console

## File
- `airflow_ai_demo.py`

## Requirements
- Python 3.9+
- Airflow 2.2.0 API accessible (e.g., `http://localhost:8080`)
- Minimal Python deps: `agno`, `httpx`

Install (recommended in a virtualenv):
```bash
pip install agno httpx
```

## Environment variables
Set these before running the script.

PowerShell (Windows):
```powershell
$env:AIRFLOW_BASE_URL="http://localhost:8080"
$env:AIRFLOW_USERNAME="admin"
$env:AIRFLOW_PASSWORD="admin"

$env:LLM_BASE_URL="https://your-llm-cluster.example.com/v1"
$env:LLM_API_KEY="YOUR_KEY"
# Optional
$env:LLM_MODEL="gpt-4o-mini"
```

Bash (macOS/Linux):
```bash
export AIRFLOW_BASE_URL="http://localhost:8080"
export AIRFLOW_USERNAME="admin"
export AIRFLOW_PASSWORD="admin"

export LLM_BASE_URL="https://your-llm-cluster.example.com/v1"
export LLM_API_KEY="YOUR_KEY"
# Optional
export LLM_MODEL="gpt-4o-mini"
```

Notes:
- `LLM_BASE_URL` should be OpenAI-compatible if you’re routing to a custom cluster.
- The script uses basic auth for Airflow. Adjust if your deployment differs.

## Run
From the project root:
```bash
python airflow_ai_alert_system/airflow_ai_demo.py --max-dags 10 --max-runs 5 --include-logs
```
Flags:
- `--max-dags`   Max DAGs to scan (default 20)
- `--max-runs`   Max recent runs per DAG (default 5)
- `--include-logs`  Also fetch small log snippets for failed tasks

## Example output
```
Airflow health: healthy

===== Prompt to Agent =====
# Airflow Failed DAG Runs
...
===========================

===== Agent Response =====
Root cause: ...
Fix steps:
1) ...
2) ...
Prevention tips:
- ...
==========================
```

## Troubleshooting
- 401/403 from Airflow: check `AIRFLOW_USERNAME`/`AIRFLOW_PASSWORD`, and API access.
- Connection error: verify `AIRFLOW_BASE_URL` and that Airflow is running.
- No failed runs found: deliberately trigger a failure to test the flow.
- LLM auth errors: verify `LLM_BASE_URL` and `LLM_API_KEY`.

## Next steps (optional)
- Add Slack/Email notifications
- Persist results (DB)
- Serve as an API or UI
- Use MCP for Airflow instead of direct REST
