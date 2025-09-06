# Airflow AI Alert – Minimal Demo (Single File)

This demo script fetches failed DAG runs from an Airflow 2.2.0 instance and asks an Agno agent (backed by your LLM) to propose concise fixes. No database, Redis, webhooks, or UI — just console output.

## What it does
- Calls Airflow REST API (basic auth)
- Lists recent DAG runs and filters the failed ones
- Optionally pulls short log snippets for failed tasks
- Sends a compact summary to an Agno agent (provider-selectable)
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

### Common (Airflow)
PowerShell (Windows):
```powershell
$env:AIRFLOW_BASE_URL="http://localhost:8080"
$env:AIRFLOW_USERNAME="admin"
$env:AIRFLOW_PASSWORD="admin"
```
Bash (macOS/Linux):
```bash
export AIRFLOW_BASE_URL="http://localhost:8080"
export AIRFLOW_USERNAME="admin"
export AIRFLOW_PASSWORD="admin"
```

### Choose provider: OpenAI-like or Ollama

- `LLM_PROVIDER` accepts `openailike` (default) or `ollama`.

#### OpenAI-like (custom cluster or compatible API)
PowerShell:
```powershell
$env:LLM_PROVIDER="openailike"
$env:LLM_BASE_URL="https://your-llm-cluster.example.com/v1"
$env:LLM_API_KEY="YOUR_KEY"
$env:LLM_MODEL="gpt-4o-mini"  # or your server's model id
```
Bash:
```bash
export LLM_PROVIDER="openailike"
export LLM_BASE_URL="https://your-llm-cluster.example.com/v1"
export LLM_API_KEY="YOUR_KEY"
export LLM_MODEL="gpt-4o-mini"  # or your server's model id
```

#### Ollama (local/remote Ollama server)
PowerShell:
```powershell
$env:LLM_PROVIDER="ollama"
$env:OLLAMA_BASE_URL="http://localhost:11434"
$env:LLM_MODEL="llama3.1"  # e.g., llama3.1, qwen2.5, mistral
```
Bash:
```bash
export LLM_PROVIDER="ollama"
export OLLAMA_BASE_URL="http://localhost:11434"
export LLM_MODEL="llama3.1"  # e.g., llama3.1, qwen2.5, mistral
```

Notes:
- OpenAI-like mode requires both `LLM_BASE_URL` and `LLM_API_KEY`.
- Ollama mode uses `OLLAMA_BASE_URL` and does not require an API key.

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
- LLM auth errors: verify provider-specific variables.

## Next steps (optional)
- Add Slack/Email notifications
- Persist results (DB)
- Serve as an API or UI
- Use MCP for Airflow instead of direct REST
