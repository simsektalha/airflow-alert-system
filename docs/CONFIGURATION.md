# Configuration

All configuration lives in one Airflow Variable, `v_callback_fetch_failed_task` (JSON). The callback reads it, removes `auth`, and hands the rest to the responder over stdin.

## Full example

```json
{
  "base_url": "https://airflow.example.com",
  "auth": { "basic": { "username": "svc_alerts", "password": "..." } },
  "tls": { "verify": "/etc/ssl/certs/internal-ca.pem", "insecure_global_patch": false },
  "timeouts": { "connect": 10, "total": 30 },
  "pagination": { "max_bytes": 5000000 },

  "llm": {
    "base_url": "https://llm-gateway.example.com/v1",
    "api_key": "...",
    "model": "qwen2.5-72b-instruct",
    "temperature": 0.1,
    "max_tokens": 4000,
    "timeout": 120,
    "response_format": "json_schema",
    "verify": true
  },

  "knowledge": {
    "embedder": {
      "base_url": "https://llm-gateway.example.com/v1",
      "api_key": "...",
      "id": "bge-m3",
      "timeout": 30,
      "verify": true
    },
    "vector_db": { "db_url": "postgresql://kb:...@pg.example.com:5432/kb" },
    "table": "airflow_kb_rows",
    "top_k": 5
  },

  "confluence": {
    "base_url": "https://confluence.example.com",
    "page_ids": ["123456"],
    "page_url": "https://confluence.example.com/x/AbCd",
    "auth": { "username": "svc_alerts", "password": "..." }
  },

  "output": {
    "method": "teams",
    "teams_webhook": "https://...powerplatform.com/...",
    "teams_verify_ssl": true
  },

  "script_path": "/opt/airflow/dags/airflow_failure_responder/airflow_failure_responder.py",
  "invoke": { "python": "/opt/venvs/responder/bin/python", "mode": "detach", "timeout_sec": 30 }
}
```

## Airflow API

| Key | Default | Notes |
|---|---|---|
| `base_url` | (required) | Airflow webserver URL. Also builds the "Log'a Git" link on the alert card. `api_base_url` is accepted as an alias. |
| `auth.basic.username` / `password` | (required) | FAB user that may read task instances and logs. Used only by the callback to get a JWT from `/auth/token`. Never passed to the responder. |
| `tls.verify` | `true` | `true`, `false`, or a CA bundle path. Applies to the Airflow API and Confluence, and is the default for the LLM and embedder. |
| `tls.insecure_global_patch` | `false` | Disables TLS verification for every client in the responder. Last resort; prefer a CA bundle path. |
| `timeouts.connect` / `total` | `10` / `30` | Seconds, for Airflow API calls. |
| `pagination.max_bytes` | `5000000` | Task log is truncated to this many characters after fetching. |

## LLM

Any OpenAI-compatible chat completions endpoint: vLLM, LiteLLM, Ollama (`/v1`), or OpenAI.

| Key | Default | Notes |
|---|---|---|
| `llm.base_url` | (none) | If missing, no analysis is made and every failure gets a fallback alert. Old Ollama-style `llm.host` still works (`/v1` is appended). |
| `llm.api_key` | `"none"` | Sent as a Bearer token. |
| `llm.model` | `llama3.1` | |
| `llm.temperature` | `0.1` | |
| `llm.max_tokens` | `4000` | Lower values can truncate the JSON analysis. |
| `llm.timeout` | `120` | Seconds. |
| `llm.response_format` | `json_schema` | `json_schema` sends the analysis schema (vLLM guided decoding, OpenAI structured outputs). Use `json_object` or `none` if the gateway rejects it; the response parser tolerates fences and surrounding prose. |
| `llm.verify` | `tls.verify` | Override when the gateway's certificate is signed by a different CA than Airflow's. |

`llm.driver` from older configs is ignored.

## Knowledge base

**Incident records** from Confluence, stored in Postgres with the `pgvector` extension. The table is created on first use (`CREATE EXTENSION IF NOT EXISTS vector` needs the extension to be installed, or a role allowed to create it).

| Key | Default | Notes |
|---|---|---|
| `knowledge.vector_db.db_url` | (none) | Postgres URL. If missing, the analysis runs without incident records. `postgresql+psycopg://` URLs from older configs are accepted. |
| `knowledge.table` | `airflow_kb_rows` | One row per incident record. |
| `knowledge.top_k` | `5` | Incident records put in the prompt. |
| `knowledge.embedder.base_url` | (none) | OpenAI-compatible `/embeddings` endpoint, e.g. an embedding model served by vLLM on a GPU cluster, LiteLLM, or Ollama. Ollama-style `embedder.host` still works. |
| `knowledge.embedder.id` | `nomic-embed-text` | Embedding model. Changing it re-embeds every incident record on the next sync. |
| `knowledge.embedder.api_key` | `"none"` | |
| `knowledge.embedder.timeout` | `30` | Seconds. |
| `knowledge.embedder.verify` | `tls.verify` | Same as `llm.verify`. |

The query sent to the embedder is the last error line plus the last 3,000 characters of the redacted task log.

## Confluence

The responder reads the **first table** of each page (Confluence Data Center REST API, `body.storage`). Headers are matched case- and space-insensitively:

| Field | Accepted headers |
|---|---|
| Date | `Tarih`, `Date` |
| Environment | `Ortam`, `Environment` |
| Service | `Servis / Sistem`, `Servis/Sistem`, `Service / System` |
| Owner | `Sorumlu`, `Owner` |
| Summary | `Hata / Olay Özeti`, `Hata/Olay Özeti`, `Incident Summary` |
| Resolution | `Çözüm / Not`, `Çözüm/Not`, `Resolution / Note` |
| Status | `Durum`, `Status` |
| Link | `Link`, `URL` |

| Key | Default | Notes |
|---|---|---|
| `confluence.base_url` | (none) | |
| `confluence.page_ids` | (none) | List of page IDs. A single `confluence.page_id` is still accepted. |
| `confluence.auth.username` / `password` | (none) | |
| `confluence.page_url` | (none) | Shown as the "📄 Confluence" button on the alert card; the button is hidden when unset. |

Sync runs before each analysis. When nothing changed, the cost is one page fetch and one query. If a page parses to zero rows (layout change, partial page), the stored incident records are kept.

## Output

| Key | Default | Notes |
|---|---|---|
| `output.method` | `stdout` | `stdout`, `file`, or `teams`. |
| `output.file_path` | `/tmp/failed_task_log.json` | For `file`. |
| `output.teams_webhook` | (none) | Teams Workflows (Power Automate) webhook URL. Falls back to stdout if missing. |
| `output.teams_verify_ssl` | `true` | Set `false` only behind a TLS-inspecting proxy without its CA. |

## Running the responder

| Key | Default | Notes |
|---|---|---|
| `script_path` | `/usr/local/airflow/dags/airflow_failure_responder/airflow_failure_responder.py` | |
| `invoke.python` | the worker's interpreter | Interpreter with `requirements.txt` installed. |
| `invoke.mode` | `detach` | `detach` returns immediately. `run` blocks the callback until the responder exits or times out. |
| `invoke.timeout_sec` | `30` | Only for `run`. Too short for a typical LLM call. |

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Every alert is a fallback alert ("Otomatik analiz yapılamadı") | `llm.base_url` missing, gateway unreachable, or it rejects `json_schema` → set `llm.response_format`. The responder log shows the exception. |
| `CERTIFICATE_VERIFY_FAILED` on LLM or embedder | `tls.verify` points at the Airflow CA → set `llm.verify` / `knowledge.embedder.verify`. |
| "Kullanılan Dokümanlar" is always empty | No `knowledge.vector_db.db_url`, empty table, or no incident record was relevant. Check the `[kb] retrieved N docs (distances=...)` log line. |
| `different vector dimensions` | Rows embedded by another model in the same table. Fixed on the next sync; if the page now parses to zero rows, clear the table. |
| Callback logs `token request failed` | Wrong `base_url` or FAB credentials, or `/auth/token` is not reachable from the worker. |
