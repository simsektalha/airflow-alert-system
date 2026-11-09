# Configuration

Non-secret settings live in one Airflow Variable, `v_callback_fetch_failed_task` (JSON). Secrets live in Airflow Connections; the `failure_analyzer` task reads both and fills the secret keys from the Connections.

## Connections

| Conn id | Fields used | Fills |
|---|---|---|
| `failure_responder_airflow_api` | host = Airflow API base URL (e.g. `https://airflow.example.com`), login, password | `base_url`; the FAB user gets a JWT from `/auth/token`. The user needs to read task instances and logs and to trigger `failure_analyzer`. **Required.** |
| `failure_responder_llm` | password = API key | `llm.api_key` and `knowledge.embedder.api_key` |
| `failure_responder_kb_db` | Postgres connection (host, port, schema = database, login, password) | `knowledge.vector_db.db_url` |
| `failure_responder_confluence` | login, password | `confluence.auth` (only if `confluence` is in the Variable) |
| `failure_responder_teams` | host = full webhook URL | `output.teams_webhook` |

A missing optional Connection leaves that part unconfigured (no LLM → fallback alert, no DB → no incident records), the same as a missing key. Secret keys in the Variable are overwritten when the Connection exists; keep them out of the Variable.

## Full example

```json
{
  "tls": { "verify": "/etc/ssl/certs/internal-ca.pem", "insecure_global_patch": false },
  "timeouts": { "connect": 10, "total": 30 },
  "pagination": { "max_bytes": 5000000 },

  "llm": {
    "base_url": "https://llm-gateway.example.com/v1",
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
      "id": "bge-m3",
      "timeout": 30,
      "verify": true
    },
    "table": "airflow_kb_rows",
    "top_k": 5
  },

  "confluence": {
    "base_url": "https://confluence.example.com",
    "page_ids": ["123456"],
    "page_url": "https://confluence.example.com/x/AbCd"
  },

  "dedup": { "cooldown_minutes": 30 },

  "output": {
    "method": "teams",
    "teams_verify_ssl": true
  }
}
```

## Airflow API

The base URL and FAB credentials come from `failure_responder_airflow_api`; the URL also builds the "Log'a Git" link on the alert card.

| Key | Default | Notes |
|---|---|---|
| `tls.verify` | `true` | `true`, `false`, or a CA bundle path. Applies to the Airflow API (callback and analyzer) and Confluence, and is the default for the LLM and embedder. |
| `tls.insecure_global_patch` | `false` | Disables TLS verification for every client in the responder. Last resort; prefer a CA bundle path. |
| `timeouts.connect` / `total` | `10` / `30` | Seconds, for Airflow API calls. |
| `pagination.max_bytes` | `5000000` | Task log is truncated to this many characters after fetching. |

## LLM

Any OpenAI-compatible chat completions endpoint: vLLM, LiteLLM, Ollama (`/v1`), or OpenAI.

| Key | Default | Notes |
|---|---|---|
| `llm.base_url` | (none) | If missing, no analysis is made and every failure gets a fallback alert. Old Ollama-style `llm.host` still works (`/v1` is appended). |
| `llm.api_key` | `"none"` | From `failure_responder_llm`. Sent as a Bearer token. |
| `llm.model` | `llama3.1` | |
| `llm.temperature` | `0.1` | |
| `llm.max_tokens` | `4000` | Lower values can truncate the JSON analysis. |
| `llm.timeout` | `120` | Seconds. |
| `llm.response_format` | `json_schema` | `json_schema` sends the analysis schema (vLLM guided decoding, OpenAI structured outputs). Use `json_object` or `none` if the gateway rejects it; the response parser tolerates fences and surrounding prose. |
| `llm.verify` | `tls.verify` | Override when the gateway's certificate is signed by a different CA than Airflow's. |
| `llm.tools` | `true` | Before the analysis the model may read Airflow context through tools (below). Set `false` for a gateway without tool calling; a tool-calling error also falls back to the single call on its own. |
| `llm.max_tool_rounds` | `4` | Tool-calling rounds before the final structured call. |

`llm.driver` from older configs is ignored.

**Context tools.** All are read-only `/api/v2` calls with the analyzer's JWT, scoped to the failure being analyzed (they take no arguments, so the model cannot read other DAGs). Each output is redacted and cut to 8,000 characters.

| Tool | Returns |
|---|---|
| `get_task_instance` | operator, state, tries, timing, host, pool, queue, executor (no rendered fields or executor config: they can hold secrets) |
| `get_dag_source` | the DAG file, redacted |
| `get_last_successful_run` | the last `success` run of the DAG, with this task's duration and tries in it |
| `get_other_failures_in_run` | other `failed` / `upstream_failed` tasks of the same run (ids only) |

The model calls them in tool rounds; what it read is then put into the final call as plain text, and that call uses `llm.response_format`. Similar incident records are not a tool: they are always in the prompt as `K1..Kn`. The FAB user needs read access to DAG code (`DAG Code.can_read`) for `get_dag_source`; without it the tool returns an error and the analysis goes on.

## Knowledge base

**Incident records** from Confluence, stored in Postgres with the `pgvector` extension. The table is created on first use (`CREATE EXTENSION IF NOT EXISTS vector` needs the extension to be installed, or a role allowed to create it).

| Key | Default | Notes |
|---|---|---|
| `knowledge.vector_db.db_url` | (none) | From `failure_responder_kb_db`. If missing, the analysis runs without incident records. `postgresql+psycopg://` URLs from older configs are accepted. |
| `knowledge.table` | `airflow_kb_rows` | One row per incident record. |
| `knowledge.top_k` | `5` | Incident records put in the prompt. |
| `knowledge.embedder.base_url` | (none) | OpenAI-compatible `/embeddings` endpoint, e.g. an embedding model served by vLLM on a GPU cluster, LiteLLM, or Ollama. Ollama-style `embedder.host` still works. |
| `knowledge.embedder.id` | `nomic-embed-text` | Embedding model. Changing it re-embeds every incident record on the next `kb_ingest` run. Trigger `kb_ingest` right after the change: until it runs, stored vectors are from the old model (dimension error, or meaningless matches if the dimension is the same). |
| `knowledge.embedder.api_key` | `"none"` | From `failure_responder_llm`. |
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
| `confluence.auth.username` / `password` | (none) | From `failure_responder_confluence`. |
| `confluence.page_url` | (none) | Shown as the "📄 Confluence" button on the alert card; the button is hidden when unset. |

Sync runs in the `kb_ingest` DAG (`@daily`, `retries=2`, one run at a time), not per failure. When nothing changed, the cost is one page fetch and one query per page. If a page parses to zero rows (layout change, partial page), the stored incident records are kept. Trigger `kb_ingest` by hand after deploy or after a big page edit. A failed sync leaves the previous incident records in place; alerts keep working.

## Output

| Key | Default | Notes |
|---|---|---|
| `output.method` | `stdout` | `stdout`, `file`, or `teams`. |
| `output.file_path` | `/tmp/failed_task_log.json` | For `file`. |
| `output.teams_webhook` | (none) | From `failure_responder_teams`. Teams Workflows (Power Automate) webhook URL. Falls back to stdout if missing. |
| `output.teams_verify_ssl` | `true` | Set `false` only behind a TLS-inspecting proxy without its CA. |

## Dedup

One alert card per **fingerprint** within a cooldown. The fingerprint is global (the DAG is not part of it), so one broken database that fails 40 tasks in different DAGs gives one alert card and one summary.

| Key | Default | Notes |
|---|---|---|
| `dedup.cooldown_minutes` | `30` | `0` turns dedup off. Dedup is also off without `failure_responder_kb_db`; its state is the `failure_dedup` table in the same database. |

- **Fingerprint**: exception type + message from the last error line of the redacted task log. In the message, UUIDs, timestamps, IPs (with port), hex ids (8+ chars) and other numbers are replaced, whitespace is collapsed and case is ignored. Quoted names and paths are kept, so `No module named 'x'` and `'y'` stay different. Without an exception type the whole line is used. A log with no error line is never deduplicated.
- The first failure of a fingerprint gets the normal analysis and opens the window. Later failures in the window get no LLM call and no card; they are counted (the first 20 are listed).
- After the window, one **dedup summary** card ("🔁 Aynı hata: N task daha etkilendi") is sent by the next analyzer run of any failure, or at the latest by the daily `kb_ingest` run. If the send fails, the counts are kept and the next run tries again.
- A broken dedup store never costs an alert: the failure is analyzed as if dedup were off.

## failure_analyzer DAG

The callback only triggers a run; the analysis runs as the `analyze` task of `failure_analyzer`, so it is visible in the Airflow UI.

- One run per failed try. Its run id is `fail__{dag_id}__{run_id}__{task_id}__{try}` (hashed tail above 250 characters). A repeated callback gets HTTP 409 and is skipped.
- `conf`: `{"dag_id", "run_id", "task_id", "try_number"}`. To re-run an analysis, clear the task or trigger the DAG with this conf.
- `retries=1`: in practice only an output (Teams) failure fails the task; fetch and LLM errors already end in a fallback alert.
- `max_active_runs=4` keeps a mass failure from flooding the LLM gateway; extra runs wait queued.
- No `on_failure_callback` on this DAG, so a failing analyzer never triggers another analyzer.
- `requirements.txt` must be installed in the worker image.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `[llm] tool calling failed` in the analyzer log | The gateway rejects `tools`. Analysis still runs as a single call; set `llm.tools: false` to skip the failed attempt. |
| Every alert is a fallback alert ("Otomatik analiz yapılamadı") | `llm.base_url` missing, gateway unreachable, or it rejects `json_schema` → set `llm.response_format`. The responder log shows the exception. |
| `CERTIFICATE_VERIFY_FAILED` on LLM or embedder | `tls.verify` points at the Airflow CA → set `llm.verify` / `knowledge.embedder.verify`. |
| "Kullanılan Dokümanlar" is always empty | No `knowledge.vector_db.db_url`, empty table, or no incident record was relevant. Check the `[kb] retrieved N docs (distances=...)` log line. |
| `different vector dimensions` | Rows embedded by another model in the same table. Fixed on the next sync; if the page now parses to zero rows, clear the table. |
| Callback logs `token request failed` | Wrong host or credentials in `failure_responder_airflow_api`, or `/auth/token` is not reachable from the worker. |
| Callback logs `[trigger] status=403` | The FAB user may not trigger `failure_analyzer`. |
| No `failure_analyzer` runs appear | `dags/failure_analyzer.py` not deployed or paused; check the callback's `[trigger]` log line in the failed task's log. |
