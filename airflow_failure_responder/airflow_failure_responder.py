# airflow_failure_responder.py
"""
Failure analysis, called by the failure_analyzer DAG task (dags/failure_analyzer.py).

  - The task gets a JWT and the full log_url, and fills secrets from Airflow Connections (apply_connections).
  - Fetch log -> crop/redact -> pgvector retrieval -> one LLM call -> output.
  - Confluence sync runs in the daily kb_ingest DAG, not here.
  - If fetching or LLM analysis fails, a fallback card (raw error line) is still sent.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, Optional
from urllib.parse import quote

from logging_cfg import configure_logging, _mask
from airflow_log_client import fetch_log_using_token, _derive_verify
from log_processing import crop_log_window, process_logs, last_error_line
from knowledge import retrieve_knowledge
from llm_agent import build_llm_client, ask_llm_for_analysis
from airflow_tools import make_tool_runner
from output import output_to_stdout, output_to_file, output_to_teams, output_summary
from dedup import claim_failure, fingerprint, flush_summaries
from feedback import enabled as feedback_enabled, past_analysis_context, record_pending
from ssl_patch import apply_insecure_ssl_patch

configure_logging()

LOG = logging.getLogger("airflow-failure-responder")


def apply_connections(cfg: Dict[str, Any], get_conn: Callable[[str], Any]) -> Dict[str, Any]:
    """Fill secret config fields from Airflow Connections; a missing Connection leaves its subsystem unconfigured."""
    api = get_conn("failure_responder_airflow_api")
    if api is not None and api.host:
        cfg["base_url"] = api.host.rstrip("/")

    llm = get_conn("failure_responder_llm")
    if llm is not None and llm.password:
        cfg.setdefault("llm", {})["api_key"] = llm.password
        # shortcut: embedder shares the LLM gateway key, add failure_responder_embedder if they ever differ
        cfg.setdefault("knowledge", {}).setdefault("embedder", {})["api_key"] = llm.password

    db = get_conn("failure_responder_kb_db")
    if db is not None:
        cfg.setdefault("knowledge", {}).setdefault("vector_db", {})["db_url"] = db.get_uri()

    conf = get_conn("failure_responder_confluence")
    if conf is not None and "confluence" in cfg:
        cfg["confluence"]["auth"] = {"username": conf.login, "password": conf.password}

    teams = get_conn("failure_responder_teams")
    if teams is not None and teams.host:
        cfg.setdefault("output", {})["teams_webhook"] = teams.host
    return cfg


async def run_analysis(cfg: Dict[str, Any], token: str, log_url: str, identifiers: Dict[str, Optional[Any]],
                       analyzer_run_id: Optional[str] = None) -> Dict[str, Any]:
    """Returns {"feedback": bool, ...}; feedback=True only for a stored LLM analysis the team can rate."""
    LOG.info("[start] log_url=%s", log_url)

    # shortcut: global TLS bypass kept as opt-in for hosts without the internal CA; prefer tls.verify=<ca path>
    if (cfg.get("tls") or {}).get("insecure_global_patch"):
        LOG.warning("[tls] insecure_global_patch enabled: TLS verification disabled process-wide")
        apply_insecure_ssl_patch()

    # Safe config summary (no secrets)
    llm_cfg = cfg.get("llm", {})
    LOG.info(
        "[config] tls.verify=%s timeouts(connect=%s,total=%s) llm.base_url=%s model=%s",
        (cfg.get("tls", {}).get("verify", True)),
        (cfg.get("timeouts", {}).get("connect", 10.0)),
        (cfg.get("timeouts", {}).get("total", 30.0)),
        (llm_cfg.get("base_url") or llm_cfg.get("host")),
        (llm_cfg.get("model") or "llama3.1"),
    )

    tls = cfg.get("tls") or {}
    timeouts_cfg = cfg.get("timeouts") or {}
    connect_timeout = float(timeouts_cfg.get("connect", 10.0))
    total_timeout = float(timeouts_cfg.get("total", 30.0))
    verify = _derive_verify(tls)

    # Optional pagination limits
    pagination = cfg.get("pagination") or {}
    max_pages = int(pagination.get("max_pages", 200))
    max_bytes = int(pagination.get("max_bytes", 5_000_000))

    # 1) Fetch full log using token (run blocking requests in thread)
    t_fetch = time.time()
    try:
        # fetch_log_using_token(log_url, token, cfg=None, max_bytes=None)
        full_log = await asyncio.to_thread(
            fetch_log_using_token,
            log_url,
            token,
            cfg,  # reduced cfg may include tls/timeouts/max_bytes
            max_bytes=max_bytes,
        )
    except Exception:
        # Keep going: the team still gets a card even without the log
        LOG.exception("[fetch] Failed to fetch log from %s", log_url)
        full_log = ""

    LOG.info("[parse] full_log_len=%d (%.2fs)", len(full_log), time.time() - t_fetch)

    # 2) Focused log window (search for exit/exception hints)
    focused_log = crop_log_window(full_log, end_hint="Task exited with return code", max_chars=50_000)

    # 3) Redaction + truncation
    processed_log = process_logs(focused_log, max_chars=50_000)
    LOG.info("[parse] processed_log_len=%d", len(processed_log))

    fp = fingerprint(last_error_line(processed_log))

    # 3b) Dedup: send due "N tasks affected" summaries, then suppress this failure if its fingerprint is in cooldown
    try:
        flush_summaries(cfg, lambda summary: output_summary(cfg, summary))
        entry = {
            "dag_id": identifiers.get("dag_id"),
            "task_id": identifiers.get("task_id"),
            "run_id": identifiers.get("dag_run_id"),
            "try_number": identifiers.get("try_number"),
        }
        suppressed, _ = claim_failure(cfg, last_error_line(processed_log), entry)
        if suppressed:
            LOG.info("[dedup] suppressed: fp=%s already alerted within cooldown", fp[:12])
            return {"feedback": False}
    except Exception:
        # Dedup is an optimization; a broken dedup store must never cost an alert
        LOG.exception("[dedup] failed; analyzing without dedup")

    # 4) Knowledge base: retrieve similar rows (the daily kb_ingest DAG keeps them in sync)
    try:
        # Embed the error-focused tail, not the whole 50k window
        query = (last_error_line(processed_log) + "\n" + processed_log[-3000:]).strip()
        docs = retrieve_knowledge(cfg, query, verify)
    except Exception:
        LOG.exception("[kb] Knowledge retrieval failed; analyzing without knowledge")
        docs = []

    try:
        past = past_analysis_context(cfg, fp)
    except Exception:
        LOG.exception("[feedback] reading past feedback failed; analyzing without it")
        past = ""

    # 5) Ask LLM for analysis
    analyzed = False
    try:
        client = build_llm_client(cfg, verify)
        if client is None:
            raise RuntimeError("LLM not configured")
        run_tool = None
        if cfg.get("base_url"):
            run_tool = make_tool_runner(cfg["base_url"], token, verify, (connect_timeout, total_timeout), identifiers)
        analysis = ask_llm_for_analysis(client, cfg, processed_log, identifiers, docs, run_tool, past)
        analyzed = True
    except Exception as e:
        # Never stay silent: send the raw error line instead of an LLM analysis
        LOG.exception("[llm] LLM analysis failed; sending fallback card")
        analysis = {
            "error_summary": last_error_line(processed_log) or "Task failed - log unavailable",
            "root_cause": f"Otomatik analiz yapılamadı ({type(e).__name__}). Log'u inceleyin.",
            "confidence": 0.0,
        }

    # Ensure required schema fields exist
    analysis.setdefault("error_summary", "Task failed - analysis unavailable")
    analysis.setdefault("root_cause", "")
    analysis.setdefault("category", "other")
    analysis.setdefault("knowledge_fix_steps", [])
    analysis.setdefault("generic_fix_steps", [])
    analysis.setdefault("prevention", [])
    analysis.setdefault("needs_rerun", True)
    analysis.setdefault("confidence", 0.5)
    analysis.setdefault("used_docs", [])
    analysis.setdefault("used_knowledge", False)

    # 6) Compose result - keep identifiers even if None
    result = {
        "dag_id": identifiers.get("dag_id"),
        "dag_run_id": identifiers.get("dag_run_id"),
        "task_id": identifiers.get("task_id"),
        "try_number": identifiers.get("try_number"),
        "log_url": log_url,
        "extracted": {
            "error_summary": analysis.get("error_summary", "Unknown error"),
            "processed_log_length": len(processed_log),
        },
        "analysis": analysis,
    }

    # 6b) Feedback: only an LLM analysis can be rated; the HITL task skips otherwise
    feedback = False
    if analyzed and analyzer_run_id and cfg.get("base_url") and feedback_enabled(cfg):
        try:
            record_pending(cfg, analyzer_run_id, fp, analysis)
            result["feedback_url"] = (
                f"{cfg['base_url'].rstrip('/')}/dags/failure_analyzer/runs/{quote(analyzer_run_id, safe='')}/tasks/feedback"
            )
            feedback = True
        except Exception:
            LOG.exception("[feedback] storing the analysis failed; no feedback step for this run")

    LOG.info("[result] response=%s", result)

    # 7) Output
    try:
        output_config = cfg.get("output", {})
        output_method = output_config.get("method", "stdout")

        LOG.info("[output] Using output method: %s", output_method)

        if output_method == "stdout":
            output_to_stdout(result)
        elif output_method == "file":
            file_path = output_config.get("file_path")
            if not file_path:
                file_path = "/tmp/failed_task_log.json"
                LOG.warning("[output] No file_path specified, using default: %s", file_path)
            output_to_file(result, file_path)
        elif output_method == "teams":
            webhook_url = output_config.get("teams_webhook")
            if not webhook_url:
                LOG.error("[output] Teams output method requires 'teams_webhook' in configuration")
                LOG.info("[output] Falling back to stdout")
                output_to_stdout(result)
            else:
                verify_ssl = output_config.get("teams_verify_ssl", True)  # Teams webhooks use a public cert
                output_to_teams(result, webhook_url, verify_ssl, cfg.get('base_url'), (cfg.get("confluence") or {}).get("page_url"))
        else:
            LOG.warning("[output] Unknown output method '%s', falling back to stdout", output_method)
            output_to_stdout(result)

    except Exception as e:
        LOG.exception("Failed to output analysis result")
        raise RuntimeError(f"Failed to output analysis result: {e}") from e

    return {"feedback": feedback, "error_summary": analysis.get("error_summary"), "root_cause": analysis.get("root_cause")}
