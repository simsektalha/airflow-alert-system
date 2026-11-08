# airflow_failure_responder.py
"""
Responder entrypoint.

Expectations (per our agreed flow):
  - trigger_failure_responder.py obtains JWT and constructs full log_url,
    then invokes this script with:
      argv:  --log-url <full-log-url> [--dag-id ... --dag-run-id ... --task-id ... --try-number ...]
      stdin: {"token": "<jwt>", "config": {<reduced cfg>}}
  - reduced cfg does NOT contain auth/basic credentials,
    but may contain base_url, tls, timeouts, pagination, llm, output, knowledge, confluence, etc.
  - If fetching or LLM analysis fails, a fallback card (raw error line) is still sent.

This script:
  - Decodes config
  - Fetches the log using airflow_log_client.fetch_log_using_token (runs in thread)
  - Processes/log-window/redaction
  - Optionally ingests Confluence
  - Runs LLM analysis (with knowledge filtering based on DAG)
  - Outputs result according to cfg.output
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
import argparse
from typing import Any, Dict, Optional

from logging_cfg import configure_logging, _mask
from airflow_log_client import fetch_log_using_token, _derive_verify
from log_processing import crop_log_window, process_logs, last_error_line
from knowledge import setup_knowledge_base, ingest_confluence_page_to_kb  # keep KB setup/ingest helpers
from llm_agent import build_agent_from_cfg, ask_llm_for_analysis
from output import output_to_stdout, output_to_file, output_to_teams
from ssl_patch import apply_insecure_ssl_patch

configure_logging()

LOG = logging.getLogger("airflow-failure-responder")


async def main_async() -> None:
    p = argparse.ArgumentParser(
        description="Fetch a failed task's log (using provided token), parse/redact, analyze with LLM, and emit JSON."
    )
    p.add_argument("--log-url", required=True, help="Absolute Airflow v2 Logs endpoint for the failing attempt.")
    # Optional identifiers (trigger may omit these)
    p.add_argument("--dag-id", required=False, help="Optional DAG id (provided by trigger if available).")
    p.add_argument("--dag-run-id", required=False, help="Optional DAG run id.")
    p.add_argument("--task-id", required=False, help="Optional task id.")
    p.add_argument("--try-number", required=False, type=int, help="Optional try number (int).")

    args = p.parse_args()

    # Token + config come via stdin, never argv (argv is visible in `ps` and task logs)
    try:
        payload = json.loads(sys.stdin.read())
        cfg = payload["config"]
        token = payload["token"]
    except Exception:
        LOG.exception("Failed to read {token, config} JSON from stdin")
        raise SystemExit("Invalid stdin payload")

    LOG.info("[start] log_url=%s", args.log_url)

    # shortcut: global TLS bypass kept as opt-in for hosts without the internal CA; prefer tls.verify=<ca path>
    if (cfg.get("tls") or {}).get("insecure_global_patch"):
        LOG.warning("[tls] insecure_global_patch enabled: TLS verification disabled process-wide")
        apply_insecure_ssl_patch()

    # Safe config summary (no secrets)
    llm_cfg = cfg.get("llm", {})
    LOG.info(
        "[config] tls.verify=%s timeouts(connect=%s,total=%s) llm.driver=%s model=%s",
        (cfg.get("tls", {}).get("verify", True)),
        (cfg.get("timeouts", {}).get("connect", 10.0)),
        (cfg.get("timeouts", {}).get("total", 30.0)),
        (llm_cfg.get("driver") or "openai_like"),
        (llm_cfg.get("model") or "llama"),
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

    # Validate token presence (trigger should supply)
    if not token:
        LOG.error("No token provided (trigger must pass it on stdin)")
        raise SystemExit("Missing token")

    # 1) Fetch full log using token (run blocking requests in thread)
    t_fetch = time.time()
    try:
        # fetch_log_using_token(log_url, token, cfg=None, max_bytes=None)
        full_log = await asyncio.to_thread(
            fetch_log_using_token,
            args.log_url,
            token,
            cfg,  # reduced cfg may include tls/timeouts/max_bytes
            max_bytes=max_bytes,
        )
    except Exception:
        # Keep going: the team still gets a card even without the log
        LOG.exception("[fetch] Failed to fetch log from %s", args.log_url)
        full_log = ""

    LOG.info("[parse] full_log_len=%d (%.2fs)", len(full_log), time.time() - t_fetch)

    # 2) Focused log window (search for exit/exception hints)
    focused_log = crop_log_window(full_log, end_hint="Task exited with return code", max_chars=50_000)

    # 3) Redaction + truncation
    processed_log = process_logs(focused_log, max_chars=50_000)
    LOG.info("[parse] processed_log_len=%d", len(processed_log))

    # 4) Knowledge base (optional) + service-aware retrieval via Agent filters
    kb = setup_knowledge_base(cfg)
    if kb:
        confluence_cfg = cfg.get("confluence")
        if confluence_cfg:
            try:
                LOG.info("[kb] Starting Confluence ingestion")
                await ingest_confluence_page_to_kb(
                    kb=kb,
                    base_url=confluence_cfg.get("base_url"),
                    page_id=confluence_cfg.get("page_id"),
                    auth=(
                        confluence_cfg.get("auth", {}).get("username"),
                        confluence_cfg.get("auth", {}).get("password"),
                    ),
                    verify=cfg.get("tls", {}).get("verify", True),
                )
                LOG.info("[kb] Confluence ingestion finished")
            except Exception as e:
                LOG.exception("[kb] Confluence ingestion failed: %s", e)
        else:
            LOG.info("[kb] No confluence config found; skipping ingestion")
    else:
        LOG.warning("[kb] Knowledge not initialized; skipping ingestion")

    # Build agent and pass service filter for knowledge retrieval (Agent is expected to accept knowledge_filters)
    # We derive service string exactly as required: "Airflow - <dag_id>"
    identifiers: Dict[str, Optional[Any]] = {
        "dag_id": getattr(args, "dag_id", None),
        "dag_run_id": getattr(args, "dag_run_id", None),
        "task_id": getattr(args, "task_id", None),
        "try_number": getattr(args, "try_number", None),
    }

    dag_id_val = identifiers.get("dag_id")
    knowledge_filters = None
    if dag_id_val:
        service_str = f"Airflow - {dag_id_val}"
        #knowledge_filters = {"service": service_str}
        LOG.info("[kb] Will build agent with knowledge_filters=%s", knowledge_filters)
    else:
        LOG.info("[kb] No dag_id provided; building agent without knowledge_filters")

    agent = build_agent_from_cfg(cfg, kb, knowledge_filters=knowledge_filters)
    LOG.info("[llm] Single Agent available=%s", bool(agent))

    # 5) Ask LLM for analysis, pass the service-matched docs implicitly via agent filters
    try:
        if agent is None:
            raise RuntimeError("LLM agent not configured")
        analysis = await ask_llm_for_analysis(
            agent=agent,
            full_log=processed_log,
            identifiers=identifiers,
        )
    except Exception as e:
        # Never stay silent: send the raw error line instead of an LLM analysis
        LOG.exception("[llm] LLM analysis failed; sending fallback card")
        analysis = {
            "error_summary": last_error_line(processed_log) or "Task failed - log unavailable",
            "root_cause": f"Otomatik analiz yapılamadı ({type(e).__name__}). Log'u inceleyin.",
            "confidence": 0.0,
        }

    # Optionally, if the agent can report used docs back, we expect analysis to include "used_docs".
    # If not provided by agent, analysis["used_docs"] will remain absent and Adaptive Card may show nothing.
    # You may later add a small metadata-only post-query for UI if needed.

    # Ensure required schema fields exist
    analysis.setdefault("error_summary", "Task failed - analysis unavailable")
    analysis.setdefault("root_cause", "")
    analysis.setdefault("category", "other")
    analysis.setdefault("knowledge_fix_steps", [])
    analysis.setdefault("generic_fix_steps", [])
    analysis.setdefault("prevention", [])
    analysis.setdefault("needs_rerun", True)
    analysis.setdefault("confidence", 0.5)
    analysis.setdefault("used_docs", analysis.get("used_docs", []))

    # 6) Compose result - keep identifiers even if None
    result = {
        "dag_id": identifiers.get("dag_id"),
        "dag_run_id": identifiers.get("dag_run_id"),
        "task_id": identifiers.get("task_id"),
        "try_number": identifiers.get("try_number"),
        "log_url": args.log_url,
        "extracted": {
            "error_summary": analysis.get("error_summary", "Unknown error"),
            "processed_log_length": len(processed_log),
        },
        "analysis": analysis,
    }

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
                verify_ssl = output_config.get("teams_verify_ssl", False)
                output_to_teams(result, webhook_url, verify_ssl, cfg.get('base_url'), (cfg.get("confluence") or {}).get("page_url"))
        else:
            LOG.warning("[output] Unknown output method '%s', falling back to stdout", output_method)
            output_to_stdout(result)

    except Exception as e:
        LOG.exception("Failed to output analysis result")
        raise SystemExit(f"Failed to output analysis result: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        sys.exit(130)
