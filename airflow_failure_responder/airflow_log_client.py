"""
Airflow log client (token-based).

This module expects the caller to supply:
  - a full `log_url` (Airflow API v2 logs endpoint)
  - a Bearer `token` (JWT) already acquired by the caller (trigger script)
  - an optional `cfg` dict with TLS/timeouts/max_bytes settings (reduced config; must NOT contain auth/base_url)

Behavior:
  - Does NOT attempt to acquire a token.
  - Sends GET to `log_url` with Authorization: Bearer <token>.
  - If response is JSON in the shape you provided ({"content": [...]}) it parses
    it into a single human-readable text blob (preserves group markers,
    timestamped events, sources, error_detail frames, etc).
  - If response is not JSON, returns raw text.
  - Raises on HTTP error or invalid input (no silent fallback).
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple, Union

import requests

LOG = logging.getLogger(__name__)
if not LOG.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def _derive_verify(tls_cfg: Optional[Dict[str, Any]]) -> Union[bool, str]:
    """Normalize tls.verify config: bool or path-to-CA-file."""
    if not tls_cfg:
        return True
    verify = tls_cfg.get("verify", True)
    if isinstance(verify, bool):
        return verify
    if isinstance(verify, str):
        return verify
    return True


def _http_timeout(timeouts_cfg: Optional[Dict[str, Any]]) -> Tuple[float, float]:
    """Convert config timeouts into requests-compatible (connect, read) tuple."""
    connect = float((timeouts_cfg or {}).get("connect", 10.0))
    total = float((timeouts_cfg or {}).get("total", 30.0))
    return (connect, total)


def _parse_airflow_json_content(j: Dict[str, Any]) -> str:
    """
    Parse the exact JSON shape you provided:
      { "content": [ { ... }, ... ], ... }
    Produces a human-readable single text blob.
    """
    out_lines = []
    content = j.get("content") or []
    for rec in content:
        # Event-only records (no timestamp) — e.g., group markers
        if "timestamp" not in rec and "event" in rec:
            ev = rec.get("event", "")
            out_lines.append(ev)
            if "sources" in rec and isinstance(rec["sources"], list):
                out_lines.append("sources: " + ", ".join(rec["sources"]))
            continue

        # Records with timestamp + event
        ts = rec.get("timestamp")
        event = rec.get("event", "") or ""
        level = rec.get("level", "") or ""
        logger = rec.get("logger", "") or ""
        filename = rec.get("filename")
        lineno = rec.get("lineno")

        primary = (f"{ts}" if ts else "NO_TIMESTAMP")
        if level:
            primary += f" [{level}]"
        if logger:
            primary += f" {logger}:"
        primary += f" {event}"
        out_lines.append(primary)

        # Optional file/lineno info
        if filename or lineno is not None:
            fileinfo = "  file:"
            if filename:
                fileinfo += f" {filename}"
            if lineno is not None:
                fileinfo += f":{lineno}"
            out_lines.append(fileinfo)

        # pid if present
        if "pid" in rec:
            out_lines.append(f"  pid: {rec.get('pid')}")

        # error_detail structured list (pretty-print)
        if "error_detail" in rec and isinstance(rec["error_detail"], list):
            for err in rec["error_detail"]:
                exc_type = err.get("exc_type")
                exc_value = err.get("exc_value")
                if exc_type or exc_value:
                    out_lines.append(f"  Exception: {exc_type or ''}: {exc_value or ''}")
                frames = err.get("frames") or []
                for fr in frames:
                    fr_file = fr.get("filename", "<unknown>")
                    fr_lineno = fr.get("lineno", "<?>")
                    fr_name = fr.get("name", "<module>")
                    out_lines.append(f"    File \"{fr_file}\", line {fr_lineno}, in {fr_name}")
                nested = err.get("exceptions")
                if nested:
                    out_lines.append("  nested_exceptions:")
                    for ne in nested:
                        out_lines.append("    " + str(ne))

    return "\n".join(out_lines)


def fetch_log_using_token(
    log_url: str,
    token: str,
    cfg: Optional[Dict[str, Any]] = None,
    *,
    max_bytes: Optional[int] = None,
) -> str:
    """
    Fetch an Airflow log using a pre-acquired Bearer token.

    Args:
      log_url: Full URL to the Airflow logs endpoint (constructed by trigger).
      token: Bearer JWT string (already acquired by the trigger).
      cfg: Reduced config dict (optional). May include:
        - tls: { "verify": True | False | "/path/to/cacert.pem" }
        - timeouts: { "connect": seconds, "total": seconds }
        - max_bytes: integer to truncate returned text
      max_bytes: optional override truncation length.

    Returns:
      Parsed text blob (str). Raises on HTTP errors or invalid args.
    """
    if not log_url:
        raise RuntimeError("log_url is required")
    if not token:
        raise RuntimeError("token is required")

    cfg = cfg or {}
    verify = _derive_verify(cfg.get("tls") or {})
    timeouts = _http_timeout(cfg.get("timeouts") or {})

    headers = {"Authorization": f"Bearer {token}"}
    LOG.info("[airflow_log_client] GET %s (using Bearer token, token_len=%d)", log_url, len(token))

    try:
        r = requests.get(log_url, headers=headers, verify=verify, timeout=timeouts)
    except Exception:
        LOG.exception("[airflow_log_client] GET failed for %s", log_url)
        raise

    try:
        r.raise_for_status()
    except Exception:
        LOG.error("[airflow_log_client] status=%s body=%s", r.status_code, (r.text or "")[:2000])
        raise

    # Try to parse as JSON in the exact shape expected; otherwise return raw text
    try:
        j = r.json()
    except Exception:
        LOG.debug("[airflow_log_client] response not JSON; returning raw text")
        text = r.text or ""
        if max_bytes is None:
            max_bytes = cfg.get("max_bytes")
        if max_bytes and len(text) > max_bytes:
            text = text[:max_bytes]
        return text

    # Parse the specific JSON shape you provided
    parsed = _parse_airflow_json_content(j)
    if max_bytes is None:
        max_bytes = cfg.get("max_bytes")
    if max_bytes and len(parsed) > max_bytes:
        parsed = parsed[:max_bytes]
    return parsed
