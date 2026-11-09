# dedup.py
"""
Dedup of alert cards by a global root-cause fingerprint (no dag_id in it).

  - First failure of a fingerprint opens a cooldown window and gets the normal analysis + card.
  - Later failures inside the window are suppressed (no LLM call, no card) and counted.
  - After the window, flush_summaries sends one "N tasks affected" card per fingerprint.
    It runs at the start of every analyzer run and in the daily kb_ingest run (no extra scheduler).
State lives in the knowledge base Postgres. dedup.cooldown_minutes = 0 turns it off.
"""
from __future__ import annotations

import hashlib
import logging
import re
from typing import Any, Callable, Dict, Optional, Tuple

import psycopg
from psycopg.types.json import Jsonb

from knowledge import _db_url, _pg_dsn

LOG = logging.getLogger("airflow-failure-responder")

TABLE = "failure_dedup"
MAX_AFFECTED = 20  # listed on the summary card; the count stays exact
DEFAULT_COOLDOWN_MIN = 30

_TYPE_MSG_RE = re.compile(r"\b([A-Za-z_][\w.]*(?:Error|Exception))\s*:\s*(.*)$")
# Order matters: specific shapes first, plain digit runs last
_NORMALIZERS = [
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I), "<uuid>"),
    (re.compile(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:[.,]\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?"), "<ts>"),
    (re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b"), "<ip>"),
    (re.compile(r"\b(?=[0-9a-f]*[a-f])(?=[0-9a-f]*\d)[0-9a-f]{8,}\b", re.I), "<hex>"),
    (re.compile(r"\d+"), "<n>"),
]


def normalize_message(msg: str) -> str:
    # Quoted names and paths are kept: "No module named 'x'" and "'y'" are different root causes
    for rx, token in _NORMALIZERS:
        msg = rx.sub(token, msg)
    return " ".join(msg.split()).lower()[:300]


def fingerprint(error_line: str) -> Optional[str]:
    """sha256 of exception type + normalized message; None when there is no error line."""
    line = (error_line or "").strip()
    if not line:
        return None
    m = _TYPE_MSG_RE.search(line)
    exc_type, msg = (m.group(1).rsplit(".", 1)[-1], m.group(2)) if m else ("", line)
    return hashlib.sha256(f"{exc_type}\n{normalize_message(msg)}".encode("utf-8")).hexdigest()


def cooldown_minutes(cfg: Dict[str, Any]) -> int:
    return int((cfg.get("dedup") or {}).get("cooldown_minutes", DEFAULT_COOLDOWN_MIN))


def _connect(cfg: Dict[str, Any]) -> Optional[psycopg.Connection]:
    db_url = _db_url(cfg)
    if not db_url or cooldown_minutes(cfg) <= 0:
        return None
    # autocommit: each conn.transaction() below is a real transaction that holds its row locks
    conn = psycopg.connect(_pg_dsn(db_url), autocommit=True)
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {TABLE} ("
        " fp text PRIMARY KEY,"
        " title text NOT NULL,"
        " window_start timestamptz NOT NULL,"
        " last_seen timestamptz NOT NULL,"
        " suppressed int NOT NULL DEFAULT 0,"
        " affected jsonb NOT NULL DEFAULT '[]')"
    )
    # Failure that opened the window; its own retry must not be suppressed (added after the first release)
    conn.execute(f"ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS lead_key text")
    return conn


def claim(conn: psycopg.Connection, fp: str, title: str, entry: Dict[str, Any], cooldown: int) -> bool:
    """True = suppress this failure (inside an open window); False = send the full analysis."""
    key = "|".join(str(entry.get(k)) for k in ("dag_id", "run_id", "task_id", "try_number"))
    with conn.transaction():
        # -infinity: a brand-new row counts as an expired window
        conn.execute(
            f"INSERT INTO {TABLE} (fp, title, window_start, last_seen) VALUES (%s, %s, '-infinity', now())"
            " ON CONFLICT (fp) DO NOTHING",
            (fp, title[:300]),
        )
        in_window, lead_key = conn.execute(
            f"SELECT window_start > now() - make_interval(mins => %s), lead_key FROM {TABLE} WHERE fp = %s FOR UPDATE",
            (cooldown, fp),
        ).fetchone()
        if in_window and lead_key == key:
            # Retry or manual clear of the failure that opened the window (e.g. its card failed to send)
            return False
        if in_window:
            conn.execute(
                f"UPDATE {TABLE} SET suppressed = suppressed + 1, last_seen = now(),"
                " affected = CASE WHEN jsonb_array_length(affected) < %s THEN affected || %s ELSE affected END"
                " WHERE fp = %s",
                (MAX_AFFECTED, Jsonb([entry]), fp),
            )
            return True
        # Counts of a window whose summary failed to send are kept and reported with the next one
        conn.execute(
            f"UPDATE {TABLE} SET window_start = now(), last_seen = now(), title = %s, lead_key = %s WHERE fp = %s",
            (title[:300], key, fp),
        )
        return False


def flush(conn: psycopg.Connection, cooldown: int, send: Callable[[Dict[str, Any]], None]) -> int:
    """Send one summary per expired window with suppressed failures. A failed send rolls back and retries later."""
    due = conn.execute(
        f"SELECT fp FROM {TABLE} WHERE suppressed > 0 AND window_start <= now() - make_interval(mins => %s)",
        (cooldown,),
    ).fetchall()
    sent = 0
    for (fp,) in due:
        try:
            with conn.transaction():
                # Lock + read first: RETURNING would give the already-reset values
                row = conn.execute(
                    f"SELECT title, suppressed, affected, window_start, last_seen FROM {TABLE}"
                    " WHERE fp = %s AND suppressed > 0 AND window_start <= now() - make_interval(mins => %s)"
                    " FOR UPDATE",
                    (fp, cooldown),
                ).fetchone()
                if row:  # another run may have flushed it meanwhile
                    title, count, affected, start, last = row
                    conn.execute(f"UPDATE {TABLE} SET suppressed = 0, affected = '[]' WHERE fp = %s", (fp,))
                    send({"title": title, "count": count, "affected": affected,
                          "window_start": start.isoformat(), "last_seen": last.isoformat()})
                    sent += 1
        except Exception:
            LOG.exception("[dedup] summary send failed for fp=%s; will retry on the next run", fp[:12])
    return sent


def claim_failure(cfg: Dict[str, Any], error_line: str, entry: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """(suppress?, fp). Never suppresses when dedup is off or the line has no fingerprint."""
    fp = fingerprint(error_line)
    if fp is None:
        return False, None
    conn = _connect(cfg)
    if conn is None:
        return False, fp
    with conn:
        return claim(conn, fp, error_line, entry, cooldown_minutes(cfg)), fp


def flush_summaries(cfg: Dict[str, Any], send: Callable[[Dict[str, Any]], None]) -> int:
    conn = _connect(cfg)
    if conn is None:
        return 0
    with conn:
        return flush(conn, cooldown_minutes(cfg), send)
