# feedback.py
"""
Human feedback on analyses (Airflow 3.1 HITL step in failure_analyzer).

  - record_pending: after an LLM analysis, store it under the analyzer run id (verdict 'pending').
  - record_verdict: the HITL answer ('Doğru' / 'Kısmen' / 'Yanlış') or 'unanswered' after 24h.
  - past_analysis_context: the latest answered analysis with the same fingerprint, as a prompt block,
    so later analyses of the same error reuse a confirmed explanation and avoid a rejected one.
State lives in the knowledge base Postgres; without it there is no feedback.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.types.json import Jsonb

from knowledge import _db_url, _pg_dsn
from redaction import redact_text

LOG = logging.getLogger("airflow-failure-responder")

TABLE = "analysis_feedback"
VERDICTS = ("Doğru", "Kısmen", "Yanlış")
UNANSWERED = "unanswered"


def enabled(cfg: Dict[str, Any]) -> bool:
    return bool(_db_url(cfg))


def _connect(cfg: Dict[str, Any]) -> psycopg.Connection:
    conn = psycopg.connect(_pg_dsn(_db_url(cfg)), autocommit=True)
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {TABLE} ("
        " run_id text PRIMARY KEY,"
        " fp text,"
        " error_summary text NOT NULL,"
        " root_cause text NOT NULL,"
        " fix_steps jsonb NOT NULL,"
        " verdict text NOT NULL DEFAULT 'pending',"
        " note text,"
        " responded_by text,"
        " created_at timestamptz NOT NULL DEFAULT now(),"
        " answered_at timestamptz)"
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS {TABLE}_fp ON {TABLE} (fp, answered_at)")
    return conn


def record_pending(cfg: Dict[str, Any], run_id: str, fp: Optional[str], analysis: Dict[str, Any]) -> None:
    steps: List[str] = list(analysis.get("knowledge_fix_steps") or []) + list(analysis.get("generic_fix_steps") or [])
    with _connect(cfg) as conn:
        # A retried analyze task overwrites its own pending row
        conn.execute(
            f"INSERT INTO {TABLE} (run_id, fp, error_summary, root_cause, fix_steps) VALUES (%s, %s, %s, %s, %s)"
            " ON CONFLICT (run_id) DO UPDATE SET fp = EXCLUDED.fp, error_summary = EXCLUDED.error_summary,"
            " root_cause = EXCLUDED.root_cause, fix_steps = EXCLUDED.fix_steps, verdict = 'pending'",
            (run_id, fp, analysis.get("error_summary") or "", analysis.get("root_cause") or "", Jsonb(steps)),
        )


def parse_hitl_answer(answer: Optional[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    """HITL XCom -> {verdict, note, responded_by}. A timeout (no responder) is 'unanswered'."""
    answer = answer or {}
    # 3.1 returns responded_by_user ({"id","name"}); newer providers also set `timedout`
    who = answer.get("responded_by_user") or answer.get("responded_by") or answer.get("user_id")
    chosen = (answer.get("chosen_options") or [None])[0]
    if answer.get("timedout") or not who or chosen not in VERDICTS:
        return {"verdict": UNANSWERED, "note": None, "responded_by": None}
    note = ((answer.get("params_input") or {}).get("note") or "").strip() or None
    name = (who.get("name") or who.get("id")) if isinstance(who, dict) else str(who)
    return {"verdict": chosen, "note": note, "responded_by": name}


def record_verdict(cfg: Dict[str, Any], run_id: str, verdict: str, note: Optional[str], responded_by: Optional[str]) -> None:
    with _connect(cfg) as conn:
        conn.execute(
            f"UPDATE {TABLE} SET verdict = %s, note = %s, responded_by = %s, answered_at = now() WHERE run_id = %s",
            (verdict, note, responded_by, run_id),
        )


def past_analysis_context(cfg: Dict[str, Any], fp: Optional[str]) -> str:
    """Prompt block for the latest answered analysis of this fingerprint; '' when there is none."""
    if not fp or not enabled(cfg):
        return ""
    with _connect(cfg) as conn:
        row = conn.execute(
            f"SELECT verdict, root_cause, fix_steps, note FROM {TABLE}"
            " WHERE fp = %s AND verdict IN ('Doğru', 'Kısmen', 'Yanlış') ORDER BY answered_at DESC LIMIT 1",
            (fp,),
        ).fetchone()
    if not row:
        return ""
    verdict, root_cause, steps, note = row
    head = (
        "YANLIŞ BULUNMUŞ GEÇMİŞ ANALİZ (aynı hata). Bu açıklamayı tekrarlama; ekibin notunu dikkate al."
        if verdict == "Yanlış" else
        f"ONAYLANMIŞ GEÇMİŞ ANALİZ (aynı hata, ekip '{verdict}' dedi). Log aksini göstermiyorsa bunu temel al."
    )
    lines = [head, f"Kök neden: {root_cause}"]
    lines += [f"- {s}" for s in (steps or [])[:5]]
    if note:
        lines.append(f"Ekip notu: {note}")
    return redact_text("\n".join(lines))[:3000]
