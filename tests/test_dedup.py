import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "airflow_failure_responder"))

from dedup import fingerprint, normalize_message  # noqa: E402
from output import _build_summary_card  # noqa: E402


def test_normalize_replaces_volatile_parts_only():
    msg = "run 3f2b8c1e-1a2b-4c3d-8e9f-001122334455 at 2026-11-09T10:15:00Z from 10.1.2.3:5432 id deadbeef42 in /data/2026/11/x.csv 'col_x'"
    assert normalize_message(msg) == "run <uuid> at <ts> from <ip> id <hex> in /data/<n>/<n>/x.csv 'col_x'"


def test_fingerprint_groups_same_cause_and_splits_different_ones():
    a = fingerprint("[2026-11-09 10:00:01] ERROR - psycopg.OperationalError: connection to 10.0.0.1:5432 failed after 3 tries")
    b = fingerprint("[2026-11-09 11:42:59] ERROR - OperationalError: connection to 10.0.0.9:5432 failed after 7 tries")
    assert a == b
    assert fingerprint("ModuleNotFoundError: No module named 'x'") != fingerprint("ModuleNotFoundError: No module named 'y'")
    assert fingerprint("KeyError: 'a'") != fingerprint("ValueError: 'a'")


def test_fingerprint_without_type_uses_whole_line_and_empty_is_none():
    assert fingerprint("Task exited with return code 137") == fingerprint("Task exited with return code 1")
    assert fingerprint("") is None and fingerprint("   ") is None


def test_summary_card_lists_affected_and_remaining_count():
    card = _build_summary_card({
        "title": "KeyError: 'col_x'", "count": 22,
        "affected": [{"dag_id": "d", "task_id": f"t{i}", "run_id": "r"} for i in range(20)],
        "window_start": "s", "last_seen": "e",
    })
    texts = [b["text"] for b in card["body"]]
    assert "22 task daha etkilendi" in texts[0]
    assert "- d.t0 (r)" in texts[3] and "... ve 2 task daha" in texts[3]


# ---------- Postgres integration (runs only when PG_TEST_DSN is set) ----------
PG = os.environ.get("PG_TEST_DSN")


@pytest.mark.skipif(not PG, reason="set PG_TEST_DSN to a Postgres")
def test_claim_suppresses_within_cooldown_and_flush_sends_once():
    import dedup

    cfg = {"knowledge": {"vector_db": {"db_url": PG}}, "dedup": {"cooldown_minutes": 30}}
    conn = dedup._connect(cfg)
    conn.execute(f"DELETE FROM {dedup.TABLE}")
    fp = fingerprint("KeyError: 'x'")
    e = {"dag_id": "d", "task_id": "t", "run_id": "r", "try_number": 1}
    e2, e3 = dict(e, task_id="t2"), dict(e, task_id="t3")

    assert dedup.claim(conn, fp, "KeyError: 'x'", e, 30) is False  # first: full analysis
    # Retry of the same failure (its card failed to send) is analyzed again, not counted
    assert dedup.claim(conn, fp, "KeyError: 'x'", e, 30) is False
    assert dedup.claim(conn, fp, "KeyError: 'x'", e2, 30) is True
    assert dedup.claim(conn, fp, "KeyError: 'x'", e3, 30) is True

    sent = []
    assert dedup.flush(conn, 30, sent.append) == 0  # window still open

    # Expire the window; a failed send keeps the counts for the next run
    conn.execute(f"UPDATE {dedup.TABLE} SET window_start = now() - interval '31 minutes'")

    def boom(_):
        raise RuntimeError("teams down")

    assert dedup.flush(conn, 30, boom) == 0
    assert dedup.flush(conn, 30, sent.append) == 1
    assert sent[0]["count"] == 2 and len(sent[0]["affected"]) == 2
    assert dedup.flush(conn, 30, sent.append) == 0  # sent only once

    # After the window a new failure opens a new window and is analyzed again
    assert dedup.claim(conn, fp, "KeyError: 'x'", e2, 30) is False
    conn.close()


def test_dedup_off_without_db_or_with_zero_cooldown():
    import dedup

    assert dedup.claim_failure({}, "KeyError: 'x'", {}) == (False, fingerprint("KeyError: 'x'"))
    cfg = {"knowledge": {"vector_db": {"db_url": "postgresql://nowhere/db"}}, "dedup": {"cooldown_minutes": 0}}
    assert dedup.claim_failure(cfg, "KeyError: 'x'", {})[0] is False
    assert dedup.flush_summaries(cfg, lambda s: None) == 0
