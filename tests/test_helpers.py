import json
import sys
from pathlib import Path

# Package modules use flat imports (they run as a script from their own folder)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "airflow_failure_responder"))

from log_processing import last_error_line  # noqa: E402
from output import _build_adaptive_card  # noqa: E402
from redaction import redact_text  # noqa: E402


def test_last_error_line_picks_last_error():
    log = "INFO start\nValueError: first\nINFO retry\nKeyError: 'col_x'\nINFO done"
    assert last_error_line(log) == "KeyError: 'col_x'"


def test_last_error_line_empty_when_no_error():
    assert last_error_line("INFO all good\nINFO bye") == ""
    assert last_error_line("") == ""


def _result(analysis):
    return {"dag_id": "d", "task_id": "t", "dag_run_id": "r", "try_number": 1, "analysis": analysis}


def test_card_shows_root_cause_when_present():
    card = _build_adaptive_card(_result({"error_summary": "boom", "root_cause": "metastore down"}), "https://af")
    text = json.dumps(card, ensure_ascii=False)
    assert "Kök Neden" in text and "metastore down" in text


def test_card_skips_root_cause_when_missing():
    card = _build_adaptive_card(_result({"error_summary": "boom"}), "https://af")
    assert "Kök Neden" not in json.dumps(card, ensure_ascii=False)


def test_confluence_button_only_when_configured():
    without = json.dumps(_build_adaptive_card(_result({"error_summary": "boom"}), "https://af"))
    with_url = json.dumps(_build_adaptive_card(_result({"error_summary": "boom"}), "https://af", "https://wiki/x/1"))
    assert "Confluence" not in without
    assert "https://wiki/x/1" in with_url


def test_redaction_hides_password_and_conn_string():
    out = redact_text("password=hunter2 postgresql://u:p@db/x")
    assert "hunter2" not in out and "u:p@" not in out
