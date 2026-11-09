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


def _callback_module():
    import types

    # Callback imports airflow.sdk at module level; a stub keeps tests Airflow-free
    stub = types.ModuleType("airflow.sdk")
    stub.Connection = types.SimpleNamespace(get=lambda conn_id: None)
    stub.Variable = types.SimpleNamespace(get=lambda key: "{}")
    sys.modules.setdefault("airflow", types.ModuleType("airflow"))
    sys.modules.setdefault("airflow.sdk", stub)
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))
    from callbacks import trigger_failure_responder

    return trigger_failure_responder


def test_analyzer_run_id_short_and_long():
    cb = _callback_module()
    assert cb.analyzer_run_id("d", "r", "t", 2) == "fail__d__r__t__2"
    long_a = cb.analyzer_run_id("d" * 200, "manual__2026", "t", 1)
    long_b = cb.analyzer_run_id("d" * 200, "manual__2027", "t", 1)
    assert len(long_a) <= 250 and long_a != long_b
    assert long_a == cb.analyzer_run_id("d" * 200, "manual__2026", "t", 1)


class _Conn:
    def __init__(self, host=None, login=None, password=None, uri=None):
        self.host, self.login, self.password, self._uri = host, login, password, uri

    def get_uri(self):
        return self._uri


def test_apply_connections_fills_secrets_and_keeps_other_keys():
    from airflow_failure_responder import apply_connections

    conns = {
        "failure_responder_airflow_api": _Conn(host="https://af/"),
        "failure_responder_llm": _Conn(password="k"),
        "failure_responder_kb_db": _Conn(uri="postgresql://u:p@h/db"),
        "failure_responder_confluence": _Conn(login="cu", password="cp"),
        "failure_responder_teams": _Conn(host="https://hook?sig=x"),
    }
    cfg = {"llm": {"model": "m"}, "confluence": {"base_url": "https://c"}}
    out = apply_connections(cfg, conns.get)
    assert out["base_url"] == "https://af"
    assert out["llm"] == {"model": "m", "api_key": "k"}
    assert out["knowledge"]["embedder"]["api_key"] == "k"
    assert out["knowledge"]["vector_db"]["db_url"] == "postgresql://u:p@h/db"
    assert out["confluence"] == {"base_url": "https://c", "auth": {"username": "cu", "password": "cp"}}
    assert out["output"]["teams_webhook"] == "https://hook?sig=x"


def test_apply_connections_missing_connections_leave_cfg_alone():
    from airflow_failure_responder import apply_connections

    assert apply_connections({"llm": {"model": "m"}}, lambda _id: None) == {"llm": {"model": "m"}}


def test_load_responder_config_uses_variable_dir_then_default(tmp_path, monkeypatch):
    import json as _json

    cb = _callback_module()
    custom = tmp_path / "anywhere"
    custom.mkdir()
    (custom / "airflow_failure_responder.py").write_text("")
    default = tmp_path / "default"
    default.mkdir()
    (default / "airflow_failure_responder.py").write_text("")

    monkeypatch.setattr(cb.Variable, "get", lambda key: _json.dumps({"responder_dir": str(custom), "llm": {}}))
    cfg = cb.load_responder_config(str(default))
    assert cfg["llm"] == {} and sys.path[0] == str(custom)

    monkeypatch.setattr(cb.Variable, "get", lambda key: "{}")
    cb.load_responder_config(str(default))
    assert sys.path[0] == str(default)

    monkeypatch.setattr(cb.Variable, "get", lambda key: _json.dumps({"responder_dir": str(tmp_path / "missing")}))
    try:
        cb.load_responder_config(str(default))
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "missing" in str(e)
    sys.path.remove(str(custom))
    sys.path.remove(str(default))
