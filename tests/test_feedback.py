import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "airflow_failure_responder"))

from feedback import UNANSWERED, parse_hitl_answer  # noqa: E402
from llm_agent import ask_llm_for_analysis  # noqa: E402
from output import _build_adaptive_card  # noqa: E402

VALID = ('{"category":"code","error_summary":"x","root_cause":"y","confidence":0.7,"used_doc_ids":[],'
         '"knowledge_fix_steps":[],"generic_fix_steps":[],"prevention":[],"needs_rerun":false}')


def test_parse_hitl_answer_answered_and_timeouts():
    answered = {"chosen_options": ["Yanlış"], "params_input": {"note": " HMS değil, disk dolu "},
                "responded_by_user": {"id": "7", "name": "ayse"}}
    assert parse_hitl_answer(answered) == {"verdict": "Yanlış", "note": "HMS değil, disk dolu", "responded_by": "ayse"}
    # Timeout: default option applied, nobody responded
    assert parse_hitl_answer({"chosen_options": ["Kısmen"], "params_input": {}})["verdict"] == UNANSWERED
    assert parse_hitl_answer({"chosen_options": ["Doğru"], "responded_by_user": {"id": "1"}, "timedout": True})["verdict"] == UNANSWERED
    assert parse_hitl_answer(None)["verdict"] == UNANSWERED


def test_past_analysis_goes_into_the_prompt():
    seen = {}

    def create(**kw):
        seen.update(kw)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=VALID, tool_calls=None))])

    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    ask_llm_for_analysis(client, {"llm": {}}, "log", {}, [], None, "ONAYLANMIŞ GEÇMİŞ ANALİZ ...")
    assert seen["messages"][-1]["content"].startswith("ONAYLANMIŞ GEÇMİŞ ANALİZ")


def test_card_has_feedback_button_only_with_url():
    base = {"dag_id": "d", "task_id": "t", "dag_run_id": "r", "try_number": 1, "analysis": {"error_summary": "e"}}
    with_url = json.dumps(_build_adaptive_card(dict(base, feedback_url="https://af/fb"), "https://af"), ensure_ascii=False)
    assert "Geri bildirim ver" in with_url and "https://af/fb" in with_url
    assert "Geri bildirim ver" not in json.dumps(_build_adaptive_card(base, "https://af"), ensure_ascii=False)


# ---------- Postgres integration (runs only when PG_TEST_DSN is set) ----------
PG = os.environ.get("PG_TEST_DSN")


@pytest.mark.skipif(not PG, reason="set PG_TEST_DSN to a Postgres")
def test_feedback_roundtrip_and_latest_answer_wins():
    import feedback

    cfg = {"knowledge": {"vector_db": {"db_url": PG}}}
    with feedback._connect(cfg) as conn:
        conn.execute(f"DELETE FROM {feedback.TABLE}")

    a1 = {"error_summary": "s", "root_cause": "HMS down", "knowledge_fix_steps": ["HMS restart"], "generic_fix_steps": []}
    feedback.record_pending(cfg, "run1", "fp1", a1)
    assert feedback.past_analysis_context(cfg, "fp1") == ""  # pending is not used

    feedback.record_verdict(cfg, "run1", "Doğru", None, "ayse")
    ctx = feedback.past_analysis_context(cfg, "fp1")
    assert ctx.startswith("ONAYLANMIŞ") and "HMS down" in ctx and "- HMS restart" in ctx

    feedback.record_pending(cfg, "run2", "fp1", dict(a1, root_cause="network"))
    feedback.record_verdict(cfg, "run2", "Yanlış", "disk dolu", "mehmet")
    ctx = feedback.past_analysis_context(cfg, "fp1")
    assert ctx.startswith("YANLIŞ") and "network" in ctx and "disk dolu" in ctx

    feedback.record_pending(cfg, "run3", "fp1", a1)
    feedback.record_verdict(cfg, "run3", feedback.UNANSWERED, None, None)
    assert feedback.past_analysis_context(cfg, "fp1").startswith("YANLIŞ")  # unanswered is ignored
    assert feedback.past_analysis_context(cfg, "other") == ""
