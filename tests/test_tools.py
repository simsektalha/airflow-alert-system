import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "airflow_failure_responder"))

import airflow_tools  # noqa: E402
from llm_agent import ask_llm_for_analysis  # noqa: E402

VALID = ('{"category":"code","error_summary":"x","root_cause":"y","confidence":0.7,"used_doc_ids":[],'
         '"knowledge_fix_steps":[],"generic_fix_steps":[],"prevention":[],"needs_rerun":false}')


def _msg(content="", calls=()):
    tc = [SimpleNamespace(id=f"c{i}", function=SimpleNamespace(name=n, arguments="{}")) for i, n in enumerate(calls)]
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content, tool_calls=tc or None))])


class ScriptedClient:
    """Mimics openai.OpenAI: returns scripted messages in order, records every request."""

    def __init__(self, *replies, fail_with_tools=False):
        self.replies, self.requests, self.fail_with_tools = list(replies), [], fail_with_tools
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.requests.append(kwargs)
        if self.fail_with_tools and "tools" in kwargs:
            raise RuntimeError("gateway: tools not supported")
        return self.replies.pop(0)


def test_tool_context_goes_into_final_structured_call():
    calls = []
    client = ScriptedClient(_msg(calls=["get_dag_source"]), _msg("TAMAM"), _msg(VALID))
    out = ask_llm_for_analysis(client, {"llm": {}}, "log", {"dag_id": "d"}, [],
                               lambda name: calls.append(name) or "dag source text")
    assert out["root_cause"] == "y" and calls == ["get_dag_source"]
    final = client.requests[-1]
    assert "tools" not in final and final["response_format"]["type"] == "json_schema"
    assert "EK BAĞLAM" in final["messages"][-1]["content"] and "dag source text" in final["messages"][-1]["content"]
    # The tool round saw the assistant tool_calls and the tool result
    assert [m["role"] for m in client.requests[1]["messages"]][-2:] == ["assistant", "tool"]


def test_gateway_without_tools_falls_back_to_single_call():
    client = ScriptedClient(_msg(VALID), fail_with_tools=True)
    out = ask_llm_for_analysis(client, {"llm": {}}, "log", {}, [], lambda name: "unused")
    assert out["error_summary"] == "x"
    assert not any("EK BAĞLAM" in m["content"] for m in client.requests[-1]["messages"])


def test_tools_off_and_round_limit():
    client = ScriptedClient(_msg(VALID))
    ask_llm_for_analysis(client, {"llm": {"tools": False}}, "log", {}, [], lambda name: "x")
    assert len(client.requests) == 1

    looping = ScriptedClient(_msg(calls=["get_task_instance"]), _msg(calls=["get_task_instance"]), _msg(VALID))
    ask_llm_for_analysis(looping, {"llm": {"max_tool_rounds": 2}}, "log", {}, [], lambda name: "ti")
    assert len(looping.requests) == 3  # 2 tool rounds + final


class _Resp:
    def __init__(self, data, status=200):
        self.data, self.status_code = data, status

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code}")

    def json(self):
        return self.data


def test_runner_whitelists_redacts_caps_and_caches(monkeypatch):
    seen = []

    def fake_get(url, **kw):
        seen.append((url, kw.get("params")))
        if url.endswith("/dagSources/d"):
            return _Resp({"content": "conn = 'password=hunter2'\n" + "x" * 20000})
        if "/taskInstances/t" in url:
            return _Resp({"operator": "PythonOperator", "state": "failed", "rendered_fields": {"sql": "secret"}})
        if url.endswith("/taskInstances"):
            return _Resp({"task_instances": [{"task_id": "t", "state": "failed"}, {"task_id": "u", "state": "upstream_failed"}]})
        if url.endswith("/dagRuns"):
            return _Resp({}, 500)
        raise AssertionError(url)

    monkeypatch.setattr(airflow_tools.requests, "get", fake_get)
    run = airflow_tools.make_tool_runner("https://af/", "T", True, (1, 1), {"dag_id": "d", "dag_run_id": "r", "task_id": "t"})

    ti = run("get_task_instance")
    assert "PythonOperator" in ti and "rendered_fields" not in ti and "secret" not in ti
    src = run("get_dag_source")
    assert "hunter2" not in src and len(src) == airflow_tools.TOOL_OUTPUT_CHARS
    assert '"u"' in run("get_other_failures_in_run") and '"task_id": "t"' not in run("get_other_failures_in_run")
    assert run("get_last_successful_run").startswith("error:")
    assert run("nope").startswith("error: unknown tool")
    n = len(seen)
    run("get_dag_source")
    assert len(seen) == n  # cached
    assert all(u.startswith("https://af/api/v2/") for u, _ in seen)


@pytest.mark.parametrize("spec", airflow_tools.TOOL_SPECS)
def test_tool_specs_take_no_arguments(spec):
    assert spec["function"]["parameters"] == {"type": "object", "properties": {}}
