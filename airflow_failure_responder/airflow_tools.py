# airflow_tools.py
"""
Read-only Airflow context the LLM may ask for during analysis (OpenAI tool calling).

Every tool is scoped to the failure being analyzed and takes no arguments, so the model
cannot read other DAGs. Outputs are redacted and capped before they reach the model.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Callable, Dict, Tuple, Union
from urllib.parse import quote

import requests

from redaction import redact_text

LOG = logging.getLogger("airflow-failure-responder")

TOOL_OUTPUT_CHARS = 8000
# rendered_fields / executor_config can hold secrets, so only these TI fields are shown
TI_FIELDS = ("operator", "operator_name", "state", "try_number", "max_tries", "start_date", "end_date",
             "duration", "hostname", "pool", "queue", "executor")

_DESCRIPTIONS = {
    "get_task_instance": "Failed task instance metadata: operator, state, tries, timing, host, pool, queue.",
    "get_dag_source": "Python source of the failing DAG (secrets redacted).",
    "get_last_successful_run": "Last successful run of this DAG, with this task's duration and tries in it.",
    "get_other_failures_in_run": "Other failed / upstream_failed tasks in the same DAG run.",
}
TOOL_SPECS = [
    {"type": "function", "function": {"name": n, "description": d, "parameters": {"type": "object", "properties": {}}}}
    for n, d in _DESCRIPTIONS.items()
]


def _pick(d: Dict[str, Any], keys) -> Dict[str, Any]:
    return {k: d.get(k) for k in keys if d.get(k) is not None}


def make_tool_runner(base_url: str, token: str, verify: Union[bool, str], timeouts: Tuple[float, float],
                     ids: Dict[str, Any]) -> Callable[[str], str]:
    """runner(tool_name) -> redacted, capped text. Errors come back as text so the loop can go on."""
    api = f"{base_url.rstrip('/')}/api/v2"
    dag = quote(str(ids.get("dag_id")), safe="")
    run = quote(str(ids.get("dag_run_id")), safe="")
    task = quote(str(ids.get("task_id")), safe="")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    def get(path: str, params=None) -> Dict[str, Any]:
        r = requests.get(api + path, headers=headers, params=params, verify=verify, timeout=timeouts)
        r.raise_for_status()
        return r.json()

    def task_instance():
        return _pick(get(f"/dags/{dag}/dagRuns/{run}/taskInstances/{task}"), TI_FIELDS)

    def dag_source():
        return get(f"/dagSources/{dag}").get("content") or ""

    def last_successful_run():
        runs = get(f"/dags/{dag}/dagRuns", {"state": "success", "order_by": "-start_date", "limit": 1}).get("dag_runs") or []
        if not runs:
            return {"last_successful_run": None}
        out = _pick(runs[0], ("dag_run_id", "start_date", "end_date", "run_type"))
        try:
            ok_run = quote(runs[0]["dag_run_id"], safe="")
            out["task"] = _pick(get(f"/dags/{dag}/dagRuns/{ok_run}/taskInstances/{task}"),
                                ("duration", "try_number", "start_date", "end_date", "hostname"))
        except requests.HTTPError:
            out["task"] = None  # task did not exist in that run
        return out

    def other_failures():
        tis = get(f"/dags/{dag}/dagRuns/{run}/taskInstances",
                  {"state": ["failed", "upstream_failed"], "limit": 100}).get("task_instances") or []
        return [{"task_id": t.get("task_id"), "state": t.get("state")}
                for t in tis if t.get("task_id") != ids.get("task_id")]

    tools = {
        "get_task_instance": task_instance,
        "get_dag_source": dag_source,
        "get_last_successful_run": last_successful_run,
        "get_other_failures_in_run": other_failures,
    }
    cache: Dict[str, str] = {}

    def run_tool(name: str) -> str:
        if name in cache:
            return cache[name]
        fn = tools.get(name)
        if fn is None:
            return f"error: unknown tool {name}"
        try:
            out = fn()
            text = out if isinstance(out, str) else json.dumps(out, ensure_ascii=False, default=str)
        except Exception as e:
            LOG.warning("[tools] %s failed: %s", name, type(e).__name__)
            text = f"error: {type(e).__name__}"
        cache[name] = redact_text(text)[:TOOL_OUTPUT_CHARS]
        LOG.info("[tools] %s -> %d chars", name, len(cache[name]))
        return cache[name]

    return run_tool
