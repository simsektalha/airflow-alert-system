"""
failure_analyzer: one run per failed task try, triggered by callbacks.trigger_failure_responder.

conf = {dag_id, run_id, task_id, try_number}. Non-secret config comes from the Variable
'v_callback_fetch_failed_task'; secrets come from the failure_responder_* Connections.
No on_failure_callback here: a failing analyzer must not trigger another analyzer run.
"""
import json
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from airflow.sdk import DAG, Connection, Variable, get_current_context, task

# Responder modules use flat imports; they are deployed as dags/airflow_failure_responder/
RESPONDER_DIR = str(Path(__file__).resolve().parent / "airflow_failure_responder")


def _get_conn(conn_id):
    try:
        return Connection.get(conn_id)
    except Exception:
        return None


with DAG(
    dag_id="failure_analyzer",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=4,  # protects the LLM gateway during mass failures
    default_args={"retries": 1},
    tags=["failure-responder"],
):

    @task
    def analyze():
        import asyncio

        if RESPONDER_DIR not in sys.path:
            # Front of path: dags/ also holds the airflow_failure_responder/ folder of the same name
            sys.path.insert(0, RESPONDER_DIR)
        # Imported here so DAG parsing does not load openai/psycopg
        from airflow_failure_responder import apply_connections, run_analysis
        from callbacks.trigger_failure_responder import _acquire_token_or_raise, load_api_access, load_tls_timeouts

        conf = get_current_context()["dag_run"].conf or {}
        ids = {k: conf.get(k) for k in ("dag_id", "run_id", "task_id", "try_number")}
        if not all(ids.values()):
            raise ValueError(f"conf needs dag_id, run_id, task_id, try_number; got {sorted(conf)}")

        cfg = apply_connections(json.loads(Variable.get("v_callback_fetch_failed_task")), _get_conn)
        base_url, username, password = load_api_access()
        verify, timeouts = load_tls_timeouts()
        token = _acquire_token_or_raise(base_url, username, password, verify, timeouts)

        log_url = (
            f"{base_url}/api/v2/dags/{quote(ids['dag_id'], safe='')}"
            f"/dagRuns/{quote(ids['run_id'], safe='')}"
            f"/taskInstances/{quote(ids['task_id'], safe='')}"
            f"/logs/{int(ids['try_number'])}"
        )
        identifiers = {
            "dag_id": ids["dag_id"],
            "dag_run_id": ids["run_id"],
            "task_id": ids["task_id"],
            "try_number": int(ids["try_number"]),
        }
        asyncio.run(run_analysis(cfg, token, log_url, identifiers))

    analyze()
