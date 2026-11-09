"""
failure_analyzer: one run per failed task try, triggered by callbacks.trigger_failure_responder.

conf = {dag_id, run_id, task_id, try_number}. Non-secret config comes from the Variable
'v_callback_fetch_failed_task'; secrets come from the failure_responder_* Connections.
No on_failure_callback here: a failing analyzer must not trigger another analyzer run.

analyze -> feedback (HITL, 24h) -> record_feedback. analyze short-circuits the feedback steps for
fallback alerts, dedup-suppressed failures and setups without a knowledge base database.
"""
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

from airflow.providers.standard.operators.hitl import HITLOperator
from airflow.sdk import DAG, Param, Variable, get_current_context, task

# Responder modules use flat imports; they are deployed as dags/airflow_failure_responder/
RESPONDER_DIR = str(Path(__file__).resolve().parent / "airflow_failure_responder")


with DAG(
    dag_id="failure_analyzer",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    # Runs stay open up to 24h waiting for feedback; the LLM limit is on the analyze task instead
    max_active_runs=200,
    default_args={"retries": 1},
    tags=["failure-responder"],
):

    @task.short_circuit(max_active_tis_per_dag=4)  # protects the LLM gateway during mass failures
    def analyze():
        import asyncio

        if RESPONDER_DIR not in sys.path:
            # Front of path: dags/ also holds the airflow_failure_responder/ folder of the same name
            sys.path.insert(0, RESPONDER_DIR)
        # Imported here so DAG parsing does not load openai/psycopg
        from airflow_failure_responder import apply_connections, run_analysis
        from callbacks.trigger_failure_responder import (
            _acquire_token_or_raise,
            get_conn_or_none,
            load_api_access,
            load_tls_timeouts,
        )

        conf = get_current_context()["dag_run"].conf or {}
        ids = {k: conf.get(k) for k in ("dag_id", "run_id", "task_id", "try_number")}
        if not all(ids.values()):
            raise ValueError(f"conf needs dag_id, run_id, task_id, try_number; got {sorted(conf)}")

        cfg = apply_connections(json.loads(Variable.get("v_callback_fetch_failed_task")), get_conn_or_none)
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
        run_id = get_current_context()["dag_run"].run_id
        outcome = asyncio.run(run_analysis(cfg, token, log_url, identifiers, analyzer_run_id=run_id))
        # Falsy return skips the feedback steps; the dict is the XCom the HITL body reads
        return outcome if outcome.get("feedback") else False

    feedback = HITLOperator(
        task_id="feedback",
        subject="Analiz doğru mu?",
        body=(
            "**Hata:** {{ ti.xcom_pull(task_ids='analyze')['error_summary'] }}\n\n"
            "**Kök neden:** {{ ti.xcom_pull(task_ids='analyze')['root_cause'] }}"
        ),
        options=["Doğru", "Kısmen", "Yanlış"],
        # Applied on timeout so the task does not fail; record_feedback stores it as 'unanswered'
        defaults=["Kısmen"],
        params={"note": Param("", type=["null", "string"], description="Doğru kök neden / not (isteğe bağlı)")},
        # response wait; newer providers move this to response_timeout themselves
        execution_timeout=timedelta(hours=24),
        retries=0,
    )

    @task(retries=2)
    def record_feedback(answer):
        if RESPONDER_DIR not in sys.path:
            sys.path.insert(0, RESPONDER_DIR)
        from airflow_failure_responder import apply_connections
        from callbacks.trigger_failure_responder import get_conn_or_none
        from feedback import parse_hitl_answer, record_verdict

        cfg = apply_connections(json.loads(Variable.get("v_callback_fetch_failed_task")), get_conn_or_none)
        parsed = parse_hitl_answer(answer)
        record_verdict(cfg, get_current_context()["dag_run"].run_id, **parsed)
        return parsed["verdict"]

    record_feedback(feedback.output)
    analyze() >> feedback
