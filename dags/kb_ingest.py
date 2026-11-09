"""
kb_ingest: daily Confluence incident table -> pgvector knowledge base sync.

Only new or changed rows are embedded; an empty page parse keeps the stored rows.
The failure_analyzer only queries the knowledge base, so a failed sync means stale
incident records, never a missing alert. Trigger it by hand after deploy or a page edit.
"""
import json
import sys
from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG, Variable, task

RESPONDER_DIR = str(Path(__file__).resolve().parent / "airflow_failure_responder")

with DAG(
    dag_id="kb_ingest",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,  # two syncs on one table would race on the delete of removed rows
    default_args={"retries": 2},
    tags=["failure-responder"],
):

    @task
    def sync_confluence():
        if RESPONDER_DIR not in sys.path:
            # Front of path: dags/ also holds the airflow_failure_responder/ folder of the same name
            sys.path.insert(0, RESPONDER_DIR)
        from airflow_failure_responder import apply_connections
        from airflow_log_client import _derive_verify
        from callbacks.trigger_failure_responder import get_conn_or_none
        from knowledge import ingest_confluence

        cfg = apply_connections(json.loads(Variable.get("v_callback_fetch_failed_task")), get_conn_or_none)
        if (cfg.get("tls") or {}).get("insecure_global_patch"):
            from ssl_patch import apply_insecure_ssl_patch

            apply_insecure_ssl_patch()
        # Fetch/embed/DB errors raise so the task fails and retries; missing config only logs a skip
        return ingest_confluence(cfg, _derive_verify(cfg.get("tls") or {}))

    sync_confluence()
