# callbacks/trigger_failure_responder.py

import base64
import json
import logging
import shlex
import subprocess
import sys
from urllib.parse import quote

from airflow.models import Variable
import requests

LOG = logging.getLogger(__name__)
if not LOG.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

def _derive_verify(tls_cfg):
    """
    tls.verify may be:
      - True/False (bool)
      - string path to a CA bundle file
    """
    if not tls_cfg:
        return True
    verify = tls_cfg.get("verify", True)
    if isinstance(verify, bool):
        return verify
    if isinstance(verify, str):
        return verify
    return True

def _http_timeout(timeouts_cfg):
    connect = float((timeouts_cfg or {}).get("connect", 10.0))
    total = float((timeouts_cfg or {}).get("total", 30.0))
    # requests uses (connect, read) tuple; map total to read
    return (connect, total)

def _get_current_try_number_via_api(base_url, auth_user, auth_pass, verify, timeouts, dag_id, run_id, task_id):
    """
    Ask Airflow's REST API for the task instance; use its try_number as the source of truth.
    GET /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}
    """
    url = (
        f"{base_url.rstrip('/')}/api/v1/dags/{quote(dag_id, safe='')}"
        f"/dagRuns/{quote(run_id, safe='')}"
        f"/taskInstances/{quote(task_id, safe='')}"
    )
    LOG.info("[ti-meta] GET %s", url)
    r = requests.get(url, auth=(auth_user, auth_pass), verify=verify, timeout=timeouts)
    try:
        r.raise_for_status()
    except Exception:
        LOG.error("[ti-meta] status=%s body=%s", r.status_code, r.text[:300])
        raise
    data = r.json()
    tn = int(data.get("try_number", 1))
    LOG.info("[ti-meta] state=%s api.try_number=%s max_tries=%s", data.get("state"), tn, data.get("max_tries"))
    return tn

def on_failure_trigger_fetcher(context):
    """
    Airflow task on_failure_callback:
    - Fetch TaskInstance metadata (including correct try_number) via REST API.
    - Build the Airflow v1 Logs API URL using that try_number.
    - Invoke airflow_failure_responder.py (which will fetch + analyze logs).
    - All configuration is taken from a single Airflow Variable 'v_callback_fetch_failed_task' (JSON).
    """
    ti = context.get("ti") or context.get("task_instance")
    if not ti:
        LOG.error("No task instance in callback context")
        return

    dag_id = ti.dag_id
    task_id = ti.task_id
    dag_run = context.get("dag_run")
    run_id = context.get("run_id") or (dag_run.run_id if dag_run else None)
    if not run_id:
        LOG.error("No run_id/dag_run_id in context for %s.%s", dag_id, task_id)
        return

    # Load single JSON config from one Airflow Variable
    try:
        cfg_raw = Variable.get("v_callback_fetch_failed_task")
        cfg = json.loads(cfg_raw)
    except Exception as e:
        LOG.exception("Failed to load v_callback_fetch_failed_task Airflow Variable: %s", e)
        return

    base_url = (cfg.get("base_url") or "").rstrip("/")
    auth = (cfg.get("auth") or {}).get("basic") or {}
    username = auth.get("username") or ""
    password = auth.get("password") or ""
    tls = cfg.get("tls") or {}
    timeouts = _http_timeout(cfg.get("timeouts") or {})

    if not base_url or not username or not password:
        LOG.error("Config must include base_url and auth.basic.username/password")
        return

    verify = _derive_verify(tls)

    # Log local (Airflow) numbers for visibility — but don't trust them for the URL.
    ti_prop_try = getattr(ti, "try_number", None)
    ti_raw_try = getattr(ti, "try_number_raw", None)
    LOG.info("[ti] local: try_number=%s, try_number_raw=%s, state=%s", ti_prop_try, ti_raw_try, getattr(ti, "state", None))

    # Authoritative try_number from Airflow API (avoids off-by-one across versions)
    try:
        try_number_for_log = _get_current_try_number_via_api(
            base_url, username, password, verify, timeouts, dag_id, run_id, task_id
        )
    except Exception:
        LOG.exception("Failed to obtain try_number via API; falling back to local heuristic")
        if ti_raw_try:
            try_number_for_log = int(ti_raw_try)
        else:
            try_number_for_log = max(1, int(ti_prop_try or 1) - 1)
        LOG.info("[ti] fallback try_number_for_log=%s", try_number_for_log)

    # Build exact v1 Logs endpoint for this failing attempt
    log_url = (
        f"{base_url}/api/v1/dags/{quote(dag_id, safe='')}"
        f"/dagRuns/{quote(run_id, safe='')}"
        f"/taskInstances/{quote(task_id, safe='')}"
        f"/logs/{try_number_for_log}"
    )

    # Path to your responder script; override via config.script_path if needed
    script_path = cfg.get("script_path") or "/usr/local/airflow/dags/airflow_failure_responder.py"


    # Pass entire config as base64 to the script (no env vars required)
    cfg_b64 = base64.b64encode(json.dumps(cfg).encode("utf-8")).decode("ascii")

    # ---- interpreter override (NEW) ----
    invoke_cfg = cfg.get("invoke") or {}
    python_exec = invoke_cfg.get("python") or sys.executable
    # <-- honors config.invoke.python
    mode = (invoke_cfg.get("mode") or "detach").lower()
    # "detach" (Popen) or "run" (blocking)
    timeout_sec = int(invoke_cfg.get("timeout_sec") or 30)

    cmd = [
        python_exec,
        script_path,
        "--log-url", log_url,
        "--config-b64", cfg_b64,
        "--dag-id", dag_id,
        "--dag-run-id", run_id,
        "--task-id", task_id,
        "--try-number", str(try_number_for_log),
    ]

    # Safe logs (no secrets)
    LOG.info("[invoke] python=%s", python_exec)
    LOG.info("[invoke] script=%s", script_path)
    LOG.info("[invoke] base_url=%s dag_id=%s run_id=%s task_id=%s try=%s", base_url, dag_id, run_id, task_id, try_number_for_log)
    LOG.info("[invoke] command: %s", " ".join(shlex.quote(c) for c in cmd))

    try:
        if mode == "run":
            subprocess.run(cmd, check=False, timeout=timeout_sec)
        else:
            subprocess.Popen(cmd, start_new_session=True)
    except Exception:
        LOG.exception("Failed to invoke failure responder script")