# callbacks/trigger_failure_responder.py
"""
Airflow 3.x on_failure_callback: triggers one `failure_analyzer` DAG run per failed try.

  - Airflow API URL + FAB credentials come from Connection 'failure_responder_airflow_api'
    (host = API base URL, login/password).
  - tls/timeouts come from Airflow Variable 'v_callback_fetch_failed_task'.
  - POST {base}/auth/token -> JWT, then POST /api/v2/dags/failure_analyzer/dagRuns with ids only in conf.
  - dag_run_id is deterministic per failed try, so a repeated callback gets 409 instead of a second analysis.
  - Never raises: a failed trigger is logged, the failing task is not affected.
"""
import hashlib
import json
import logging
from typing import Tuple, Union
from urllib.parse import urljoin

import requests
from airflow.sdk import Connection, Variable

LOG = logging.getLogger(__name__)
if not LOG.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

ANALYZER_DAG_ID = "failure_analyzer"
API_CONN_ID = "failure_responder_airflow_api"
MAX_RUN_ID_LEN = 250


def _derive_verify(tls_cfg: dict) -> Union[bool, str]:
    if not tls_cfg:
        return True
    verify = tls_cfg.get("verify", True)
    if isinstance(verify, bool):
        return verify
    if isinstance(verify, str):
        return verify
    return True


def _http_timeout(timeouts_cfg: dict) -> Tuple[float, float]:
    connect = float((timeouts_cfg or {}).get("connect", 10.0))
    total = float((timeouts_cfg or {}).get("total", 30.0))
    return (connect, total)


def _acquire_token_or_raise(base_url: str, username: str, password: str, verify: Union[bool, str], timeouts: Tuple[float, float]) -> str:
    token_url = urljoin(base_url.rstrip("/") + "/", "auth/token")
    LOG.info("[auth] acquiring token from %s", token_url)
    try:
        r = requests.post(token_url, json={"username": username, "password": password}, verify=verify, timeout=timeouts)
    except Exception:
        LOG.exception("[auth] token request failed")
        raise

    try:
        r.raise_for_status()
    except Exception:
        LOG.error("[auth] token endpoint returned status=%s body=%s", r.status_code, (r.text or "")[:1000])
        raise

    try:
        data = r.json()
    except Exception:
        LOG.error("[auth] token response not JSON; body=%s", (r.text or "")[:2000])
        raise RuntimeError("token response not JSON")

    token = data.get("access_token") or data.get("token") or data.get("jwt") or data.get("accessToken")
    if not token:
        LOG.error("[auth] token missing in response; keys=%s", list(data.keys()))
        raise RuntimeError("token missing in auth response")
    # Do NOT log the token value; only its length for debug
    LOG.info("[auth] token acquired (len=%d)", len(token))
    return token


def analyzer_run_id(dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    rid = f"fail__{dag_id}__{run_id}__{task_id}__{try_number}"
    if len(rid) <= MAX_RUN_ID_LEN:
        return rid
    # Keep a readable prefix; the hash of the full id keeps it unique and deterministic
    digest = hashlib.sha256(rid.encode("utf-8")).hexdigest()[:12]
    return f"{rid[:MAX_RUN_ID_LEN - 14]}__{digest}"


def get_conn_or_none(conn_id):
    """Connection or None, so an optional subsystem degrades instead of failing the task."""
    try:
        return Connection.get(conn_id)
    except Exception:
        return None


def load_api_access(conn_getter=Connection.get) -> Tuple[str, str, str]:
    """(base_url, login, password) from the API Connection. Shared with the analyzer DAG."""
    conn = conn_getter(API_CONN_ID)
    base_url = (conn.host or "").rstrip("/")
    if not base_url or not conn.login or not conn.password:
        raise RuntimeError(f"Connection {API_CONN_ID} needs host (API base URL), login and password")
    return base_url, conn.login, conn.password


def load_tls_timeouts() -> Tuple[Union[bool, str], Tuple[float, float]]:
    try:
        cfg = json.loads(Variable.get("v_callback_fetch_failed_task"))
    except Exception:
        LOG.warning("[cfg] v_callback_fetch_failed_task unreadable; using default tls/timeouts")
        cfg = {}
    return _derive_verify(cfg.get("tls") or {}), _http_timeout(cfg.get("timeouts") or {})


def on_failure_trigger_fetcher(context):
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
    try_number = int(getattr(ti, "try_number", None) or 1)

    try:
        base_url, username, password = load_api_access()
        verify, timeouts = load_tls_timeouts()
        token = _acquire_token_or_raise(base_url, username, password, verify, timeouts)
    except Exception:
        LOG.exception("[trigger] cannot authenticate to Airflow API; no analysis for %s.%s", dag_id, task_id)
        return

    body = {
        "dag_run_id": analyzer_run_id(dag_id, run_id, task_id, try_number),
        "logical_date": None,
        "conf": {"dag_id": dag_id, "run_id": run_id, "task_id": task_id, "try_number": try_number},
    }
    url = f"{base_url}/api/v2/dags/{ANALYZER_DAG_ID}/dagRuns"
    try:
        r = requests.post(url, json=body, headers={"Authorization": f"Bearer {token}"}, verify=verify, timeout=timeouts)
    except Exception:
        LOG.exception("[trigger] POST %s failed", url)
        return

    if r.status_code == 409:
        LOG.info("[trigger] analyzer run %s already exists", body["dag_run_id"])
    elif r.ok:
        LOG.info("[trigger] analyzer run %s queued", body["dag_run_id"])
    else:
        LOG.error("[trigger] status=%s body=%s", r.status_code, (r.text or "")[:1000])
