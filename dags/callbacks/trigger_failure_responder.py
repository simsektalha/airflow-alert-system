# callbacks/trigger_failure_responder.py
"""
Airflow 3.x compatible on_failure_callback (single-method, no fallbacks).

Behaviour:
  - Read JSON config from Airflow Variable 'v_callback_fetch_failed_task'
  - Expect credentials in cfg['auth']['basic']['username','password'] and api_base_url (or base_url)
  - POST {api_base_url}/auth/token -> get JWT
  - GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id} with Bearer token -> extract try_number
  - Build logs URL (/api/v2/.../logs/{try})
  - Invoke responder script with --log-url and dag/run/task/try identifiers in argv,
    and {"token": <jwt>, "config": <cfg without auth>} as JSON on stdin.
    Secrets never go to argv (visible in `ps`) or to the task log.
  - No fallback, no BasicAuth reuse.
"""
import json
import logging
import subprocess
import sys
from typing import Tuple, Union
from urllib.parse import quote, urljoin

import requests
from airflow.sdk import Variable

LOG = logging.getLogger(__name__)
if not LOG.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


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


def _get_current_try_number_via_api_v2(base_url: str, token: str, verify: Union[bool, str], timeouts: Tuple[float, float], dag_id: str, run_id: str, task_id: str) -> int:
    base = base_url.rstrip("/")
    headers = {"Authorization": f"Bearer {token}"}
    url = (
        f"{base}/api/v2/dags/{quote(dag_id, safe='')}"
        f"/dagRuns/{quote(run_id, safe='')}"
        f"/taskInstances/{quote(task_id, safe='')}"
    )
    LOG.info("[ti-meta] GET %s", url)
    try:
        r = requests.get(url, headers=headers, verify=verify, timeout=timeouts)
    except Exception:
        LOG.exception("[ti-meta] GET request failed")
        raise

    try:
        r.raise_for_status()
    except Exception:
        LOG.error("[ti-meta] status=%s body=%s", r.status_code, (r.text or "")[:2000])
        raise

    try:
        data = r.json()
    except Exception:
        LOG.error("[ti-meta] failed to parse JSON response; body=%s", (r.text or "")[:2000])
        raise

    try:
        tn = int(data.get("try_number", 1))
    except Exception:
        LOG.error("[ti-meta] invalid try_number in response: %s", data.get("try_number"))
        raise RuntimeError("invalid try_number in API response")

    LOG.info("[ti-meta] state=%s api.try_number=%s max_tries=%s", data.get("state"), tn, data.get("max_tries"))
    return tn


def on_failure_trigger_fetcher(context):
    """
    Single-method on_failure_callback. Starts the responder script with
    --log-url and identifiers in argv; token + reduced config go via stdin.
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

    # Load config JSON from Airflow Variable
    try:
        cfg_raw = Variable.get("v_callback_fetch_failed_task")
        cfg = json.loads(cfg_raw)
    except Exception:
        LOG.exception("Failed to load v_callback_fetch_failed_task Airflow Variable")
        return

    base_url = (cfg.get("api_base_url") or cfg.get("base_url") or "").rstrip("/")
    if not base_url:
        LOG.error("Config must include api_base_url or base_url")
        return

    auth_basic = (cfg.get("auth") or {}).get("basic") or {}
    username = auth_basic.get("username")
    password = auth_basic.get("password")
    if not username or not password:
        LOG.error("Config must include auth.basic.username and auth.basic.password")
        return

    verify = _derive_verify(cfg.get("tls") or {})
    timeouts = _http_timeout(cfg.get("timeouts") or {})

    # Local logging of task instance numbers (not authoritative)
    ti_prop_try = getattr(ti, "try_number", None)
    ti_raw_try = getattr(ti, "try_number_raw", None)
    LOG.info("[ti] local: try_number=%s, try_number_raw=%s, state=%s", ti_prop_try, ti_raw_try, getattr(ti, "state", None))

    # STEP 1: Acquire token (strict)
    try:
        token = _acquire_token_or_raise(base_url, username, password, verify, timeouts)
    except Exception:
        LOG.exception("Failed to acquire token from auth endpoint; aborting callback")
        return

    # STEP 2: Query API v2 for authoritative try_number using the token
    try:
        try_number_for_log = _get_current_try_number_via_api_v2(base_url, token, verify, timeouts, dag_id, run_id, task_id)
    except Exception:
        LOG.exception("Failed to obtain try_number via API v2; aborting callback")
        return

    # STEP 3: Build the logs endpoint URL (API v2 path)
    log_url = (
        f"{base_url}/api/v2/dags/{quote(dag_id, safe='')}"
        f"/dagRuns/{quote(run_id, safe='')}"
        f"/taskInstances/{quote(task_id, safe='')}"
        f"/logs/{try_number_for_log}"
    )

    # responder script path (override allowed)
    script_path = cfg.get("script_path") or "/usr/local/airflow/dags/airflow_failure_responder/airflow_failure_responder.py"

    # Responder needs base_url (UI links) but never the Airflow credentials
    reduced_cfg = {k: v for k, v in cfg.items() if k != "auth"}
    stdin_payload = json.dumps({"token": token, "config": reduced_cfg})

    # Invocation config (interpreter + mode)
    invoke_cfg = cfg.get("invoke") or {}
    python_exec = invoke_cfg.get("python") or sys.executable
    mode = (invoke_cfg.get("mode") or "detach").lower()  # "detach" or "run"
    timeout_sec = int(invoke_cfg.get("timeout_sec") or 30)

    # Build command: non-secret identifiers only
    cmd = [
        python_exec,
        script_path,
        "--log-url", log_url,
        "--dag-id", dag_id,
        "--dag-run-id", run_id,
        "--task-id", task_id,
        "--try-number", str(try_number_for_log),
    ]

    # Safe logging only (do not print token or config)
    LOG.info("[invoke] python=%s script=%s dag_id=%s run_id=%s task_id=%s try=%s", python_exec, script_path, dag_id, run_id, task_id, try_number_for_log)
    LOG.info("[invoke] token_len=%d", len(token))

    # Invoke
    try:
        if mode == "run":
            subprocess.run(cmd, input=stdin_payload, text=True, check=False, timeout=timeout_sec)
        else:
            # detach, start new session; child reads stdin first, so the write does not block for long
            proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, text=True, start_new_session=True)
            proc.stdin.write(stdin_payload)
            proc.stdin.close()
    except Exception:
        LOG.exception("Failed to invoke failure responder script")
