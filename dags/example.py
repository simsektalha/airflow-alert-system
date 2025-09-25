from datetime import datetime
import hashlib
import logging
import os
import random
import time
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from callbacks.trigger_failure_responder import on_failure_trigger_fetcher

LOG = logging.getLogger("realistic-failures")

default_args = {
    "on_failure_callback": on_failure_trigger_fetcher,
}

with DAG(
    dag_id="example_realistic_failures",
    start_date=datetime(2025, 9, 8),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description="Intentionally fail with one realistic error type chosen randomly each run",
) as dag:
    # --- Individual failure scenarios ---
    def missing_dependency():
        import importlib
        importlib.import_module("this_package_definitely_does_not_exist")  # ModuleNotFoundError

    def network_refused():
        # Port 9 on localhost is almost always closed; quick reliable connection error
        requests.get("http://127.0.0.1:9", timeout=1).raise_for_status()

    def permission_denied():
        # Assuming Airflow worker doesn't have read perms for /root
        with open("/root/secret.txt", "r") as f:
            f.read()

    def bad_configuration():
        # Simulate missing critical env var / secret
        os.environ["MISSING_DB_URL"]  # KeyError

    def data_parsing_error():
        int("not-an-int")  # ValueError

    def simulated_timeout():
        # Throw a timeout-like exception (quicker than actually sleeping for minutes)
        raise TimeoutError("Simulated operator timeout / long-running job exceeded limit")

    ERROR_BUILDERS = {
        "missing_dependency": missing_dependency,
        "network_refused": network_refused,
        "permission_denied": permission_denied,
        "bad_configuration": bad_configuration,
        "data_parsing_error": data_parsing_error,
        "simulated_timeout": simulated_timeout,
    }

    def chaos_monkey():
        """
        Pick one error at random (seeded by run_id so it's reproducible per run)
        and raise it to exercise the failure responder end-to-end.
        """
        ctx = get_current_context()
        run_id = ctx.get("run_id") or ctx.get("dag_run").run_id
        
        # Deterministic randomness per run_id
        seed_int = int(hashlib.sha256(str(run_id).encode("utf-8")).hexdigest(), 16)
        rnd = random.Random(seed_int)
        
        name = rnd.choice(list(ERROR_BUILDERS.keys()))
        LOG.warning("Choosing failure scenario: %s (run_id=%s)", name, run_id)
        
        # Execute the chosen failure
        ERROR_BUILDERS[name]()  # noqa

    PythonOperator(
        task_id="chaos_monkey",
        python_callable=chaos_monkey,
    )