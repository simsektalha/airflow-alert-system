"""
Airflow AI Alert - Minimal Demo (single file)

What it does (MVP):
- Calls Airflow 2.2.0 REST API
- Finds recent failed DAG runs and fetches brief error context (optionally task logs)
- Sends a concise summary to an Agno agent (using your external LLM API)
- Prints proposed fixes to the console

No DB, no Redis, no webhooks. Keep it simple.

Env vars required:
- AIRFLOW_BASE_URL (e.g., http://localhost:8080)
- AIRFLOW_USERNAME (e.g., admin)
- AIRFLOW_PASSWORD (e.g., admin)
- LLM_BASE_URL (your external LLM API base, e.g., https://llm.your.org/v1)
- LLM_API_KEY (your external LLM API key)
- LLM_MODEL (default: gpt-4o-mini)

Usage:
  python airflow_ai_demo.py --max-dags 10 --max-runs 5 --include-logs
"""

import os
import sys
import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx
from agno.agent import Agent
from agno.models.openai import OpenAIChat


# ----------------------------
# Env & simple config helpers
# ----------------------------
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")

LLM_BASE_URL = os.getenv("LLM_BASE_URL", "")
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

# fallback: some Airflow APIs return datetimes ending in Z (Zulu)

def _parse_dt(dt: str) -> datetime:
    try:
        if dt.endswith("Z"):
            dt = dt.replace("Z", "+00:00")
        return datetime.fromisoformat(dt)
    except Exception:
        return datetime.utcnow()


# ----------------------------
# Airflow Client (minimal)
# ----------------------------
class AirflowClient:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = (username, password)

    async def health(self) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(f"{self.base_url}/api/v1/health", auth=self.auth)
            r.raise_for_status()
            return r.json()

    async def list_dags(self, limit: int = 100) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(
                f"{self.base_url}/api/v1/dags",
                auth=self.auth,
                params={"limit": limit},
            )
            r.raise_for_status()
            return r.json().get("dags", [])

    async def list_dag_runs(self, dag_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
                auth=self.auth,
                params={"limit": limit, "order_by": "-start_date"},
            )
            r.raise_for_status()
            return r.json().get("dag_runs", [])

    async def list_task_instances(self, dag_id: str, dag_run_id: str) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
                auth=self.auth,
            )
            r.raise_for_status()
            return r.json().get("task_instances", [])

    async def get_task_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1) -> str:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}",
                auth=self.auth,
            )
            if r.status_code >= 400:
                return ""
            return r.text


# ----------------------------
# Agent (Agno + external LLM)
# ----------------------------

def build_agent() -> Agent:
    if not LLM_BASE_URL or not LLM_API_KEY:
        print("LLM_BASE_URL and LLM_API_KEY must be set in the environment.")
        sys.exit(1)

    model = OpenAIChat(
        id=LLM_MODEL,
        api_key=LLM_API_KEY,
        base_url=LLM_BASE_URL,  # external cluster
        temperature=0.2,
        max_tokens=1200,
    )

    return Agent(
        model=model,
        name="Airflow Failure Analyst",
        instructions=[
            "You are an expert Apache Airflow and Data Engineering assistant.",
            "Given failed DAG runs and minimal logs, identify likely root causes and propose concise fixes.",
            "Prefer actionable steps. If logs are partial, call out assumptions.",
            "Output: brief root cause summary, 3-7 step fix list, and prevention tips.",
        ],
        markdown=True,
    )


# ----------------------------
# Orchestration
# ----------------------------
async def collect_failed_runs(
    af: AirflowClient,
    max_dags: int = 20,
    max_runs: int = 5,
    include_logs: bool = False,
) -> List[Dict[str, Any]]:
    """Return a compact list of failed dag runs across DAGs."""
    failed: List[Dict[str, Any]] = []
    dags = await af.list_dags(limit=max_dags)

    for dag in dags:
        dag_id = dag.get("dag_id")
        if not dag_id:
            continue

        runs = await af.list_dag_runs(dag_id, limit=max_runs)
        for run in runs:
            if run.get("state") != "failed":
                continue

            dag_run_id = run.get("dag_run_id")
            start = run.get("start_date")
            end = run.get("end_date")

            item: Dict[str, Any] = {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "state": run.get("state"),
                "start_date": start,
                "end_date": end,
            }

            if include_logs:
                # fetch failed task instances and grab a small slice of logs
                try:
                    tis = await af.list_task_instances(dag_id, dag_run_id)
                    failed_tis = [ti for ti in tis if ti.get("state") == "failed"]
                    logs: List[Dict[str, Any]] = []
                    for ti in failed_tis[:2]:  # keep it short
                        task_id = ti.get("task_id")
                        # try_number is not always present; best effort with 1
                        raw_log = await af.get_task_log(dag_id, dag_run_id, task_id, try_number=1)
                        logs.append(
                            {
                                "task_id": task_id,
                                "snippet": (raw_log or "").strip()[:1200],
                            }
                        )
                    if logs:
                        item["logs"] = logs
                except Exception:
                    pass

            failed.append(item)

    return failed


def make_agent_prompt(failed_runs: List[Dict[str, Any]]) -> str:
    if not failed_runs:
        return "No failed DAG runs found."

    lines: List[str] = [
        "# Airflow Failed DAG Runs",
        "Provide a short root-cause analysis and a concrete fix plan.",
        "",
    ]

    for i, fr in enumerate(failed_runs, 1):
        lines.append(f"## {i}. DAG: {fr.get('dag_id')} | Run: {fr.get('dag_run_id')}")
        lines.append(f"- State: {fr.get('state')}")
        lines.append(f"- Start: {fr.get('start_date')}")
        lines.append(f"- End:   {fr.get('end_date')}")
        if "logs" in fr:
            for log in fr["logs"]:
                lines.append(f"- Task: {log.get('task_id')}")
                lines.append("```log")
                lines.append(log.get("snippet", "") or "(empty)")
                lines.append("```")
        lines.append("")

    lines.append(
        "Answer format: 'Root cause', 'Fix steps' (numbered), 'Prevention tips' (bullets). Keep it concise."
    )

    return "\n".join(lines)


async def run_demo(max_dags: int, max_runs: int, include_logs: bool) -> None:
    # 1) Check Airflow health
    af = AirflowClient(AIRFLOW_BASE_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    try:
        health = await af.health()
        print(f"Airflow health: {health.get('status', 'unknown')}")
    except Exception as e:
        print(f"Failed to contact Airflow at {AIRFLOW_BASE_URL}: {e}")
        sys.exit(1)

    # 2) Collect failures
    failed_runs = await collect_failed_runs(af, max_dags=max_dags, max_runs=max_runs, include_logs=include_logs)
    if not failed_runs:
        print("No failed DAG runs found. Nothing to analyze.")
        return

    # 3) Build prompt and ask agent
    prompt = make_agent_prompt(failed_runs)
    agent = build_agent()

    print("\n===== Prompt to Agent =====\n")
    print(prompt)
    print("\n===========================\n")

    resp = await agent.arun(prompt)

    print("\n===== Agent Response =====\n")
    print(resp.content)
    print("\n==========================\n")


def parse_args():
    import argparse

    p = argparse.ArgumentParser(description="Airflow AI Alert - Minimal Demo")
    p.add_argument("--max-dags", type=int, default=20, help="Max number of DAGs to scan")
    p.add_argument("--max-runs", type=int, default=5, help="Max recent runs per DAG to inspect")
    p.add_argument("--include-logs", action="store_true", help="Fetch small log snippets for failed tasks")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Basic env guardrails for external LLM cluster preference
    if not LLM_BASE_URL or not LLM_API_KEY:
        print(
            "Missing LLM_BASE_URL or LLM_API_KEY. Please export them in your environment."
        )
        sys.exit(1)

    asyncio.run(run_demo(args.max_dags, args.max_runs, args.include_logs))
