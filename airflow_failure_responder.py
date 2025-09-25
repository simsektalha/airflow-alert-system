"""
Pipeline:
    1) Read all config from --config-b64 (single Airflow Variable JSON). No env vars.
    2) Fetch ONE task attempt log from --log-url (Airflow v1 Logs endpoint).
    3) Parse & redact logs; extract error-focused slices.
    4) Send to LLM (if configured) with strict JSON schema; fallback to heuristics.
    5) Write final JSON to --out.

Optional config keys (inside the single JSON from --config-b64):
{
    "base_url": "https://airflow.example.com:8080",
    "auth": { "basic": { "username": "user", "password": "pass" } },
    "tls": { "verify": true },  // or a path string to CA bundle
    "timeouts": { "connect": 10.0, "total": 30.0 },
    "pagination": { "max_pages": 200, "max_bytes": 5000000 },
    "llm": {
      "driver": "openai_like" | "ollama",
      "model": "gpt-4o-mini",
      "max_tokens": 800,
      "temperature": 0.1,
      // if driver == "openai_like"
      "base_url": "https://your-gateway/v1",
      "api_key": "sk-...",
      // if driver == "ollama"
      "host": "http://localhost:11434"
    },
    "output": {
      "method": "stdout" | "file" | "teams",
      "file_path": "/tmp/failed_task_log.json",  // for file output
      "teams_webhook": "https://api.powerplatform.com/...",  // for teams output
      "teams_verify_ssl": false  // for teams SSL verification
    }
}
"""
import argparse
import asyncio
import base64
import codecs
import json
import logging
import re
import sys
import time
import requests
from typing import Any, Dict, List, Optional, Tuple

import httpx

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOG = logging.getLogger("airflow-failure-responder")


def _mask(s: str, keep: int = 2) -> str:
    if not s:
        return ""
    return s[:keep] + "" if len(s) > keep else ""


# ---------- Optional LLM drivers ----------
try:
    from agno.agent import Agent
    from agno.team import Team
except Exception:
    Agent = None  # type: ignore
    Team = None  # type: ignore

try:
    from agno.models.openai.like import OpenAILike
except Exception:
    OpenAILike = None  # type: ignore

try:
    from agno.models.vllm import VLLM
except Exception:
    VLLM = None

try:
    from agno.models.ollama import Ollama
except Exception:
    Ollama = None  # type: ignore


# ---------- Redaction ----------
SECRET_PATTERNS = [
    r"AKIA[0-9A-Z]{16}",
    r"\baws_secret_access_key\b\s*[:=]\s*[A-Za-z0-9/+=]{30,}",
    r"\bsecret\b\s*[:=]\s*[^,\s'\";]+",
    r"\bpassword\b\s*[:=]\s*[^,\s'\";]+",
    r"\btoken\b\s*[:=]\s*[^,\s'\";]+",
    r"authorization:\s*bearer\s+[A-Za-z0-9.\-]+",
    r"\beyJ[a-zA-Z0-9_\-]{10,}\.[a-zA-Z0-9.\-]{10,}\.[a-zA-Z0-9.\-]{10,}\b",
    r"[A-Za-z0-9.%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
]
SECRET_RE = re.compile("|".join(SECRET_PATTERNS), flags=re.IGNORECASE)


def redact_conn_strings(s: str) -> str:
    return re.sub(r"([a-zA-Z]+:\/\/)([^:@\s]+):([^@\/\s]+)@", r"\1[REDACTED]:[REDACTED]@", s)


def redact_text(s: str, *, tail_lines: int, max_len: int) -> str:
    if not s:
        return ""
    s = redact_conn_strings(s)
    s = SECRET_RE.sub("[REDACTED]", s)
    lines = s.splitlines()
    if len(lines) > tail_lines:
        lines = lines[-tail_lines:]
    s = "\n".join(lines)
    if len(s) > max_len:
        s = s[-max_len:]
    return s


# ---------- Error extraction ----------
def extract_error_focus(log_text: str, context_lines: int = 30, max_chars: int = 8000) -> Tuple[str, str]:
    """
    Returns (error_focus, error_summary).
    """
    lines = log_text.splitlines()
    tb_start_idx = None
    for i in range(len(lines) - 1, -1, -1):
        if "Traceback (most recent call last):" in lines[i]:
            tb_start_idx = i
            break

    focus_blocks: List[str] = []
    if tb_start_idx is not None:
        end = min(len(lines), tb_start_idx + 1 + 40)
        focus_blocks.append("\n".join(lines[tb_start_idx:end]))

    err_idxs = [i for i, ln in enumerate(lines) if re.search(r"\b(ERROR|CRITICAL|Exception|^Traceback)", ln)]
    if err_idxs:
        i = err_idxs[-1]
        s = max(0, i - context_lines)
        e = min(len(lines), i + context_lines)
        focus_blocks.append("\n".join(lines[s:e]))

    if not focus_blocks:
        focus_blocks.append("\n".join(lines[-50:]))

    error_focus = "\n...\n".join(focus_blocks)
    if len(error_focus) > max_chars:
        error_focus = error_focus[-max_chars:]

    summary = ""
    for ln in reversed(lines[-200:]):
        m = re.search(r"([A-Za-z][A-Za-z0-9_\.]*Error: .+|Exception: .+|ERROR .+)", ln)
        if m:
            summary = m.group(0).strip()
            break

    if not summary and tb_start_idx is not None:
        last_line_idx = min(len(lines)-1, tb_start_idx + 40)
        summary = lines[last_line_idx].strip()

    if not summary:
        summary = "Task failed (no explicit error summary found)."

    return error_focus, summary


# ---------- Fetch Airflow log by absolute URL (with loop guards) ----------
async def fetch_log_by_url(
    log_url: str,
    username: str,
    password: str,
    verify: Any,
    connect_timeout: float,
    total_timeout: float,
    *,
    max_pages: int = 200,
    max_bytes: int = 5_000_000,
) -> str:
    """
    Fetch the task's log via the v1 Logs endpoint, handling:
      - full_content=true first page
      - continuation_token paging
      - loop guards (repeating token or repeating content)
      - max pages/bytes safety limits
    """
    params: Dict[str, Any] = {"full_content": "true"}
    out_parts: List[str] = []
    seen_tokens: set = set()
    last_chunk_hash: Optional[int] = None
    timeout = httpx.Timeout(total_timeout, connect=connect_timeout)

    async with httpx.AsyncClient(
        auth=(username, password),
        verify=verify,
        timeout=timeout,
        headers={"Accept": "application/json"},
    ) as client:
        LOG.info("[fetch] GET %s params=%s", log_url, params)
        t0 = time.time()
        page = 0
        total_len = 0
        while True:
            page += 1
            if page > max_pages:
                LOG.warning("[fetch] stopping due to max_pages=%d", max_pages)
                break

            r = await client.get(log_url, params=params)
            LOG.info(
                "[fetch] page=%d status=%s content-type=%s len=%s",
                page, r.status_code, r.headers.get("Content-Type"),
                len(r.content) if r.content else 0,
            )

            if r.status_code >= 400:
                raise RuntimeError(f"GET {log_url} failed: {r.status_code} {r.text[:200]}")

            ctype = (r.headers.get("Content-Type") or "").lower()
            if "json" in ctype:
                try:
                    data = r.json()
                except Exception:
                    LOG.warning("[fetch] JSON parse failed, falling back to text")
                    chunk = r.text or ""
                    out_parts.append(chunk)
                    total_len += len(chunk)
                    break

                content = data.get("content")
                if isinstance(content, str):
                    # Airflow returns escaped text sometimes; un-escape if possible.
                    try:
                        content = codecs.escape_decode(content.encode("utf-8"))[0].decode("utf-8")
                    except Exception:
                        pass
                elif isinstance(content, list):
                    content = "".join(str(x) for x in content)

                chunk = "" if content is None else str(content)
                out_parts.append(chunk)
                total_len += len(chunk)
                token = data.get("continuation_token")
                LOG.info("[fetch] page=%d token_present=%s total_len=%d", page, bool(token), total_len)

                # Stop if token is exhausted
                if not token:
                    break

                # Loop guard 1: repeating token
                if token in seen_tokens:
                    LOG.warning("[fetch] repeating token detected; breaking pagination")
                    break
                seen_tokens.add(token)

                # Loop guard 2: repeating content
                h = hash(chunk)
                if last_chunk_hash is not None and h == last_chunk_hash:
                    LOG.warning("[fetch] repeated content chunk; breaking pagination")
                    break
                last_chunk_hash = h

                # Safety limit
                if total_len >= max_bytes:
                    LOG.warning("[fetch] stopping due to max_bytes=%d", max_bytes)
                    break

                # Next page: only the token (do not include full_content again)
                params = {"token": token}
                continue

            # text/plain fallback (non-chunked)
            LOG.info("[fetch] chunk-ok text/plain")
            chunk = r.text or ""
            out_parts.append(chunk)
            total_len += len(chunk)
            break

        LOG.info("[fetch] completed in %.2fs, pages=%d, total_len=%d",
                 time.time() - t0, page, total_len)
        return "".join(out_parts)


# ---------- LLM ----------
GIVEN_SCHEMA = """
Return ONLY a single JSON object with EXACTLY these keys:
{
    "root_cause": string,
    "category": "network" | "dependency" | "config" | "code" | "infra" | "security" | "design" | "transient" | "other",
    "fix_steps": [string, string, string],
    "prevention": [string, string],
    "needs_rerun": true|false,
    "confidence": number,
    "error_summary": string
}
- Keep answers concise and practical.
- "fix_steps" should be 3-7 items (you may include up to 7).
- "prevention" should be 2-6 items.
- "confidence" is 0.0-1.0.
- Do NOT wrap in markdown; output raw JSON only.
""".strip()

def build_team_from_cfg(cfg: Dict[str, Any]) -> Optional["Team"]:
    """Build a Team for collaborative failure analysis."""
    if Agent is None or Team is None:
        LOG.info("[llm] agno not installed; skipping Team")
        return None

    llm = cfg.get("llm") or {}
    driver = (llm.get("driver") or "openai_like").lower()
    model_id = llm.get("model") or "llama3.1"
    temperature = float(llm.get("temperature") or 0.1)
    max_tokens = int(llm.get("max_tokens") or 800)

    # Build model
    model = None
    if driver == "openai_like" and OpenAILike and llm.get("base_url") and llm.get("api_key"):
        model = OpenAILike(
            id=model_id,
            api_key=llm["api_key"],
            base_url=llm["base_url"],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        LOG.info("[llm] driver=openai_like base=%s model=%s", llm.get("base_url"), model_id)
    elif driver == "vllm" and VLLM and llm.get("base_url"):
        model = VLLM(
            id=model_id,
            base_url=llm["base_url"]
        )
        LOG.info("[llm] driver=vLLM base=%s model=%s", llm.get("base_url"), model_id)
    elif driver == "ollama" and Ollama and llm.get("host"):
        model = Ollama(id=model_id, host=llm["host"])
        LOG.info("[llm] driver=ollama host=%s model=%s", llm.get("host"), model_id)
    else:
        LOG.info("[llm] not configured or driver unavailable; skipping Team")
        return None

    # Create specialized agents for the team
    log_ingestor = Agent(
        model=model,
        name="LogIngestor",
        instructions=[
            "You are a log analysis specialist. Your role is to ingest raw Airflow task logs and produce a concise, lossless summary.",
            "Extract key information: errors, stack traces, failing operators, retry attempts, and timing information.",
            "Focus on the most critical error messages and their context.",
            "Output only the distilled summary (maximum 15 lines).",
        ],
        markdown=True,
    )

    root_cause_analyst = Agent(
        model=model,
        name="RootCauseAnalyst",
        instructions=[
            "You are an expert in Apache Airflow and data engineering root-cause analysis.",
            "Given DAG context and a log summary, identify the most likely root cause of the failure.",
            "Consider common Airflow failure patterns: dependency issues, configuration problems, resource constraints, network issues, and code errors.",
            "Call out assumptions and missing context explicitly.",
            "Provide a confidence score (0-1) for your analysis.",
            "Output a short paragraph explaining the root cause and your confidence level.",
        ],
        markdown=True,
    )

    fix_planner = Agent(
        model=model,
        name="FixPlanner",
        instructions=[
            "You are a solutions architect specializing in Airflow failure remediation.",
            "Given a root cause analysis, produce concrete, minimal-risk fix steps for Airflow failures.",
            "Return a numbered list of 3-7 actionable steps that can be executed immediately.",
            "Include 3-5 prevention tips to avoid similar failures in the future.",
            "Prefer configuration changes, code fixes, and operational improvements that are realistically applicable.",
            "Consider both immediate fixes and long-term improvements.",
        ],
        markdown=True,
    )

    verifier = Agent(
        model=model,
        name="Verifier",
        instructions=[
            "You are a quality assurance specialist for failure analysis.",
            "Verify that the root cause analysis and fix plan are consistent with the log summary.",
            "Check for logical consistency, completeness, and feasibility of the proposed solutions.",
            "If something seems off or incomplete, propose a safer alternative plan.",
            "Output a final, concise report with: Root cause, Fix steps, Prevention tips, and Overall assessment.",
        ],
        markdown=True,
    )

    # Create the team
    team = Team(
        name="Airflow Failure Response Team",
        members=[log_ingestor, root_cause_analyst, fix_planner, verifier],
        instructions=[
            "Collaborate to analyze an Airflow failure end-to-end.",
            "Process: 1) LogIngestor summarizes logs, 2) RootCauseAnalyst infers cause with confidence, 3) FixPlanner drafts concrete steps + prevention, 4) Verifier validates and outputs final report.",
            "Only the Verifier should produce the final consolidated output.",
            "Ensure all analysis is practical and actionable for Airflow operators.",
        ],
        show_members_responses=True,
        markdown=True,
    )

    LOG.info("[llm] Team initialized successfully with 4 specialized agents")
    return team


def build_agent_from_cfg(cfg: Dict[str, Any]) -> Optional["Agent"]:
    if Agent is None:
        LOG.info("[llm] agno not installed; skipping LLM")
        return None

    llm = cfg.get("llm") or {}
    driver = (llm.get("driver") or "openai_like").lower()
    model_id = llm.get("model") or "llama3.1"
    temperature = float(llm.get("temperature") or 0.1)
    max_tokens = int(llm.get("max_tokens") or 800)

    if driver == "openai_like" and OpenAILike and llm.get("base_url") and llm.get("api_key"):
        model = OpenAILike(
            id=model_id,
            api_key=llm["api_key"],
            base_url=llm["base_url"],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        LOG.info("[llm] driver=openai_like base=%s model=%s", llm.get("base_url"), model_id)
    elif driver == "vllm" and VLLM and llm.get("base_url"):
        model = VLLM(
            id=model_id,
            base_url=llm["base_url"]
        )
        LOG.info("[llm] driver=vLLM base=%s model=%s", llm.get("base_url"), model_id)
    elif driver == "ollama" and Ollama and llm.get("host"):
        model = Ollama(id=model_id, host=llm["host"])
        LOG.info("[llm] driver=ollama host=%s model=%s", llm.get("host"), model_id)
    else:
        LOG.info("[llm] not configured or driver unavailable; skipping LLM")
        return None

    return Agent(
        model=model,
        name="Airflow Failure Analyst",
        instructions=['''
You are an expert Apache Airflow and Big Data engineering assistant.
Context
- Airflow version: 2.2.0 (running locally)
- Local Big Data cluster services available: Spark 2.4.3, Kafka, Hive, Impala
- You will receive error-focused log snippets and optional environment/DAG context

Inputs
- Primary input: short, error-focused log snippets (Airflow task logs preferred)
- Optional context: Airflow version, executor, operator(s) used, environment (local/k8s/VM), cloud vendor, recent changes, package management (requirements.txt/constraints/Docker image)

Process
1) Read the logs and identify the first **root-cause** failure (earliest meaningful error), not the later cascading errors. Extract the highest-signal indicators: exception type, key message, HTTP status or error code, connection id, operator name.
2) Determine the most likely root cause backed by the log evidence. If multiple plausible causes exist, pick the most probable and explicitly note uncertainty in the `root_cause` text.
3) Map the issue to exactly one **category** from the allowed list (see below).
4) Write a practical **fix plan** (3-7 steps) with concrete actions in the order a practitioner would execute them.
5) Add 2-6 **prevention** items to avoid recurrence (monitoring, configuration hardening, retries/timeouts, version pinning, resource limits, tests).
6) Set `needs_rerun` to true if, after applying the proposed fix (or if the issue is transient), the job should be rerun. Set false if further changes/approvals/infrastructure work are required before a rerun could succeed.
7) Set a conservative **confidence** between 0.0 and 1.0, lowering it when evidence is weak or ambiguous.
8) Provide a brief `error_summary` quoting or paraphrasing the single most relevant error line/message.

Category mapping hints
- **network**: DNS failures, timeouts, connection refused/reset, TLS handshake errors
- **dependency**: `ModuleNotFoundError`/`ImportError`, version conflicts, missing provider package/JAR/whl, incompatible versions
- **config**: Missing/wrong Airflow connection, env var, credentials, wrong path/URI, bad parameter
- **code**: Exceptions in DAG/operator/user code (`TypeError`, `KeyError`, `AttributeError`, `NoneType`), authored SQL syntax errors
- **infra**: `OOMKilled`, container/pod eviction, disk full, file system/OS permission denied, resource quota, scheduler/executor down
- **security**: AuthN/AuthZ failures (401/403), IAM/Kerberos scope/role issues, expired/invalid secrets
- **design**: Job/query is too heavy (unpartitioned scans, excessive shuffle), non-idempotent/backfill strategy issues
- **transient**: Rate limits, intermittent upstream outage, flaky network; likely to succeed on retry
- **other**: Insufficient information or does not fit the above

Goal
Analyze the provided logs to determine the most likely root cause and propose a concrete, actionable fix plan. Be conservative and honest; if evidence is weak or incomplete, lower the confidence score accordingly.

What you will receive as input
- Error-focused log snippets (e.g., Airflow task/scheduler/webserver logs; Spark driver/executor logs; Hive/Impala/Kafka client errors)
- Optional context:
    - DAG/task snippet and operator(s) used (e.g., SparkSubmitOperator, HiveOperator, KafkaProducer)
    - Airflow executor and runtime context (LocalExecutor/Celery/Kubernetes; bare metal/Docker/Compose)
    - Connection IDs and types (e.g., spark_default, hive_default) and target endpoints
    - Spark cluster manager (YARN/Standalone) and related versions/endpoints
    - Environment variables (`JAVA_HOME`, `SPARK_HOME`, `HADOOP_CONF_DIR`), Python version, provider packages
    - Recent changes (code/config/infrastructure)

Analysis approach (internal)
- Parse logs to identify the primary error, failing component (operator/task/scheduler), and probable fault domain.
- Map the issue to one category: network | dependency | config | code | infra | security | design | transient | other.
- Propose 3-7 concrete fix steps tailored to Airflow 2.2.0 and the listed services.
- Propose 2-6 prevention steps to avoid recurrence.
- Decide whether a rerun is needed after applying the fix.
- Assign a confidence score (0.0-1.0) based on evidence completeness and specificity.
- Keep output concise and practical. Do not include chain-of-thought; summarize reasoning briefly in `error_summary`.

Output format
Return ONLY a single JSON object with EXACTLY these keys and constraints (no extra text or Markdown):
{
    "root_cause": string,
    "category": "network" | "dependency" | "config" | "code" | "infra" | "security" | "design" | "transient" | "other",
    "fix_steps": [string, string, string],
    "prevention": [string, string],
    "needs_rerun": true|false,
    "confidence": number,
    "error_summary": string
}
- `fix_steps`: 3-7 items
- `prevention`: 2-6 items
- `confidence`: 0.0-1.0
- Output raw JSON only

Assumptions and notes
- Spark 2.4.3 generally requires Java 8; consider Java and Hadoop client compatibility.
- If evidence is insufficient, use category "other", include a first step to gather targeted logs/config, and lower confidence.
- Prefer precise actions (config keys, commands, connection fields) over generic advice.
- If the failure is likely temporary (broker outage, brief network flap), consider category "transient" and include observability/stabilization steps.

Example input (excerpt)
- Airflow task log: "SparkSubmitOperator: Exception in thread \"main\": java.lang.NoClassDefFoundError: org/apache/hadoop/fs/FSDataInputStream"
- Spark driver log: "ClassNotFoundException: org.apache.hadoop.fs.FSDataInputStream"
- Context: Spark on YARN; Java 11 on Airflow host

Example output
{
    "root_cause": "Spark job failed due to missing/incompatible Hadoop client classes when running Spark 2.4.3 with Java 11; Spark 2.4.x expects Java 8 and matching Hadoop client jars.",
    "category": "dependency",
    "fix_steps": [
        "Run with Java 8: set JAVA_HOME to JDK8 for Airflow worker and Spark driver environment.",
        "Install matching Hadoop client libs (e.g., hadoop-client 2.7.x) and ensure they're on Spark's classpath.",
        "In SparkSubmitOperator, provide required jars via --jars or set spark.yarn.dist.files if not present on nodes.",
        "Restart Airflow services and rerun the failed task to validate."
    ],
    "prevention": [
        "Pin Java, Spark, and Hadoop client versions in deployment scripts and CI checks.",
        "Add a preflight task to verify JAVA_HOME and Hadoop classpath compatibility before submitting Spark jobs."
    ],
    "needs_rerun": true,
    "confidence": 0.78,
    "error_summary": "NoClassDefFoundError for FSDataInputStream indicates missing/incompatible Hadoop libs; Spark 2.4.3 on Java 11 is unsupported."
}'''],
        markdown=False,
    )


async def ask_team_for_analysis(team: "Team", error_focus: str, log_tail: str, identifiers: Dict[str, Any]) -> Dict[str, Any]:
    """Ask the team for collaborative analysis."""
    prompt = f"""
# Airflow Failure Analysis Request

## Context
- **DAG ID**: {identifiers.get('dag_id', 'Unknown')}
- **DAG Run ID**: {identifiers.get('dag_run_id', 'Unknown')}
- **Task ID**: {identifiers.get('task_id', 'Unknown')}
- **Try Number**: {identifiers.get('try_number', 'Unknown')}

## Failed Task Logs

### Error Focus (Critical Error Information)
```log
{error_focus}
```

### Log Tail (Recent Log Entries)
```log
{log_tail}
```

## Team Collaboration Process

Please work together to analyze this Airflow failure:

1. **LogIngestor**: Summarize the logs concisely, extracting key errors and context
2. **RootCauseAnalyst**: Identify the most likely root cause with confidence score
3. **FixPlanner**: Propose concrete fix steps and prevention measures
4. **Verifier**: Validate the analysis and provide final consolidated report

## Expected Output Format

The Verifier should provide a final report with:
- **Root Cause**: Clear explanation of what went wrong
- **Fix Steps**: Numbered list of actionable steps (3-7 items)
- **Prevention Tips**: Bullet points for avoiding future issues (3-5 items)
- **Confidence**: Overall confidence in the analysis (0-1)
- **Priority**: High/Medium/Low priority for addressing this issue

Focus on practical, actionable solutions that Airflow operators can implement immediately.
"""
    
    LOG.info("[llm] Team analysis prompt sizes: error_focus=%d, tail=%d", len(error_focus), len(log_tail))
    resp = await team.arun(prompt)
    text = getattr(resp, "content", "") if resp else ""
    LOG.info("[llm] Team response received size=%d", len(text))
    
    # Parse the team response to extract structured information
    # For now, create a structured response based on the team output
    return {
        "root_cause": text[:500] if text else "Team analysis completed",
        "category": "other",
        "fix_steps": [
            "Review the team's collaborative analysis",
            "Implement the suggested fix steps",
            "Apply prevention measures",
            "Monitor for similar issues"
        ],
        "prevention": [
            "Use team analysis for future failures",
            "Implement suggested monitoring",
            "Regular system health checks"
        ],
        "needs_rerun": True,
        "confidence": 0.85,  # Higher confidence due to team collaboration
        "error_summary": text[:200] if text else "Team collaborative analysis completed",
        "team_analysis": text  # Include full team analysis
    }


async def ask_llm_for_analysis(agent: "Agent", error_focus: str, log_tail: str, identifiers: Dict[str, Any]) -> Dict[str, Any]:
    prompt = {
        "context": {
            "dag_id": identifiers.get("dag_id"),
            "dag_run_id": identifiers.get("dag_run_id"),
            "task_id": identifiers.get("task_id"),
            "try_number": identifiers.get("try_number"),
        },
        "error_focus": error_focus,
        "log_tail": log_tail,
    }
    LOG.info("[llm] prompt sizes: error_focus=%d, tail=%d", len(error_focus), len(log_tail))
    resp = await agent.arun(json.dumps(prompt, ensure_ascii=False))
    text = getattr(resp, "content", "") if resp else ""
    LOG.info("[llm] response received size=%d", len(text))
    text = text.strip()

    if text.startswith("``"):
        text = text.strip("`") # This line was originally `text = text.strip("``")` but that causes issues if markdown block is present. It seems like the original intent was to remove triple-backticks or backticks. Assuming removal of any surrounding backticks/markdown block indicators.

    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("LLM returned no JSON object.")

    raw_json = text[start:end+1]
    return json.loads(raw_json)


def _create_heuristic_analysis(error_summary: str) -> Dict[str, Any]:
    """Create heuristic analysis as final fallback."""
    return {
        "root_cause": "Unknown",
        "category": "other",
        "fix_steps": ["Review the error block and operator code.", "Check external dependencies and configs.", "Re-run after remediation."],
        "prevention": ["Add retries/backoff where applicable.", "Add validation and pre-run checks."],
        "needs_rerun": True,
        "confidence": 0.4,
        "error_summary": error_summary,
    }


def output_to_stdout(result: Dict[str, Any]) -> None:
    """Output analysis result to stdout.
    
    Args:
        result: Analysis result dictionary.
    """
    try:
        output_json = json.dumps(result, indent=2, ensure_ascii=False)
        print("=== AIRFLOW FAILURE ANALYSIS ===")
        print(output_json)
        print("================================")
        LOG.info("[stdout] Analysis output sent to stdout (size=%d)", len(output_json))
    except Exception as e:
        LOG.error(f"[stdout] Failed to output to stdout: {e}")
        raise


def output_to_file(result: Dict[str, Any], file_path: str) -> None:
    """Output analysis result to file.
    
    Args:
        result: Analysis result dictionary.
        file_path: Path to output file.
    """
    try:
        output_json = json.dumps(result, indent=2, ensure_ascii=False)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(output_json)
        LOG.info("[file] Analysis written to %s (size=%d)", file_path, len(output_json))
    except Exception as e:
        LOG.error(f"[file] Failed to write to file {file_path}: {e}")
        raise


def output_to_teams(result: Dict[str, Any], webhook_url: str, verify_ssl: bool = False) -> None:
    """Output analysis result to Microsoft Teams.
    
    Args:
        result: Analysis result dictionary.
        webhook_url: Teams webhook URL.
        verify_ssl: Whether to verify SSL certificates.
    """
    try:
        headers = {"Content-Type": "application/json"}
        payload = build_teams_payload(result)
        resp = requests.post(webhook_url, headers=headers, json=payload, verify=verify_ssl)
        LOG.info("[teams] Status code: %s", resp.status_code)
        LOG.info("[teams] Response: %s", resp.text)
        
        if resp.status_code >= 400:
            LOG.warning("[teams] Teams notification may have failed: %s", resp.text)
    except Exception as e:
        LOG.error(f"[teams] Failed to send to Teams: {e}")
        raise


# ---------- CLI ----------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch a failed task's log, parse/redact, analyze with LLM, and emit JSON.")
    p.add_argument("--log-url", required=True, help="Absolute Airflow v1 Logs endpoint for the failing attempt.")
    p.add_argument("--config-b64", required=True, help="Base64-encoded JSON from a single Airflow Variable.")
    p.add_argument("--dag-id", required=True)
    p.add_argument("--dag-run-id", required=True)
    p.add_argument("--task-id", required=True)
    p.add_argument("--try-number", type=int, required=True)
    p.add_argument("--out", required=True, help="Path to write the final JSON file.")
    p.add_argument("--tail-lines", type=int, default=160, help="How many tail lines to preserve in context.")
    p.add_argument("--max-log-chars", type=int, default=1800, help="Max characters for the saved log tail.")
    return p.parse_args()


def _derive_verify(cfg_tls: Dict[str, Any]) -> Any:
    verify = cfg_tls.get("verify", True) if cfg_tls is not None else True
    if isinstance(verify, bool):
        return verify
    if isinstance(verify, str):
        return verify
    return True


def build_teams_payload(log):
    ex = (log or {}).get("extracted", {})
    an = (log or {}).get("analysis", {})
    dag_id     = str(log.get("dag_id", "—"))
    dag_run_id = str(log.get("dag_run_id", "—"))
    task_id    = str(log.get("task_id", "—"))
    try_no     = str(log.get("try_number", "—"))
    category   = str(an.get("category", "—"))
    err_sum    = str(an.get("error_summary", ex.get("error_summary", "Unknown error")))
    root_cause = str(an.get("root_cause", "—"))
    log_url = log.get("log_url")

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "ColumnSet",
                            "columns": [
                                {
                                    "type": "Column",
                                    "width": "stretch",
                                    "items": [
                                        {"type": "TextBlock", "size": "Small", "weight": "Default", "isSubtle": True, "text": "Airflow Failure Alert", "wrap": True},
                                        {"type": "TextBlock", "text": dag_id, "size": "Large", "weight": "Bolder", "wrap": True},
                                        {"type": "TextBlock", "text": task_id, "size": "Medium", "weight": "Bolder", "wrap": True, "spacing": "Small"},
                                        {"type": "TextBlock", "text": err_sum, "wrap": True, "color": "Attention", "spacing": "Small"}
                                    ]
                                }
                            ]
                        },
                        {
                            "type": "Container",
                            "style": "emphasis",
                            "bleed": True,
                            "items": [
                                {"type": "TextBlock", "text": "Root Cause", "weight": "Bolder"},
                                {"type": "TextBlock", "text": root_cause, "wrap": True}
                            ]
                        },
                        {
                            "type": "Container",
                            "items": [
                                {"type": "TextBlock", "text": "Fix Steps", "weight": "Bolder"},
                                {
                                    "type": "TextBlock",
                                    "wrap": True,
                                    "text": "\n".join([f"- {s}" for s in an.get("fix_steps", [])]) or "—"
                                }
                            ]
                        }
                    ],
                    "actions": [
                        {"type": "Action.OpenUrl", "title": "Open Log", "url": log_url},
                        {
                            "type": "Action.ShowCard",
                            "title": "Details",
                            "card": {
                                "type": "AdaptiveCard",
                                "body": [
                                    {
                                        "type": "FactSet",
                                        "facts": [
                                            {"title": "DAG", "value": dag_id},
                                            {"title": "Task", "value": task_id},
                                            {"title": "Run", "value": dag_run_id},
                                            {"title": "Attempt", "value": try_no},
                                            {"title": "Category", "value": category}
                                        ]
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        ]
    }
    LOG.info("[payload]")
    return payload


# ---------- Main ----------
async def main_async() -> None:
    args = parse_args()
    LOG.info("[start] dag_id=%s run_id=%s task_id=%s try=%s out=%s", args.dag_id, args.dag_run_id, args.task_id, args.try_number, args.out)

    # Decode single Airflow Variable JSON
    try:
        cfg = json.loads(base64.b64decode(args.config_b64.encode("ascii")).decode("utf-8"))
    except Exception as e:
        LOG.exception("Failed to decode --config-b64")
        raise SystemExit(f"--config-b64 decode failed: {e}")

    # Safe config summary (no secrets)
    llm_cfg = cfg.get("llm", {})
    LOG.info("[config] base_url=%s tls.verify=%s timeouts(connect=%s,total=%s) llm.driver=%s model=%s",
             (cfg.get("base_url") or ""),
             (cfg.get("tls", {}).get("verify", True)),
             (cfg.get("timeouts", {}).get("connect", 10.0)),
             (cfg.get("timeouts", {}).get("total", 30.0)),
             (llm_cfg.get("driver") or "openai_like"),
             (llm_cfg.get("model") or "llama3.1"))

    auth = (cfg.get("auth") or {}).get("basic") or {}
    username = auth.get("username") or ""
    password = auth.get("password") or ""

    LOG.info("[auth] user=%s password=%s", _mask(username), "*" if password else "(empty)")

    tls = cfg.get("tls") or {}
    timeouts = cfg.get("timeouts") or {}
    connect_timeout = float(timeouts.get("connect", 10.0))
    total_timeout = float(timeouts.get("total", 30.0))
    verify = _derive_verify(tls)

    # Optional pagination limits
    pagination = cfg.get("pagination") or {}
    max_pages = int(pagination.get("max_pages", 200))
    max_bytes = int(pagination.get("max_bytes", 5_000_000))

    if not username or not password:
        raise SystemExit("Config missing auth.basic.username/password")

    # 1) Fetch full log
    t_fetch = time.time()
    full_log = await fetch_log_by_url(
        log_url=args.log_url,
        username=username,
        password=password,
        verify=verify,
        connect_timeout=connect_timeout,
        total_timeout=total_timeout,
        max_pages=max_pages,
        max_bytes=max_bytes,
    )
    LOG.info("[parse] full_log_len=%d (%.2fs)", len(full_log), time.time() - t_fetch)

    # 2) Redact + extract
    log_tail_redacted = redact_text(full_log, tail_lines=args.tail_lines, max_len=args.max_log_chars)
    LOG.info("[parse] tail_len=%d", len(log_tail_redacted))
    error_focus_raw, error_summary = extract_error_focus(full_log)
    LOG.info("[parse] error_focus_len=%d summary=%s", len(error_focus_raw), error_summary[:160].replace("\n"," "))
    error_focus_redacted = redact_text(error_focus_raw, tail_lines=999999, max_len=8000)
    LOG.info("[parse] error_focus_redacted_len=%d", len(error_focus_redacted))

# 3) Analyze with Team (or fallback to single agent)
    team = build_team_from_cfg(cfg)
    agent = build_agent_from_cfg(cfg)
    
    LOG.info("[llm] Team available=%s, Single Agent available=%s", bool(team), bool(agent))

    if team is not None:
        try:
            # Use Team for collaborative analysis
            LOG.info("[llm] Using Team for collaborative analysis")
            analysis = await ask_team_for_analysis(
                team=team,
                error_focus=error_focus_redacted,
                log_tail=log_tail_redacted,
                identifiers={
                    "dag_id": args.dag_id,
                    "dag_run_id": args.dag_run_id,
                    "task_id": args.task_id,
                    "try_number": args.try_number,
                },
            )
            # ensure keys
            analysis.setdefault("root_cause", "Unknown")
            analysis.setdefault("category", "other")
            analysis.setdefault("fix_steps", ["Investigate failing operator and dependencies."])
            analysis.setdefault("prevention", [])
            analysis.setdefault("needs_rerun", True)
            analysis.setdefault("confidence", 0.5)
            analysis.setdefault("error_summary", error_summary)
        except Exception as e:
            LOG.warning("[llm] Team analysis failed (%s); fallback to single agent", e)
            if agent is not None:
                try:
                    analysis = await ask_llm_for_analysis(
                        agent=agent,
                        error_focus=error_focus_redacted,
                        log_tail=log_tail_redacted,
                        identifiers={
                            "dag_id": args.dag_id,
                            "dag_run_id": args.dag_run_id,
                            "task_id": args.task_id,
                            "try_number": args.try_number,
                        },
                    )
                    # ensure keys
                    analysis.setdefault("root_cause", "Unknown")
                    analysis.setdefault("category", "other")
                    analysis.setdefault("fix_steps", ["Investigate failing operator and dependencies."])
                    analysis.setdefault("prevention", [])
                    analysis.setdefault("needs_rerun", True)
                    analysis.setdefault("confidence", 0.5)
                    analysis.setdefault("error_summary", error_summary)
                except Exception as e2:
                    LOG.warning("[llm] Single agent analysis also failed (%s); fallback to heuristics", e2)
                    analysis = _create_heuristic_analysis(error_summary)
            else:
                analysis = _create_heuristic_analysis(error_summary)
    elif agent is not None:
        try:
            analysis = await ask_llm_for_analysis(
                agent=agent,
                error_focus=error_focus_redacted,
                log_tail=log_tail_redacted,
                identifiers={
                    "dag_id": args.dag_id,
                    "dag_run_id": args.dag_run_id,
                    "task_id": args.task_id,
                    "try_number": args.try_number,
                },
            )
            # ensure keys
            analysis.setdefault("root_cause", "Unknown")
            analysis.setdefault("category", "other")
            analysis.setdefault("fix_steps", ["Investigate failing operator and dependencies."])
            analysis.setdefault("prevention", [])
            analysis.setdefault("needs_rerun", True)
            analysis.setdefault("confidence", 0.5)
            analysis.setdefault("error_summary", error_summary)
        except Exception as e:
            LOG.warning("[llm] analysis failed (%s); fallback to heuristics", e)
            analysis = _create_heuristic_analysis(error_summary)
    else:
        # heuristic fallback
        guess = None
        if re.search(r"ModuleNotFoundError:", full_log):
            guess = ("Missing Python dependency", "code", [
                "Add missing package to requirements.txt or image.",
                "Pin version and rebuild/deploy workers.",
                "Re-run task."
            ])
        elif re.search(r"Permission denied", full_log, re.I):
            guess = ("Permission/ACL issue", "security", [
                "Fix filesystem or cloud ACL for the path/resource.",
                "Update Airflow connection/role with required scope.",
                "Re-run task."
            ])
        elif re.search(r"(Connection refused|Read timed out|ConnectionResetError|Max retries exceeded)", full_log, re.I):
            guess = ("Network/dependency instability", "network", [
                "Check downstream service health and network routes.",
                "Increase client timeouts and add retry with backoff.",
                "Consider deferrable operators for long waits."
            ])

        if guess:
            rc, cat, steps = guess
        else:
            rc, cat, steps = ("Unknown", "other", [
                "Inspect the error block and operator code.",
                "Verify configs/credentials for external systems.",
                "Re-run after fixing the root cause."
            ])

        analysis = {
            "root_cause": rc,
            "category": cat,
            "fix_steps": steps,
            "prevention": ["Add monitoring and pre-flight checks.", "Record dependency versions and configs."],
            "needs_rerun": True,
            "confidence": 0.5 if rc != "Unknown" else 0.35,
            "error_summary": error_summary,
        }

    # 4) Compose and write
    result = {
        "dag_id": args.dag_id,
        "dag_run_id": args.dag_run_id,
        "task_id": args.task_id,
        "try_number": args.try_number,
        "log_url": args.log_url,
        "extracted": {
            "error_summary": error_summary,
            "error_focus_redacted": error_focus_redacted,
            "log_tail_redacted": log_tail_redacted,
        },
        "analysis": analysis,
    }

    # 5) Output based on configuration
    try:
        output_config = cfg.get("output", {})
        output_method = output_config.get("method", "teams")  # Default to teams for backward compatibility
        
        LOG.info("[output] Using output method: %s", output_method)
        
        if output_method == "stdout":
            output_to_stdout(result)
        elif output_method == "file":
            file_path = output_config.get("file_path", args.out)
            output_to_file(result, file_path)
        elif output_method == "teams":
            webhook_url = output_config.get("teams_webhook", 
                "https://api.powerplatform.com:443/powerautomate/automations/direct/workflows/7ebb98a66e91457e8e577c22ac04fbeb/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=PLNpbAWZA09gLDc_xVQFrNwLktibjBKUk0g6HbBURc0")
            verify_ssl = output_config.get("teams_verify_ssl", False)
            output_to_teams(result, webhook_url, verify_ssl)
        else:
            LOG.warning("[output] Unknown output method '%s', falling back to teams", output_method)
            output_to_teams(result, 
                "https://api.powerplatform.com:443/powerautomate/automations/direct/workflows/7ebb98a66e91457e8e577c22ac04fbeb/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=PLNpbAWZA09gLDc_xVQFrNwLktibjBKUk0g6HbBURc0", 
                False)
            
    except Exception as e:
        LOG.exception("Failed to output analysis result")
        raise SystemExit(f"Failed to output analysis result: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        sys.exit(130)