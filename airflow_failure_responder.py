import argparse
import asyncio
import base64
import codecs
import json
import logging
import os
import re
import sys
import time
import requests
from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel

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

# Agno 2.0.3 imports
try:
    from agno.knowledge import Knowledge
    from agno.knowledge.embedder.openai import OpenAIEmbedder
    from agno.knowledge.embedder.ollama import OllamaEmbedder
except Exception:
    Knowledge = None
    OpenAIEmbedder = None
    OllamaEmbedder = None

# Vector databases (separate imports due to optional dependencies)
try:
    from agno.vectordb.pgvector import PgVector
except Exception:
    PgVector = None

try:
    from agno.vectordb.chroma import Chroma
except Exception:
    Chroma = None

try:
    from agno.vectordb.lancedb import LanceDb, SearchType
except Exception:
    LanceDb = None
    SearchType = None


# ---------- Pydantic Models ----------
class KnowledgeSource(BaseModel):
    """Represents a single knowledge artifact used to ground the answer.

    Attributes:
        title: Human-readable title of the source document or section (e.g., PDF filename).
        path: Local or network file path for the PDF when available.
        page: 1-based page number in the PDF if applicable.
        chunk_id: Implementation-defined chunk identifier within the vector store.
        snippet: Short excerpt that was retrieved and used for grounding.
    """
    title: str
    path: Optional[str] = None
    page: Optional[int] = None
    chunk_id: Optional[str] = None
    snippet: Optional[str] = None

class AirflowFailureAnalysis(BaseModel):
    """Pydantic model for Airflow failure analysis output."""
    root_cause: str
    category: str  # "network" | "dependency" | "config" | "code" | "infra" | "security" | "design" | "transient" | "other"
    fix_steps: List[str]
    prevention: List[str]
    needs_rerun: bool
    confidence: float
    error_summary: str
    used_knowledge: bool
    knowledge_sources: Optional[List[KnowledgeSource]] = None

# ---------- Redaction ----------
SECRET_PATTERNS = [
    # AWS credentials
    r"AKIA[0-9A-Z]{16}",
    r"\baws_secret_access_key\b\s*[:=]\s*[A-Za-z0-9/+=]{30,}",
    r"\baws_access_key_id\b\s*[:=]\s*[A-Za-z0-9/+=]{20,}",
    r"\baws_session_token\b\s*[:=]\s*[A-Za-z0-9/+=]{100,}",
    
    # Generic secrets
    r"\bsecret\b\s*[:=]\s*[^,\s'\";]+",
    r"\bpassword\b\s*[:=]\s*[^,\s'\";]+",
    r"\btoken\b\s*[:=]\s*[^,\s'\";]+",
    r"\bkey\b\s*[:=]\s*[A-Za-z0-9/+=]{20,}",
    r"\bapi_key\b\s*[:=]\s*[A-Za-z0-9/+=]{20,}",
    
    # JWT tokens
    r"authorization:\s*bearer\s+[A-Za-z0-9.\-]+",
    r"\beyJ[a-zA-Z0-9_\-]{10,}\.[a-zA-Z0-9.\-]{10,}\.[a-zA-Z0-9.\-]{10,}\b",
    
    # Email addresses
    r"[A-Za-z0-9.%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
    
    # Database connection strings
    r"postgresql://[^@]+@[^/]+",
    r"mysql://[^@]+@[^/]+",
    r"mongodb://[^@]+@[^/]+",
    r"redis://[^@]+@[^/]+",
    
    # URLs with credentials
    r"https?://[^@]+@[^/\s]+",
    
    # Private keys
    r"-----BEGIN [A-Z ]+ PRIVATE KEY-----",
    r"-----BEGIN [A-Z ]+ KEY-----",
]
SECRET_RE = re.compile("|".join(SECRET_PATTERNS), flags=re.IGNORECASE)


def redact_conn_strings(s: str) -> str:
    """Redact connection strings with credentials."""
    # Database connection strings
    s = re.sub(r"([a-zA-Z]+:\/\/)([^:@\s]+):([^@\/\s]+)@", r"\1[REDACTED]:[REDACTED]@", s)
    # URLs with credentials
    s = re.sub(r"(https?://)([^@]+)@([^/\s]+)", r"\1[REDACTED]@\3", s)
    return s


def redact_text(s: str) -> str:
    """Redact sensitive information from log text."""
    if not s:
        return ""
    
    # Redact connection strings first
    s = redact_conn_strings(s)
    
    # Redact other sensitive patterns
    s = SECRET_RE.sub("[REDACTED]", s)
    
    return s


# ---------- Log processing ----------
def crop_log_window(log_text: str,
                    *,
                    start_hint: Optional[str] = None,
                    end_hint: Optional[str] = None,
                    max_chars: int = 50000):
    """
    Crop a long Airflow task log to the high-signal window.
    """
    if not log_text:
        return ""
    
    lines = log_text.splitlines()

    start_patterns = []
    end_patterns = []

    if start_hint:
        start_patterns.append(re.escape(start_hint))
    
    else:
        start_patterns.extend([
            r"Using connection to",
            r"Spark-submit cmd:",
            r"Executing <Task\(SparkSubmitOperator\)",
            r"Started process .* to run task",
        ])
    
    if end_hint:
        end_patterns.append(re.escape(end_hint))
    
    else:
        end_patterns.extend([
            r"Task exited with return code \d+",
            r"Marking task as FAILED\.",
            r"airflow\.exceptions\.AirflowException",
        ])

    start_idx = None
    for i, line in enumerate(lines):
        if any(re.search(pat, line) for pat in start_patterns):
            start_idx = max(0, i)
            break

    end_idx = None
    if start_idx is not None:
        for j in range(start_idx, len(lines)):
            if any(re.search(pat, lines[j]) for pat in end_patterns):
                end_idx = min(len(lines) - 1, j)
                break
    
    if start_idx is None or end_idx is None:
        err_pat = re.compile(r"(Traceback|ERROR|CRITICAL|AnalysisException|AirflowException)", re.IGNORECASE)
        err_line = None
        for i in range(len(lines) - 1, -1, -1):
            if err_pat.search(lines[i]):
                err_line = i
                break
        
        if err_line is None:
            cropped = "\n".join(lines[-5000:])
            return cropped[-max_chars:] if len(cropped) > max_chars else cropped

    cropped_lines = lines[start_idx:end_idx + 1]
    cropped = "\n".join(cropped_lines)

    if len(cropped_lines) > max_chars:
        cropped_lines = cropped_lines[-max_chars:]
    
    return cropped
        

def process_logs(full_log: str, max_chars: int = 50000) -> str:
    """
    Process full task logs with redaction and intelligent truncation.
    Returns the processed log text ready for AI analysis.
    """
    if not full_log:
        return ""
    
    # Redact sensitive information
    processed_log = redact_text(full_log)
    
    # If log is too long, truncate intelligently
    if len(processed_log) > max_chars:
        lines = processed_log.splitlines()
        
        # Keep the beginning (task setup) and end (failure) of the log
        # This preserves context while staying within limits
        start_lines = lines[:max_chars // 4]  # First quarter
        end_lines = lines[-(max_chars // 2):]  # Last half
        
        # Add truncation indicator
        truncated_log = "\n".join(start_lines) + "\n\n... [LOG TRUNCATED] ...\n\n" + "\n".join(end_lines)
        processed_log = truncated_log
    
    return processed_log


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

async def setup_pdf_knowledge_base(cfg: Dict[str, Any]) -> Optional["Knowledge"]:
    """Set up PDF knowledge base from configuration using Agno 2.0.3 API."""
    if Knowledge is None:
        LOG.info("[pdf] Knowledge class not available; skipping")
        return None
    
    pdf_config = cfg.get("pdf_knowledge", {})
    pdf_path = pdf_config.get("path")
    
    if not pdf_path:
        LOG.info("[pdf] No PDF path configured; skipping knowledge base")
        return None
    
    if not os.path.exists(pdf_path):
        LOG.warning("[pdf] PDF file not found: %s", pdf_path)
        return None
    
    try:
        # Configure vector database
        vector_db_config = pdf_config.get("vector_db", {})
        vector_db_type = vector_db_config.get("type", "lancedb")
        
        # Configure embedder first (needed for vector database)
        embedder_config = pdf_config.get("embedder", {})
        embedder_type = embedder_config.get("type", "ollama")
        embedder = None
        
        if embedder_type == "openai" and OpenAIEmbedder and embedder_config.get("api_key"):
            embedder = OpenAIEmbedder(
                api_key=embedder_config["api_key"],
                base_url=embedder_config.get("base_url", "https://api.openai.com/v1"),
                id=embedder_config.get("id", "text-embedding-3-small")
            )
            LOG.info("[pdf] Using OpenAI embedder: %s", embedder_config.get("id", "text-embedding-3-small"))
        elif embedder_type == "ollama" and OllamaEmbedder:
            # Create Ollama embedder with optional host parameter
            ollama_kwargs = {
                "id": embedder_config.get("id", "nomic-embed-text")
            }
            if embedder_config.get("host"):
                ollama_kwargs["host"] = embedder_config["host"]
            
            embedder = OllamaEmbedder(**ollama_kwargs)
            LOG.info("[pdf] Using Ollama embedder: %s", embedder_config.get("id", "nomic-embed-text"))
        elif embedder_type == "custom" and embedder_config.get("base_url"):
            # Custom embedder for GPU cluster
            embedder = OpenAIEmbedder(
                api_key=embedder_config.get("api_key", "dummy-key"),  # Some endpoints don't require auth
                base_url=embedder_config["base_url"],
                id=embedder_config.get("id", "embedding"),
                headers=embedder_config.get("headers", {}),
                timeout=embedder_config.get("timeout", 30)
            )
            LOG.info("[pdf] Using custom GPU cluster embedder: %s at %s", 
                    embedder_config.get("id", "embedding"), 
                    embedder_config["base_url"])
        
        # Configure vector database with embedder
        vector_db = None
        if vector_db_type == "pgvector" and PgVector:
            vector_db = PgVector(
                table_name=vector_db_config.get("table_name", "pdf_documents"),
                db_url=vector_db_config.get("db_url", "postgresql+psycopg://ai:ai@localhost:5532/ai"),
                embedder=embedder,
            )
        elif vector_db_type == "chroma" and Chroma:
            vector_db = Chroma(
                collection_name=vector_db_config.get("collection_name", "pdf_documents"),
                persist_directory=vector_db_config.get("persist_directory", "./chroma_db"),
                embedder=embedder,
            )
        elif vector_db_type in ("lancedb", "lance", "lance_db") and LanceDb:
            # LanceDB local, file-based vector store
            vector_db = LanceDb(
                table_name=vector_db_config.get("table_name", "vectors"),
                uri=vector_db_config.get("uri", "./lancedb"),
                embedder=embedder,
            )
        
        if vector_db is None:
            LOG.warning("[pdf] No vector database configured; using default LanceDB")
            if LanceDb:
                vector_db = LanceDb(
                    table_name="vectors",
                    uri="./lancedb",
                    embedder=embedder,
                )
            elif Chroma:
                LOG.warning("[pdf] LanceDB not available; falling back to Chroma")
                vector_db = Chroma(
                    collection_name="pdf_documents", 
                    persist_directory="./chroma_db",
                    embedder=embedder,
                )
            else:
                LOG.warning("[pdf] No vector database available; creating knowledge base without vector DB")
        
        
        # Create Knowledge instance with Agno 2.0.3 API
        pdf_kb = Knowledge(
            name="pdf_knowledge_base",
            vector_db=vector_db,
        )
        
        # Add PDF reader
        pdf_reader = pdf_kb.pdf_reader
        pdf_reader.chunk = True  # Enable chunking
        pdf_kb.add_reader(pdf_reader)
        
        # Load the knowledge base
        LOG.info("[pdf] Loading PDF knowledge base from: %s", pdf_path)
        await pdf_kb.add_content_async(path=pdf_path)
        
        LOG.info("[pdf] PDF knowledge base loaded successfully")
        return pdf_kb
        
    except FileNotFoundError as e:
        LOG.error("[pdf] PDF file not found: %s", e)
        return None
    except PermissionError as e:
        LOG.error("[pdf] Permission denied accessing PDF file: %s", e)
        return None
    except ValueError as e:
        LOG.error("[pdf] Invalid PDF file or configuration: %s", e)
        return None
    except Exception as e:
        LOG.exception("[pdf] Failed to setup PDF knowledge base: %s", e)
        return None

async def build_team_from_cfg(cfg: Dict[str, Any]) -> Optional["Team"]:
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

    # Set up PDF knowledge base
    pdf_kb = await setup_pdf_knowledge_base(cfg)
    if pdf_kb:
        LOG.info("[pdf] PDF knowledge base available for agents")

    # Create specialized agents for the team
    log_ingestor = Agent(
        model=model,
        name="LogIngestor",
        instructions=[
            "You are a log analysis expert specializing in Airflow failure logs.",
            "Your ONLY job is to extract and summarize error information from full Airflow task logs.",
            "Input: Full Airflow task log text (complete log from start to failure)",
            "Process:",
            "1. Analyze the complete log to understand the task execution flow",
            "2. Identify where the task started, what it was doing, and where it failed",
            "3. Extract error messages, stack traces, failing operators, and retry attempts",
            "4. Focus on the most critical error messages and their context",
            "5. Provide a concise summary of what went wrong (maximum 15 lines)",
            "Output: Clean error summary containing key failure information",
            "Do NOT analyze root causes or provide solutions - just extract and summarize what failed.",
        ],
        knowledge=pdf_kb,
        search_knowledge=True,
        add_knowledge_to_context=False,
        markdown=False,
    )

    root_cause_analyst = Agent(
        model=model,
        name="RootCauseAnalyst",
        instructions=[
            "You are an expert in Apache Airflow and data engineering root-cause analysis.",
            "Your ONLY job is to analyze error summary and identify the root cause.",
            "Input: Error summary from LogIngestor (extracted from full task logs)",
            "Process:",
            "1. Search the knowledge base for similar error patterns and solutions",
            "2. Analyze the error summary to understand what went wrong",
            "3. Identify the PRIMARY root cause (not symptoms) using knowledge base insights",
            "4. Determine the failure category: network, dependency, config, code, infra, security, design, transient, other",
            "5. Explain why the error occurred based on documentation and past cases",
            "6. Assess confidence level (0.0-1.0)",
            "Output: Root cause analysis with category and confidence",
            "Use the knowledge base to find similar problems and their documented solutions.",
            "Do NOT provide solutions - just identify and categorize the problem.",
        ],
        knowledge=pdf_kb,
        search_knowledge=True,
        add_knowledge_to_context=False,
        markdown=False,
    )

    fix_planner = Agent(
        model=model,
        name="FixPlanner",
        instructions=[
            "You are a solutions architect specializing in Airflow failure remediation.",
            "Your ONLY job is to create solutions based on root cause analysis using your knowledge base.",
            "Input: Root cause analysis from RootCauseAnalyst",
            "Process:",
            "1. Search the knowledge base for documented solutions to similar problems",
            "2. Take the root cause analysis and understand the problem",
            "3. Find proven solutions from your PDF documentation and past cases",
            "4. Create 3-7 specific, actionable fix steps based on documented solutions",
            "5. Include 2-6 prevention measures from best practices in your knowledge base",
            "6. Determine if the task needs a rerun after fixes",
            "Output: Detailed fix plan with steps, prevention measures, and rerun recommendation",
            "CRITICAL: Always search your knowledge base first for documented solutions before creating new ones.",
            "Reference specific solutions from your PDF documents when available.",
            "Focus on practical, actionable solutions that Airflow operators can implement immediately.",
        ],
        knowledge=pdf_kb,
        search_knowledge=True,
        add_knowledge_to_context=False,
        markdown=False,
    )

    verifier = Agent(
        model=model,
        name="Verifier",
        instructions=[
            "You are a quality assurance specialist for failure analysis.",
            "Your ONLY job is to consolidate all team inputs into the final JSON response.",
            "Input: Error summary from LogIngestor, root cause analysis from RootCauseAnalyst, fix plan from FixPlanner",
            "Process:",
            "1. Take the error summary from LogIngestor",
            "2. Take the root cause analysis from RootCauseAnalyst",
            "3. Take the fix plan from FixPlanner",
            "4. Search your knowledge base to validate solutions against documented best practices",
            "5. Verify logical consistency between all inputs",
            "6. Ensure solutions are based on proven methods from your documentation",
            "7. Consolidate everything into the required JSON format",
            "",
            "CRITICAL: You must output ONLY a single JSON object with EXACTLY these keys:",
            """{
                "root_cause": string,
                "category": "network" | "dependency" | "config" | "code" | "infra" | "security" | "design" | "transient" | "other",
                "fix_steps": [string, string, string],
                "prevention": [string, string],
                "needs_rerun": true|false,
                "confidence": number,
                "error_summary": string,
                "used_knowledge": true|false,
                "knowledge_sources": [
                    {
                        "title": string,
                        "path": string | null,
                        "page": number | null,
                        "chunk_id": string | null,
                        "snippet": string | null
                    }
                ] | null
            }""",
            "- 'fix_steps' should be 3-7 items. don't use numbers in the list.",
            "- 'prevention' should be 2-6 items.",
            "- 'confidence' is 0.0-1.0.",
            "- Do NOT wrap in markdown; output raw JSON only.",
            "- Validate solutions against your Knowledge base documentation.",
            "- Set 'used_knowledge' to true if any Knowledge in Vector DB given in Knowledge was used; otherwise false.",
            "- If 'used_knowledge' is true, populate 'knowledge_sources' with the consulted PDF sources in Vector DB given in Knowledge only.",
            "- Keep 'snippet' short (<= 280 chars) and redact secrets.",
        ],
        knowledge=pdf_kb,
        search_knowledge=True,
        add_knowledge_to_context=False,
        markdown=False,
    )

    # Create the team
    team = Team(
        model=model,
        name="Airflow Failure Response Team",
        members=[log_ingestor, root_cause_analyst, fix_planner, verifier],
        output_schema=AirflowFailureAnalysis,
        instructions=[
            "You are a sequential AI team for Airflow failure analysis. Follow this EXACT workflow:",
            "",
            "1. LogIngestor processes the full task logs and extracts error information",
            "2. RootCauseAnalyst takes LogIngestor's output and identifies root cause using Knowledge base",
            "3. FixPlanner takes RootCauseAnalyst's output and creates solutions using Knowledge base",
            "4. Verifier takes ALL previous outputs and produces the final structured response",
            "",
            "Use the Knowledge base to find proven solutions and best practices.",
            "The output will be automatically structured according to the schema.",
        ],
        show_members_responses=True,
        markdown=False,
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


async def ask_team_for_analysis(team: "Team", full_log: str, identifiers: Dict[str, Any]) -> Dict[str, Any]:
    """Ask the team for sequential analysis workflow."""
    # Simple prompt since team already has detailed instructions
    prompt = f"""
# Airflow Failure Analysis

**DAG ID**: {identifiers.get('dag_id', 'Unknown')}
**Task ID**: {identifiers.get('task_id', 'Unknown')}

## Full Task Logs

{full_log}

Execute your sequential workflow and output ONLY the final JSON response from the Verifier.
"""
    
    LOG.info("[llm] Team analysis prompt size: full_log=%d", len(full_log))
    resp = await team.arun(prompt)
    
    # With output_schema, the response content should be a Pydantic model instance
    if hasattr(resp, 'content') and isinstance(resp.content, AirflowFailureAnalysis):
        LOG.info("[llm] Successfully received structured response from team")
        # Convert Pydantic model to dict
        result = resp.content.model_dump()
        return result
    else:
        LOG.error("[llm] Team response is not a valid AirflowFailureAnalysis model")
        raise ValueError("Team response is not a valid AirflowFailureAnalysis model")


async def ask_llm_for_analysis(agent: "Agent", full_log: str, identifiers: Dict[str, Any]) -> Dict[str, Any]:
    prompt = {
        "context": {
            "dag_id": identifiers.get("dag_id"),
            "dag_run_id": identifiers.get("dag_run_id"),
            "task_id": identifiers.get("task_id"),
            "try_number": identifiers.get("try_number"),
        },
        "full_log": full_log,
    }
    LOG.info("[llm] prompt size: full_log=%d", len(full_log))
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
    LOG.info("[start] dag_id=%s run_id=%s task_id=%s try=%s", args.dag_id, args.dag_run_id, args.task_id, args.try_number)

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

    # 2) Process full logs with redaction and intelligent truncation
    processed_log = process_logs(full_log, max_chars=50000)
    LOG.info("[parse] processed_log_len=%d", len(processed_log))

# 3) Analyze with Team (or fallback to single agent)
    team = await build_team_from_cfg(cfg)
    agent = build_agent_from_cfg(cfg)
    
    LOG.info("[llm] Team available=%s, Single Agent available=%s", bool(team), bool(agent))

    if team is not None:
        try:
            # Use Team for collaborative analysis
            LOG.info("[llm] Using Team for collaborative analysis")
            analysis = await ask_team_for_analysis(
                team=team,
                full_log=processed_log,
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
            # error_summary will be provided by the team response
        except Exception as e:
            LOG.warning("[llm] Team analysis failed (%s); fallback to single agent", e)
            if agent is not None:
                try:
                    analysis = await ask_llm_for_analysis(
                        agent=agent,
                        full_log=processed_log,
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
                    # error_summary will be provided by the agent response
                except Exception as e2:
                    LOG.warning("[llm] Single agent analysis also failed (%s); fallback to heuristics", e2)
                    analysis = _create_heuristic_analysis("Task failed - analysis unavailable")
            else:
                analysis = _create_heuristic_analysis("Task failed - analysis unavailable")
    elif agent is not None:
        try:
            analysis = await ask_llm_for_analysis(
                agent=agent,
                full_log=processed_log,
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
            # error_summary will be provided by the agent response
        except Exception as e:
            LOG.warning("[llm] analysis failed (%s); fallback to heuristics", e)
            analysis = _create_heuristic_analysis("Task failed - analysis unavailable")
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
            "error_summary": "Task failed - using heuristic analysis",
        }

    # 4) Compose and write
    result = {
        "dag_id": args.dag_id,
        "dag_run_id": args.dag_run_id,
        "task_id": args.task_id,
        "try_number": args.try_number,
        "log_url": args.log_url,
        "extracted": {
            "error_summary": analysis.get("error_summary", "Unknown error"),
            "processed_log_length": len(processed_log),
        },
        "analysis": analysis,
    }

    # 5) Output based on configuration
    try:
        output_config = cfg.get("output", {})
        output_method = output_config.get("method", "stdout") 
        
        LOG.info("[output] Using output method: %s", output_method)
        
        if output_method == "stdout":
            output_to_stdout(result)
        elif output_method == "file":
            file_path = output_config.get("file_path")
            if not file_path:
                file_path = "/tmp/failed_task_log.json"
                LOG.warning("[output] No file_path specified, using default: %s", file_path)
            output_to_file(result, file_path)
        elif output_method == "teams":
            webhook_url = output_config.get("teams_webhook")
            if not webhook_url:
                LOG.error("[output] Teams output method requires 'teams_webhook' in configuration")
                LOG.info("[output] Falling back to stdout")
                output_to_stdout(result)
            else:
                verify_ssl = output_config.get("teams_verify_ssl", False)
                output_to_teams(result, webhook_url, verify_ssl)
        else:
            LOG.warning("[output] Unknown output method '%s', falling back to stdout", output_method)
            output_to_stdout(result)
            
    except Exception as e:
        LOG.exception("Failed to output analysis result")
        raise SystemExit(f"Failed to output analysis result: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        sys.exit(130)