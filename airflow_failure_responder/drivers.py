"""Optional imports for Agno and vector DB backends.

This module keeps import errors isolated so the main program can still run
without optional dependencies installed.
"""
from typing import Any

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
    VLLM = None  # type: ignore

try:
    from agno.models.ollama import Ollama
except Exception:
    Ollama = None  # type: ignore

# Agno 2.x knowledge
try:
    from agno.knowledge import Knowledge
    from agno.knowledge.embedder.openai import OpenAIEmbedder
    from agno.knowledge.embedder.ollama import OllamaEmbedder
except Exception:
    Knowledge = None  # type: ignore
    OpenAIEmbedder = None  # type: ignore
    OllamaEmbedder = None  # type: ignore

# Vector DBs
try:
    from agno.vectordb.pgvector import PgVector
except Exception:
    PgVector = None  # type: ignore

try:
    from agno.vectordb.chroma import Chroma
except Exception:
    Chroma = None  # type: ignore

try:
    from agno.vectordb.lancedb import LanceDb, SearchType
except Exception:
    LanceDb = None  # type: ignore
    SearchType = None  # type: ignore

if SearchType is None:
    from enum import Enum

    class SearchType(Enum):
        SIMILARITY = 'similarity'
        HYBRID = 'hybrid'
        MMR = 'mmr'
