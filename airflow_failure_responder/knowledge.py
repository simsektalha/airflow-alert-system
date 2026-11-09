import os
import re
import hashlib
from typing import Any, Dict, Optional, Tuple, List
import logging
import requests

from drivers import Knowledge, OpenAIEmbedder, OllamaEmbedder, PgVector, Chroma, LanceDb, SearchType
from agno.db.postgres.postgres import PostgresDb
from bs4 import BeautifulSoup
from datetime import datetime

from logging_cfg import configure_logging

configure_logging()

LOG = logging.getLogger("airflow-failure-responder")


def setup_knowledge_base(cfg: Dict[str, Any]) -> Optional["Knowledge"]:
    """
    Initialize Knowledge base with configured embedder and vector database.
    No readers, no chunking, no ingestion here.
    """

    if Knowledge is None:
        LOG.info("[kb] Knowledge class not available; skipping")
        return None

    try:
        kb_config = cfg.get("knowledge", {}) or {}

        # ---------- Embedder ----------
        embedder_config = kb_config.get("embedder", {}) or {}
        embedder_type = embedder_config.get("type", "ollama")

        embedder = None

        if embedder_type == "openai" and OpenAIEmbedder:
            embedder = OpenAIEmbedder(
                api_key=embedder_config.get("api_key"),
                base_url=embedder_config.get("base_url", "https://api.openai.com/v1"),
                id=embedder_config.get("id", "text-embedding-3-small"),
            )
            LOG.info("[kb] Using OpenAI embedder: %s", embedder_config.get("id"))

        elif embedder_type == "ollama" and OllamaEmbedder:
            embedder = OllamaEmbedder(
                id=embedder_config.get("id", "nomic-embed-text"),
                dimensions=int(embedder_config.get("dimensions", 768)),
                host=embedder_config.get("host"),
            )
            LOG.info("[kb] Using Ollama embedder: %s", embedder_config.get("id"))

        elif embedder_type == "custom" and OpenAIEmbedder:
            embedder = OpenAIEmbedder(
                api_key=embedder_config.get("api_key", "dummy-key"),
                base_url=embedder_config.get("base_url"),
                id=embedder_config.get("id", "embedding"),
                dimensions=int(embedder_config.get("dimensions", 4096))
            )
            LOG.info("[kb] Using custom embedder: %s dimensions:%s at %s", embedder_config.get("id"), embedder_config.get("dimensions"), embedder_config.get("base_url"))

        else:
            LOG.warning("[kb] No valid embedder configured")
            return None

        # ---------- Vector Database ----------
        vector_db_config = kb_config.get("vector_db", {}) or {}
        vector_db_type = vector_db_config.get("type", "pgvector")

        vector_db = None

        if vector_db_type == "pgvector" and PgVector:
            vector_db = PgVector(
                table_name=vector_db_config.get("table_name", "knowledge_documents"),
                db_url=vector_db_config.get(
                    "db_url",
                    "postgresql+psycopg://ai:ai@localhost:5532/ai",
                ),
                embedder=embedder,
            )
            LOG.info(
                "[kb] Using PgVector table=%s",
                vector_db_config.get("table_name", "knowledge_documents"),
            )

        elif vector_db_type == "lancedb" and LanceDb:
            vector_db = LanceDb(
                table_name=vector_db_config.get("table_name", "knowledge_documents"),
                uri=vector_db_config.get("uri", "./lancedb"),
                embedder=embedder,
            )
            LOG.info("[kb] Using LanceDB")

        elif vector_db_type == "chroma" and Chroma:
            vector_db = Chroma(
                collection_name=vector_db_config.get("collection_name", "knowledge_documents"),
                persist_directory=vector_db_config.get("persist_directory", "./chroma_db"),
                embedder=embedder,
            )
            LOG.info("[kb] Using Chroma")

        else:
            LOG.warning("[kb] No valid vector database configured")
            return None

        # ---------- Knowledge ----------
        contents_db = PostgresDb(
            db_url=vector_db_config.get("db_url", "postgresql+psycopg://ai:ai@localhost:5532/ai"),
            knowledge_table="knowledge_contents",
        )

        test_vec = embedder.get_embedding("test")
        LOG.info("[embed] len(test)=%s", len(test_vec))

        kb = Knowledge(
            name="knowledge_base",
            vector_db=vector_db,
            contents_db=contents_db,
        )

        LOG.info("[kb] Knowledge base initialized successfully")
        return kb

    except Exception as e:
        LOG.exception("[kb] Failed to initialize knowledge base: %s", e)
        return None


def _sha12(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()[:12]


def _normalize_date(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    for fmt in ("%d %b %Y", "%d %B %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return raw


def _norm_header(h: str) -> str:
    # Confluence tablolarda başlıklar bazen boşluk/ünlem vs ile değişebiliyor
    return " ".join((h or "").strip().lower().split())


def _get_row_val(row: Dict[str, str], *keys: str) -> str:
    # header normalize ile toleranslı okuma
    norm = {_norm_header(k): (v or "").strip() for k, v in row.items()}
    for k in keys:
        v = norm.get(_norm_header(k))
        if v is not None and v != "":
            return v
    return ""


async def ingest_confluence_page_to_kb(
    kb,
    base_url: str,
    page_id: str,
    auth: Tuple[Optional[str], Optional[str]],
    *,
    verify: bool = True,
    timeout: int = 30,
    max_new_inserts: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Confluence DC sayfasından (page_id) body.storage çekip ilk tabloyu satır satır Knowledge'a ekler.
    Contents DB bağlı varsayımıyla çalışır; yine de tek kayda düşmeyi engellemek için unique marker ekler.

    Returns summary dict.
    """
    username, password = auth
    if not username or not password:
        raise ValueError("Confluence auth tuple must include (username, password)")

    # 1) Fetch page (storage XHTML)
    s = requests.Session()
    s.auth = (username, password)

    api = f"{base_url.rstrip('/')}/rest/api/content/{page_id}"
    params = {"expand": "body.storage,title,version"}
    r = s.get(api, params=params, verify=verify, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    title = data.get("title", f"page_{page_id}")
    storage = (((data.get("body") or {}).get("storage") or {}).get("value") or "")
    rows_total = 0
    inserted = 0
    skipped = 0

    # 2) Parse first table
    soup = BeautifulSoup(storage, "html.parser")
    table = soup.find("table")
    if not table:
        LOG.warning("[ingest] No table found on page_id=%s", page_id)
        return {"page_id": page_id, "title": title, "rows": 0, "inserted": 0, "skipped": 0}

    header_tr = table.find("tr")
    if not header_tr:
        LOG.warning("[ingest] No header row found on page_id=%s", page_id)
        return {"page_id": page_id, "title": title, "rows": 0, "inserted": 0, "skipped": 0}

    def _cell_text(td) -> str:
        # Linkleri de text'in içine eklemeye çalış
        a = td.find("a")
        if a and a.get("href"):
            t = td.get_text(" ", strip=True)
            return f"{t} ({a.get('href')})" if t else a.get("href")
        return td.get_text(" ", strip=True)

    header_cells = header_tr.find_all(["th", "td"])
    headers = [c.get_text(" ", strip=True) for c in header_cells]

    # header boşsa otomatik üret
    if not any(h.strip() for h in headers):
        # kolon sayısını 1. data satırından tahmin et
        first_data_tr = None
        for tr in table.find_all("tr")[1:]:
            if tr.find_all("td"):
                first_data_tr = tr
                break
        ncols = len(first_data_tr.find_all("td")) if first_data_tr else len(headers)
        headers = [f"col_{i}" for i in range(1, ncols + 1)]
        LOG.warning("[ingest] header row empty; using synthetic headers=%r", headers)

    # 3) Extract data rows
    data_trs = table.find_all("tr")[1:]
    rows_total = len(data_trs)
    LOG.info("[ingest] page_id=%s title=%r rows=%d", page_id, title, rows_total)
    LOG.info("[ingest] headers=%r", headers)

    # 4) Insert rows
    for idx, tr in enumerate(data_trs, start=1):
        tds = tr.find_all("td")
        if not tds:
            continue

        cells = [td.get_text(" ", strip=True) for td in tds]
        if len(cells) < len(headers):
            cells += [""] * (len(headers) - len(cells))

        row = dict(zip(headers, cells))

        date_raw = _get_row_val(row, "Tarih", "Date")
        env = _get_row_val(row, "Ortam", "Environment")
        service = _get_row_val(row, "Servis / Sistem", "Servis/Sistem", "Service / System")
        owner = _get_row_val(row, "Sorumlu", "Owner")
        summary = _get_row_val(row, "Hata / Olay Özeti", "Hata/Olay Özeti", "Incident Summary")
        solution = _get_row_val(row, "Çözüm / Not", "Çözüm/Not", "Resolution / Note")
        status = _get_row_val(row, "Durum", "Status")
        link = _get_row_val(row, "Link", "URL")

        date = _normalize_date(date_raw)

        row_key = f"{page_id}:{idx}"  # deterministik ve unique

        base_text = (
            f"Kaynak: {title} (Confluence)\n"
            f"PAGE_ID: {page_id}\n"
            f"ROW_INDEX: {idx}\n\n"
            f"OLAY_TARIHI: {date}\n"
            f"ORTAM: {env}\n"
            f"SERVIS: {service}\n"
            f"SORUMLU: {owner}\n"
            f"DURUM: {status}\n\n"
            f"HATA_OZETI:\n{summary}\n\n"
            f"COZUM:\n{solution}\n\n"
            f"LINK: {link}\n"
        ).strip()

        # Agno'nun normalize edebileceği kısımlardan bağımsız şekilde unique marker ekle
        unique_marker = f"\n\n===UNIQUE_ROW_KEY:{row_key}===\n"
        text = base_text + unique_marker

        meta: Dict[str, Any] = {
            "row_key": row_key,
            "row_index": idx,
            "page_id": page_id,
            "page_title": title,
            "date": date,
            "env": env,
            "service": service,
            "owner": owner,
            "status": status,
            "link": link,
            "source": "confluence_weekly_reports",
        }

        name = f"confluence_row_{row_key}"

        # Debug: text gerçekten farklı mı?
        LOG.info(
            "[ingest] idx=%d name=%s row_key=%s my_sha12=%s text_len=%d summary_len=%d service=%r date=%r",
            idx, name, row_key, _sha12(text), len(text), len(summary or ""), service, date
        )
        LOG.debug("[ingest] text_head=%r", text[:120])

        try:
            await kb.add_content_async(
                name=name,
                text_content=text,
                metadata=meta,
                skip_if_exists=True,
            )
            inserted += 1
        except Exception as e:
            LOG.exception("[ingest] insert failed idx=%d row_key=%s: %s", idx, row_key, e)

        if max_new_inserts and inserted >= max_new_inserts:
            LOG.info("[ingest] reached max_new_inserts=%d => stopping", max_new_inserts)
            break

    LOG.info("[ingest] finished: inserted=%d skipped=%d", inserted, skipped)
    return {"page_id": page_id, "title": title, "rows": rows_total, "inserted": inserted, "skipped": skipped}
