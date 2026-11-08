"""
Knowledge base: Confluence incident-table rows stored in Postgres + pgvector.

No framework: embeddings come from an OpenAI-compatible /embeddings endpoint,
storage/search is plain SQL. Each row's key is derived from its content, so
re-ingesting only embeds new/changed rows and deletes rows removed from the page.
"""
import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import httpx
import psycopg
import requests
from bs4 import BeautifulSoup
from openai import OpenAI
from psycopg import sql

LOG = logging.getLogger("airflow-failure-responder")

DEFAULT_TABLE = "airflow_kb_rows"
EMBED_BATCH = 64

Embedder = Callable[[List[str]], List[List[float]]]


def _sha12(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()[:12]


def _pg_dsn(db_url: str) -> str:
    # Config keeps the SQLAlchemy-style URL used before; psycopg wants plain postgresql://
    return db_url.replace("postgresql+psycopg://", "postgresql://", 1)


def _vec(v: List[float]) -> str:
    return "[" + ",".join(repr(float(x)) for x in v) + "]"


def _table(cfg: Dict[str, Any]) -> str:
    return (cfg.get("knowledge") or {}).get("table") or DEFAULT_TABLE


def _db_url(cfg: Dict[str, Any]) -> Optional[str]:
    return ((cfg.get("knowledge") or {}).get("vector_db") or {}).get("db_url")


def make_embedder(cfg: Dict[str, Any], verify: Union[bool, str]) -> Embedder:
    """OpenAI-compatible embeddings client (vLLM, LiteLLM, Ollama /v1, OpenAI)."""
    e = (cfg.get("knowledge") or {}).get("embedder") or {}
    base_url = e.get("base_url") or (e["host"].rstrip("/") + "/v1" if e.get("host") else None)
    client = OpenAI(
        base_url=base_url,
        api_key=e.get("api_key") or "none",
        http_client=httpx.Client(verify=verify, timeout=float(e.get("timeout", 30))),
    )
    model = e.get("id", "nomic-embed-text")

    def embed(texts: List[str]) -> List[List[float]]:
        out: List[List[float]] = []
        for i in range(0, len(texts), EMBED_BATCH):
            resp = client.embeddings.create(model=model, input=texts[i:i + EMBED_BATCH])
            out.extend(d.embedding for d in resp.data)
        return out

    return embed


def ensure_table(conn: psycopg.Connection, table: str) -> None:
    conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    # shortcut: unconstrained `vector` + seq scan is fine for a few thousand rows; add dims + HNSW index if the table grows
    conn.execute(sql.SQL(
        "CREATE TABLE IF NOT EXISTS {} ("
        " row_key text PRIMARY KEY,"
        " page_id text NOT NULL,"
        " content text NOT NULL,"
        " meta jsonb NOT NULL,"
        " embedding vector NOT NULL,"
        " updated_at timestamptz NOT NULL DEFAULT now())"
    ).format(sql.Identifier(table)))


def upsert_page_rows(conn: psycopg.Connection, table: str, page_id: str,
                     rows: List[Dict[str, Any]], embed: Embedder) -> Dict[str, int]:
    """Sync one page: embed only rows not stored yet, delete rows gone from the page."""
    ensure_table(conn, table)
    existing = {r[0] for r in conn.execute(
        sql.SQL("SELECT row_key FROM {} WHERE page_id = %s").format(sql.Identifier(table)), (page_id,))}
    current = {r["row_key"] for r in rows}

    new_rows = [r for r in rows if r["row_key"] not in existing]
    vectors = embed([r["content"] for r in new_rows]) if new_rows else []
    for r, v in zip(new_rows, vectors):
        conn.execute(
            sql.SQL("INSERT INTO {} (row_key, page_id, content, meta, embedding) VALUES (%s, %s, %s, %s, %s::vector) "
                    "ON CONFLICT (row_key) DO NOTHING").format(sql.Identifier(table)),
            (r["row_key"], page_id, r["content"], json.dumps(r["meta"], ensure_ascii=False), _vec(v)),
        )

    stale = list(existing - current)
    if stale:
        conn.execute(sql.SQL("DELETE FROM {} WHERE row_key = ANY(%s)").format(sql.Identifier(table)), (stale,))
    conn.commit()
    return {"rows": len(rows), "inserted": len(new_rows), "deleted": len(stale), "unchanged": len(current & existing)}


def search(conn: psycopg.Connection, table: str, query_vec: List[float], top_k: int) -> List[Dict[str, Any]]:
    """Nearest rows by cosine distance."""
    cur = conn.execute(
        sql.SQL("SELECT row_key, content, meta, embedding <=> %s::vector AS dist FROM {} ORDER BY dist LIMIT %s")
        .format(sql.Identifier(table)),
        (_vec(query_vec), top_k),
    )
    return [{"row_key": k, "content": c, "meta": m, "distance": float(d)} for k, c, m, d in cur.fetchall()]


def retrieve_knowledge(cfg: Dict[str, Any], query: str, verify: Union[bool, str]) -> List[Dict[str, Any]]:
    """Top-k knowledge rows for the failure; [] when the KB is not configured."""
    db_url = _db_url(cfg)
    if not db_url or not query.strip():
        LOG.info("[kb] knowledge.vector_db.db_url not set or empty query; skipping retrieval")
        return []
    top_k = int((cfg.get("knowledge") or {}).get("top_k", 5))
    embed = make_embedder(cfg, verify)
    qvec = embed([query])[0]
    with psycopg.connect(_pg_dsn(db_url)) as conn:
        ensure_table(conn, _table(cfg))
        docs = search(conn, _table(cfg), qvec, top_k)
    LOG.info("[kb] retrieved %d docs (distances=%s)", len(docs), [round(d["distance"], 3) for d in docs])
    return docs


# ---------- Confluence ----------
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


def parse_confluence_table(storage_html: str, page_id: str, title: str) -> List[Dict[str, Any]]:
    """First table of a Confluence storage-format page -> KB rows {row_key, content, meta}."""
    soup = BeautifulSoup(storage_html or "", "html.parser")
    table = soup.find("table")
    if not table:
        LOG.warning("[ingest] No table found on page_id=%s", page_id)
        return []

    header_tr = table.find("tr")
    if not header_tr:
        LOG.warning("[ingest] No header row found on page_id=%s", page_id)
        return []

    headers = [c.get_text(" ", strip=True) for c in header_tr.find_all(["th", "td"])]

    # header boşsa otomatik üret
    if not any(h.strip() for h in headers):
        # kolon sayısını 1. data satırından tahmin et
        first_data_tr = next((tr for tr in table.find_all("tr")[1:] if tr.find_all("td")), None)
        ncols = len(first_data_tr.find_all("td")) if first_data_tr else len(headers)
        headers = [f"col_{i}" for i in range(1, ncols + 1)]
        LOG.warning("[ingest] header row empty; using synthetic headers=%r", headers)

    rows: List[Dict[str, Any]] = []
    seen = set()
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue

        cells = [td.get_text(" ", strip=True) for td in tds]
        if len(cells) < len(headers):
            cells += [""] * (len(headers) - len(cells))
        row = dict(zip(headers, cells))

        date = _normalize_date(_get_row_val(row, "Tarih", "Date"))
        env = _get_row_val(row, "Ortam", "Environment")
        service = _get_row_val(row, "Servis / Sistem", "Servis/Sistem", "Service / System")
        owner = _get_row_val(row, "Sorumlu", "Owner")
        summary = _get_row_val(row, "Hata / Olay Özeti", "Hata/Olay Özeti", "Incident Summary")
        solution = _get_row_val(row, "Çözüm / Not", "Çözüm/Not", "Resolution / Note")
        status = _get_row_val(row, "Durum", "Status")
        link = _get_row_val(row, "Link", "URL")

        content = (
            f"Kaynak: {title} (Confluence)\n"
            f"OLAY_TARIHI: {date}\n"
            f"ORTAM: {env}\n"
            f"SERVIS: {service}\n"
            f"SORUMLU: {owner}\n"
            f"DURUM: {status}\n\n"
            f"HATA_OZETI:\n{summary}\n\n"
            f"COZUM:\n{solution}\n\n"
            f"LINK: {link}"
        ).strip()

        # Content-derived key: row reordering does not re-embed; edits replace the row
        row_key = f"{page_id}:{_sha12(content)}"
        if row_key in seen:
            continue
        seen.add(row_key)

        rows.append({
            "row_key": row_key,
            "content": content,
            "meta": {
                "page_id": page_id, "page_title": title, "date": date, "env": env,
                "service": service, "owner": owner, "status": status, "link": link,
                "summary": summary[:300], "source": "confluence",
            },
        })
    return rows


def fetch_confluence_page(base_url: str, page_id: str, auth: Tuple[Optional[str], Optional[str]],
                          verify: Union[bool, str] = True, timeout: int = 30) -> Tuple[str, str]:
    """Confluence DC REST: returns (title, storage_html)."""
    username, password = auth
    if not username or not password:
        raise ValueError("Confluence auth tuple must include (username, password)")
    r = requests.get(
        f"{base_url.rstrip('/')}/rest/api/content/{page_id}",
        params={"expand": "body.storage,title,version"},
        auth=(username, password), verify=verify, timeout=timeout,
    )
    r.raise_for_status()
    data = r.json()
    storage = (((data.get("body") or {}).get("storage") or {}).get("value") or "")
    return data.get("title", f"page_{page_id}"), storage


def ingest_confluence(cfg: Dict[str, Any], verify: Union[bool, str]) -> List[Dict[str, Any]]:
    """Sync every configured Confluence page (confluence.page_ids or legacy page_id) into the KB."""
    c = cfg.get("confluence") or {}
    page_ids = c.get("page_ids") or ([c["page_id"]] if c.get("page_id") else [])
    db_url = _db_url(cfg)
    if not page_ids or not db_url:
        LOG.info("[ingest] confluence page ids or knowledge.vector_db.db_url missing; skipping")
        return []

    auth = ((c.get("auth") or {}).get("username"), (c.get("auth") or {}).get("password"))
    embed = make_embedder(cfg, verify)
    summaries = []
    with psycopg.connect(_pg_dsn(db_url)) as conn:
        for page_id in page_ids:
            title, storage = fetch_confluence_page(c.get("base_url"), str(page_id), auth, verify)
            rows = parse_confluence_table(storage, str(page_id), title)
            if not rows:
                # An empty parse (layout change, partial page) must not wipe the stored rows
                LOG.warning("[ingest] page_id=%s parsed 0 rows; keeping existing KB rows", page_id)
                continue
            summary = {"page_id": page_id, "title": title, **upsert_page_rows(conn, _table(cfg), str(page_id), rows, embed)}
            LOG.info("[ingest] %s", summary)
            summaries.append(summary)
    return summaries
