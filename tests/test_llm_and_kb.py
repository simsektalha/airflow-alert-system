import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "airflow_failure_responder"))

from knowledge import parse_confluence_table  # noqa: E402
from llm_agent import _parse_analysis, ask_llm_for_analysis  # noqa: E402

VALID = ('{"category":"infra","error_summary":"Metastore erişilemedi","root_cause":"HMS down",'
         '"confidence":0.8,"used_doc_ids":["K2","K9"],"knowledge_fix_steps":["HMS restart"],'
         '"generic_fix_steps":[],"prevention":[],"needs_rerun":true}')


class StubClient:
    """Mimics openai.OpenAI just enough: records the request, returns fixed content."""

    def __init__(self, content):
        self.content, self.kwargs = content, None
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.kwargs = kwargs
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=self.content))])


def _docs():
    return [
        {"content": "HATA_OZETI: tablo yok", "meta": {"date": "2026-01-02", "service": "Hive", "summary": "tablo yok"}},
        {"content": "HATA_OZETI: HMS timeout", "meta": {"date": "2026-03-04", "service": "Metastore", "summary": "HMS timeout"}},
    ]


def test_used_docs_come_from_real_rows_and_unknown_ids_are_dropped():
    client = StubClient(VALID)
    out = ask_llm_for_analysis(client, {"llm": {"model": "m"}}, "log", {"dag_id": "d"}, _docs())
    assert out["used_docs"] == ["Confluence 2026-03-04 · Metastore: HMS timeout"]  # K9 does not exist
    assert out["used_knowledge"] is True
    assert "used_doc_ids" not in out
    assert client.kwargs["response_format"]["type"] == "json_schema"
    assert "[K2]" in client.kwargs["messages"][1]["content"]


def test_knowledge_steps_cleared_when_no_real_doc_used():
    client = StubClient(VALID.replace('["K2","K9"]', '["K7"]'))
    out = ask_llm_for_analysis(client, {"llm": {"response_format": "none"}}, "log", {}, _docs())
    assert out["used_knowledge"] is False and out["knowledge_fix_steps"] == []
    assert "response_format" not in client.kwargs


def test_parse_tolerates_fences_and_rejects_garbage():
    assert _parse_analysis("```json\n" + VALID + "\n```").root_cause == "HMS down"
    with pytest.raises(RuntimeError):
        _parse_analysis("no json here")


TABLE = """<table>
<tr><th>Tarih</th><th>Servis / Sistem</th><th>Hata / Olay Özeti</th><th>Çözüm / Not</th></tr>
<tr><td>2 Jan 2026</td><td>Hive</td><td>tablo yok</td><td>MSCK REPAIR</td></tr>
<tr><td>2 Jan 2026</td><td>Hive</td><td>tablo yok</td><td>MSCK REPAIR</td></tr>
<tr><td>4 Mar 2026</td><td>Metastore</td><td>HMS timeout</td><td>restart</td></tr>
</table>"""


def test_parse_confluence_table_dedups_and_keys_by_content():
    rows = parse_confluence_table(TABLE, "123", "Olaylar")
    assert len(rows) == 2  # duplicate row collapsed
    assert rows[0]["meta"]["date"] == "2026-01-02" and rows[0]["meta"]["service"] == "Hive"
    assert all(r["row_key"].startswith("123:") for r in rows)
    # Reordering rows keeps the same keys (no re-embedding)
    lines = TABLE.splitlines()
    reordered = "\n".join([lines[0], lines[1], lines[4], lines[2], lines[5]])
    assert {r["row_key"] for r in parse_confluence_table(reordered, "123", "Olaylar")} == {r["row_key"] for r in rows}


# ---------- pgvector integration (runs only when PG_TEST_DSN is set) ----------
PG = os.environ.get("PG_TEST_DSN")


@pytest.mark.skipif(not PG, reason="set PG_TEST_DSN to a Postgres with pgvector")
def test_upsert_and_search_against_pgvector():
    import psycopg
    from knowledge import search, upsert_page_rows

    calls = []

    def fake_embed(texts):
        calls.append(len(texts))
        return [[1.0, 0.0] if "Hive" in t else [0.0, 1.0] for t in texts]

    rows = parse_confluence_table(TABLE, "123", "Olaylar")
    rows = [dict(r, content=r["content"] + " " + r["meta"]["service"]) for r in rows]
    with psycopg.connect(PG) as conn:
        conn.execute("DROP TABLE IF EXISTS kb_test")
        conn.commit()
        assert upsert_page_rows(conn, "kb_test", "123", rows, fake_embed)["inserted"] == 2
        # Second sync: nothing new, nothing embedded
        assert upsert_page_rows(conn, "kb_test", "123", rows, fake_embed) == {"rows": 2, "inserted": 0, "deleted": 0, "unchanged": 2}
        assert calls == [2]
        # Row removed from the page is deleted
        assert upsert_page_rows(conn, "kb_test", "123", rows[:1], fake_embed)["deleted"] == 1

        upsert_page_rows(conn, "kb_test", "123", rows, fake_embed)
        hits = search(conn, "kb_test", [0.0, 1.0], 1)
        assert hits[0]["meta"]["service"] == "Metastore" and hits[0]["distance"] < 0.01
