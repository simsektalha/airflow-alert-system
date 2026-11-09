import json
import logging
from typing import Any, Dict, Optional

from drivers import Agent, OpenAILike, VLLM, Ollama, Knowledge
from models import AirflowFailureAnalysis

LOG = logging.getLogger("airflow-failure-responder")


def build_agent_from_cfg(cfg: Dict[str, Any], kb: Optional["Knowledge"], knowledge_filters: Optional[Dict[str, Any]] = None) -> Optional["Agent"]:
    """
    Build Agent with optional knowledge_filters.
    """

    if Agent is None:
        LOG.info("[llm] agno not installed; skipping LLM")
        return None

    llm = cfg.get("llm") or {}
    driver = (llm.get("driver") or "openai_like").lower()
    model_id = llm.get("model") or "llama3.1"
    temperature = float(llm.get("temperature") or 0.1)
    # 800 truncated structured JSON in practice; 4000 leaves room for the full schema
    max_tokens = int(llm.get("max_tokens") or 4000)

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
            base_url=llm["base_url"],
            api_key=llm["api_key"],
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
        name="Airflow Failure RAG Agent",
        instructions=['''
Sen Apache Airflow ve Big Data sistemlerinde çalışan deneyimli bir SRE ve Data Platform mühendisisin.

Uzmanlık alanların:
- Airflow task hataları
- Spark / ETL job’ları
- Data pipeline bağımlılık problemleri
- Metastore / tablo erişim hataları
- Dağıtık sistemler

DAVRANIŞ PRENSİPLERİ:

1) NET VE PRATİK OL
- Akademik açıklama yapma
- Direkt uygulanabilir çözüm üret

2) HATANIN ÖZÜNE ODAKLAN
- İlk hatayı bul
- Secondary / zincirleme hataları ignore et

3) KNOWLEDGE ÖNCELİĞİ
- Eğer benzer bir geçmiş kayıt varsa mutlaka kullan
- Çözüm varsa doğrudan onu öner
- Uydurma bilgi üretme

4) KISA YAZ
- Gereksiz detay verme
- Her madde 1-2 cümle max

5) TEKRAR ETME
- Generic ve knowledge adımları aynı olamaz

6) KARAR VER
- Emin değilsen bile makul default ver
- Boş alan bırakma

7) PRODUCTION ODAKLI OL
- Gerçek sistemde uygulanabilir öneriler ver
- Airflow, Spark, DAG mantığını dikkate al

Amaç:
Hızlı, doğru ve uygulanabilir incident analizi üretmek.
'''],
        knowledge=kb,
        search_knowledge=bool(kb),
        knowledge_filters=knowledge_filters,
        output_schema=AirflowFailureAnalysis,
        use_json_mode=True,
        markdown=False,
    )


async def ask_llm_for_analysis(agent: "Agent", full_log: str, identifiers: Dict[str, Any]) -> Dict[str, Any]:
    """Tek RAG agent'ı ile AirflowFailureAnalysis şemasına uygun sonuç üret."""

    dag_id = identifiers.get("dag_id")
    dag_run_id = identifiers.get("dag_run_id")
    task_id = identifiers.get("task_id")
    try_number = identifiers.get("try_number")

    # STRICT prompt: force JSON-only output and exact schema.
    prompt = f"""
Bir Airflow task hatasını analiz etmeni istiyorum.

Aşağıda REDACTED ve kısaltılmış log verilmiştir.

Amacın:
- Hatanın kök nedenini bulmak
- Eğer varsa knowledge ile eşleştirmek
- Kısa ve uygulanabilir çözüm üretmek

-----------------------
ÇIKTI FORMATI
-----------------------

SADECE JSON döndür. Başka hiçbir şey yazma.

JSON şu alanları içermelidir:

{{
  "error_summary": string,
  "root_cause": string,
  "category": string,
  "used_knowledge": boolean,
  "used_docs": [string],
  "knowledge_fix_steps": [string],
  "generic_fix_steps": [string],
  "prevention": [string],
  "matched_patterns": [string],
  "needs_rerun": boolean,
  "confidence": number
}}

-----------------------
ALAN KURALLARI
-----------------------

error_summary:
- Hatanın özünü tek cümlede açıkla

root_cause:
- Semptomu değil, hatayı başlatan asıl sebebi 1-2 cümlede yaz
- Emin değilsen bunu belirt ve confidence'ı düşür

category:
- Şunlardan biri:
  "network","dependency","config","code","infra","security","design","transient","other"

used_knowledge:
- Eğer knowledge search kullandıysan true

used_docs:
- Knowledge kaynaklarından gelen kısa tek satırlık açıklamalar
- max 3 adet
- Örnek:
  "Confluence: tablo yok hatası — restart ile çözülmüş"
- Detaylı locator/snippet yazma

KRİTİK:
- used_knowledge = true ise used_docs BOŞ OLAMAZ

knowledge_fix_steps:
- Knowledge varsa burası DOLU olmalı
- Knowledge cozum yontemi ile cok yakin oneriler olmali
- Knowledge disinda bir yorum katilmamali

generic_fix_steps:
- Genel öneriler
- max 2 madde
- knowledge ile tekrar etme

prevention:
- 1-3 kısa öneri

matched_patterns:
- Log’dan çıkan kısa pattern’ler
- max 3 adet

needs_rerun:
- Task tekrar çalıştırılmalı mı

confidence:
- 0.0 - 1.0 arası

-----------------------
DAVRANIŞ KURALLARI
-----------------------

- Knowledge varsa:
  → used_knowledge = true
  → used_docs doldur
  → knowledge_fix_steps üret

- Knowledge yoksa:
  → used_knowledge = false
  → used_docs = []

- Asla used_knowledge=true + used_docs=[] dönme

- Kısa, net ve teknik yaz

-----------------------
INPUT
-----------------------
- dag_id: {dag_id}
- dag_run_id: {dag_run_id}
- task_id: {task_id}
- try_number: {try_number}

-----------------------
LOG START
-----------------------

{full_log}

-----------------------
LOG END
-----------------------
"""

    LOG.info("[llm] prompt size: full_log=%d", len(full_log))
    resp = await agent.arun(prompt)

    # Agno 2.3.x: output_schema verildiğinde, resp.content Pydantic model oluyor
    if hasattr(resp, "content") and isinstance(resp.content, AirflowFailureAnalysis):
        LOG.info("[llm] Structured Pydantic response from agent")
        return resp.content.model_dump()

    # Güvenlik için: yine de text'ten JSON yakalamayı dene
    text = ""
    if hasattr(resp, "content") and isinstance(resp.content, str):
        text = resp.content
    elif hasattr(resp, "text") and isinstance(resp.text, str):
        text = resp.text
    else:
        text = str(resp)

    text = text.strip()
    if text.startswith("```"):
        # strip fences but keep inner JSON
        text = "\n".join(text.splitlines()[1:-1]).strip()

    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("LLM returned no JSON object.")

    raw_json = text[start: end + 1]
    parsed = json.loads(raw_json)

    # Minimal validation: ensure keys exist and types are plausible
    # (deeper validation delegated to caller or Pydantic model)
    required_keys = [
        "error_summary",
        "root_cause",
        "category",
        "used_knowledge",
        "used_docs",
        "knowledge_fix_steps",
        "generic_fix_steps",
        "prevention",
        "matched_patterns",
        "needs_rerun",
        "confidence",
    ]
    missing = [k for k in required_keys if k not in parsed]
    if missing:
        LOG.error("[llm] Parsed analysis missing keys: %s", missing)
    # proceed but caller may reject — we still return parsed for visibility
    return parsed
