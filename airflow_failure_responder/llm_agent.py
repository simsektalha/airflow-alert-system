"""
LLM analysis over an OpenAI-compatible endpoint (vLLM, LiteLLM, Ollama /v1, OpenAI).

Knowledge is retrieved by our own code (knowledge.retrieve_knowledge) and placed in
the prompt with short IDs (K1..Kn). The model returns the IDs it used; we map them
back to the real rows, so the card never shows a source the model made up.
"""
import json
import logging
from typing import Any, Dict, List, Optional, Union

import httpx
from openai import OpenAI

from models import AirflowFailureAnalysis

LOG = logging.getLogger("airflow-failure-responder")

SYSTEM_PROMPT = '''
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
'''

DOC_CHARS = 1500  # per knowledge row in the prompt


def build_llm_client(cfg: Dict[str, Any], verify: Union[bool, str]) -> Optional[OpenAI]:
    """OpenAI-compatible client from cfg.llm; None when not configured."""
    llm = cfg.get("llm") or {}
    # `host` kept for old ollama-style configs; Ollama serves the OpenAI API under /v1
    base_url = llm.get("base_url") or (llm["host"].rstrip("/") + "/v1" if llm.get("host") else None)
    if not base_url:
        LOG.info("[llm] llm.base_url not configured; skipping LLM")
        return None
    LOG.info("[llm] base=%s model=%s", base_url, llm.get("model"))
    return OpenAI(
        base_url=base_url,
        api_key=llm.get("api_key") or "none",
        # llm.verify overrides tls.verify (the Airflow CA may not sign the gateway)
        http_client=httpx.Client(verify=llm.get("verify", verify), timeout=float(llm.get("timeout", 120))),
    )


def _format_docs(docs: List[Dict[str, Any]]) -> str:
    if not docs:
        return "(Knowledge kaydı bulunamadı)"
    return "\n\n".join(f"[K{i}]\n{d['content'][:DOC_CHARS]}" for i, d in enumerate(docs, start=1))


def _doc_label(doc: Dict[str, Any]) -> str:
    m = doc.get("meta") or {}
    head = " · ".join(x for x in (m.get("date"), m.get("service")) if x)
    return f"Confluence {head}: {m.get('summary') or ''}".strip()


def _parse_analysis(text: str) -> AirflowFailureAnalysis:
    """Strict JSON first; tolerate fences/prose for gateways that ignore response_format."""
    text = (text or "").strip()
    try:
        return AirflowFailureAnalysis.model_validate_json(text)
    except Exception:
        start, end = text.find("{"), text.rfind("}")
        if start == -1 or end <= start:
            raise RuntimeError("LLM returned no JSON object.")
        return AirflowFailureAnalysis.model_validate(json.loads(text[start:end + 1]))


def _response_format(cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    mode = ((cfg.get("llm") or {}).get("response_format") or "json_schema").lower()
    if mode == "json_schema":
        return {"type": "json_schema", "json_schema": {
            "name": "airflow_failure_analysis", "schema": AirflowFailureAnalysis.model_json_schema()}}
    if mode == "json_object":
        return {"type": "json_object"}
    return None  # "none": rely on the prompt + _parse_analysis


def ask_llm_for_analysis(client: OpenAI, cfg: Dict[str, Any], full_log: str,
                         identifiers: Dict[str, Any], docs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Single LLM call -> analysis dict (incl. used_docs/used_knowledge mapped from real rows)."""
    llm = cfg.get("llm") or {}

    prompt = f"""
Bir Airflow task hatasını analiz etmeni istiyorum.

Aşağıda REDACTED ve kısaltılmış log ile benzer geçmiş olaylar (KNOWLEDGE) verilmiştir.

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
  "used_doc_ids": [string],
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

used_doc_ids:
- Aşağıdaki KNOWLEDGE listesinden bu hatayla GERÇEKTEN ilgili olan kayıtların ID'leri (örn. "K1")
- max 3 adet
- Sadece listede olan ID'leri yaz; ilgili kayıt yoksa []

knowledge_fix_steps:
- used_doc_ids doluysa burası DOLU olmalı
- Kayıtlardaki çözüm yöntemine çok yakın öneriler olmalı
- Kayıt dışında bir yorum katılmamalı
- used_doc_ids boşsa []

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
INPUT
-----------------------
- dag_id: {identifiers.get("dag_id")}
- dag_run_id: {identifiers.get("dag_run_id")}
- task_id: {identifiers.get("task_id")}
- try_number: {identifiers.get("try_number")}

-----------------------
KNOWLEDGE
-----------------------

{_format_docs(docs)}

-----------------------
LOG START
-----------------------

{full_log}

-----------------------
LOG END
-----------------------
"""

    kwargs: Dict[str, Any] = {}
    rf = _response_format(cfg)
    if rf:
        kwargs["response_format"] = rf

    LOG.info("[llm] prompt size: full_log=%d docs=%d", len(full_log), len(docs))
    resp = client.chat.completions.create(
        model=llm.get("model") or "llama3.1",
        messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": prompt}],
        temperature=float(llm.get("temperature") or 0.1),
        # 800 truncated structured JSON in practice; 4000 leaves room for the full schema
        max_tokens=int(llm.get("max_tokens") or 4000),
        **kwargs,
    )
    analysis = _parse_analysis(resp.choices[0].message.content).model_dump()

    # Map IDs back to real rows; unknown IDs are dropped, not shown
    by_id = {f"K{i}": d for i, d in enumerate(docs, start=1)}
    used = [by_id[i] for i in analysis.pop("used_doc_ids", []) if i in by_id][:3]
    analysis["used_docs"] = [_doc_label(d) for d in used]
    analysis["used_knowledge"] = bool(used)
    if not used:
        analysis["knowledge_fix_steps"] = []  # steps without a real source would be invented
    return analysis
