from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class AirflowFailureAnalysis(BaseModel):
    """
    LLM çıktı şeması (response_format=json_schema ile gönderilir):
    - used_doc_ids: prompt'ta verilen KNOWLEDGE kayıtlarından kullanılanların ID'leri (K1, K2...).
      Kod bu ID'leri gerçek kayıtlara çevirir (used_docs/used_knowledge); LLM kaynak metni uyduramaz.
    - knowledge_fix_steps: used_doc_ids doluysa o kayıtlardan türetilmiş adımlar
    - generic_fix_steps: LLM/uzman tavsiyeleri (knowledge ile çakışmamalı)
    """

    category: str = Field(
        ...,
        description='Kategoriler: "network","dependency","config","code","infra","security","design","transient","other"',
    )
    error_summary: str = Field(..., description="Bir cümlelik Türkçe hata özeti")
    root_cause: str = Field(..., description="Kök neden: semptom değil, hatayı başlatan asıl sebep (1-2 cümle)")
    confidence: float = Field(..., ge=0.0, le=1.0, description="0.0 - 1.0 arası güven")

    used_doc_ids: List[str] = Field(
        default_factory=list, description="Kullanılan KNOWLEDGE kayıtlarının ID'leri (örn. K1), max 3"
    )

    knowledge_fix_steps: List[str] = Field(
        default_factory=list,
        description="Knowledge'dan türetilmiş uygulanabilir adımlar",
    )
    generic_fix_steps: List[str] = Field(
        default_factory=list,
        description="Generic/troubleshooting adımları (knowledge ile tekrar etmeyecek)",
    )

    prevention: List[str] = Field(
        default_factory=list, description="Kısa prevention önerileri (1-4 madde)"
    )

    matched_patterns: Optional[List[str]] = Field(
        default=None, description="Log ve evidence'den çıkarılan 1-5 kısa pattern"
    )
    needs_rerun: bool = Field(True, description="Task yeniden çalıştırılmalı mı?")
