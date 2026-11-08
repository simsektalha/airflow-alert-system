from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class AirflowFailureAnalysis(BaseModel):
    """
    Yeni canonical model:
    - used_docs: knowledge kaynaklarından türetilmiş kısa tek-satırlık string listesi (max 3)
    - knowledge_fix_steps: bilgi (used_docs) varsa buradan türetilmiş adımlar
    - generic_fix_steps: LLM/uzman tavsiyeleri (knowledge ile çakışmamalı)
    """

    category: str = Field(
        ...,
        description='Kategoriler: "network","dependency","config","code","infra","security","design","transient","other"',
    )
    error_summary: str = Field(..., description="Bir cümlelik Türkçe hata özeti")
    confidence: float = Field(..., ge=0.0, le=1.0, description="0.0 - 1.0 arası güven")

    used_knowledge: bool = Field(False, description="Knowledge kullanıldı mı?")
    used_docs: List[str] = Field(
        default_factory=list, description="Kısa tek-satırlık bilgi kayıtları (max 3)"
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
