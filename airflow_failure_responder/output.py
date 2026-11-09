import json
import logging
from typing import Any, Dict, List, Optional
import requests
from urllib.parse import quote

LOG = logging.getLogger("airflow-failure-responder")


def _shorten(text: str, max_len: int = 300) -> str:
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[:max_len].rstrip() + "…"


# Convert steps -> textblocks
def steps_to_textblocks(steps: List[str]) -> List[Dict[str, Any]]:
    if not steps:
        return [{"type": "TextBlock", "text": "- (Yok)", "wrap": True, "isSubtle": True, "spacing": "None", "size": "Small"}]
    blocks = []
    for s in steps:
        blocks.append({"type": "TextBlock", "text": f"- {s}", "wrap": True, "spacing": "None", "size": "Small"})
    return blocks


def _build_adaptive_card(result: Dict[str, Any], base_url, confluence_url: Optional[str] = None) -> Dict[str, Any]:
    """
    Build an Adaptive Card payload (as Python dict) for Teams.
    Robust: accepts used_docs as list[str] or list[dict], falls back to evidence if needed,
    and enforces brevity for generic/prevention sections.
    Shows used_docs in a separate "Kullanılan Dokümanlar" section (not under Knowledge).
    """
    analysis = result.get("analysis", {}) or {}
    error_summary = analysis.get("error_summary", "Unknown error")
    root_cause = analysis.get("root_cause") or ""
    # Prefer canonical used_docs (short strings). Back-compat: accept 'evidence' if present.
    raw_used_docs = analysis.get("used_docs") or analysis.get("evidence") or []
    # Normalize used_docs -> list of short single-line strings (max 3 items)
    used_docs: List[str] = []
    for ev in (raw_used_docs or [])[:3]:
        if isinstance(ev, str):
            s = ev.strip().replace("\n", " ")[:200]
            if s:
                used_docs.append(s)
            continue
        if isinstance(ev, dict):
            # Prefer short human-friendly fields if available
            s = ev.get("title") or ev.get("snippet") or ev.get("content") or ev.get("locator") or ""
            s = str(s).strip().replace("\n", " ")[:200]
            if s:
                used_docs.append(s)
            continue
        # Fallback
        try:
            s = str(ev)[:200]
            used_docs.append(s)
        except Exception:
            continue

    # Steps
    knowledge_steps = analysis.get("knowledge_fix_steps") or []
    generic_steps = analysis.get("generic_fix_steps") or []
    prevention = analysis.get("prevention") or []
    needs_rerun = analysis.get("needs_rerun", True)
    confidence = analysis.get("confidence", 0.5)

    dag = result.get("dag_id") or "—"
    task = result.get("task_id") or "—"
    run = result.get("dag_run_id") or "—"
    try_number = result.get("try_number")
    try_str = str(try_number) if try_number is not None else "—"

    # Build a safe airflow_ui_url if identifiers present
    airflow_ui_url = None
    if dag != "—" and task != "—" and run != "—" and try_number is not None and base_url:
        from urllib.parse import quote
        dag_enc = quote(str(dag), safe="")
        run_enc = quote(str(run), safe=":+")
        task_enc = quote(str(task), safe="")
        airflow_ui_url = f"{base_url}/dags/{dag_enc}/runs/{run_enc}/tasks/{task_enc}?try_number={try_number}"

    # Helper shorten (local override to ensure availability)
    def _shorten(text: str, max_len: int = 300) -> str:
        if not text:
            return ""
        text = str(text)
        if len(text) <= max_len:
            return text
        return text[:max_len].rstrip() + "…"

    # Convert steps -> textblocks
    def steps_to_textblocks(steps: List[str]) -> List[Dict[str, Any]]:
        if not steps:
            return [{"type": "TextBlock", "text": "- (Yok)", "wrap": True, "isSubtle": True, "spacing": "None", "size": "Small"}]
        blocks = []
        for s in steps:
            blocks.append({"type": "TextBlock", "text": f"- {s}", "wrap": True, "spacing": "None", "size": "Small"})
        return blocks

    # Knowledge section: only knowledge_fix_steps (no used_docs text)
    knowledge_section_items: List[Dict[str, Any]] = [
        {"type": "TextBlock", "text": "📚 Knowledge Kaynaklı Öneriler", "weight": "Bolder", "size": "Default", "spacing": "None"},
        {"type": "TextBlock", "text": "", "spacing": "Small"}
    ]

    if not knowledge_steps:
        knowledge_section_items.append({"type": "TextBlock", "text": "- (Yok)", "wrap": True, "isSubtle": True, "spacing": "None", "size": "Small"})
    else:
        knowledge_section_items += steps_to_textblocks(knowledge_steps)

    knowledge_section = {
        "type": "Container",
        "id": "knowledge_section",
        "spacing": "Medium",
        "items": knowledge_section_items
    }

    # Documents section: show used_docs short strings under a separate heading
    documents_section_items: List[Dict[str, Any]] = [
        {"type": "TextBlock", "text": "📄 Kullanılan Dokümanlar", "weight": "Bolder", "size": "Default", "spacing": "None"},
        {"type": "TextBlock", "text": "", "spacing": "Small"}
    ]

    if used_docs:
        for u in used_docs:
            documents_section_items.append({"type": "TextBlock", "text": f"- { _shorten(u, 300) }", "wrap": True, "spacing": "None", "size": "Small"})
        documents_section_items.append({"type": "TextBlock", "text": "", "spacing": "Small"})
    else:
        documents_section_items.append({"type": "TextBlock", "text": "- (Kullanılan doküman bulunamadı)", "wrap": True, "isSubtle": True, "spacing": "None", "size": "Small"})
        documents_section_items.append({"type": "TextBlock", "text": "", "spacing": "Small"})

    documents_section = {
        "type": "Container",
        "id": "documents_section",
        "spacing": "Medium",
        "items": documents_section_items
    }

    # Generic section
    generic_section = {
        "type": "Container",
        "id": "generic_section",
        "spacing": "Medium",
        "items": [
            {"type": "TextBlock", "text": "🔧 Genel Öneriler", "weight": "Bolder", "size": "Default", "spacing": "None"},
            {"type": "TextBlock", "text": "", "spacing": "Small"}
        ] + steps_to_textblocks(generic_steps)
    }

    # Details container (collapsed by default)
    details_container = {
        "type": "Container",
        "id": "details_section",
        "isVisible": False,
        "items": [
            {"type": "TextBlock", "text": "**Detaylar**", "weight": "Bolder", "size": "Small"},
            {"type": "FactSet", "facts": [
                {"title": "DAG", "value": str(dag)},
                {"title": "Task", "value": str(task)},
                {"title": "Run", "value": str(run)},
                {"title": "Try", "value": try_str},
                {"title": "Needs rerun", "value": str(needs_rerun)},
                {"title": "Confidence", "value": f"{confidence:.2f}"}
            ]}
        ]
    }

    # Card body
    body: List[Dict[str, Any]] = [
        {
            "type": "Container",
            "items": [
                {"type": "TextBlock", "text": "🚨 Airflow Task Failure", "weight": "Bolder", "size": "Large", "wrap": True},
                {"type": "TextBlock", "text": _shorten(error_summary, 1000), "wrap": True, "spacing": "Small"},
                {
                    "type": "ColumnSet",
                    "columns": [
                        {"type": "Column", "width": "stretch", "items": [
                            {"type": "TextBlock", "text": f"**DAG:** `{dag}`", "wrap": True, "size": "Small", "spacing": "None"}
                        ]}
                    ],
                    "spacing": "Small"
                }
            ]
        },
        {
            "type": "ActionSet",
            "id": "buttons_row",
            "actions": [a for a in [
                # Log link if available
                {"type": "Action.OpenUrl", "title": "Log'a Git", "url": airflow_ui_url or (result.get("log_url") or "")},
                # Confluence button only when configured (confluence.page_url)
                {"type": "Action.OpenUrl", "title": "📄 Confluence", "url": confluence_url} if confluence_url else None,
                # Only LLM analyses stored for feedback carry this link (Airflow HITL task of the analyzer run)
                {"type": "Action.OpenUrl", "title": "📝 Geri bildirim ver", "url": result["feedback_url"]} if result.get("feedback_url") else None,
                {"type": "Action.ToggleVisibility", "title": "Detaylar", "targetElements": ["details_section"]}
            ] if a]
        },
        details_container,
    ]

    if root_cause:
        body.append({
            "type": "Container",
            "style": "emphasis",
            "spacing": "Medium",
            "items": [
                {"type": "TextBlock", "text": "🎯 Kök Neden", "weight": "Bolder", "size": "Default", "spacing": "None"},
                {"type": "TextBlock", "text": _shorten(root_cause, 1000), "wrap": True, "spacing": "Small"},
            ]
        })

    body.append({"type": "TextBlock", "text": "---", "separator": True, "spacing": "Medium"})

    # Order: Documents (used_docs) first (after header), then Knowledge steps, then Generic, then Prevention
    body.append(documents_section)
    body.append({"type": "TextBlock", "text": "---", "separator": True, "spacing": "Medium"})
    body.append(knowledge_section)
    body.append({"type": "TextBlock", "text": "---", "separator": True, "spacing": "Medium"})
    body.append(generic_section)

    # Optional prevention
    if prevention:
        body.append({"type": "TextBlock", "text": "---", "separator": True, "spacing": "Medium"})
        prevention_container = {
            "type": "Container",
            "items": [
                {"type": "TextBlock", "text": "🛡️ Önleme (Prevention)", "weight": "Bolder", "size": "Default"},
                {"type": "TextBlock", "text": "", "spacing": "Small"}
            ] + steps_to_textblocks(prevention)
        }
        body.append(prevention_container)

    card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": body,
        "msteams": {"width": "Full"}
    }

    return card


def output_to_teams(result: Dict[str, Any], webhook_url: str, verify_ssl: bool = True, base_url: Optional[str] = None, confluence_url: Optional[str] = None) -> None:
    """
    Send an Adaptive Card to Teams with nice layout and toggles.
    """

    card = _build_adaptive_card(result, base_url, confluence_url)
    text = f"Airflow failure: {result.get('dag_id')} / {result.get('task_id')} - {result.get('analysis', {}).get('error_summary', '')}"
    _post_card(card, text, webhook_url, verify_ssl)


def _post_card(card: Dict[str, Any], text: str, webhook_url: str, verify_ssl: bool) -> None:
    # The Teams incoming webhook expects a top-level JSON with attachments:
    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": card
            }
        ],
        # fallback text for older/unsupported clients
        "text": text
    }

    headers = {"Content-Type": "application/json"}
    try:
        LOG.info("[teams] Posting Adaptive Card to teams webhook (masked url)")
        r = requests.post(webhook_url, json=payload, headers=headers, verify=verify_ssl, timeout=(5, 20))
        r.raise_for_status()
        LOG.info("[teams] posted Adaptive Card, status=%s", r.status_code)
    except Exception:
        LOG.exception("[teams] Failed to post Adaptive Card")
        raise


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


def _build_summary_card(summary: Dict[str, Any]) -> Dict[str, Any]:
    affected = summary.get("affected") or []
    lines = [f"- {a.get('dag_id')}.{a.get('task_id')} ({a.get('run_id')})" for a in affected]
    more = summary["count"] - len(affected)
    if more > 0:
        lines.append(f"- ... ve {more} task daha")
    body = [
        {"type": "TextBlock", "text": f"🔁 Aynı hata: {summary['count']} task daha etkilendi", "weight": "Bolder", "size": "Large", "wrap": True},
        {"type": "TextBlock", "text": _shorten(summary.get("title") or "", 300), "wrap": True, "fontType": "Monospace", "spacing": "Small"},
        {"type": "TextBlock", "text": f"{summary.get('window_start')} → {summary.get('last_seen')}", "isSubtle": True, "spacing": "Small", "wrap": True},
        {"type": "TextBlock", "text": "\n".join(lines), "wrap": True, "spacing": "Medium"},
    ]
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": body,
        "msteams": {"width": "Full"},
    }


def output_summary(cfg: Dict[str, Any], summary: Dict[str, Any]) -> None:
    """Send a dedup "N tasks affected" summary with the configured output method; raises on failure."""
    out = cfg.get("output") or {}
    method = out.get("method", "stdout")
    if method == "teams" and out.get("teams_webhook"):
        card = _build_summary_card(summary)
        _post_card(card, f"Aynı hata: {summary['count']} task daha etkilendi", out["teams_webhook"], out.get("teams_verify_ssl", True))
    elif method == "file":
        output_to_file({"dedup_summary": summary}, out.get("file_path") or "/tmp/failed_task_log.json")
    else:
        output_to_stdout({"dedup_summary": summary})
