from __future__ import annotations

import json
from typing import Any

from .utils import normalize_text, sha256_hex


def event_hash(event_data: dict[str, Any]) -> dict[str, str]:
    payload = json.dumps(
        event_data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {"algo": "sha256", "value": sha256_hex(payload.encode("utf-8"))}


def dedupe_key(adapter: str, locator: str, idx: int, content: str) -> str:
    span_key = f"idx:{idx}"
    norm_text = normalize_text(content)
    material = f"{adapter}|{locator}|{span_key}|{norm_text}"
    return sha256_hex(material.encode("utf-8"))


def dedupe_key_from_event(event: dict[str, Any]) -> str | None:
    dedupe_value = event.get("dedupe_key")
    if dedupe_value:
        return dedupe_value
    if event.get("type") != "chat.message":
        return None
    source = event.get("source", {})
    adapter_name = source.get("kind")
    locator = source.get("locator")
    if not adapter_name or not locator:
        return None
    idx = None
    refs = event.get("refs", [])
    if refs:
        idx = refs[0].get("span", {}).get("idx")
    if idx is None:
        return None
    payload = event.get("payload", {})
    content = payload.get("content") or event.get("text")
    if not content:
        return None
    return dedupe_key(adapter_name, locator, idx, content)


def dedupe_key_from_draft(draft: dict[str, Any]) -> str | None:
    if draft.get("type") != "chat.message":
        return None
    source = draft.get("source", {})
    adapter_name = source.get("kind")
    locator = source.get("locator")
    if not adapter_name or not locator:
        return None
    refs = draft.get("refs", [])
    idx = None
    if refs:
        idx = refs[0].get("span", {}).get("idx")
    if idx is None:
        return None
    payload = draft.get("payload", {})
    content = payload.get("content") or draft.get("text")
    if not content:
        return None
    return dedupe_key(adapter_name, locator, idx, content)
