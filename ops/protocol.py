from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class EventDraft:
    schema_version: str
    ts: str
    type: str
    source: dict[str, Any]
    refs: list[dict[str, Any]]
    tags: list[str]
    text: str
    payload: dict[str, Any]


@dataclass
class BatchRequest:
    events: list[EventDraft]
    atomic: bool = False


@dataclass
class BatchResult:
    status: str
    event_id: str | None = None
    existing_event_id: str | None = None
    dedupe_key: str | None = None
    hash: str | None = None
    error: str | None = None


@dataclass
class BatchResponse:
    inserted: int
    skipped: int
    failed: int
    results: list[BatchResult]
