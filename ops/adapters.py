from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from .errors import AdapterError


def load_chat_json(path: Path) -> list[dict]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise AdapterError(f"Failed to read input: {exc}") from exc
    stripped = text.lstrip()
    if not stripped:
        return []
    try:
        if stripped.startswith("["):
            data = json.loads(text)
            if not isinstance(data, list):
                raise AdapterError("chat_json must be a JSON array")
            return data
        entries: list[dict] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            entries.append(json.loads(line))
        return entries
    except json.JSONDecodeError as exc:
        raise AdapterError(f"Invalid JSON input: {exc}") from exc


def iter_chat_messages(path: Path) -> Iterable[dict]:
    data = load_chat_json(path)
    for item in data:
        if not isinstance(item, dict):
            raise AdapterError("chat_json entries must be objects")
        yield item
