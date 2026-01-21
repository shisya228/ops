from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import ConfigError


@dataclass
class OpsConfig:
    workspace: Path
    timezone: str
    default_redaction: bool
    fts_enabled: bool
    max_snippet_len: int


DEFAULT_CONFIG_TEXT = """workspace: "./data"
timezone: "Asia/Tokyo"
privacy:
  default_redaction: false
index:
  fts: true
  max_snippet_len: 160
"""


def write_default_config(path: Path) -> None:
    path.write_text(DEFAULT_CONFIG_TEXT, encoding="utf-8")


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    if not value:
        return ""
    if value.startswith("\"") and value.endswith("\""):
        return value[1:-1]
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    if value.isdigit():
        return int(value)
    return value


def load_config(path: Path) -> OpsConfig:
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    lines = path.read_text(encoding="utf-8").splitlines()
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(0, root)]
    for raw_line in lines:
        line = raw_line.split("#", 1)[0].rstrip("\n")
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        key_value = line.strip().split(":", 1)
        if len(key_value) != 2:
            raise ConfigError(f"Invalid config line: {raw_line}")
        key = key_value[0].strip()
        value = key_value[1]
        while stack and indent < stack[-1][0]:
            stack.pop()
        if not stack:
            raise ConfigError(f"Invalid indentation: {raw_line}")
        current = stack[-1][1]
        if value.strip() == "":
            current[key] = {}
            stack.append((indent + 2, current[key]))
        else:
            current[key] = _parse_scalar(value)
    try:
        workspace = Path(root["workspace"])
        timezone = str(root["timezone"])
        privacy = root.get("privacy", {})
        default_redaction = bool(privacy.get("default_redaction", False))
        index = root.get("index", {})
        fts_enabled = bool(index.get("fts", True))
        max_snippet_len = int(index.get("max_snippet_len", 160))
    except Exception as exc:  # noqa: BLE001
        raise ConfigError("Config missing required fields") from exc
    return OpsConfig(
        workspace=workspace,
        timezone=timezone,
        default_redaction=default_redaction,
        fts_enabled=fts_enabled,
        max_snippet_len=max_snippet_len,
    )
