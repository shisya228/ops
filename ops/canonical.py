from __future__ import annotations

import json
import os
from pathlib import Path

from .errors import IOError


def append_event(path: Path, event: dict) -> None:
    try:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        raise IOError(f"Failed to append canonical event: {exc}") from exc
