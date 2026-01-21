from __future__ import annotations

import hashlib
import os
import random
import re
from datetime import datetime
from zoneinfo import ZoneInfo

CROCKFORD_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
ULID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")


def iso_now(tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    return datetime.now(tz=tz).isoformat()


def iso_from_timestamp(ts: float, tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    return datetime.fromtimestamp(ts, tz=tz).isoformat()


def _encode_base32(value: int, length: int) -> str:
    chars = []
    for _ in range(length):
        chars.append(CROCKFORD_ALPHABET[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def generate_ulid() -> str:
    timestamp_ms = int(datetime.utcnow().timestamp() * 1000)
    rand_hi = random.getrandbits(80)
    return _encode_base32(timestamp_ms, 10) + _encode_base32(rand_hi, 16)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in text.split("\n")]
    normalized = "\n".join(lines)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    return normalized


def ensure_ulid(value: str) -> bool:
    return bool(ULID_RE.match(value))
