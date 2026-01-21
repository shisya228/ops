from __future__ import annotations

import sqlite3
from pathlib import Path

from .errors import DatabaseError

DDL = """
CREATE TABLE IF NOT EXISTS events (
rowid INTEGER PRIMARY KEY AUTOINCREMENT,
id TEXT NOT NULL UNIQUE,
schema_version TEXT NOT NULL,
ts TEXT NOT NULL,
type TEXT NOT NULL,
tags_json TEXT NOT NULL DEFAULT '[]',
text TEXT NOT NULL DEFAULT '',
payload_json TEXT NOT NULL DEFAULT '{}',
source_kind TEXT NOT NULL,
source_locator TEXT NOT NULL,
source_meta_json TEXT NOT NULL DEFAULT '{}',
hash_algo TEXT NOT NULL,
hash_value TEXT NOT NULL,
dedupe_key TEXT,
created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_ts   ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
CREATE INDEX IF NOT EXISTS idx_events_dedupe ON events(dedupe_key);

CREATE TABLE IF NOT EXISTS refs (
id INTEGER PRIMARY KEY AUTOINCREMENT,
event_id TEXT NOT NULL,
ref_kind TEXT NOT NULL,
uri TEXT NOT NULL,
span_json TEXT NOT NULL DEFAULT '{}',
digest_algo TEXT,
digest_value TEXT,
FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_refs_event ON refs(event_id);
CREATE INDEX IF NOT EXISTS idx_refs_uri   ON refs(uri);

CREATE TABLE IF NOT EXISTS dedupe (
dedupe_key TEXT PRIMARY KEY,
event_id TEXT NOT NULL,
first_seen_ts TEXT NOT NULL,
FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE
);

CREATE VIRTUAL TABLE IF NOT EXISTS events_fts
USING fts5(
text,
content='events',
content_rowid='rowid',
tokenize='unicode61 remove_diacritics 2'
);

CREATE TRIGGER IF NOT EXISTS events_ai AFTER INSERT ON events BEGIN
INSERT INTO events_fts(rowid, text) VALUES (new.rowid, new.text);
END;
CREATE TRIGGER IF NOT EXISTS events_ad AFTER DELETE ON events BEGIN
INSERT INTO events_fts(events_fts, rowid, text) VALUES('delete', old.rowid, old.text);
END;
CREATE TRIGGER IF NOT EXISTS events_au AFTER UPDATE OF text ON events BEGIN
INSERT INTO events_fts(events_fts, rowid, text) VALUES('delete', old.rowid, old.text);
INSERT INTO events_fts(rowid, text) VALUES (new.rowid, new.text);
END;
"""


PRAGMAS = [
    "PRAGMA journal_mode=WAL;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA foreign_keys=ON;",
    "PRAGMA temp_store=MEMORY;",
    "PRAGMA busy_timeout=5000;",
]


def connect(db_path: Path) -> sqlite3.Connection:
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        for pragma in PRAGMAS:
            conn.execute(pragma)
        return conn
    except sqlite3.Error as exc:
        raise DatabaseError(f"SQLite connection error: {exc}") from exc


def init_db(db_path: Path) -> None:
    try:
        conn = connect(db_path)
        with conn:
            conn.executescript(DDL)
    except sqlite3.Error as exc:
        raise DatabaseError(f"SQLite init error: {exc}") from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass
