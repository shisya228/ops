# Change: Add opsd single-writer daemon and daemon-aware ingest

## Why
Concurrent writers can corrupt the canonical JSONL and derived SQLite index. A single-writer daemon with a daemon-aware ingest path provides safe serialization while keeping canonical data append-only.

## What Changes
- Add an opsd HTTP daemon that serializes canonical writes and updates the SQLite index.
- Add daemon lifecycle CLI commands and daemon-aware ingest that falls back to a locked local write.
- Add file locks for daemon exclusivity and local fallback safety.
- Add tests covering daemon health, batch dedupe, and concurrent requests.

## Impact
- Affected specs: opsd-daemon, ingest
- Affected code: ops daemon implementation, CLI ingest path, locking, tests
