# Project Context

## Purpose
`ops` is a local event indexing tool that stores events in JSONL as the canonical source of truth and provides a rebuildable SQLite (FTS5) index for querying and inspection.

## Tech Stack
- Python 3.11+ (packaged via `pyproject.toml`)
- SQLite + FTS5 (built-in `sqlite3` module)
- JSONL files for canonical event storage

## Project Conventions

### Code Style
- Python modules are small and focused, with type annotations and dataclasses where appropriate.
- Prefer explicit error types (`OpsError` subclasses) over bare exceptions.
- Keep canonical event records normalized and hash-stable (sorted keys, UTF-8, compact JSON).

### Architecture Patterns
- Canonical event data is append-only in `data/canonical/events.jsonl`.
- SQLite (`data/index/brain.sqlite`) is derived data and can be rebuilt from canonical events.
- CLI operations live in `ops/cli.py` with supporting modules for config, adapters, canonical storage, and DB utilities.

### Testing Strategy
- Pytest is used; run `pytest` from the repo root.
- Tests live under `tests/` and exercise CLI workflows and indexing behavior.

### Git Workflow
- No explicit workflow documented; use short-lived feature branches and keep commits scoped.

## Domain Context
- Events represent local artifacts (e.g., chat logs) ingested into a normalized, queryable store.
- `chat_json` adapter ingests JSON or JSONL messages and deduplicates them based on content hashes.

## Important Constraints
- Canonical events MUST be append-only and hash-stable.
- Index data is non-authoritative and may be wiped/rebuilt.
- Timezone defaults to Asia/Tokyo unless configured in `ops.yml`.

## External Dependencies
- Local filesystem for canonical data and raw ingested artifacts.
- SQLite FTS5 for text search.
