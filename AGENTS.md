<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

## Project Notes

### What this repo is
`ops` is a local event indexing tool that treats JSONL as the canonical source of truth and builds a rebuildable SQLite (FTS5) index for querying and inspection.

### Quick Commands
- Initialize workspace: `python -m ops init`
- Ingest chat JSON/JSONL: `python -m ops ingest chat_json path/to/chat.json --tag demo --json`
- Query index: `python -m ops query "demo" --json`
- Show event: `python -m ops show <event_id> --json`
- Rebuild index: `python -m ops index rebuild --wipe`

### Conventions
- Canonical data lives at `data/canonical/events.jsonl` and is append-only.
- The SQLite index at `data/index/brain.sqlite` is derived data and can be rebuilt.
