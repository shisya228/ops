## Context
ops must shift to an HTTP-first interaction model with opsd as the single source of truth, while preserving canonical JSONL invariants and derived SQLite indexes.

## Goals / Non-Goals
- Goals:
  - Provide a v0.2 HTTP API with object-based endpoints for sources, ingests, views, jobs, artifacts, and events.
  - Keep canonical JSONL append-only with fsync-before-index semantics.
  - Keep the CLI as a thin HTTP client with explicit offline options.
- Non-Goals:
  - Replacing the canonical storage model.
  - Introducing new non-stdlib dependencies.

## Decisions
- Decision: Store sources/views/jobs/job_runs in SQLite with JSON-serialized config fields.
  - Why: keeps metadata durable and queryable without new services.
- Decision: Reuse existing canonical append/indexing pipeline for ingest and artifact emission.
  - Why: preserves invariants and avoids re-implementing hashing/dedupe logic.
- Decision: Implement views as stored JSON query objects with controlled merging.
  - Why: avoids exposing raw SQL while still supporting reusable queries.

## Risks / Trade-offs
- Risk: HTTP/CLI parity may miss edge cases.
  - Mitigation: add end-to-end pytest coverage for CRUD, ingest, views, jobs, and artifacts.

## Migration Plan
- Add schema tables and meta schema_version on init.
- Implement opsd v0.2 endpoints and update CLI to call them.
- Validate end-to-end flows with pytest.

## Open Questions
- None.
