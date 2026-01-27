## Context
Concurrent writers can corrupt append-only canonical JSONL and its derived SQLite index. The daemon introduces a single-writer path while preserving the canonical-as-SOT model.

## Goals / Non-Goals
- Goals:
  - Serialize all canonical writes through a local-only daemon.
  - Keep canonical append-only and SQLite rebuildable.
  - Provide CLI fallback with a global lock when the daemon is unavailable.
- Non-Goals:
  - Distributed or remote multi-writer support.
  - Moving query/show/rebuild into the daemon.

## Decisions
- Decision: Provide a localhost HTTP daemon with batch ingest and health endpoints.
  - Rationale: Small surface area and easy integration with existing CLI tools.
- Decision: Use file locks for daemon exclusivity and local fallback serialization.
  - Rationale: Cross-process mutual exclusion with minimal coordination.

## Risks / Trade-offs
- Local daemon adds a long-running process and dependency management.
  - Mitigation: Keep protocol small and fallback to local locked writes.

## Migration Plan
- Add daemon and CLI integration without changing read-only workflows.
- Ensure rebuild still consumes canonical JSONL as source of truth.

## Open Questions
- None.
