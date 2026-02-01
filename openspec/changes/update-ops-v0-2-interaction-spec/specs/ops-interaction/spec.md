## ADDED Requirements
### Requirement: HTTP v0.2 core API
The system MUST expose an HTTP API that provides health, events, sources, ingests, views, jobs, and artifacts endpoints as the single source of truth for operations.

#### Scenario: Health check
- **WHEN** a client requests GET /health
- **THEN** the service responds with ok=true and version/schema_version set to 0.2

### Requirement: Canonical-first ingest
The system MUST append events to canonical JSONL with fsync before indexing and MUST persist dedupe_key in canonical events.

#### Scenario: Ingest with dedupe
- **WHEN** the same events are ingested twice with dedupe enabled
- **THEN** the second ingest reports skipped events and no duplicates are appended

### Requirement: Stored sources and views
The system MUST store sources and views in SQLite with JSON configuration and allow CRUD via HTTP endpoints.

#### Scenario: Create and query a view
- **WHEN** a user creates a view with filters and queries it
- **THEN** the response returns matching events in the configured order

### Requirement: Jobs and artifacts
The system MUST support jobs that generate artifacts and record artifact.created events referencing output files.

#### Scenario: Daily digest run
- **WHEN** a daily_digest job is executed
- **THEN** it writes a markdown artifact and records an artifact.created event referencing it

### Requirement: HTTP-first CLI
The CLI MUST call the HTTP API by default and support explicit offline mode for local ingest operations.

#### Scenario: CLI offline ingest
- **WHEN** ops ingest is invoked with --offline and the service is unavailable
- **THEN** the CLI ingests locally using canonical append and indexing logic
