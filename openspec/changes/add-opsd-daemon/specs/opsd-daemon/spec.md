## ADDED Requirements
### Requirement: Opsd single-writer daemon
The system SHALL provide a local-only HTTP daemon that serializes canonical writes and updates the SQLite index for a workspace.

#### Scenario: Health check
- **WHEN** a client requests `GET /health`
- **THEN** the daemon returns a JSON payload with ok status, version, workspace path, and pid

#### Scenario: Batch ingest serialization
- **WHEN** multiple concurrent batch ingest requests are received
- **THEN** the daemon processes them sequentially so canonical writes are mutually exclusive

### Requirement: Opsd exclusive instance lock
The daemon MUST acquire an exclusive lock for the workspace before accepting requests.

#### Scenario: Second daemon startup
- **WHEN** a second daemon tries to start on the same workspace
- **THEN** startup fails because the exclusive lock cannot be acquired

### Requirement: Opsd batch dedupe and write contract
The daemon SHALL accept batch ingest requests, compute dedupe keys and hashes server-side, and append canonical data before writing to SQLite.

#### Scenario: Existing dedupe key
- **WHEN** an event with an existing dedupe key is submitted
- **THEN** the daemon returns a skipped result that includes the existing event id

#### Scenario: SQLite write failure
- **WHEN** canonical append succeeds but SQLite write fails
- **THEN** the daemon reports a failure for that event while leaving canonical data intact
