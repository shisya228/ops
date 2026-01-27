## ADDED Requirements
### Requirement: Daemon-aware ingest
The CLI ingest flow SHALL prefer sending drafts to the local daemon, and fall back to a locked local write when the daemon is unavailable or disabled.

#### Scenario: Daemon available
- **WHEN** ingest runs with the daemon reachable
- **THEN** events are sent via the daemon batch API and results are summarized in the CLI output

#### Scenario: Daemon unavailable
- **WHEN** ingest runs and the daemon cannot be reached
- **THEN** ingest performs a local write while holding the global write lock
