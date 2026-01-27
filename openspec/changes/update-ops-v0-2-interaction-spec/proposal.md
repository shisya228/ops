# Change: Implement ops v0.2 interaction spec

## Why
The repo needs a clean, HTTP-first interaction model with a thin CLI client so ops can be driven consistently by automation and local workflows.

## What Changes
- Add a v0.2 HTTP API surface in opsd for sources, ingests, views, jobs, artifacts, and events.
- Introduce SQLite schema tables for sources, views, jobs, and job runs with a schema_version meta record.
- Replace CLI flows with an HTTP-first thin client that supports offline mode where required.
- Add end-to-end tests that validate the new API and CLI behaviors.

## Impact
- Affected specs: ops-interaction
- Affected code: opsd HTTP handlers, CLI entrypoints, DB schema, tests
