# SQLite Storage Engine - Requirements

SQLite provides Ankurah's embedded SQL storage option. It must satisfy the
shared [`StorageEngine` contract](../architecture.md) and the following
backend-specific requirements.

## Functional requirements

- Support persistent file databases and an isolated in-memory test database.
- Store canonical entity states and events once, independent of models.
- Durably retain entity-to-model associations and refresh all associated
  materializations after a state write.
- Maintain one query materialization per model without exposing its private
  bucket handle through the public storage trait.
- Push eligible AnkQL predicates and ordering into SQLite and post-filter any
  unsupported remainder in Rust.
- Project registered properties by durable `PropertyId`, not display label.
- Support nested JSON predicates through SQLite JSON functions.

## Naming requirements

- Consult durable model and property name maps before consulting the catalog
  resolver.
- Use resolver labels only as sanitized, lowercase naming seeds.
- Deduplicate equal labels belonging to unrelated ids.
- Keep assigned physical names stable across reopen and catalog rename.
- Bootstrap built-in system models without requiring a warm registered
  catalog.

## Reliability requirements

- Persist canonical state and a newly observed entity-model association in one
  SQLite transaction.
- Serialize materialization DDL and recheck durable assignments under the
  lock.
- Use WAL for file-backed databases and retain foreign-key and connection
  health checks.
- Never lose canonical state, events, or entity-model associations when a
  derived materialization is rebuilt or removed.

## Validation

Tests must cover colliding model/property labels, cross-model projection
refresh, protocol compatibility metadata, nested JSON behavior, query
pushdown/fallback, and reopen stability.
