# SQLite Storage Engine - Design

This document describes SQLite-specific implementation choices. The semantic
contract shared by every engine is defined in
[`../architecture.md`](../architecture.md).

## Interface

`SqliteStorageEngine` implements the flattened `StorageEngine` trait. Core
does not receive or inject `SqliteBucket` handles. Buckets are private,
model-scoped materialization helpers created by the engine.

The catalog resolver is injected once into `SqliteStorageEngine` by `Node`.
The engine shares it with a bucket when property projection requires catalog
metadata. Durable model/property naming registries are always checked before
the resolver is consulted.

## Durable layout

SQLite keeps canonical and derived data separate:

- `_ankurah_entity`: one canonical state, head, and attestation record per
  `EntityId`;
- `_ankurah_event`: one canonical event per `EventId`, indexed by
  `entity_id`;
- `_ankurah_entity_model`: the engine-private durable association between an
  entity and every model through which it has been accepted;
- `_ankurah_sqlite_model_map`: durable `ModelId` to unique physical
  materialization-table assignment;
- `_ankurah_sqlite_column_map`: durable `(ModelId, PropertyId)` to unique
  physical-column assignment;
- one projected materialization table per model.

Canonical state and events do not contain a `ModelId`. A state write records
its explicit access model, then refreshes every materialization already
associated with the entity.

## Physical naming

Registered labels are lowercase, sanitized hints rather than addresses. On a
durable-map miss, SQLite asks the resolver for the registered label,
deduplicates it against existing physical assignments, persists the winner,
and then uses it. Two unrelated ids with the same label therefore receive
different physical names. Renaming catalog metadata never moves an existing
table or column.

Built-in system models and properties use fixed bootstrap names.

## Materialized values

Materialization tables contain the entity id plus projected fields used by
query planning and indexes. The canonical state remains authoritative.

| Value kind | SQLite representation |
|---|---|
| string | `TEXT` |
| integer / boolean | `INTEGER` |
| floating point | `REAL` |
| bytes and opaque state | `BLOB` |
| JSON | SQLite JSONB `BLOB` |

Nested JSON predicates use `json_extract`, preserving SQLite-native scalar
comparison behavior. Missing projected columns are added under a per-bucket
DDL mutex, with the durable column map rechecked while holding the lock.

## Query execution

`fetch_states(model, selection)`:

1. resolves durable property ids to the model materialization's physical
   columns;
2. splits pushdown-capable predicates from Rust post-filtering;
3. executes filtering, ordering, and eligible limits against the
   materialization table;
4. hydrates matching canonical records from `_ankurah_entity`.

The AST remains logical and identity-addressed. Physical names never leak back
into AnkQL.

## Connections

The engine uses a `bb8` pool around `rusqlite`. Synchronous connection work is
run through the connection wrapper's blocking boundary. File-backed databases
enable WAL and the engine's standard performance pragmas; the in-memory engine
uses a one-connection pool so all operations observe the same database.
