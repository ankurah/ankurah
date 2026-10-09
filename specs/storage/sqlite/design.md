# SQLite Storage Engine - Design

This document describes SQLite-specific implementation choices. The semantic
contract shared by every engine is defined in
[`../architecture.md`](../architecture.md).

## Interface

`SqliteStorageEngine` implements the flattened `StorageEngine` trait. Core
does not receive or inject materialization handles. The engine owns its private,
model-scoped `Materialization` helpers.

The catalog resolver is injected once into `SqliteStorageEngine` by `Node`.
Materializations consult it only for optional labels when assigning physical
names. Durable model/property naming registries are always checked first.

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
2. makes sure the index the shared planner's plan reads exists on the
   serving model's table, creating it on first use under the engine's index
   DDL lock, before the read snapshot opens;
3. splits pushdown-capable predicates from Rust post-filtering;
4. executes filtering, ordering, and eligible limits against the
   materialization table;
5. hydrates matching canonical records from `_ankurah_entity`.

The AST remains logical and identity-addressed. Physical names never leak back
into AnkQL.

The indexes are the ones sled and IndexedDB build for the same query: the
shared planner's `Plan::Index` for the selection lowered to the table's
columns, created with `CREATE INDEX IF NOT EXISTS` on the key parts' columns
in their directions, a JSON sub-path as its `json_extract` expression, and
named sled's way behind the table name. An existing index serves a shorter key
only past trailing `id` parts, which is sled's rule. The catalog
(`pragma_index_list` and `pragma_index_xinfo`) is the only record of what
exists; it is cached per materialization and read again under the lock before
anything is created. SQLite builds the index inside the statement, so the
first query waits for it, and the query's SQL answers with or without the
index.

## Connections

The engine uses a `bb8` pool around `rusqlite`. Synchronous connection work is
run through the connection wrapper's blocking boundary. File-backed databases
enable WAL and the engine's standard performance pragmas; the in-memory engine
uses a one-connection pool so all operations observe the same database.
