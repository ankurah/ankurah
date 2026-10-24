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
2. splits pushdown-capable predicates from Rust post-filtering;
3. executes filtering, ordering, and eligible limits against the
   materialization table;
4. hydrates matching canonical records from `_ankurah_entity`.

The AST remains logical and identity-addressed. Physical names never leak back
into AnkQL.

## Connections

The engine writes its SQL once, against the executor boundary (the
`executor` module), so that one engine serves every SQLite driver. A
`SqliteExecutor` runs one statement per call on one SQLite connection, and
each `Row` it returns holds each value with the storage class SQLite reports
for it, unconverted. Everything above the boundary decides which SQL to run;
an executor below it runs that SQL on one driver's connection. The module
holds what every executor shares: the trait and its contract, `Row`,
transactions and savepoints, the session pragmas, and the scan
(`executor/statement_scan`) that finds where a statement ends before SQLite
prepares it, so that SQL holding a second statement is refused on every
driver. Each target compiles one executor:

- `executor/native`: natively, the engine uses a `bb8` pool around
  `rusqlite`. Synchronous connection work is run through the connection
  wrapper's blocking boundary. File-backed databases enable WAL and the
  engine's standard performance pragmas; the in-memory engine uses a
  one-connection pool so all operations observe the same database.
- `executor/nodejs`: on wasm32, behind the `nodejs` feature, the engine runs
  inside Node.js on one database object, synchronously on the calling
  thread. `open` and `open_in_memory` construct a node:sqlite `DatabaseSync`
  and give it the native connections' session settings. A host that built a
  database itself, a node:sqlite `DatabaseSync` or a better-sqlite3
  `Database`, hands it to `SqliteStorageEngine::from_database`, which keeps
  the host's session settings and refuses a database the executor cannot
  read exactly. Among those is a database whose node:sqlite skips a numbered
  parameter as it binds a call's values, as node:sqlite does before Node.js
  22.20.0 on 22 and 24.7.0 on 24: the executor binds one and reads it back,
  whatever the release. The executor reads each query's rows through a
  typed reader, a WITH clause that nests the query and reads each value with
  its storage class; where that cannot read as rusqlite does, the query is
  refused before it runs. A PRAGMA or an EXPLAIN, which SQLite does not
  nest, is the exception: its TEXT is read as the driver's string, and
  refused after it runs if it holds U+FFFD, which both drivers put in place
  of a byte that is not UTF-8. `SqliteExecutor`'s documentation lists each
  way the drivers differ.

Every driver's errors are typed. Natively they are rusqlite's, as
`SqliteError::Rusqlite`. On Node.js an error SQLite raised is a
`SqliteError::Sqlite`, with SQLite's extended result code; a refusal of the
executor's, of a call or a database it cannot run or read as rusqlite does,
is a `SqliteError::Refused`; and any other JavaScript exception is a
`SqliteError::JavaScript`. `SqliteError::sqlite_code` gives SQLite's
extended result code, reading better-sqlite3's name for a code as its
number, and `SqliteError::refusal` gives a refused call's `Refusal`,
natively by mapping each of rusqlite's own refusals to the one the Node.js
executor raises for the same call. Both answer the same on every driver,
except where their documentation says.

Every connection the engine opens has double-quoted string literals off, for
DML and DDL, so that a double-quoted name that matches no column fails with
"no such column" instead of reading as the string of its name: a native
connection switches them off, and node:sqlite's `DatabaseSync` has them off
unless constructed with them on. `from_database` refuses a database that reads
such a name as a string. A schema stored with the literals on still opens, but
a view or trigger whose body relies on one fails when used, and SQLite then
refuses to rename any table on that file, or to rename or drop any column,
since each of those reads every view and trigger again. ADD COLUMN, the only
change the engine makes to a table, still runs.

## Dump

The dump pages through the events and then the states, each page read
through a connection the pool hands out for it. A page of states reads its
states and their memberships in one snapshot, so that each state's
memberships are read as of the state itself. The engine takes the snapshot
through the executor's savepoint, which begins a transaction on a
connection outside one, and nests in one an embedder left open. Each page is
a snapshot of its own, so a dump of a store that is being written is
consistent within each page, not across the dump.
