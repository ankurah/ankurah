# Storage Engines

Ankurah's storage boundary persists three different kinds of truth:

1. immutable, model-independent events;
2. one canonical state and causal head per entity; and
3. model-specific materializations used for querying and indexing.

The boundary is the single `StorageEngine` trait in `core/src/storage.rs`.
Physical tables, object stores, trees, index structures, name registries, and
entity-to-model association layouts are private to each engine.

```text
                   Node / Context / Reactor
                              |
                 event validation and replay
                              |
                     StorageEngine
                    /     |      \
          canonical      durable     per-model
        entities/events  associations materializations
                    \     |      /
                 engine-private layout
```

An entity does not belong to one model. A `ModelId` describes the usage context
through which an entity is created, authorized, projected, or queried. The
canonical entity and its events therefore carry no singular model identity.
The engine durably remembers every model through which an entity has been
accepted and refreshes all of those materializations whenever canonical state
changes.

## The `StorageEngine` contract

The trait groups operations by semantic responsibility:

| Method | Contract |
|---|---|
| `append_events` | Blindly and idempotently append validated, attested events by `EventId`; preserve input order in the inserted/not-inserted result |
| `commit_batch` | Atomically compare canonical heads, replace canonical states, add model associations, and refresh every associated materialization |
| `get_state` / `get_states` | Read canonical state by `EntityId`, independent of model |
| `fetch_states` | Query one model's materialized view and return the corresponding canonical states |
| `get_events` / `dump_entity_events` | Read model-independent event history |
| `list_materializations` | List already-created model materializations without creating new ones |
| `delete_all` | Delete only engine-owned data while retaining compatibility metadata |
| `set_catalog_resolver` | Inject the catalog name source once the node has constructed its catalog |

`PreparedEntityWrite` contains the exact durable `expected_head`, the proposed
attested canonical state, and the model usages which this write must associate
with the entity. `StorageWriteBatch` contains at most one write per entity and
is all-or-nothing.

`commit_batch` returns one of:

- `Committed(StorageCommitResult)`, with per-entity canonical-change,
  newly-added-association, and complete refreshed-materialization details; or
- `Conflict { observed }`, containing the canonical state (or absence) observed
  for every entity while checking the batch. A conflict commits nothing.

The observed states are part of the concurrency protocol, not diagnostics.
Core uses them to rebuild a monotonic candidate and retry from the exact head
the engine saw.

## Write and retry flow

Events are validated and attested before storage. They are then appended
blindly: storing the same content-addressed `EventId` twice is harmless, and an
event may be durable before any canonical head references it.

For canonical state, core's `commit_resident_writes` coordinator:

1. reads the current canonical states;
2. rebuilds each candidate from that durable base;
3. replays the transaction's already-durable events, preserving each event's
   original model and policy context;
4. submits one `StorageWriteBatch`; and
5. on conflict, replaces its bases with the engine-returned states, causally
   merges/replays, and retries with those observed heads as the next compare
   values.

State-only replication uses the same coordinator. Its validated snapshot is
the seed; a conflicting durable state is merged into that seed before the next
attempt. If the causal comparison needs events, the node's event getter obtains
them from local storage or a durable peer.

Within a successful `commit_batch`, the following are one engine transaction:

- every canonical entity replacement;
- every newly accepted entity/model association;
- every projection for the entity's complete associated model set; and
- the affected secondary-index maintenance.

A stale head on one entity rejects the entire batch. Consequently, readers can
never observe a new canonical head with an old projection, or half of a
multi-entity application transaction.

## Associations and materializations

The engine chooses how to represent the durable set
`(EntityId, ModelId)`. Core supplies only newly exercised model contexts.
Before writing, the engine unions those with its stored associations and
refreshes the complete model set from the proposed canonical state.

This distinction matters in two cases:

- Editing an entity through model A must also update its previously associated
  model B projection.
- Using an unchanged entity through a new model still adds the association and
  creates that model's projection, without claiming that canonical state
  changed.

Queries do not create associations merely by scanning a materialization:
anything returned by that materialization is already associated. Accepting a
state or event under a new model context does create the association.

## Catalog resolver and physical names

The node injects a weak `CatalogResolver` into the storage engine during node
construction. The engine decides when and where to use it. SQL and IndexedDB
engines consult it only when a durable physical-name lookup misses:

- `ModelId -> materialization name`, seeded from the registered model name;
- `(ModelId, PropertyId) -> physical field`, seeded from the registered
  property name.

The engine's durable map is authoritative after assignment. A catalog rename
does not move a table or column. Labels are sanitized to lower case and
deduplicated by durable identity. SQL engines also treat every existing
application table as occupying its name, so a model cannot accidentally claim
or overwrite a neighboring table in a shared database.

Sled does not need human-readable model-name assignments: its tree names encode
`ModelId` reversibly (`modelid-...` or `system-...`). It still keeps a durable
`PropertyId <-> u32` map for compact projected keys.

## Engine layouts

Only the semantic responsibilities above are public. Current private layouts
are:

| Engine | Canonical entity/event storage | Associations | Model materializations and property addressing |
|---|---|---|---|
| PostgreSQL | `_ankurah_entity` and `_ankurah_event` | `_ankurah_entity_model` | One projected table per model; `_ankurah_postgres_model_map` assigns table names and `_ankurah_postgres_column_map` assigns columns |
| SQLite | `_ankurah_entity` and `_ankurah_event` | `_ankurah_entity_model` | One projected table per model; `_ankurah_sqlite_model_map` assigns table names and `_ankurah_sqlite_column_map` assigns columns |
| IndexedDB | `entities` and `events` object stores | `entity_models` object store | Shared `materializations` store scoped by durable materialization name; `model_registrations` and `property_columns` store assignments |
| sled | Shared `entities` and `events` trees | `_ankurah_sled_entity_models` tree | One reversible identity-named tree per model; `_ankurah_sled_property_map` assigns numeric property slots |

PostgreSQL serializes competing entity inserts and updates with transaction
advisory locks before comparing heads. SQLite uses `BEGIN IMMEDIATE`. Sled uses
one multi-tree transaction. IndexedDB uses one read-write transaction spanning
the canonical entity, association, and materialization stores.

## Query execution

`fetch_states(model, selection)` queries the model's materialized surface but
returns canonical attested states. Model projections contain the fields and
indexes needed to select entity ids; canonical buffers, heads, and
attestations remain in the shared entity store.

`storage/common` carries the shared planning and residual-evaluation machinery:

- `Planner` enumerates index, table-scan, and empty plans and accounts for
  engine capabilities such as descending indexes.
- Bounds and key encoding provide the common lexicographic model used by the
  key/value engines.
- Filtering and sorting streams evaluate residual predicates, ordering, and
  limits after a scan.

PostgreSQL and SQLite split predicates into a SQL-pushable portion and a Rust
residual. If a residual remains, SQL `LIMIT` is deferred until after
post-filtering. Sled and IndexedDB use the shared planner to choose native
indexes or scans, then apply residual filtering/sorting in Rust.

Property references in the AST stay logical `PropertyId`s. Each engine resolves
that identity to its private physical column, object field, or numeric slot
only when planning or emitting the operation.

## Event retrieval and staging

The retrieval layer remains separate from physical storage. `GetEvents`
supports causal DAG walks, while `SuspenseEvents` adds an in-memory staging map
so an incoming event is discoverable before an in-memory head references it.

Permanent event append is an explicit `StorageEngine::append_events`
operation; canonical state persistence happens later through `commit_batch`.
The durable/ephemeral distinction is exposed by
`storage_is_definitive()`:

- a durable node's event miss is authoritative;
- an ephemeral node may fetch the missing event from a durable peer and cache
  it locally.

See [Event Retrieval and Staging](retrieval.md) and
[The Event DAG](event-dag.md) for the causal comparison protocol.

## Lifecycle operations

The protocol-version record is checked on every engine open. A recognizable
Ankurah store without a version record is refused, as is a store written by a
different protocol version. Unrelated tables in a shared SQL database do not
make that database an Ankurah store.

`delete_all` is an Ankurah reset, not a database reset. PostgreSQL and SQLite
delete their fixed internal tables plus dynamic tables recorded in their model
registries; unrelated application tables survive. Engine compatibility
metadata remains so the emptied store can reopen under the same protocol
version.

`list_materializations` is deliberately non-creating. Catalog warm-up uses it
to inspect only model materializations which already exist.

## Implementing another engine

An implementation must:

1. provide model-independent canonical entity and event storage;
2. durably represent entity/model associations;
3. maintain a query materialization for every associated model;
4. implement exact-head, all-or-nothing `commit_batch` semantics and return
   complete observed states on conflict;
5. keep event append idempotent by `EventId`;
6. keep physical names private and stable by durable identity;
7. resolve logical `PropertyId`s only at the engine boundary;
8. preserve unrelated embedding-application data during initialization and
   `delete_all`; and
9. exercise the same query, collision, reopen, atomicity, and concurrent-writer
   scenarios as the existing engine suites.

The normative storage contract and required cross-engine scenarios are in
`specs/storage/architecture.md`.
