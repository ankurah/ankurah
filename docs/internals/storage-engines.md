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

An entity does not belong to one model. Its canonical state carries a set of
explicit, event-derived model memberships; its events carry no singular model
identity. A request's model says which projection is being used, but it never
creates membership implicitly. The engine durably indexes the canonical
membership set and refreshes all of those materializations whenever canonical
state changes.

## The `StorageEngine` contract

The trait groups operations by semantic responsibility:

| Method | Contract |
|---|---|
| `append_events` | Blindly and idempotently append validated, attested events by `EventId`; preserve input order in the inserted/not-inserted result |
| `transaction` | Return an engine-owned handle for one atomic storage transaction |
| `get_state` / `get_states` | Read canonical state by `EntityId`, independent of model |
| `fetch_states` | Query one model's materialized view and return the corresponding canonical states |
| `get_events` / `dump_entity_events` | Read model-independent event history |
| `list_materializations` | List already-created model materializations without creating new ones |
| `delete_all` | Delete only engine-owned data while retaining compatibility metadata |
| `set_catalog_resolver` | Inject the catalog name source once the node has constructed its catalog |

`StorageTransaction::set_state` borrows the expected head and proposed attested
state. Successive calls for an entity replace its state within the transaction;
the expected head must match that preceding state, or storage on the first call.
Engines may execute writes immediately inside their transaction or buffer them
until commit. The complete model set is in the state's memberships, with no
separate association instruction. `add_events` writes events in the same transaction.

`StorageTransaction::commit` commits the native data transaction and returns one of:

- `Committed(StorageCommitResult)`, identifying which entity heads changed; or
- `Conflict { observed }`, containing the canonical state (or absence) observed
  for every entity while checking the batch. A conflict commits nothing.

The observed states are part of the concurrency protocol, not diagnostics.
Core uses them to rebuild a monotonic candidate and retry from the exact head
the engine saw.

## Write and retry flow

Events are validated and attested before storage, then included in the same
batch as the candidate states. Duplicate content-addressed `EventId`s are
harmless. Separately cached lineage may already be durable without a canonical
head referencing it.

Local and remote commits validate events on resident-backed forks and write
each event and resulting state directly to a storage transaction. Residents
receive the transaction's events only after storage succeeds. The node serializes
storage commit and resident publication. On conflict, a new attempt forks the
residents and repeats validation; the winner alone publishes its changes.

State-only replication merges its validated snapshot with the current state
and writes through a storage transaction too. On conflict it reforks the resident
and retries the merge; only success publishes the merged state to the resident.

Within a successful storage commit, the following are one engine transaction:

- every staged event;
- every canonical entity replacement;
- every newly accepted entity/model association;
- every projection for the entity's complete associated model set; and
- the affected secondary-index maintenance.

A stale head on one entity rejects the entire batch. Consequently, readers can
never observe a new canonical head with an old projection, or half of a
multi-entity application transaction.

## Associations and materializations

The engine chooses how to represent the durable set
`(EntityId, ModelId)`. Before writing, it compares its stored associations with
the complete membership set in the proposed canonical state, inserts any new
memberships, rejects removals in this revision, and refreshes every member
model's materialization.

This distinction matters in two cases:

- Editing an entity through model A must also update its previously associated
  model B projection.
- Merely using or requesting an entity through model B cannot add an
  association. A future add-to-existing operation must change the canonical
  membership set explicitly before model B is materialized.

Queries do not create associations merely by scanning a materialization:
anything returned by that materialization is already associated. In the
current protocol, only a genesis event's attested `Membership::Add` operation
creates an association; add-to-existing is deferred.

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

Transaction events, canonical states, and materializations persist atomically
through `StorageTransaction::commit`. Standalone `StorageEngine::append_events` remains for
caching retrieved lineage without changing canonical state.
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

`list_materializations` is deliberately non-creating: inspection does not
create new model materializations.

## Implementing another engine

An implementation must:

1. provide model-independent canonical entity and event storage;
2. durably represent entity/model associations;
3. maintain a query materialization for every associated model;
4. implement exact-head, all-or-nothing storage commit semantics and return
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
