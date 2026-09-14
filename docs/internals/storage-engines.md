# Storage Engine Layer

The storage engine layer is the bottom of the Ankurah stack -- the only part
that actually touches a disk, a browser database, or a SQL server. Everything
above it (the [event retrieval and staging layer](retrieval.md), entity
persistence, the [replication protocol](node-architecture.md#the-replication-protocol))
is written against two traits and never names a concrete backend. Swapping
sled for Postgres changes where bytes land; it does not change a line of the
node, the applier, or the [compare-apply cycle](compare-apply-cycle.md).

```text
        Node / Context / Reactor
                  |
     Event retrieval & staging  (retrieval.md)
        LocalEventGetter / CachedEventGetter
                  |
     StorageCollectionWrapper  (Arc<dyn StorageCollection>)
                  |
   +--------+-----+------+-----------------+
   |        |            |                 |
  sled   postgres   indexeddb-wasm      sqlite      <- engines
   |        |            |                 |
  disk    SQL server   browser IDB      single file
```

The traits live in `core/src/storage.rs`; the engines are separate crates
under `storage/`. A large body of shared query machinery lives in
`storage/common`, so engines implement placement and I/O, not query planning
from scratch.


## The Two Traits

`StorageEngine` is the collection factory and lifecycle handle. It is small:

- `collection(&CollectionId) -> Arc<dyn StorageCollection>` -- open or create
  the storage for one collection. This is where an engine does per-collection
  setup: sled opens a tree, Postgres and SQLite run `CREATE TABLE IF NOT
  EXISTS` for the state and event tables under a DDL lock, IndexedDB hands back
  a bucket bound to the shared object stores.
- `delete_all_collections()` -- drop everything (used by tests and resets).
- an associated `Value` type -- the engine's native value representation
  (`Vec<u8>` for sled, `PGValue` for Postgres, `SqliteValue`, `JsValue`).

`StorageCollection` is the real contract -- the interface the rest of the
system depends on. Grouped by responsibility rather than method-by-method:

| Responsibility | Methods | Notes |
|----------------|---------|-------|
| Entity state, by id | `set_state`, `get_state`, `set_states`, `get_states` | `set_states`/`get_states` have default loop implementations over the singular forms |
| Entity state, by query | `fetch_states(&Selection)` | The predicate path -- see below |
| Events, write | `add_event` | Append one attested event |
| Events, read | `get_events(Vec<EventId>)`, `dump_entity_events(EntityId)` | Point lookups and a per-entity dump |

`StorageCollectionWrapper` (`core/src/storage.rs`) is a thin `Deref` newtype
around `Arc<dyn StorageCollection>` that the retrieval layer holds; it adds no
behavior, only a stable handle to clone.


## One Collection, Two Stores

Every collection persists two kinds of data, and the split mirrors the
event-sourcing model directly:

- **Entity state snapshots** -- the materialized current view of each entity.
  A snapshot carries the serialized property state plus the entity's **head
  clock** (the set of event ids that produced it). This is what `get_state`
  and `fetch_states` return, and what queries run against.
- **Events** -- the immutable history. Each event names its parent clock and
  the operations it applied. This is what `add_event`/`get_events` persist and
  the [event DAG](event-dag.md) walks during comparison.

State is derived; events are authoritative. State exists so that reads and
predicate queries do not have to replay history, and the head clock on each
snapshot is the join point back to the DAG.

Physically, engines keep the two stores in separate namespaces, but the naming
is engine-specific -- there is no single scheme enforced across the layer:

- **Postgres / SQLite** give each collection two tables: the state table is
  named for the collection itself (bare `{collection}`) and the event table is
  `{collection}_event`.
- **Sled / IndexedDB** use two shared stores for *all* collections -- an
  `entities` tree/object-store and an `events` tree/object-store -- plus a
  *per-collection* `collection_{id}` tree (sled) or index object stores
  (IndexedDB) holding the materialized, indexable projection used for queries.

There is no shared naming helper in the storage layer: each engine builds these
names inline, so the schemes above describe current engine behavior rather than
a single convention enforced by common code.

**Write ordering.** The state snapshot and its events are written by different
calls, and the order matters. The invariant enforced one layer up is: commit
the events to permanent storage *before* persisting the entity state that
references them (see [event retrieval and staging](retrieval.md#the-event-lifecycle-stage-apply-commit-persist)
and [entity lifecycle -> persistence ordering](entity-lifecycle.md#persistence-ordering)).
A crash between the two leaves events on disk with a stale head, which the next
delivery heals via BFS -- never state pointing at events that were never
stored. The storage layer itself does not span the two writes in a
transaction; the ordering discipline lives in the caller.


## The Predicate Fetch Path

`fetch_states` is the one method that takes a query rather than an id. Its
argument is an `ankql::ast::Selection` -- a predicate plus optional `ORDER BY`
and `LIMIT`. Turning that into an engine operation is where most of the
per-engine complexity would be, so it is deliberately factored into
`storage/common` and shared:

- **`Planner`** (`storage/common/src/planner.rs`) takes the `Selection` and the
  primary-key field name and enumerates candidate `Plan`s: `Index { .. }`
  scans, a `TableScan` fallback, or `EmptyScan` when the predicate can never
  match. It splits the predicate into conjuncts (`predicate.rs`), separates
  equalities from inequalities, chooses index key parts, and computes how much
  of the `ORDER BY` a scan direction can satisfy versus what must be sorted
  in memory (the `OrderByComponents` presort/spill split in `types.rs`). It is
  capability-aware via `PlannerConfig`: `supports_desc_indexes` is `true` for
  engines with real descending indexes and `false` for IndexedDB, which only
  has ascending index parts.
- **`bounds.rs`** normalizes per-column index bounds into a single canonical
  lexicographic range that each KV engine lowers to its own cursor range.
- **`filtering.rs` / `sorting.rs`** provide streaming combinators
  (`filter_predicate`, `sort_by`, `top_k`, `limit`) over any stream of
  `Filterable` items. Residual predicates the index could not satisfy are
  evaluated here in Rust via `core`'s `evaluate_predicate`, so no engine
  reimplements predicate evaluation.

What differs between engines is **how much of the query is pushed down** to the
backend versus evaluated with the shared Rust combinators:

- **Postgres and SQLite push the predicate into SQL.** Each has a
  `split_predicate_for_*` pass (`sql_builder.rs`) that partitions the predicate
  into a `sql_predicate` (translated into a `WHERE` clause) and a
  `remaining_predicate` that SQL cannot express. The pushable part becomes a
  real query; the residual is post-filtered in Rust. When a residual exists,
  `LIMIT` is *dropped* from the SQL and re-applied after post-filtering, so the
  database is never allowed to truncate rows that the residual might have kept.
  Debug builds record the spilled predicate so tests can assert full pushdown.
- **Sled and IndexedDB run the `Planner` and then scan.** They pick the first
  viable plan, open an index cursor (or a full collection scan for a
  `TableScan`), materialize candidate rows, and run the residual predicate,
  sort, and limit through the shared `filtering`/`sorting` streams. Sled reads
  ids from an index tree and does a secondary lookup into the shared
  `entities` tree to hydrate each state; IndexedDB drives IDB index cursors.


## Engine Matrix

Only claims verified against the code in each crate.

| Engine | Platform / context | State layout | Event layout | Predicate handling | Durability |
|--------|--------------------|--------------|--------------|--------------------|------------|
| **sled** (`storage/sled`) | Native, embedded KV; the default for servers and dev | Canonical state in a shared `entities` tree; a per-collection `collection_{id}` tree holds the materialized property projection that indexes and scans use | Shared `events` tree keyed by event id | Shared `Planner` picks index vs. table scan; residual predicate/sort/limit via shared streams; sled ops run on `spawn_blocking` | On-disk sled db; `new()` under `~/.ankurah`, plus a temporary in-memory mode for tests |
| **postgres** (`storage/postgres`) | Native, production server backend | One table per collection (bare `{collection}`); columns added on demand as properties appear; each row carries `state_buffer`, `head`, `attestations` | `{collection}_event` table keyed by `id`, with an `entity_id` column | Predicate split into pushdown `WHERE` + Rust post-filter; `LIMIT` deferred past post-filter; DDL serialized with advisory locks | Full SQL server; connection pooled via `bb8` |
| **indexeddb-wasm** (`storage/indexeddb-wasm`) | Browser (WASM) client storage | Shared `entities` object store; per-collection index object stores for queries | Shared `events` object store with a `by_entity_id` index | Shared `Planner` in `PlannerConfig::indexeddb()` mode (ascending-only indexes); IDB index cursors + residual filter/sort in Rust | Browser IndexedDB; `!Send`, wrapped in `SendWrapper` |
| **sqlite** (`storage/sqlite`) | Embedded single-file SQL; native incl. mobile (iOS/Android) | One table per collection (bare `{collection}`), columns added on demand; row carries `state_buffer`, `head`, `attestations` | `{collection}_event` table with an explicit `entity_id` index for `dump_entity_events` | Same pushdown/post-filter split as Postgres, using SQLite JSON/JSONB operators for JSON paths | Single-file (or in-memory) SQLite via `rusqlite` "bundled"; pooled via `bb8` |

Two notes the code makes explicit. SQLite positions itself in its crate docs as
sitting "between Sled (pure KV) and Postgres (full SQL server)" and requires
SQLite 3.45+ for JSONB; its implementation is a full pushdown engine, not a
stub. On the KV side, `dump_entity_events` is a full scan of the shared events
tree in sled (flagged as acceptable only because it is test-facing), whereas
SQLite and IndexedDB index events by `entity_id` for that lookup.


## Index Maintenance

For the KV engines, secondary indexes are a real subsystem, not a free
byproduct of the store. Sled's `IndexManager` (`storage/sled/src/index.rs`)
maintains per-collection index trees: `set_state` calls
`update_indexes_for_entity` with the old and new materialized property tuples
so index entries stay consistent with state, and `fetch_states` calls
`assure_index_exists` to create an index on demand when a plan needs one. The
key encoding these indexes share -- ordered, typed, multi-column keys -- lives
in `core/src/indexing` (`KeySpec`, `IndexKeyPart`, and the tuple encoder),
which is also what the `storage/common` planner reasons about when it decides
which index a query can use. IndexedDB follows the same shape using native IDB
indexes. The SQL engines lean on the database's own indexing and add columns
lazily as properties appear.


## How Event Retrieval Layers On Top

The [event retrieval and staging layer](retrieval.md) is the immediate
consumer of `StorageCollection`. Its concrete getters call straight into these
methods:

- **`LocalEventGetter`** (durable path) checks an in-memory staging map, then
  falls back to `collection.get_events(..)`; `commit_event` calls
  `add_event`. Its `storage_is_definitive()` returns the `durable` flag it was
  constructed with.
- **`CachedEventGetter`** (ephemeral path) adds a third tier: staging, then
  `get_events`, then a request to a durable peer whose response it writes back
  via `add_event`. Its `storage_is_definitive()` is always `false`.

That `storage_is_definitive` bit is exactly the durable/ephemeral distinction
surfacing at the storage boundary. On a
[durable node](node-architecture.md#durable-vs-ephemeral-nodes) the local store
holds every event, so `event_stored() == false` is conclusive and enables cheap
guards without a DAG walk; on an
[ephemeral node](node-architecture.md#durable-vs-ephemeral-nodes) the same
store is a cache, a miss means "not here yet," and the getter must go to a peer.
The two lookup strategies are covered in
[retrieval -> durable vs ephemeral lookup](retrieval.md#durable-vs-ephemeral-lookup-strategies).


## Writing a New Engine

The contract is small and the shared code carries the hard parts:

1. **Implement `StorageEngine`** -- a `collection()` factory that opens/creates
   the state and event stores for a collection, and `delete_all_collections()`.
2. **Implement `StorageCollection`** -- the state (`set_state`/`get_state`/
   `fetch_states`), and event (`add_event`/`get_events`/`dump_entity_events`)
   methods. `set_states`/`get_states` come for free from the defaults.
3. **Use `storage/common`** -- run the `Planner` (with the right
   `PlannerConfig` for your index capabilities), lower `bounds` to your native
   ranges, and evaluate residual predicates/sorts/limits through the
   `filtering`/`sorting` streams. Do not hand-roll predicate evaluation. If the
   backend speaks SQL, follow the Postgres/SQLite pattern: split the predicate,
   push what you can, post-filter the rest, and defer `LIMIT` when a residual
   exists.
4. **Preserve the write-ordering contract** -- `add_event` and `set_state` may
   be separate writes, but the state you persist must reference a head whose
   events are already durable (the caller guarantees the ordering; your engine
   just must not reorder or lose the event write).

Conformance is checked by exercising each engine through the same
model/query API rather than a single generic trait-test macro. The
crate-independent behavioral tests live in the workspace `tests/` crate (which
runs against sled), and each SQL/IDB engine carries a parallel suite under its
own `tests/` directory (for example `storage/postgres/tests`,
`storage/sqlite/tests`, `storage/indexeddb-wasm/tests`) covering predicate
checks, ordering, JSON semantics, and undefined-column handling. A new engine is
expected to pass the equivalent behavioral tests for its platform. See the
[Testing Strategy](testing.md) chapter for how these fit together.
