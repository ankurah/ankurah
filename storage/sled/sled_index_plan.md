# Sled indexing and planner integration design

> Historical implementation plan. Its `collection` terminology describes the
> former model-scoped storage handle and is not the current public storage
> contract. See `specs/storage/architecture.md` for the model-independent
> canonical entity/event layout and model materializations.

## Goals

- Single `entities` tree holds canonical `StateFragment` for all entities (graph-ready)
- Per-collection materialization trees store `PropertyValue`s for indexing
- Auto-create indexes on demand; first query blocks until index exists
- Maintain per-collection indexes on writes (insert/update/delete)
- Execute range scans using planner `IndexBounds` with correct inclusive/exclusive semantics
- Keep V1 simple; background builds/transactions can follow

## Decisions confirmed

- Sled does not use a `__collection` keypart. Indexes and materialized values are per-collection.
- `properties` is a global name → `u32` shortener for now. Later, `PropertyId` will be an `EntityId` and properties themselves become entities.
- Materialization and index maintenance occur at write time (no lazy/on-demand fills).
- First query may block to synchronously build a missing index; background/incremental builds can come later.
- Best-effort multi-tree consistency is acceptable in V1; repair/rebuild paths can exist.
- Non-unique index strategy: Option A. Append `entity_id` to the composite key; store empty value.
- Result hydration: always from `entities`. Spilled predicate evaluation is done against `collection_{collection}` materialized values for efficiency.
- Index tree naming: `index_{id}` (global; the index's `collection` lives in metadata).

## Storage layout (trees)

- `entities` (single, all collections)
  - key: `EntityId.to_bytes()`
  - val: `StateFragment` (bincode)
- `index_config` (metadata registry)
  - key: `u32` index id (big-endian)
  - val: `IndexRecord` (bincode)
- `properties` (global name → `u32` shortener)
  - key: `PropertyId` (string/canonical id)
  - val: `SledPropertyId(u32)`
- `collection_{collection}` (per-collection materialized values)
  - key: `EntityId.to_bytes()`
  - val: `Vec<(SledPropertyId, PropertyValue)>` (bincode)
  - Sled exposes `ProjectedEntity { id, collection, map }` per-row for filtering/sorting; implements `Filterable` and `HasEntityId`
- `index_{index_id}` (per-index tree; bound to one collection via metadata)

  - key: composite tuple bytes (per `IndexSpec`) `|| entity_id_bytes`
  - val: empty

- `events` (append-only; no secondary index in V1)
  - key: `EventId.to_bytes()`
  - val: `Attested<Event>` (bincode)

Notes:

- Canonical state is only in `entities`. Materialized values live in per-collection trees.
- Index trees are per-index; each index is tied to one collection via metadata; no `__collection` keypart is required.

### Non-unique index keys

We use Option A: make the key unique by appending `entity_id` to the composite key.

- key: `composite_tuple_bytes || entity_id_bytes`
- val: empty

This keeps maintenance simple, enables natural range scans, and provides a deterministic tie-breaker.
No separator stands between the tuple and the id: every part's encoding is prefix-free (see below),
and the id is a fixed-width suffix, so a scanner takes the last `EntityId::BYTE_LEN` bytes.

## Index metadata

```rust
struct IndexRecord {
  id: u32,                   // key in index_config (big-endian)
  collection: String,        // collection this index belongs to
  name: String,              // human-friendly label
  spec: IndexSpec,           // full spec (serde/bincode)
  created_at: SystemTime,
  build_status: BuildStatus, // NotBuilt | Building | Ready
  key_layout_version: u32,   // layout of the tree's keys; KEY_LAYOUT_VERSION in index.rs
}
```

- `index_config` maps `id` → `IndexRecord`.
- `key_layout_version` is bumped whenever the canonical key encoding changes. A record written
  before the field existed decodes as layout 0. On open, an index recorded under another layout is
  started over: its tree is dropped first, then its record is rewritten as `NotBuilt` under the
  current layout, so a crash between the two leaves the old layout recorded and the drop is
  repeated on the next open; the next use rebuilds the tree through the ordinary build path.
- `index_{id}` exists iff `build_status == Ready`.
- V1 backfill is synchronous (create meta as Building → build → Ready).

## Key encoding & collation

- Use `Collatable` to produce order-preserving bytes per component.
- Target type for planning and storage is `PropertyValue` (from core/property).
- Plan: implement `Collatable for PropertyValue` (follow-up), but for V1 we can adapt via a conversion to the existing `core::value::Value` encoding to avoid blocking.
- Tuple encoding (component-wise, preserves lex order, no separators; `core/src/indexing/encoding.rs`):
  - fixed-width parts (integers, floats, booleans, entity ids): `Collatable::to_bytes()`
  - variable-length parts (strings, binary, objects, JSON strings): the payload with every 0x00
    escaped as 0x00 0xFF, then the terminator 0x00 0x00
  - a descending part is the bitwise complement of its ascending encoding
- Composite key bytes = concat of encoded components for all keyparts (in order).

Rationale: each part's encoding is prefix-free, so concatenation keeps tuple order and part
boundaries without length prefixes or type tags, and the keys of every tuple beginning with a
given prefix are exactly the keys that begin with the prefix's bytes. The module doc carries the
argument.

Range end of a prefix (`prefix_range_end` in core):

- The least key above every key that begins with `prefix`: the prefix without its trailing 0xFF
  bytes, its last byte incremented. `None` for an empty or all-0xFF prefix, when every key from
  `prefix` on begins with it (unbounded-high). The bytewise increment-with-carry successor
  (`lex_successor`) is gone: it could land inside the key of a longer value.

## Mapping planner bounds → sled ranges

- Input: `IndexBounds` (multi-column), per-keypart `Endpoint::{Value{datum, inclusive}, UnboundedLow, UnboundedHigh}`
- Split the bounds into the equality parts (both endpoints inclusive on one value) and the one
  inequality on the part after them; the planner bounds no later part.
- `prefix = encode_tuple(equality values)`.
- Equalities only: on a leading part of the key, iterate `tree.range(prefix ..)` with the
  equality-prefix guard; on the whole key, `tree.range(prefix .. prefix_range_end(prefix))`.
- With the inequality, `bound = encode_tuple(equality values + the bound's value)`:
  - `start`: `prefix` when the low side is unbounded; `bound` for an inclusive low bound;
    `prefix_range_end(bound)` for an exclusive one (no key qualifies when that is `None`)
  - `end`: `prefix_range_end(prefix)` when the high side is unbounded; `bound` for an exclusive
    high bound; `prefix_range_end(bound)` for an inclusive one (`None` → unbounded-high)
  - A descending part reverses byte order, so its logical low and high swap sides first.
- No `entity_id` suffix is appended to either bound: `start` is at or below every key that
  begins with it, and `end` is the first key above every key that begins with the bounded value,
  so `tree.range(start .. end)` (end exclusive) covers exactly the matching tuples.
- Prefix guard for open-ended scans: stop when the tuple portion no longer matches the equality-prefix tuple

Reverse scans:

- To satisfy DESC without separate DESC indexes, iterate `rev()` over the constructed ranges and apply the same equality-prefix guard logic.

## Query execution (planner integration)

1. No `__collection` amendment; `SledStorageCollection` is already collection-scoped
2. Plan: `planner.plan(&selection)` (use common planner; collection is implicit in the bucket)
3. `assure_index_exists(collection, index_spec)`
   - If missing/not built → allocate `u32 id`, persist `IndexRecord` as Building, backfill `index_{id}` synchronously, mark Ready
4. Convert `bounds` → sled key-range over composite tuple
5. Open `index_{id}` and iterate `range(start_full..end_full)` or `range(start_full..)` + equality-prefix guard (debug flag allows disabling guard in tests)
6. Decode `EntityId` from key suffix. If spilled predicates or ORDER BY spill exist, fetch materialized values from `collection_{collection}` first and evaluate there to skip non-matching rows early
7. For rows that pass filters, hydrate canonical state from `entities`
8. ORDER BY and LIMIT are applied via the streaming pipeline rules (see below): use in-memory sort or top-K as appropriate; do not combine full sort with limit – prefer `top_k` when both are present

## Index creation & backfill

- Build from `collection_{collection}` (materialized values), not from `entities`:
  - For each `entity_id` → `Vec<(SledPropertyId, PropertyValue)>`, extract keypart values
  - Compute composite tuple bytes
  - Insert key `composite_tuple_bytes || entity_id` → empty
- Backfill in batches to limit memory; flush periodically
- After success, mark meta Ready and persist snapshot to `index_config`
- For V1, synchronous. Follow-up: batched/incremental with progress saved in meta

## Index maintenance on writes

On `set_state` for a collection:

- Upsert `entities`: write canonical `StateFragment`
- Upsert `collection_{collection}`: recompute materialized `Vec<(SledPropertyId, PropertyValue)>`
- For each index in `indexes` for this collection:
  - If old materialization exists: compute old composite key; if changed, delete old key `old_tuple || entity_id`
  - Compute new composite key and insert key `new_tuple || entity_id` with empty value

Notes:

- V1 consistency: best-effort, no cross-tree atomic guarantees; reindex command can rebuild indexes if needed
- Follow-up: use sled transactions across trees (feature-gated) or a mini-WAL in `indexes` meta

## ORDER BY and LIMIT

- When planner chooses ORDER-FIRST, the index keyparts include the order-by fields. DESC is satisfied via reverse scans over ASC keys (no separate DESC indexes in V1).
- When order cannot be satisfied natively, the streaming pipeline applies ordering/limiting on materialized rows using the following rules:
  - If only ORDER BY is present: perform a full in-memory sort over the filtered materialized rows
  - If only LIMIT is present: terminate upstream once `N` matches are yielded
  - If both ORDER BY and LIMIT are present: use a bounded top-K heap (avoid full materialization)
  - Never combine full sort with limit; prefer top-K

## Scanning and execution efficiency (streaming pipeline)

- Use canonical range normalization, `prefix_range_end` for inclusive upper and exclusive lower bounds, and prefix guards for open-ended scans.
- Pipeline is composed from engine-specific scanners and generic combinators:
  - EntityIdStream: iterates `EntityId`s
  - GetPropertyValueStream: iterates materialized rows (`MatRow = { id, mat }`), where `mat` implements `Filterable` (later renamed `GetPropertyValue`)
  - EntityStateStream: iterates hydrated `Attested<EntityState>`
- Sled concrete stream producers:
  - `SledIndexEntityIdScanner` → EntityIdStream
  - `SledCollectionEntityIdScanner` → EntityIdStream (table scan for IDs-only)
  - `SledCollectionMatValueScanner` → GetPropertyValueStream (table scan for materialized values)
  - `SledMatValueLookupFromIds` (EntityIdStream → GetPropertyValueStream)
  - `SledEntityLookup` (EntityIdStream → EntityStateStream)
- Generic combinators over GetPropertyValueStream:
  - `filter_predicate(predicate)` (evaluate on materialized values)
  - `sort_by(order_by)` (full sort)
  - `limit(n)` (early termination; generic over any stream)
  - `top_k(order_by, n)` (bounded heap; sort + limit)
- Reverse scans: leverage double-ended iteration (`rev()`) when scanning Desc over natively ordered keys.
- Maintain a small top-K heap when `order_by_spill` with `limit` is present, to avoid full materialization.
- Batch writes with `sled::Batch` for index maintenance; keep read path streaming and low-allocation.

Prefix guard toggle for testing:

- Provide a debug-only flag to disable the equality-prefix guard to validate correctness via tests that compare guarded vs unguarded scans.

## Deletion

- On delete (future API): remove entity from `entities` and delete its entries from all indexes

## Migration considerations

- Current Sled backend uses unified `entities`, `index_config`, and per-index `index_{id}` trees
- Non-breaking option: detect old layout and offer a migrator that reads old trees and writes to the new layout
- A change of the index key encoding needs no migrator: `key_layout_version` in each `IndexRecord` has the index started over on open (see Index metadata)
- For tests/dev: new test engine `SledStorageEngine::new_test()` will initialize the new layout directly

## Testing plan

- Unit tests for tuple encoding (round-trip, ordering across types)
- Range mapping tests: inclusive/exclusive bounds, open upper with prefix guard; values that extend another through 0x00 fetched through sled (`tests/string_bounds.rs`)
- Reopen: an index recorded under an older key layout is rebuilt and serves fetches (`tests/index_layout.rs`)
- Backfill: create index on existing dataset; verify entries and scans
- Maintenance: set_state replacing values updates index entries; delete path
- Planner integration: end-to-end queries with equality-only, inequality, ORDER BY, LIMIT
- No-predicate case: behavior when selection has no comparisons/order-by; support either an equality-only plan or fallback table scan.

## Follow-ups / TODOs

- Implement `Collatable for PropertyValue` (order-preserving bytes)
- Shared normalization in `storage/common` for CanonicalRange
- Cross-tree atomicity or WAL for index maintenance (crash consistency)
- Background/incremental index builds with persisted progress
- Admin endpoint/tooling to list/drop/rebuild indexes

## Remaining questions

None at this time.

## Examples: Pipeline compositions

This section shows the concrete stream compositions the engine builds for common scenarios. Streams are engine-specific producers; combinators are generic.

- Index plan, no residual predicate, no ORDER BY spill (native order satisfied):

  - `SledIndexEntityIdScanner(bounds, direction)` → EntityIdStream
  - Optional: `limit(N)` (native order allows early termination)
  - `SledEntityLookup::from_ids(...)` → EntityStateStream
  - `collect_states(...)`

- Index plan with residual predicate and/or ORDER BY spill:

  - `SledIndexEntityIdScanner(bounds, direction)` → EntityIdStream
  - `SledMatValueLookupFromIds::from_ids(...)` → GetPropertyValueStream (MatRow)
  - `filter_predicate(predicate)` (on materialized values)
  - If ORDER BY + LIMIT: `top_k(order_by, N)`
  - Else if ORDER BY only: `sort_by(order_by)`
  - Else if LIMIT only: `limit(N)`
  - `SledEntityLookup::from_ids(...)` → EntityStateStream
  - `collect_states(...)`

- Table scan (primary-key scan):
  - IDs-only path (no residual, no spill):
    - `SledCollectionEntityIdScanner(bounds, direction)` → EntityIdStream
    - Optional: `limit(N)`
    - `SledEntityLookup::from_ids(...)` → EntityStateStream
    - `collect_states(...)`
  - Values path (residual and/or spill present):
    - `SledCollectionMatValueScanner(bounds, direction)` → GetPropertyValueStream (MatRow)
    - `filter_predicate(predicate)`
    - If ORDER BY + LIMIT: `top_k(order_by, N)`
    - Else if ORDER BY only: `sort_by(order_by)`
    - Else if LIMIT only: `limit(N)`
    - `SledEntityLookup::from_ids(...)` → EntityStateStream
    - `collect_states(...)`

Notes:

- GetPropertyValueStream items carry `MatRow { id, mat }` so hydration can proceed without additional mapping.
- When only IDs are required, prefer entity-id scanners to avoid over-fetching materialized values.
