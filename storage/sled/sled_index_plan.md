# Sled indexing and planner integration design

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
- Index tree naming: `index_{collection}_{id}`.

## Storage layout (trees)

- `entities` (single, all collections)
  - key: `EntityId.to_bytes()`
  - val: `StateFragment` (bincode)
- `indexes` (metadata registry)
  - key: `index_id` (stable hash of (collection, IndexSpec))
  - val: `IndexMeta` (bincode)
- `properties` (global name → `u32` shortener)
  - key: `PropertyId` (string/canonical id)
  - val: `SledPropertyId(u32)`
- `collection_{collection}` (per-collection materialized values)
  - key: `EntityId.to_bytes()`
  - val: `Vec<(SledPropertyId, PropertyValue)>` (bincode)
- `index_{collection}_{index_id_or_name}` (per-collection index)
  - key: composite tuple bytes (per `IndexSpec`) `|| 0x00 || entity_id_bytes`
  - val: empty

Notes:

- Canonical state is only in `entities`. Materialized values live in per-collection trees.
- Index trees are per-collection; no `__collection` keypart is required.

### Non-unique index keys

We use Option A: make the key unique by appending `entity_id` to the composite key.

- key: `composite_tuple_bytes || 0x00 || entity_id_bytes`
- val: empty

This keeps maintenance simple, enables natural range scans, and provides a deterministic tie-breaker.

## Index metadata

```rust
struct IndexMeta {
  id: String,                 // stable hash of (collection, spec)
  collection: String,
  name: String,               // human-friendly label
  spec: IndexSpec,            // full spec (serde/bincode)
  created_at: SystemTime,
  build_status: BuildStatus,  // NotBuilt | Building { progress } | Ready
}
```

- `indexes` maps `id` → `IndexMeta`.
- `index_{collection}_{id}` exists iff `build_status == Ready`.
- V1 backfill is synchronous (create meta as Building → build → Ready).

## Key encoding & collation

- Use `Collatable` to produce order-preserving bytes per component.
- Target type for planning and storage is `PropertyValue` (from core/property).
- Plan: implement `Collatable for PropertyValue` (follow-up), but for V1 we can adapt via a conversion to the existing `core::value::Value` encoding to avoid blocking.
- Tuple encoding (component-wise, preserves lex order and unambiguously delimits parts):
  - component header: 1-byte type tag (String=0x10, I64=0x20, F64=0x30, Bool=0x40, Bytes=0x50)
  - component length: u32 big-endian (bytes length)
  - component body: `Collatable::to_bytes()` for the value
- Composite key bytes = concat of encoded components for all keyparts (in order).

Rationale: length-prefix + type-tag ensures lexicographic order over tuples and disambiguates boundaries without escaping. Big-endian length preserves prefix ordering.

## Mapping planner bounds → sled ranges

- Input: `IndexBounds` (multi-column), per-keypart `Endpoint::{Value{datum, inclusive}, UnboundedLow, UnboundedHigh}`
- Normalize to a canonical lexicographic interval (recommend sharing the normalizer across backends):
  - Output: `lower: Option<(Vec<PropertyValue>, lower_open)>`, `upper: Option<(Vec<PropertyValue>, upper_open)>`, and `eq_prefix_len/values`
- Build sled byte keys over the composite tuple (index key portion):
  - `encode_tuple(values: &[PropertyValue]) -> Vec<u8>` → `tuple_key`
  - `start_tuple = encode_tuple(lower_tuple)`; if `lower_open`, set `start_tuple = lex_successor(start_tuple)`
  - If `upper == None` (open-ended): use prefix guard
  - Else: `end_tuple = encode_tuple(upper_tuple)`; if `upper_open == false`, set `end_tuple = lex_successor(end_tuple)`
- Form full-range bounds for the actual sled keys that include `entity_id` suffix:
  - `start_full = start_tuple || 0x00` (smallest possible suffix)
  - If bounded upper: `end_full = end_tuple || 0x00` and use `tree.range(start_full .. end_full)` (end exclusive)
  - If unbounded upper: iterate `tree.range(start_full ..)` with a prefix guard on the equality prefix
- Prefix guard for open-ended scans: stop when the tuple portion no longer matches the equality-prefix tuple

## Query execution (planner integration)

1. No `__collection` amendment; `SledStorageCollection` is already collection-scoped
2. Plan: `planner.plan(&selection)` (sled planner config: no `__collection` keypart)
3. `assure_index_exists(collection, index_spec)`
   - Check `indexes`; if missing/not built → create meta, backfill `index_{collection}_{id}` synchronously; mark Ready
4. Convert `bounds` → sled key-range over composite tuple
5. Open `index_{collection}_{id}` and iterate `range(start_full..end_full)` or `range(start_full..)` + prefix guard
6. Decode `EntityId` from key suffix; use `collection_{collection}` to evaluate any spilled predicates (skip non-matching rows early)
7. For rows that pass filters, hydrate from `entities`
8. If `order_by_spill` is needed, sort survivors (may use materialized values where possible), then apply `limit`

## Index creation & backfill

- Build from `collection_{collection}` (materialized values), not from `entities`:
  - For each `entity_id` → `Vec<(SledPropertyId, PropertyValue)>`, extract keypart values
  - Compute composite tuple bytes
  - Insert key `composite_tuple_bytes || 0x00 || entity_id` → empty
- Backfill in batches to limit memory; flush periodically
- After success, mark meta Ready
- For V1, synchronous. Follow-up: batched/incremental with progress saved in meta

## Index maintenance on writes

On `set_state` for a collection:

- Upsert `entities`: write canonical `StateFragment`
- Upsert `collection_{collection}`: recompute materialized `Vec<(SledPropertyId, PropertyValue)>`
- For each index in `indexes` for this collection:
  - If old materialization exists: compute old composite key; if changed, delete old key `old_tuple || 0x00 || entity_id`
  - Compute new composite key and insert key `new_tuple || 0x00 || entity_id` with empty value

Notes:

- V1 consistency: best-effort, no cross-tree atomic guarantees; reindex command can rebuild indexes if needed
- Follow-up: use sled transactions across trees (feature-gated) or a mini-WAL in `indexes` meta

## ORDER BY and LIMIT

- When planner chooses ORDER-FIRST, the index keyparts include the order-by fields to satisfy native order
- Otherwise, collect rows and apply `order_by_spill` using in-memory sort (reuse collation helpers)
- LIMIT applied during scan when native order is satisfied; otherwise truncate after sort

## Scanning and execution efficiency

- Use canonical range normalization, lexicographic successor for inclusive upper bounds, and prefix guards for open-ended scans.
- Stream index scans and stop early when `limit` is reached (when native order satisfied).
- Evaluate spilled predicates using `collection_{collection}` before hydrating entities to minimize IO.
- Reverse scans: leverage double-ended iteration (`rev()`) when scanning Desc over natively ordered keys.
- Maintain a small top-K heap when `order_by_spill` with `limit` is present, to avoid full materialization.
- Batch writes with `sled::Batch` for index maintenance; keep read path streaming and low-allocation.

## Deletion

- On delete (future API): remove entity from `entities` and delete its entries from all indexes

## Migration considerations

- Current Sled backend uses one state tree per collection; new design uses unified `entities` and index trees
- Non-breaking option: detect old layout and offer a migrator that reads old trees and writes to the new layout
- For tests/dev: new test engine `SledStorageEngine::new_test()` will initialize the new layout directly

## Testing plan

- Unit tests for tuple encoding (round-trip, ordering across types)
- Range mapping tests: inclusive/exclusive bounds, open upper with prefix guard
- Backfill: create index on existing dataset; verify entries and scans
- Maintenance: set_state replacing values updates index entries; delete path
- Planner integration: end-to-end queries with equality-only, inequality, ORDER BY, LIMIT

## Follow-ups / TODOs

- Implement `Collatable for PropertyValue` (order-preserving bytes)
- Shared normalization in `storage/common` for CanonicalRange
- Cross-tree atomicity or WAL for index maintenance (crash consistency)
- Background/incremental index builds with persisted progress
- Admin endpoint/tooling to list/drop/rebuild indexes

## Remaining questions

None at this time.
