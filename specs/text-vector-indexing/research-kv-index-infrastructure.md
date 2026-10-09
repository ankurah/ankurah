# Research: Generalized KV Index Infrastructure

## Architecture Overview

Ankurah has a generalized KV index infrastructure shared by sled and IndexedDB-WASM. The architecture is a pipeline of composable layers:

1. **Planner** (`storage/common/src/planner.rs`) — Query → Plan enum
2. **Key Encoding** (`core/src/indexing/encoding.rs`) — Values → sorted bytes
3. **Bounds Normalization** — KeyBounds → engine-native ranges (per-backend)
4. **Scanners** — Plan → EntityId/record stream (per-backend)
5. **Index Maintenance** — write-time update hooks (per-backend)

Only stages 3-5 differ between sled and IndexedDB. Stages 1-2 are fully shared.

## The Planner

**Location:** `storage/common/src/planner.rs`

Produces `Plan` enum variants (`storage/common/src/types.rs:49-66`):
- `Plan::Index { index_spec, bounds, scan_direction, remaining_predicate, order_by_spill }`
- `Plan::TableScan { bounds, scan_direction, remaining_predicate, order_by_spill }`
- `Plan::EmptyScan`

Generates multiple candidate plans using strategies:
- **ORDER-FIRST**: Use index sorted by ORDER BY columns, apply equalities as prefix
- **INEQ-FIRST**: Use primary inequality field, sort later
- **EQUALITY-ONLY**: Simple equalities on all index columns
- **FALLBACK**: TableScan when no indexes apply

**Key invariant:** The planner doesn't know about individual index implementations. It works with `KeySpec` and `KeyBounds` abstractions.

`PlannerConfig` adapts behavior per-engine (e.g., `supports_desc_indexes = false` for IndexedDB).

## KeyBounds — Planner IR

**Location:** `storage/common/src/types.rs:76-142`

```
KeyBounds {
    keyparts: Vec<KeyBoundComponent>   // one per index column
}

KeyBoundComponent {
    column: String,
    low: Endpoint,     // UnboundedLow | Value { datum, inclusive }
    high: Endpoint,    // UnboundedHigh | Value { datum, inclusive }
}
```

Engine-agnostic intermediate representation. Both sled and IndexedDB consume the same KeyBounds and convert to native formats.

## Key Encoding

**Location:** `core/src/indexing/encoding.rs`

Two levels:
1. **Per-value:** `encode_component_typed()` — preserves lexicographic sort order
2. **Composite:** `encode_tuple_values_with_key_spec()` — concatenates encoded values, respects ASC/DESC (DESC inverts all bytes), appends EntityId suffix (16 bytes)

## Per-Engine Normalization

**Sled** (`storage/sled/src/planner_integration.rs`):
- KeyBounds → `SledRangeBounds { start: Vec<u8>, end: Option<Vec<u8>>, eq_prefix_guard }`
- Handles DESC columns by inverting bounds in physical space
- Prefix guard terminates scan when equality prefix changes

**IndexedDB** (`storage/indexeddb-wasm/src/planner_integration.rs`):
- KeyBounds → `CanonicalRange` → `IdbKeyRange` + prefix guard config
- Cannot handle DESC natively — planner only includes ASC columns, ORDER BY spills to in-memory sort

## Scanners

Both engines implement the same pattern:

**Sled** (`storage/sled/src/scan_index.rs`):
- `SledIndexScanner` — iterates sled tree within byte range, prefix guard terminates, decodes EntityId from last 16 bytes of key

**IndexedDB** (`storage/indexeddb-wasm/src/scanner.rs`):
- `IdbIndexScanner` — opens cursor with IdbKeyRange, prefix guard compares key array elements, yields JS Objects

Both yield streams that feed into the common filter → sort → limit pipeline.

## Index Maintenance

**Location:** `storage/sled/src/index.rs:256-298`

On every write, `update_indexes_for_entity()`:
1. For each index matching the collection
2. Build old key from old materialized properties
3. Build new key from new materialized properties
4. Remove old key, insert new key (if changed)

Indexes are built from **materialized property values** (property_id → Value tuples), not raw CRDT state.

**Backfill:** First-time index creation scans collection tree and builds all entries (lazy: NotBuilt → Building → Ready).

## Where Text/Vector Indexes Plug In

**Text index** fits naturally because a token lookup is equivalent to a single-column equality prefix scan:
- Token = equality bound on virtual `__text_{field}` column
- Existing prefix guard terminates when token changes
- Multiple tokens → multiple scans → intersect EntityId sets
- Index maintenance: tokenize → N entries per entity → set diff on write

**Vector index** diverges from the range scan model:
- kNN requires distance computation, not lexicographic ordering
- Needs a new `Plan::VectorSearch` variant
- Scanner must compute distances and maintain a top-K heap
- Brute force: scan all vectors. IVF/HNSW: structured search.

### Files That Need Changes for New Index Types

| File | Change |
|------|--------|
| `storage/common/src/planner.rs` | Handle CONTAINS/distance predicates |
| `storage/common/src/types.rs` | Add `Plan::TextIndex`, `Plan::VectorSearch` |
| `core/src/indexing/encoding.rs` | Text key encoding (token + entity_id) |
| `core/src/indexing/key_spec.rs` | `IndexSpec` enum wrapping `KeySpec` |
| `storage/sled/src/index.rs` | Polymorphic `extract_entries()` + set diff maintenance |
| `storage/sled/src/planner_integration.rs` | Text/vector bounds normalization |
| `storage/indexeddb-wasm/src/planner_integration.rs` | Same |

### Files That Don't Change

| File | Why |
|------|-----|
| `storage/common/src/sorting.rs` | Generic stream combinators |
| `storage/common/src/filtering.rs` | Predicate evaluation unchanged |
| `storage/sled/src/scan_index.rs` | Text index reuses prefix-guard pattern as-is |
