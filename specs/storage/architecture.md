# Storage Architecture

This document defines the conceptual model and naming conventions for Ankurah storage engines.

## Conceptual Model

### Entities

**Entities** exist independently and contain arbitrary properties in their `EntityState`. The EntityState holds state buffers which can be deserialized into property backends to access typed values. This is the source of truth.

### Collections

A **Collection** is like a column family - a specific set of properties associated with a model. Different models may access different/overlapping sets of properties on the same entity. An entity belongs to exactly one collection, but the same underlying data could theoretically be viewed through multiple collection "lenses" in the future.

### Projections

A **Projection** is a pre-materialized set of property values for a collection, stored for efficient querying (filtering, sorting). Projections avoid the cost of instantiating property backends for every query.

- **Sled**: Projections stored in separate `collection_{name}` tree
- **IndexedDB**: Projections stored inline on entity records (unified storage)

### Indexes

An **Index** provides efficient lookup by specific key patterns. Indexes map encoded key tuples to entity IDs.

---

## Sled Storage Engine

### Trees

| Tree Name | Contents | Key | Value |
|-----------|----------|-----|-------|
| `entities` | Entity state (state buffers) | EntityId bytes | StateFragment (bincode) |
| `collection_{name}` | Projected property values for the collection | EntityId bytes | Vec<(property_id, Value)> (bincode) |
| `index_{id}` | Index entries | Encoded key tuple + EntityId | () |
| `property_config` | Property ID ↔ name mapping | - | - |
| `index_config` | Index metadata | - | - |

### Types

| Type | Description |
|------|-------------|
| `ProjectedEntity` | Projected property values for one entity (id, collection, values map). Implements `Filterable` and `HasEntityId`. |

### Iterators

| Type | Input | Output | Operation |
|------|-------|--------|-----------|
| `SledIndexScanner` | index tree + bounds | EntityId | Sequential scan over index keys |
| `SledCollectionScanner` | projection tree + bounds | ProjectedEntity | Sequential scan over collection |
| `SledCollectionLookup<S>` | projection tree + ID stream | ProjectedEntity | Random lookup by EntityId |
| `SledEntityLookup<S>` | entities tree + ID stream | EntityState | Random lookup by EntityId |

### Query Execution Flow

```
Index Plan:
  SledIndexScanner ──→ EntityId
          ↓
  SledCollectionLookup ──→ ProjectedEntity
          ↓
  .filter_predicate() ──→ ProjectedEntity
          ↓
  .sort_by() / .top_k() ──→ ProjectedEntity
          ↓
  .extract_ids() ──→ EntityId
          ↓
  SledEntityLookup ──→ EntityState

Table Scan Plan:
  SledCollectionScanner ──→ ProjectedEntity
          ↓
  (same pipeline as above)
```

---

## IndexedDB Storage Engine

### Stores and Indexes

| Store/Index | Contents | Key Path |
|-------------|----------|----------|
| `entities` store | Unified entity records (state + projected fields) | auto-increment |
| `__collection__id` index | Primary lookup by collection + entity ID | [__collection, id] |
| User indexes | Query optimization | [field1, field2, ...] |

### Record Structure

Each record in the `entities` store contains:
- `__collection` - Collection identifier
- `id` - EntityId (base64)
- `__state_buffer` - Serialized state buffers
- `__head` - Event head references
- `__attestations` - Attestations
- Plus all projected property values as top-level fields (for indexing/filtering)

### Types

| Type | Description |
|------|-------------|
| `IdbRecord` | A record from the IndexedDB entities store. Wraps the raw JS object with lazy value extraction for filtering. Provides `entity_state()` method to convert to `EntityState` on demand. Implements `Filterable` and `HasEntityId`. |

### Iterators

| Type | Input | Output | Operation |
|------|-------|--------|-----------|
| `IdbIndexScanner` | IDB index + range + direction + prefix guard config | Object (JS) | Async cursor iteration with prefix guard |

Note: `IdbIndexScanner` yields raw JS `Object` values. The caller applies predicate filtering (via `FilterableObject`) and converts matching records to `IdbRecord` or `EntityState` as needed.

### Query Execution Flow

```
Index Plan:
  IdbIndexScanner ──→ IdbRecord (projection inline)
          ↓
  .filter_predicate() ──→ IdbRecord
          ↓
  .sort_by() / .top_k() ──→ IdbRecord
          ↓
  .entity_state() ──→ EntityState
```

Note: IndexedDB has no separate projection lookup - records contain everything.

Note: Unlike Sled, IndexedDB has no distinct "table scan" path. All scans use `IdbIndexScanner` over an index - either a user-defined index for optimized queries, or the `__collection__id` index for collection-wide scans. This is because IndexedDB requires index-based access; there is no raw key-range iteration over the store itself.

---

## Shared Infrastructure (storage-common)

### Async-First Design

All storage backends produce async `Stream` (the async equivalent of `Iterator`):

- **Sled**: Scanners implement `Stream` directly (always return `Poll::Ready` since synchronous underneath)
- **IndexedDB**: Native async cursor streams; collected results wrapped via `futures::stream::iter()`

This enables:
- Unified combinator API across all backends
- Filter predicates can short-circuit without loading all records
- Same combinator chain works for both sync and async backends

### Traits

| Trait | Methods | Description |
|-------|---------|-------------|
| `Filterable` | `collection()`, `value(name)` | Access to projected property values for filtering/sorting |
| `HasEntityId` | `entity_id()` | Provides entity ID for downstream lookup |

> **Future consideration**: Rename `Filterable` to `HasProjectedValues`? The trait represents access to projected values, not just filterability.

### Combinators (StreamExt-style)

For any `Stream` where `Item: Filterable`:

| Method | Returns | Can Stream? | Description |
|--------|---------|-------------|-------------|
| `.filter_predicate(&pred)` | FilteredStream | Yes | Filter by predicate |
| `.limit(n)` | LimitedStream | Yes | Limit results |
| `.sort_by(order_by)` | SortedStream | No (must collect) | Partition-aware sort |
| `.top_k(order_by, k)` | TopKStream | No (must collect) | Partition-aware top-K |
| `.extract_ids()` | ExtractIdsStream | Yes | Extract entity IDs |

Note: `sort_by` and `top_k` inherently require collecting all items before emitting results. `filter_predicate` and `limit` can truly stream.

---

## Naming Conventions

### Principles

1. Name after what it **IS**, not what you use it for
2. **Scanner** = sequential iteration over storage
3. **Lookup** = random access by key
4. **Projection** = pre-materialized property values (Sled - separate storage)
5. **Record** = unified storage row (IndexedDB - entity + projections together)

### Summary

| Concept | Sled | IndexedDB |
|---------|------|-----------|
| Entity state storage | `entities` tree | `entities` store (`__state_buffer` field) |
| Projected values storage | `collection_{name}` tree | `entities` store (inline fields) |
| Index storage | `index_{id}` tree | IDB index on store |
| Row type for queries | `ProjectedEntity` | `IdbRecord` |
| Sequential scan | `SledIndexScanner`, `SledCollectionScanner` | `IdbIndexScanner` |
| Random lookup | `SledCollectionLookup`, `SledEntityLookup` | N/A (cursor gives full record) |

---

## Migration from Current Code

### Completed (PR #212)

**Sled:**
- `SledMaterializeIter` → `SledCollectionLookup` ✓
- `MatEntity` → `ProjectedEntity` ✓
- `MatRow` → Removed (use `ProjectedEntity` directly) ✓

**IndexedDB:**
- `IdbRow` → `IdbRecord` ✓
- `FilterableObject` → Absorbed into `IdbRecord` ✓
  - `IdbRecord` now wraps JS Object with lazy value extraction
  - Implements `Filterable` and `HasEntityId`

### Completed (Additional)

**IndexedDB:**
- `IdbIndexScanner` integrated into `execute_plan_query` ✓
  - Cursor opening, iteration, and prefix guard logic now in scanner
  - `execute_plan_query` uses scanner's async stream interface

### Completed (Stream-based Combinators)

**Combinators (2026-01-16):**
- `ValueSetStream` trait now requires `Stream + Unpin` instead of `Iterator`
- `FilteredStream`, `ExtractIdsStream`, `LimitedStream`, `SortedStream`, `TopKStream` all implement `Stream`
- Sled scanners (`SledIndexScanner`, `SledCollectionScanner`, `SledCollectionLookup`, `SledEntityLookup`) implement `Stream` (synchronous underneath, always return `Poll::Ready`)
- IndexedDB uses `futures::stream::iter()` to wrap collected results before applying combinators
- Both backends now use the same combinator chain: `scanner.filter_predicate().top_k().entities().collect_states()`

### Notes

- No tree/store name changes - no database migration needed
- `Filterable` trait unchanged for now (possible future rename)
