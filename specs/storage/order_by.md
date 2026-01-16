# ORDER BY Pushdown and Partition-Aware Sorting

This document describes how ORDER BY clauses interact with index pushdown, and the partition-aware sorting mechanism used when indexes cannot fully satisfy the requested ordering.

## Background

When executing a query with ORDER BY, the ideal case is when an index can provide results in the exact order requested. However, this isn't always possible:

1. **Mixed directions on ASC-only indexes**: IndexedDB only supports ASC indexes. We can scan backwards (Reverse cursor) to get DESC order, but mixed directions like `ORDER BY a ASC, b DESC` cannot be fully satisfied.

2. **ORDER BY columns not in index**: If ORDER BY references columns not covered by the index being used.

3. **Gaps in ORDER BY prefix**: If the index covers columns (a, b, c) but ORDER BY is (a, c), column b creates a gap.

## OrderByComponents

The planner outputs `OrderByComponents` to describe how ORDER BY should be handled:

```rust
pub struct OrderByComponents {
    /// ORDER BY columns satisfied by the index scan direction.
    /// These define "partition boundaries" - when these values change,
    /// we're in a new partition that needs independent sorting.
    /// Empty if the entire ORDER BY must be spilled (global sort).
    pub presort: Vec<OrderByItem>,

    /// ORDER BY columns requiring in-memory sort.
    /// Empty if the index fully satisfies the ORDER BY.
    pub spill: Vec<OrderByItem>,
}
```

### Examples

**Query**: `ORDER BY category ASC, name ASC, price DESC` on IndexedDB

- Index provides: `category ASC, name ASC` (via forward scan)
- Cannot provide: `price DESC` (would need mixed direction)
- Result: `presort = [category ASC, name ASC]`, `spill = [price DESC]`

**Query**: `ORDER BY a DESC, b DESC, c ASC` on IndexedDB

- Index provides: `a DESC, b DESC` (via reverse scan - all same direction)
- Cannot provide: `c ASC` (direction change)
- Result: `presort = [a DESC, b DESC]`, `spill = [c ASC]`

**Query**: `ORDER BY x ASC, y ASC` with no suitable index

- Index provides: nothing
- Result: `presort = []`, `spill = [x ASC, y ASC]` (global sort)

## Partition-Aware Sorting

When `presort` is non-empty, results from the index arrive in `presort` order. We only need to sort **within partitions** defined by `presort` column values.

### Algorithm

```
1. If presort is empty:
   → Global sort by spill columns (collect all, sort, emit)

2. If presort is non-empty:
   → Track current partition key (values of presort columns)
   → Collect items while partition key unchanged
   → When partition key changes:
      a. Sort accumulated items by spill columns
      b. Emit sorted partition
      c. Start new partition with current item
   → At end: sort and emit final partition
```

### Why This Enables Streaming

Since partitions arrive in final order (thanks to the index), we can emit each sorted partition immediately without waiting for all results. This is memory-efficient - we only buffer one partition at a time.

### TopK Optimization

For `LIMIT K` queries with partition-aware sorting:

- Partitions arrive in final order
- Sort each partition by spill columns
- Emit items, counting toward K
- **Stop early** once K items emitted

This avoids processing all partitions when only the first few are needed.

## Relationship to Index Pushdown

The planner's job is to:

1. **Choose index columns** based on WHERE predicates and ORDER BY
2. **Determine scan direction** (Forward/Reverse) based on first ORDER BY direction
3. **Calculate bounds** for equality prefix and inequality range
4. **Compute OrderByComponents**:
   - `presort` = ORDER BY columns satisfied by index + scan direction
   - `spill` = remaining ORDER BY columns needing in-memory sort

### IndexedDB-Specific Behavior

IndexedDB cannot create DESC index columns (all indexes are ASC). The scan direction (Forward/Reverse cursor) determines the effective ordering:

| Scan Direction | Index Column | Effective Order |
|----------------|--------------|-----------------|
| Forward        | ASC          | ASC             |
| Reverse        | ASC          | DESC            |

For multi-column ORDER BY, all columns must have the **same effective direction** to avoid spilling:

- `ORDER BY a ASC, b ASC` → Forward scan, no spill
- `ORDER BY a DESC, b DESC` → Reverse scan, no spill
- `ORDER BY a ASC, b DESC` → Forward scan, spill `b DESC`
- `ORDER BY a DESC, b ASC` → Reverse scan, spill `b ASC`

### Sled Behavior

Sled uses byte-inversion to encode DESC columns directly in index keys. This means Sled can satisfy mixed-direction ORDER BY without spilling (in most cases). The `OrderByComponents` infrastructure still exists for edge cases and future flexibility.

## Implementation Locations

- **Type definition**: `storage/common/src/types.rs` - `OrderByComponents`
- **Planner logic**: `storage/common/src/planner.rs` - computes presort/spill
- **Stream wrappers**: `storage/common/src/sorting.rs` - `SortedStream`, `TopKStream`
- **IndexedDB usage**: `storage/indexeddb-wasm/src/collection.rs`

## Edge Cases

| presort | spill | Behavior |
|---------|-------|----------|
| empty | empty | No sorting needed (pass-through) |
| empty | non-empty | Global sort by spill |
| non-empty | empty | No sorting needed (index satisfies ORDER BY) |
| non-empty | non-empty | Partition-aware sort by spill |
