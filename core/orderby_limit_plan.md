# ORDER BY / LIMIT Implementation Plan

## Overview

Add ORDER BY and LIMIT support to Ankurah's query system, treating them as part of the selection criteria rather than just presentation logic. The key insight is that ORDER BY + LIMIT fundamentally changes what entities are in the result set, not just their presentation.

## AST Changes

### New Selection Structure

```rust
// In ankql/src/ast.rs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Selection {
    pub predicate: Predicate,
    pub order_by: Option<Vec<OrderByClause>>,
    pub limit: Option<u64>,
    // No offset for now - inefficient
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByClause {
    pub expr: Expr,  // Usually Identifier::Property("timestamp")
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderDirection {
    Asc,
    Desc,
}
```

### MatchArgs Update

```rust
// In core/src/context.rs
pub struct MatchArgs {
    pub selection: Selection,  // Was: predicate: Predicate
}

// Backward compatibility
impl From<Predicate> for Selection {
    fn from(predicate: Predicate) -> Self {
        Selection {
            predicate,
            order_by: None,
            limit: None,
        }
    }
}
```

## Storage Engine Implementation

### Postgres

- Append ORDER BY and LIMIT clauses to generated SQL
- Handle missing columns with NULLS LAST
- Leverage native SQL ordering

### Sled

- Full table scan with predicate filtering
- In-memory sort using `collation.rs` for type-aware comparison
- Apply LIMIT after sorting
- TODO: Add indexing for better performance

## Reactor Changes

### The Fundamental Challenge

With ORDER BY + LIMIT, the result set is no longer just "entities that match the predicate" but "the TOP N entities that match when ordered by X". This means:

1. **Cannot evaluate locally**: We can't determine if an entity should be in the result set without knowing its position relative to all other matching entities
2. **Remote trust**: When receiving updates from remote nodes, we must trust their ordering when LIMIT is present
3. **Re-query on changes**: When entities change, we may need to re-query to maintain the correct TOP N

### Key Decision Points

1. **Initial subscription (`add_query`)**: Must trust remote's ordering when LIMIT present
2. **Predicate updates (`update_query`)**: Cannot re-evaluate locally with LIMIT
3. **Entity changes (`notify_change`)**: The hardest case - requires either full re-query or gap-filling

## Gap-Filling Optimization

### The Problem

When an entity in a LIMIT query stops matching the predicate, naively re-querying the entire result set is expensive.

### The Solution: Boundary-Based Gap-Filling

Instead of re-querying everything, query only for entities that would come "next" in the sort order:

1. **Track boundary values**: Keep the ORDER BY field values of the last entity in the result set
2. **Augment predicate**: Add a comparison based on the sort order to find the next entities
3. **Query for gap only**: Fetch exactly the number of entities needed to fill the gap

### Correctness Requirements

For gap-filling to be correct:

1. **Stable ordering**: Must include a tie-breaker (e.g., entity ID) for deterministic results
2. **Understand sort direction**: Different logic for ASC vs DESC ordering
3. **Boundary tracking**: Must know exact values of ORDER BY fields for boundary entities

### Example: Gap-Fill Query Construction

Original: `status = 'active' ORDER BY timestamp DESC LIMIT 10`

When entity at position 7 is removed and last entity has timestamp '2024-01-01':

```sql
-- Gap-fill query for DESC ordering:
status = 'active'
  AND (timestamp < '2024-01-01'
       OR (timestamp = '2024-01-01' AND entity_id > 'last_entity_id'))
ORDER BY timestamp DESC, entity_id ASC
LIMIT 1
```

The key insight: We're augmenting the predicate with a comparison to the ORDER BY field (or entity ID for tie-breaking) to find the "next" entities in sort order.

### Complexity Considerations

- **Multiple ORDER BY columns**: Boundary conditions become complex quickly
- **Concurrent modifications**: May invalidate gap-fill assumptions
- **Type handling**: Different comparison logic for different data types

### Implementation Strategy

1. **Start conservative**: Only use gap-filling for single ORDER BY column cases
2. **Detect invalidation**: Fall back to full re-query if assumptions violated
3. **Version tracking**: Use versioning to detect concurrent modifications

## LiveQuery Changes

- Update method signatures to accept `Selection` instead of `Predicate`
- When LIMIT is present, `included_entities` represents the complete result set, not just new matches
- Leverage gap-filling optimization when possible

## Node Changes

- Update `fetch_entities_from_local` to accept `Selection`
- Pass through to storage engine's `fetch_states`

## Implementation Phases

### Phase 1: Core Infrastructure

1. Add Selection structure to AST
2. Update storage engine interfaces
3. Implement sorting/limiting in storage engines

### Phase 2: Reactor Integration

1. Track whether predicates have limits
2. Implement gap-filling optimization
3. Add fallback to full re-query when needed

### Phase 3: API & Polish

1. Update Context::query to accept Selection
2. Maintain backward compatibility via From<Predicate>
3. Add comprehensive test coverage

## Design Decisions

### Default Ordering

When no ORDER BY specified but LIMIT is present, we need deterministic ordering:

- Default to ordering by entity ID
- Ensures consistent results across queries

### NULL Handling

- NULLS LAST for all orderings (matches Postgres default for DESC)
- Missing properties treated as NULL

### Tie-Breaking

- Always append entity ID as final ORDER BY column
- Ensures deterministic ordering even with duplicate values

### Performance Trade-offs

**Gap-Filling vs Full Re-query**:

- Gap-filling is optimal for small changes
- Full re-query may be better for large changes
- Heuristic: Use gap-filling if gap_size < limit/2

**Buffering Strategy**:

- Could fetch LIMIT \* 1.5 entities internally
- Reduces re-queries for small changes
- Trade-off: Memory vs query frequency

## Future Enhancements

1. **Parser Support**: Extend ankql grammar for ORDER BY/LIMIT syntax
2. **Offset Support**: Add pagination (though inefficient without cursors)
3. **Index Support**: Add B-tree indices to Sled for efficient ordered queries
4. **Cursor-Based Pagination**: More efficient than OFFSET for large datasets
5. **Aggregate Functions**: COUNT, MIN, MAX, etc.
6. **Window Functions**: For more complex analytical queries
