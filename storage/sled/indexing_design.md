# Sled Indexing Design

## Overview

This document describes the design for automatic index creation and management in the Sled storage engine to support efficient ORDER BY and LIMIT queries.

## Goals

1. Automatically create indexes based on query patterns
2. Support composite indexes for equality and range queries
3. Enable efficient ORDER BY + LIMIT without full table scans
4. Maintain indexes transparently during entity updates

## Index Structure

### Key Encoding

Composite index keys use length-prefixed encoding to handle variable-length fields:

```
[index_id:4][field1_len:4][field1_bytes][field2_len:4][field2_bytes]...[entity_id:16]
```

- `index_id`: 4-byte identifier for this index
- `field_len`: 4-byte length prefix for each field
- `field_bytes`: Variable-length bytes from `Collatable::to_bytes()`
- `entity_id`: 16-byte entity ID for uniqueness and stable ordering

This encoding:

- Preserves lexicographic ordering within each field
- Handles variable-length data (especially strings)
- Allows prefix scans for partial key matches
- Ensures unique keys even for duplicate field values

### Index Trees

Each index is stored in a separate Sled tree:

- Tree name: `{collection}_idx_{index_name}`
- Key: Composite key as described above
- Value: Empty (we only need the key for index scans)

## Index Selection Algorithm

Given a query like: `WHERE foo = 1 AND bar > 0 AND baz != 9 ORDER BY qux DESC LIMIT 10`

1. **Extract Equality Predicates**: Find all `field = value` conditions
2. **Find Range Predicate**: Identify the first inequality (`>`, `<`, `>=`, `<=`)
3. **Consider ORDER BY**: Treat ORDER BY fields as potential range predicates
4. **Build Index Key**: Concatenate equality fields, then the range field

For the example above:

- Equality: `foo = 1`
- Range: `bar > 0` (first inequality)
- ORDER BY: `qux DESC` (could be used if no other range predicate on qux)
- Required index: `(foo, bar)` or `(foo, qux)` depending on selectivity

### Direction Compatibility

An index with mixed directions like `[foo ASC, bar DESC]` can be used for:

- Queries where all inequalities match the index direction
- Queries where all inequalities oppose the index direction (using reverse iteration)

Examples with index `[foo ASC, bar DESC]`:

- ✅ `foo > 5 AND bar < 10` - Forward iteration
- ✅ `foo < 5 AND bar > 10` - Reverse iteration
- ❌ `foo > 5 AND bar > 10` - Incompatible (would need `[foo ASC, bar ASC]`)
- ❌ `foo < 5 AND bar < 10` - Incompatible (would need `[foo DESC, bar DESC]`)

The rule: All inequality operators must either agree with the index direction OR all disagree (enabling reverse iteration).

## Metadata Management

### Metadata Tree

Store index metadata in a dedicated tree:

```rust
// Key: "index:{collection}:{index_name}"
// Value:
struct IndexMetadata {
    fields: Vec<(String, SortDirection)>,  // Field names and directions
    created_at: u64,                       // Timestamp
    last_used: u64,                        // For lifecycle management
    usage_count: u64,                      // For statistics
    status: IndexStatus,                   // Building, Ready, Failed
}

enum SortDirection {
    Asc,
    Desc,
}

enum IndexStatus {
    Building,
    Ready,
    Failed(String),
}
```

### Index Naming

Use human-readable names combining field names and directions:

- Single field: `"user_id_asc"`
- Composite: `"status_asc_created_at_desc"`

Later migration path: Use property IDs instead of names when available.

## Index Lifecycle

### Creation Flow

1. **Query Analysis**: Parse query to determine required index
2. **Check Existence**: Look up index in metadata tree
3. **Create if Missing**:
   ```
   a. Insert metadata with status=Building (prevents duplicate creation)
   b. Acquire creation mutex for this index specification
   c. Scan existing entities to populate index
   d. Update metadata status=Ready
   e. Release mutex
   ```
4. **Use Index**: Execute query using the index

### Maintenance During Updates

On `set_state`:

1. Begin Sled transaction
2. Read old entity state (if exists)
3. For each index affecting this collection:
   - Remove old index entry (if entity previously existed)
   - Calculate new index key from new state
   - Insert new index entry
4. Update entity state
5. Commit transaction

This ensures index consistency even if the update fails.

### Concurrency

- Use a mutex per index specification to prevent duplicate creation
- Index creation happens outside the main write path
- Queries can fall back to full scan if index is still building

## Query Execution

### With Index

1. Determine index to use based on query predicates
2. Calculate start and end keys for range scan
3. Determine iteration direction (forward or reverse)
4. Iterate through index entries
5. Apply remaining predicates not covered by index
6. Apply LIMIT
7. Load full entity states for results

### Without Index (Fallback)

1. Full collection scan
2. Apply all predicates
3. Sort results in memory (for ORDER BY)
4. Apply LIMIT
5. Return results

## Special Considerations

### NULL Handling

- NULL values sort before all non-NULL values (NULLS FIRST)
- Use special byte prefix `0x00` for NULL in index keys
- Non-NULL values prefixed with `0x01`

### Missing Fields

- Treat missing fields as NULL
- Entities missing indexed fields still get index entries (with NULL markers)

### Index Selection with Multiple Options

When multiple indexes could satisfy a query:

1. Prefer indexes that cover more equality predicates
2. Prefer indexes that match the ORDER BY clause
3. Consider index statistics (future enhancement)

## Performance Implications

### Storage Overhead

- Each index stores: indexed fields + entity ID per entity
- Approximate overhead: `(field_sizes + 16 bytes) * entity_count`

### Write Performance

- Each update touches N+1 trees (entity state + N indexes)
- Transactional updates ensure consistency but may impact latency
- Consider batching for bulk operations

### Query Performance

- Index scans: O(log n) to find start position + O(k) for k results
- Full scans: O(n) for n entities
- Index building: O(n log n) one-time cost

## Future Enhancements

1. **Query Statistics**: Track which queries would benefit from indexes
2. **Auto-cleanup**: Remove unused indexes after configurable period
3. **Partial Indexes**: Support WHERE clauses on index creation
4. **Cost-based Optimizer**: Choose best index when multiple available
5. **Property IDs**: Migrate from field names to property IDs
6. **Index Hints**: Allow queries to force/prevent specific index usage

## Implementation Phases

### Phase 1: Basic Single-Field Indexes

- Single field indexes with ASC/DESC
- Manual index creation
- ORDER BY support

### Phase 2: Composite Indexes

- Multi-field indexes
- Mixed sort directions
- Equality + range predicate optimization

### Phase 3: Automatic Management

- On-demand index creation
- Usage tracking
- Lifecycle management

### Phase 4: Advanced Features

- Query optimizer
- Partial indexes
- Index statistics
