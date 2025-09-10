# IndexedDB Indexing Plan for ORDER BY/LIMIT

## Overview

Implement ORDER BY and LIMIT functionality in IndexedDB using native indexes with on-demand creation. Indexes are created synchronously on first use, blocking the query until ready.

## Key Design Decisions

1. **On-Demand Index Creation**: Create indexes when first needed, no prediction or thresholds
2. **Full Field Extraction**: Extract ALL fields during `set_state` for comprehensive indexing
3. **Native IndexedDB Sorting**: Use IndexedDB's built-in collation rules (no custom control)
4. **Synchronous Creation**: First query blocks until index exists
5. **No Backward Compatibility**: Clean break - all entities must have extracted fields

## Architecture

### 1. Field Extraction

Extract ALL fields from state_buffer during `set_state`, similar to Postgres:

```javascript
// New entity structure
{
  id: "entity_id",           // Primary key (already indexed)
  collection: "albums",       // Already has index
  state_buffer: {...},        // Original serialized data
  head: {...},
  attestations: [...],
  // Extracted fields with _idx_ prefix
  _idx_name: "The Dark Side of the Moon",
  _idx_year: 1973,
  _idx_artist: "Pink Floyd",
  _idx_duration: 42.5
}
```

**Type Mapping for IndexedDB**:

- Integers → Number
- Floats → Number (NaN becomes null or Infinity based on IndexedDB rules)
- Strings → String
- EntityId → String (base64)
- Booleans → Boolean
- NULL/undefined → null

**Note**: IndexedDB has fixed sorting rules:

1. null/undefined
2. Numbers (including NaN)
3. Dates
4. Strings
5. Binary (ArrayBuffer)
6. Arrays

### 2. Index Management

#### Index Metadata Store

```javascript
// Object store: "index_metadata"
{
  key: "albums::name_asc_year_desc",
  value: {
    collection: "albums",
    fields: [
      { name: "name", direction: "asc" },
      { name: "year", direction: "desc" }
    ],
    version: 3,  // DB version when created
    status: "ready"
  }
}
```

#### Version Management

Simple approach - no recycling needed:

```javascript
async function ensureIndex(indexSpec) {
  // 1. Check if index exists
  if (await indexExists(indexSpec)) {
    return;
  }

  // 2. Acquire cross-tab lock (Web Locks API)
  const lock = await navigator.locks.request(
    `index_creation_${indexSpec}`,
    { mode: "exclusive" },
    async () => {
      // 3. Double-check after lock
      if (await indexExists(indexSpec)) {
        return;
      }

      // 4. Get current version
      const currentVersion = db.version;

      // 5. Close all connections
      db.close();

      // 6. Reopen with incremented version
      const request = indexedDB.open(dbName, currentVersion + 1);

      request.onupgradeneeded = (event) => {
        const db = event.target.result;
        const store = db.objectStore("entities");

        // Create the index
        const keyPath = indexSpec.fields.map((f) => `_idx_${f.name}`);
        store.createIndex(indexSpec.name, keyPath, {
          unique: false,
          multiEntry: false,
        });
      };

      // 7. Wait for upgrade to complete
      await promisify(request);

      // 8. Mark index as ready in metadata
      await markIndexReady(indexSpec);
    }
  );
}
```

### 3. Cross-Tab Coordination

**Primary**: Web Locks API

```javascript
navigator.locks.request(lockName, { mode: "exclusive" }, async () => {
  // Critical section
});
```

**Fallback**: IndexedDB transaction as mutex

```javascript
// Use a dedicated "locks" object store with transactions
const tx = db.transaction(["locks"], "readwrite");
const store = tx.objectStore("locks");
// Attempt to write lock record...
```

### 4. Connection Management

Each tab handles its own reconnection:

```javascript
class ManagedIDBConnection {
  async connect() {
    // Always use current version (don't cache)
    const request = indexedDB.open(this.dbName);
    return await promisify(request);
  }

  async executeWithRetry(operation) {
    try {
      return await operation(this.db);
    } catch (e) {
      if (e.name === "VersionError" || e.name === "InvalidStateError") {
        // Connection invalidated, reconnect
        this.db = await this.connect();
        return await operation(this.db);
      }
      throw e;
    }
  }
}
```

## Query Execution Flow

### Complete Query Flow

```rust
async fn fetch_states(selection: &Selection) -> Result<Vec<EntityState>> {
    // 1. Extract all fields during previous set_state operations
    //    (already done)

    // 2. Analyze query to determine optimal index
    let index_spec = analyze_query(selection)?;

    // 3. Ensure index exists (may block for creation)
    if index_spec.is_some() {
        ensure_index(&index_spec).await?;
    }

    // 4. Execute query
    match index_spec {
        Some(spec) => execute_indexed_query(selection, &spec).await,
        None => execute_id_optimized_query(selection).await,
    }
}
```

### Query Strategies

#### Strategy 1: ID-Optimized (No Custom Index Needed)

```javascript
// For: WHERE id > 'xyz' ORDER BY id LIMIT 10
// Use primary key (automatic index on 'id' field)

const range = IDBKeyRange.lowerBound("xyz", true);
const request = store.openCursor(range, "next");
// Iterate and stop at LIMIT
```

#### Strategy 2: Collection + Custom Index

```javascript
// For: WHERE collection = 'albums' AND year > 1970 ORDER BY year DESC LIMIT 5
// After ensuring index exists on [year]

const index = store.index("year_desc");
const range = IDBKeyRange.lowerBound(1970, true);
const request = index.openCursor(range, "prev");
// Filter by collection during iteration
// Stop at LIMIT
```

#### Strategy 3: Compound Index

```javascript
// For: WHERE status = 'active' ORDER BY created_at DESC
// After ensuring compound index on [status, created_at]

const index = store.index("status_asc_created_at_desc");
const range = IDBKeyRange.bound(["active", -Infinity], ["active", Infinity]);
const request = index.openCursor(range, "prev");
```

## Field Extraction Implementation

```rust
fn extract_all_fields(entity: &EntityState) -> HashMap<String, JsValue> {
    let mut extracted = HashMap::new();
    let mut seen_fields = HashSet::new();

    for (backend_name, state_buffer) in entity.state.state_buffers.iter() {
        let backend = backend_from_string(backend_name, Some(state_buffer))?;

        for (field_name, value) in backend.property_values() {
            // Use first occurrence (like Postgres)
            if !seen_fields.insert(field_name.clone()) {
                continue;
            }

            // Convert to IndexedDB value
            let idx_value = to_indexeddb_value(value);
            extracted.insert(format!("_idx_{}", field_name), idx_value);
        }
    }

    extracted
}

fn to_indexeddb_value(value: Option<PropertyValue>) -> JsValue {
    match value {
        None => JsValue::NULL,
        Some(PropertyValue::String(s)) => JsValue::from_str(&s),
        Some(PropertyValue::Integer(i)) => JsValue::from_f64(i as f64),
        Some(PropertyValue::Float(f)) => {
            if f.is_nan() {
                JsValue::NULL  // or JsValue::from_f64(f) - IndexedDB handles NaN
            } else {
                JsValue::from_f64(f)
            }
        }
        Some(PropertyValue::Boolean(b)) => JsValue::from_bool(b),
        Some(PropertyValue::EntityId(id)) => JsValue::from_str(&id.to_base64()),
    }
}
```

## Error Handling

### Index Creation Failures

- If `createIndex` throws in `onupgradeneeded`, the entire upgrade transaction rolls back
- Version number is NOT incremented
- Next attempt will retry with same version number

### Connection Invalidation

- When version changes, all connections in all tabs become invalid
- Each tab detects this and reconnects with current version
- Queries in flight may need retry

## Implementation Phases

### Phase 1: Foundation

- Extract ALL fields during `set_state`
- Implement ID-based ORDER BY using primary key
- Add LIMIT support with cursor termination
- No custom indexes yet

### Phase 2: Single-Field Indexes

- Implement version upgrade mechanism
- Add Web Locks API coordination
- Create single-field indexes on demand
- Handle connection invalidation gracefully

### Phase 3: Compound Indexes

- Support multi-field indexes
- Implement index selection algorithm
- Handle complex WHERE + ORDER BY patterns

### Phase 4: Polish

- Add fallback locking for older browsers
- Optimize index reuse detection
- Add telemetry for index usage

## Design Constraints

1. **No Unique Indexes**: Primary key on `id` is sufficient, no additional unique constraints needed
2. **Single Field ORDER BY**: Only support single field ORDER BY clauses, return error for multiple fields
3. **Index Naming**: Use `|` delimiter for compound index names (e.g., `status|asc|created_at|desc`)

## Next Steps

1. Verify `id` field is properly set as primary key
2. Implement Phase 1 (field extraction)
3. Test version upgrade mechanism
4. Build Web Locks coordination
5. Create test suite for query patterns
