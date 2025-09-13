# IndexedDB Indexing Plan for ORDER BY/LIMIT

## Overview

Implement ORDER BY and LIMIT functionality in IndexedDB using the common `Planner` from `ankurah-storage-common` with on-demand index creation. The `Planner` generates index plans that IndexedDB executes using native indexes.

## Key Design Decisions

1. **Common Query Planner**: Use `ankurah_storage_common::planner::Planner` for index selection
2. **Predicate Amendment**: Prefix all predicates with `__collection = 'value'` before planning
3. **First Plan Selection**: Always use the first plan from the planner
4. **On-Demand Index Creation**: Create indexes exactly as specified by the plan if they don't exist
5. **Full Field Extraction**: Extract ALL fields during `set_state` (regular fields unprefixed, special fields with `__` prefix)
6. **Native IndexedDB Sorting**: Use IndexedDB's built-in collation rules (no custom control)
7. **Synchronous Creation**: First query blocks until index exists
8. **Plan-Driven Execution**: Execute queries based on `Plan` structures from the planner

## Architecture

### 1. Field Extraction

Extract ALL fields from state_buffer during `set_state`, similar to Postgres:

```javascript
// New entity structure
{
  id: "entity_id",           // Primary key (already indexed)
  __collection: "albums",    // Special field (prefixed)
  __state_buffer: {...},     // Special field (prefixed)
  __head: {...},            // Special field (prefixed)
  __attestations: [...],    // Special field (prefixed)
  // Regular extracted fields (no prefix)
  name: "The Dark Side of the Moon",
  year: 1973,
  artist: "Pink Floyd",
  duration: 42.5
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

    // 2. Amend predicate with __collection comparison
    let amended_selection = Selection {
        predicate: Predicate::And(
            Box::new(Predicate::Comparison {
                left: Box::new(Expr::Identifier(Identifier::Property("__collection".to_string()))),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Literal::String(collection_id.to_string()))),
            }),
            Box::new(selection.predicate.clone())
        ),
        order_by: selection.order_by.clone(),
        limit: selection.limit,
    };

    // 3. Use common Planner to generate query plans
    let planner = Planner::new(PlannerConfig::default());
    let plans = planner.plan(&amended_selection);

    // 4. Pick the first plan (always)
    let plan = plans.first().ok_or("No plan generated")?;

    // 5. Ensure index exists (may block for creation)
    ensure_index_from_plan(&plan).await?;

    // 6. Execute query using the plan
    execute_plan_query(&amended_selection, &plan).await
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

## Integration with Common Planner

### Plan to IndexedDB Mapping

The common `Planner` generates `Plan` structures that need to be mapped to IndexedDB operations:

```rust
// Plan from common planner (after predicate amendment)
Plan {
    index_fields: vec![
        IndexField { name: "__collection", direction: Asc },
        IndexField { name: "age", direction: Asc }
    ],
    scan_direction: ScanDirection::Asc,  // PRIMARY: determines cursor direction
    range: Range {
        from: Bound::Exclusive([String("album"), Integer(25)]),
        to: Bound::Unbounded
    },
    remaining_predicate: Predicate::...,
    requires_sort: false,
}

// Maps to IndexedDB:
// - Index name: "__collection__age" (exactly as specified by planner)
// - IdbKeyRange: lowerBound(["album", 25], true) (direct conversion from Range)
// - Cursor direction: "next" (from scan_direction, not index_fields[].direction)
// - Post-filter: Apply remaining_predicate after fetching
```

### Index Creation from Plan

```rust
fn index_name_from_plan(plan: &Plan) -> String {
    // Create index name from all fields in the exact order specified
    // No special handling for __collection - it's just another field
    let field_names: Vec<_> = plan.index_fields
        .iter()
        .map(|f| &f.name)
        .collect();
    field_names.join("__")
}

fn create_index_from_plan(store: &IdbObjectStore, plan: &Plan) {
    let index_name = index_name_from_plan(plan);

    // Key path must match the exact field order from the plan
    let key_path = plan.index_fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<js_sys::Array>();

    store.create_index_with_str_sequence(&index_name, &key_path);
}
```

### Range Conversion

Direct conversion from `Plan::range` to `IdbKeyRange`:

```rust
fn plan_range_to_idb_range(range: &Range) -> Result<IdbKeyRange> {
    use ankurah_storage_common::planner::Bound;

    match (&range.from, &range.to) {
        (Bound::Inclusive(from_vals), Bound::Inclusive(to_vals)) => {
            IdbKeyRange::bound(
                &property_values_to_js_array(from_vals),
                &property_values_to_js_array(to_vals),
                false, // from_exclusive = false (inclusive)
                false  // to_exclusive = false (inclusive)
            )
        }
        (Bound::Exclusive(from_vals), Bound::Inclusive(to_vals)) => {
            IdbKeyRange::bound(
                &property_values_to_js_array(from_vals),
                &property_values_to_js_array(to_vals),
                true,  // from_exclusive = true
                false  // to_exclusive = false
            )
        }
        (Bound::Inclusive(from_vals), Bound::Exclusive(to_vals)) => {
            IdbKeyRange::bound(
                &property_values_to_js_array(from_vals),
                &property_values_to_js_array(to_vals),
                false, // from_exclusive = false
                true   // to_exclusive = true
            )
        }
        (Bound::Exclusive(from_vals), Bound::Exclusive(to_vals)) => {
            IdbKeyRange::bound(
                &property_values_to_js_array(from_vals),
                &property_values_to_js_array(to_vals),
                true,  // from_exclusive = true
                true   // to_exclusive = true
            )
        }
        (Bound::Inclusive(from_vals), Bound::Unbounded) => {
            IdbKeyRange::lower_bound(
                &property_values_to_js_array(from_vals),
                false  // exclusive = false (inclusive)
            )
        }
        (Bound::Exclusive(from_vals), Bound::Unbounded) => {
            IdbKeyRange::lower_bound(
                &property_values_to_js_array(from_vals),
                true   // exclusive = true
            )
        }
        (Bound::Unbounded, Bound::Inclusive(to_vals)) => {
            IdbKeyRange::upper_bound(
                &property_values_to_js_array(to_vals),
                false  // exclusive = false (inclusive)
            )
        }
        (Bound::Unbounded, Bound::Exclusive(to_vals)) => {
            IdbKeyRange::upper_bound(
                &property_values_to_js_array(to_vals),
                true   // exclusive = true
            )
        }
        (Bound::Unbounded, Bound::Unbounded) => {
            // No range constraint - scan all
            Ok(None)
        }
    }
}

fn property_values_to_js_array(values: &[PropertyValue]) -> js_sys::Array {
    let array = js_sys::Array::new();
    for value in values {
        array.push(&JsValue::from(value));
    }
    array
}
```

## Design Constraints

1. **No Unique Indexes**: Primary key on `id` is sufficient, no additional unique constraints needed
2. **Index Field Order**: Preserve exact field order from planner - never reorder
3. **Index Naming**: Use `__` delimiter for composite index names (e.g., `__collection__age`)
4. **Scan Direction**: Use `plan.scan_direction` for cursor direction, ignore `index_fields[].direction`
5. **No Descending Indexes**: IndexedDB doesn't support DESC indexes structurally (only via reverse cursor scan)

## Next Steps

1. **Update Collection::fetch_states**:

   - Amend predicate with `__collection = 'value'` comparison
   - Use `Planner` to generate plans
   - Take first plan from the list
   - Ensure index exists (create if needed)
   - Execute using plan's index and range

2. **Implement Helper Functions**:

   - `amend_predicate_with_collection()` - Add \_\_collection comparison
   - `index_name_from_plan()` - Generate index name from plan
   - `plan_range_to_idb_range()` - Convert Range to IdbKeyRange
   - `ensure_index_from_plan()` - Create index if doesn't exist
   - `execute_plan_query()` - Execute query using plan

3. **Clean Break from Old Code**:

   - Delete `indexes.rs::Optimizer` and related types
   - Remove `quarantine.rs` entirely
   - Remove intermediate `RangeConstraint` type
   - Keep only minimal index creation utilities

4. **Field Storage Updates**:

   - Regular fields: store without prefix (e.g., `name`, `age`)
   - Special fields: store with `__` prefix (e.g., `__collection`, `__head`)
   - Update `extract_all_fields()` accordingly

5. **Testing**:
   - Verify existing queries still work
   - Test ORDER BY with new planner
   - Test multiple inequality scenarios
   - Test index creation on first use
