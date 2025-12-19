## Phase 1f: JSON Path Index Pushdown (Sled/IndexedDB)

### Goal

Enable index-backed queries on JSON paths for Sled and IndexedDB storage engines, matching the Postgres JSONB pushdown already implemented.

### Prerequisites

- Phase 1a complete (PathExpr in parser/AST)
- Phase 1b complete (Json property type)
- Phase 1c complete (structured query evaluation in filter.rs)

### Current State

| Storage | JSON Path Queries | How |
|---------|-------------------|-----|
| Postgres | ✅ Pushed down | JSONB `->` `->>` operators |
| Sled | ⚠️ Works | Post-fetch filter in Rust |
| IndexedDB | ⚠️ Works | Post-fetch filter in Rust |

### Target State (Achieved)

| Storage | JSON Path Queries | How |
|---------|-------------------|-----|
| Postgres | ✅ Pushed down | JSONB operators (unchanged) |
| Sled | ✅ Index-backed | `sub_path` + `Value::extract_at_path()` |
| IndexedDB | ✅ Index-backed | Native `createIndex()` with dot-notation keyPath |

---

### Design

#### IndexKeyPart Extension

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexKeyPart {
    pub column: String,                     // Root property name
    pub sub_path: Option<Vec<String>>,      // Optional path within property value
    pub direction: IndexDirection,
    pub value_type: ValueType,
    pub nulls: Option<NullsOrder>,
    pub collation: Option<String>,
}
```

**Design Decision**: Separate `column` (property) from `sub_path` (sub-navigation) because:
- Matches storage model: entity → property → structured value → path
- `sub_path` generalizes over JSON, future Ref traversal, etc.
- Clear distinction between "which property" and "where within it"

**Alternative considered**: Use `PathExpr` from ankql directly. Rejected because `PathExpr` is parser-level (includes filter predicates, traversal syntax); `IndexKeyPart` is storage-level (just extraction paths).

#### Value Extraction

Add extraction method to `Value` type (generalizes across structured types):

```rust
impl Value {
    /// Extract value at sub-path. Returns None if path doesn't exist (missing).
    pub fn extract_at_path(&self, path: &[String]) -> Option<Value> {
        if path.is_empty() {
            return Some(self.clone());
        }
        match self {
            Value::Json(json) => {
                let mut current = json;
                for key in path {
                    current = current.get(key)?;
                }
                Some(json_to_value(current))
            }
            // Future: other structured types (Ref, Object, etc.)
            _ => None,  // Non-structured types don't support sub-paths
        }
    }
}
```

This keeps extraction logic with the data type, not scattered across storage backends.

#### Path Normalization

Sub-paths are normalized for indexing:
- Array offsets stripped: `items[0].name` → `["items", "name"]`
- Result: index covers all array elements at that path

```rust
fn normalize_sub_path(steps: &[String]) -> Vec<String> {
    steps.iter()
        .filter(|s| !s.parse::<usize>().is_ok())  // Remove numeric indices
        .cloned()
        .collect()
}
```

**Note**: This means `items[0].name = 'x'` and `items[1].name = 'x'` both use the same index. Query matches if ANY array element matches.

#### Missing vs Null

- **Missing path**: `extract_at_path` returns `None` → record NOT indexed
- **Explicit null**: `extract_at_path` returns `Some(Value::Null)` → indexed as null

This matches IndexedDB native behavior and distinguishes missing from null.

---

### Planner Changes

When the planner sees a multi-step path like `context.session_id = 'xyz'`:

```rust
// In plan_with_equalities or similar
fn path_to_keypart(path: &PathExpr, value: &Value) -> IndexKeyPart {
    let (column, sub_path) = if path.steps.len() == 1 {
        // Simple property
        (path.steps[0].clone(), None)
    } else {
        // Property + sub-path
        let (root, rest) = path.steps.split_first().unwrap();
        (root.clone(), Some(normalize_sub_path(rest)))
    };
    
    IndexKeyPart {
        column,
        sub_path,
        direction: IndexDirection::Asc,
        value_type: ValueType::of(value),  // Inferred from query literal
        ..Default::default()
    }
}
```

---

### Storage Engine Changes

#### Sled

**Index key encoding** - uses `Value::extract_at_path`:

```rust
fn extract_value_for_keypart(entity: &Entity, keypart: &IndexKeyPart) -> Option<Value> {
    let prop_value = entity.get_property_value(&keypart.column)?;
    
    match &keypart.sub_path {
        None => Some(prop_value),
        Some(path) => prop_value.extract_at_path(path),  // Returns None if missing
    }
}
```

**Index updates** - when entity changes:
1. Extract value via `extract_at_path`
2. If `None` (missing), don't add to index
3. If `Some(value)`, encode and insert

#### IndexedDB

**Index creation** - translate `IndexKeyPart` to IDB keyPath:

```rust
fn keypart_to_keypath(keypart: &IndexKeyPart) -> String {
    match &keypart.sub_path {
        None => keypart.column.clone(),
        Some(path) => {
            let mut full_path = vec![keypart.column.clone()];
            full_path.extend(path.clone());
            full_path.join(".")
        }
    }
}

// For composite indexes:
fn keyspec_to_keypaths(spec: &KeySpec) -> Vec<String> {
    spec.keyparts.iter().map(keypart_to_keypath).collect()
}
```

IndexedDB natively supports:
- Dot notation in keyPath: `"context.session_id"`
- Composite keys: `["context.session_id", "status"]`
- Missing properties → record not in index (native behavior, matches our semantics)

---

### Tests

```rust
#[tokio::test]
async fn test_json_path_index_equality() {
    let node = setup_sled_node().await;
    let ctx = node.context();
    
    // Create entities with JSON data
    let trx = ctx.begin();
    trx.create(&Detection {
        context: json!({ "session_id": "sess1", "user_id": "user1" }).into(),
        ..
    }).await?;
    trx.create(&Detection {
        context: json!({ "session_id": "sess2", "user_id": "user1" }).into(),
        ..
    }).await?;
    trx.commit().await?;
    
    // Query should use index
    let results: Vec<DetectionView> = ctx
        .fetch("context.session_id = ?", "sess1")
        .await?;
    
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_json_path_index_range() {
    // Test: context.year > 2020
}

#[tokio::test]
async fn test_json_path_composite_index() {
    // Test: context.session_id = ? AND status = ?
}

#[wasm_bindgen_test]
async fn test_indexeddb_json_path_index() {
    // Same tests for IndexedDB
}
```

---

### Acceptance Criteria

1. `context.session_id = 'x'` uses index on Sled
2. `context.session_id = 'x'` uses native IDB index
3. Range queries work: `context.year > 2020`
4. Composite indexes work: `context.session_id = ? AND status = ?`
5. Existing non-JSON indexes continue to work

---

### Files Modified

| File | Changes |
|------|---------|
| `core/src/indexing/key_spec.rs` | Add `sub_path: Option<Vec<String>>` to `IndexKeyPart` |
| `core/src/value/mod.rs` | Add `Value::extract_at_path()` method |
| `storage/common/src/planner.rs` | Generate keyparts with sub_path from multi-step PathExpr |
| `storage/sled/src/index.rs` | Use `extract_at_path`, skip indexing if None |
| `storage/indexeddb-wasm/src/collection.rs` | Translate sub_path to IDB keyPath |

---

### Design Decisions (Resolved)

1. **sub_path vs PathExpr**: Use `sub_path: Option<Vec<String>>` on `IndexKeyPart`. PathExpr is parser-level (includes filter predicates, traversal syntax); `sub_path` is just an extraction path. Name generalizes over future structured types.

2. **Missing vs Null**: Missing path → don't index. Explicit null → index as null. Matches IDB native semantics.

3. **Type inference**: Infer `value_type` from query literal. Schema-driven in Phase 3.

---

### Estimated Effort

2-3 days

---

### What's NOT in this phase

- Array wildcard syntax: `items[*].name`
- Hash-optimized equality indexes (future optimization)
- Schema-based type validation
- Ref traversal paths

