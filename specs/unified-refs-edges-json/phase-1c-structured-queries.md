## Phase 1c: Structured Property Queries

### Goal

Enable queries like `licensing.territory = ?` that navigate into Json properties.

### Prerequisites

- Phase 1a complete (parser handles multi-step paths)
- Phase 1b complete (Json type exists)

### How It Works

1. Query: `licensing.territory = ?`
2. Parse: `PathExpr { steps: ["licensing", "territory"] }`
3. Evaluate:
   - Get `licensing` property → `Value::Binary(json_bytes)`
   - Deserialize to `Json`
   - Extract `territory` → `Value::String("US")`
   - Compare with placeholder value

### Tasks

#### 1. Extend Filterable Value Resolution

Update `core/src/selection/filter.rs` to handle multi-step paths:

```rust
fn evaluate_expr<I: Filterable>(item: &I, expr: &Expr) -> Result<ExprOutput<Value>, Error> {
    match expr {
        Expr::Path(path) => evaluate_path(item, path),
        // ... other cases unchanged
    }
}

fn evaluate_path<I: Filterable>(item: &I, path: &PathExpr) -> Result<ExprOutput<Value>, Error> {
    if path.steps.is_empty() {
        return Err(Error::UnsupportedExpression("empty path"));
    }
    
    // Get the root property
    let root_name = &path.steps[0];
    let root_value = item.value(root_name)
        .ok_or_else(|| Error::PropertyNotFound(root_name.clone()))?;
    
    // If single-step path, return directly
    if path.steps.len() == 1 {
        return Ok(ExprOutput::Value(root_value));
    }
    
    // Multi-step: navigate into structured value
    let remaining_path: Vec<&str> = path.steps[1..].iter().map(|s| s.as_str()).collect();
    
    match navigate_structured(&root_value, &remaining_path) {
        Some(value) => Ok(ExprOutput::Value(value)),
        None => Ok(ExprOutput::None),  // Path doesn't exist = NULL
    }
}

fn navigate_structured(value: &Value, path: &[&str]) -> Option<Value> {
    if path.is_empty() {
        return Some(value.clone());
    }
    
    // Value must be JSON-like (Binary containing JSON)
    let json_bytes = match value {
        Value::Binary(bytes) => bytes,
        _ => return None,  // Can't navigate into non-structured value
    };
    
    // Deserialize and navigate
    let json: serde_json::Value = serde_json::from_slice(json_bytes).ok()?;
    let mut current = &json;
    
    for key in path {
        current = current.get(*key)?;
    }
    
    // Convert back to Value
    json_to_value(current)
}

fn json_to_value(json: &serde_json::Value) -> Option<Value> {
    match json {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(b) => Some(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Value::I64(i))
            } else {
                n.as_f64().map(Value::F64)
            }
        }
        serde_json::Value::String(s) => Some(Value::String(s.clone())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            // Nested structures: serialize back to binary
            serde_json::to_vec(json).ok().map(Value::Binary)
        }
    }
}
```

#### 2. Handle Missing Paths Gracefully

When a path doesn't exist in the JSON:
- Return `ExprOutput::None`
- `IS NULL` checks should return true
- Equality comparisons should return false (NULL != anything)

This matches SQL NULL semantics and the spec's "null vs missing treated equivalently" default.

#### 3. SQL Generation (Postgres)

For Postgres, multi-step JSON paths can use `->` and `->>` operators:

```rust
// In storage/postgres/src/sql_builder.rs
fn build_path_sql(path: &PathExpr) -> String {
    if path.steps.len() == 1 {
        format!(r#""{}""#, path.steps[0].replace('"', "\"\""))
    } else {
        // JSON path: column->>'key1'->>'key2'
        let root = &path.steps[0];
        let json_path: String = path.steps[1..]
            .iter()
            .enumerate()
            .map(|(i, key)| {
                if i == path.steps.len() - 2 {
                    format!("->>'{}'", key.replace('\'', "''"))  // text extraction
                } else {
                    format!("->'{}'", key.replace('\'', "''"))   // json extraction
                }
            })
            .collect();
        format!(r#""{}"{}"#, root.replace('"', "\"\""), json_path)
    }
}
```

#### 4. SQL Generation (SQLite/Sled)

For non-Postgres storage, multi-step paths in SQL generation should error:

```rust
// Structured queries require in-memory filtering for non-Postgres
if path.steps.len() > 1 {
    return Err(SqlError::UnsupportedPath(
        "Multi-step paths require post-fetch filtering on this backend"
    ));
}
```

The query still works — it just can't be pushed down to the index.

### Tests

```rust
#[derive(Model)]
pub struct Track {
    pub name: String,
    pub licensing: Json,
}

#[tokio::test]
async fn test_structured_query_simple() {
    let node = setup_node().await;
    let ctx = node.context();
    
    let trx = ctx.begin();
    trx.create(&Track {
        name: "Track 1".into(),
        licensing: json!({ "territory": "US" }).into(),
    }).await?;
    trx.create(&Track {
        name: "Track 2".into(),
        licensing: json!({ "territory": "UK" }).into(),
    }).await?;
    trx.commit().await?;
    
    let results: Vec<TrackView> = ctx.fetch("licensing.territory = ?", "US").await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name(), "Track 1");
}

#[tokio::test]
async fn test_structured_query_nested() {
    let node = setup_node().await;
    let ctx = node.context();
    
    let trx = ctx.begin();
    trx.create(&Track {
        name: "Track 1".into(),
        licensing: json!({
            "rights": { "holder": "Label A" }
        }).into(),
    }).await?;
    trx.commit().await?;
    
    let results: Vec<TrackView> = ctx.fetch("licensing.rights.holder = ?", "Label A").await?;
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_structured_query_missing_path() {
    // Track with licensing that doesn't have "territory"
    let node = setup_node().await;
    let ctx = node.context();
    
    let trx = ctx.begin();
    trx.create(&Track {
        name: "Track 1".into(),
        licensing: json!({ "other": "value" }).into(),
    }).await?;
    trx.commit().await?;
    
    // Should not match (path doesn't exist = NULL)
    let results: Vec<TrackView> = ctx.fetch("licensing.territory = ?", "US").await?;
    assert_eq!(results.len(), 0);
    
    // IS NULL should match
    let results: Vec<TrackView> = ctx.fetch("licensing.territory IS NULL").await?;
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_structured_query_type_coercion() {
    // Number in JSON compared with different types
    let node = setup_node().await;
    let ctx = node.context();
    
    let trx = ctx.begin();
    trx.create(&Track {
        name: "Track 1".into(),
        licensing: json!({ "year": 2020 }).into(),
    }).await?;
    trx.commit().await?;
    
    // Should work with i64 comparison
    let results: Vec<TrackView> = ctx.fetch("licensing.year = ?", 2020i64).await?;
    assert_eq!(results.len(), 1);
    
    // Should work with range
    let results: Vec<TrackView> = ctx.fetch("licensing.year > ?", 2019i64).await?;
    assert_eq!(results.len(), 1);
}
```

### Acceptance Criteria

1. `licensing.territory = ?` queries work end-to-end
2. Deeply nested paths work (`a.b.c.d = ?`)
3. Missing paths behave as NULL
4. Type coercion works for JSON numbers/strings/bools
5. Postgres can push down to JSON operators (optional optimization)

### Files Modified

- `core/src/selection/filter.rs` (main change)
- `storage/postgres/src/sql_builder.rs` (JSON operators)
- `ankql/src/selection/sql.rs` (path handling)

### Estimated Effort

2 days

### What's NOT in this phase

- Schema validation of JSON structure (runtime only)
- Index pushdown beyond Postgres JSON operators
- `[filter]` syntax for arrays in JSON
- Ref traversal (that's Phase 2)

