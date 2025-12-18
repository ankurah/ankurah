## Phase 1b: Json Builtin Property Type

### Goal

Introduce `ankurah::Json` as a first-class property type that can be used in models and queried with path syntax.

### Design

```rust
// Usage in models
#[derive(Model)]
pub struct Track {
    pub name: String,
    pub licensing: Json,  // stores arbitrary JSON
}

// Creating
let track = trx.create(&Track {
    name: "Paranoid Android".to_string(),
    licensing: json!({
        "territory": "US",
        "expires": "2030-01-01",
        "rights": { "holder": "Label" }
    }).into(),
}).await?;

// Reading
let licensing: serde_json::Value = track.licensing();
```

### Tasks

#### 1. Json Type (`core/src/property/json.rs`)

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// A JSON property that stores arbitrary structured data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Json(JsonValue);

impl Json {
    pub fn new(value: JsonValue) -> Self {
        Self(value)
    }
    
    pub fn as_value(&self) -> &JsonValue {
        &self.0
    }
    
    pub fn into_inner(self) -> JsonValue {
        self.0
    }
    
    /// Navigate a path and extract a value
    pub fn get_path(&self, path: &[&str]) -> Option<&JsonValue> {
        let mut current = &self.0;
        for key in path {
            current = current.get(key)?;
        }
        Some(current)
    }
}

impl From<JsonValue> for Json {
    fn from(value: JsonValue) -> Self {
        Self(value)
    }
}

impl From<Json> for JsonValue {
    fn from(json: Json) -> Self {
        json.0
    }
}
```

#### 2. Property Trait Implementation

```rust
impl Property for Json {
    fn into_value(&self) -> Result<Option<Value>, PropertyError> {
        let bytes = serde_json::to_vec(&self.0)
            .map_err(|e| PropertyError::SerializeError(Box::new(e)))?;
        Ok(Some(Value::Binary(bytes)))  // or Value::Object if we add that
    }
    
    fn from_value(value: Option<Value>) -> Result<Self, PropertyError> {
        match value {
            Some(Value::Binary(bytes)) => {
                let json_value: JsonValue = serde_json::from_slice(&bytes)
                    .map_err(|e| PropertyError::DeserializeError(Box::new(e)))?;
                Ok(Json(json_value))
            }
            Some(other) => Err(PropertyError::InvalidVariant { 
                given: other, 
                ty: "Json".to_string() 
            }),
            None => Err(PropertyError::Missing),
        }
    }
}
```

#### 3. Backend Selection

Use LWW backend for Json properties (simplest, last-write-wins semantics):

```rust
// In derive macro or backend registry
// Json fields use LWW backend by default
```

Or explicit in model:
```rust
#[derive(Model)]
pub struct Track {
    #[active_type(LWW)]
    pub licensing: Json,
}
```

#### 4. Value Type for Querying

Add a way to convert JSON values to queryable `Value`:

```rust
// In core/src/value.rs or core/src/property/json.rs
impl Json {
    /// Convert a nested JSON value to a queryable Value
    pub fn value_at_path(&self, path: &[&str]) -> Option<Value> {
        let json_val = self.get_path(path)?;
        json_to_value(json_val)
    }
}

fn json_to_value(json: &JsonValue) -> Option<Value> {
    match json {
        JsonValue::Null => None,
        JsonValue::Bool(b) => Some(Value::Bool(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Value::I64(i))
            } else if let Some(f) = n.as_f64() {
                Some(Value::F64(f))
            } else {
                None
            }
        }
        JsonValue::String(s) => Some(Value::String(s.clone())),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            // Nested structures remain as binary
            Some(Value::Binary(serde_json::to_vec(json).ok()?))
        }
    }
}
```

#### 5. Export from ankurah crate

```rust
// ankurah/src/lib.rs
pub use ankurah_core::property::Json;
```

### Tests

```rust
#[test]
fn test_json_property_roundtrip() {
    let json = Json::new(json!({
        "territory": "US",
        "expires": "2030-01-01"
    }));
    
    let value = json.into_value().unwrap().unwrap();
    let restored = Json::from_value(Some(value)).unwrap();
    
    assert_eq!(json, restored);
}

#[test]
fn test_json_path_extraction() {
    let json = Json::new(json!({
        "rights": {
            "holder": "Label",
            "year": 2020
        }
    }));
    
    assert_eq!(
        json.value_at_path(&["rights", "holder"]),
        Some(Value::String("Label".to_string()))
    );
    assert_eq!(
        json.value_at_path(&["rights", "year"]),
        Some(Value::I64(2020))
    );
    assert_eq!(
        json.value_at_path(&["nonexistent"]),
        None
    );
}

#[tokio::test]
async fn test_json_model_crud() {
    let node = setup_node().await;
    let ctx = node.context();
    
    let trx = ctx.begin();
    let track = trx.create(&Track {
        name: "Test".to_string(),
        licensing: json!({ "territory": "US" }).into(),
    }).await.unwrap();
    trx.commit().await.unwrap();
    
    let loaded: TrackView = ctx.get(track.id()).await.unwrap();
    assert_eq!(loaded.licensing().get("territory"), Some(&json!("US")));
}
```

### Acceptance Criteria

1. `Json` type can be used in models
2. Values serialize/deserialize correctly
3. Path extraction works for nested structures
4. CRUD operations work end-to-end

### Files Modified/Created

- `core/src/property/json.rs` (new)
- `core/src/property/mod.rs` (export)
- `core/src/value.rs` (json conversion helper)
- `ankurah/src/lib.rs` (re-export)

### Estimated Effort

1 day

