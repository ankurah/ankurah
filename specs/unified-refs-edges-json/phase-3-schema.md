## Phase 3: Schema Registry

### Goal

Define full runtime schema metadata for model fields and structured properties. Enables validation, PropertyId-based resolution, and future optimizations.

### Prerequisites

- Phase 1 complete (parser, Json type, structured queries)
- Phase 2 complete (ref traversal with minimal metadata)

**Note**: This phase formalizes the ad-hoc metadata from Phase 2 into a proper registry.

### Tasks

#### 2.1 Core Schema Types (`core/src/schema/mod.rs`)

- [ ] Create new `schema` module
- [ ] Define `PropertyId` type (u32)
- [ ] Define `StructuredPropertyId` type
- [ ] Define `StructuredKey` enum (Field/Index)
- [ ] Define `StructuredNode` enum (Struct/List/Scalar/Any)
- [ ] Define `ScalarType` enum
- [ ] Define `FieldSchema` struct
- [ ] Define `FieldKind` enum
- [ ] Define `ModelSchema` struct
- [ ] Implement `Serialize`/`Deserialize` for all

```rust
// core/src/schema/mod.rs

pub type PropertyId = u32;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StructuredPropertyId {
    pub property_id: PropertyId,
    pub path: Vec<StructuredKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StructuredKey {
    Field(String),
    Index(usize),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StructuredNode {
    Struct { fields: BTreeMap<String, StructuredNode> },
    List { element: Box<StructuredNode> },
    Scalar(ScalarType),
    Any,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ScalarType {
    String,
    I64,
    F64,
    Bool,
    EntityId,
    Binary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    pub name: String,
    pub property_id: PropertyId,
    pub kind: FieldKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldKind {
    Scalar(ScalarType),
    Structured(StructuredNode),
    Ref { target_model: String },
    MultiRef { target_model: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelSchema {
    pub name: String,
    pub collection_id: String,
    pub fields: Vec<FieldSchema>,
}
```

#### 2.2 Schema Registry (`core/src/schema/registry.rs`)

- [ ] Create `SchemaRegistry` struct
- [ ] Implement `register(ModelSchema)`
- [ ] Implement `get(model_name) -> Option<&ModelSchema>`
- [ ] Implement `get_field(model, field_name) -> Option<&FieldSchema>`
- [ ] Thread-safe with `RwLock` or similar

```rust
#[derive(Default)]
pub struct SchemaRegistry {
    models: RwLock<HashMap<String, ModelSchema>>,
}

impl SchemaRegistry {
    pub fn register(&self, schema: ModelSchema) { ... }
    pub fn get(&self, model_name: &str) -> Option<ModelSchema> { ... }
    pub fn get_field(&self, model: &str, field: &str) -> Option<FieldSchema> { ... }
}
```

#### 2.3 Structured Property Navigation

- [ ] Implement `StructuredNode::get_child(key: &StructuredKey) -> Option<&StructuredNode>`
- [ ] Implement `StructuredNode::navigate(path: &[StructuredKey]) -> Option<&StructuredNode>`
- [ ] Validate paths against structured schema

#### 2.4 Manual Schema Definition (for testing)

- [ ] Create test schemas for existing models (`Album`, `Pet`, `LogEntry`)
- [ ] Add structured schema for `Payload::Json` in LogEntry
- [ ] Register schemas in test setup

```rust
// Example: Album schema
ModelSchema {
    name: "Album".to_string(),
    collection_id: "album".to_string(),
    fields: vec![
        FieldSchema {
            name: "name".to_string(),
            property_id: 1,
            kind: FieldKind::Scalar(ScalarType::String),
        },
        FieldSchema {
            name: "year".to_string(),
            property_id: 2,
            kind: FieldKind::Scalar(ScalarType::String),
        },
        // Future: artist: Ref<Artist>
    ],
}
```

#### 2.5 Integration with Model trait

- [ ] Add `fn schema() -> ModelSchema` to `Model` trait (optional, with default panic)
- [ ] Update test models to implement manually
- [ ] Defer derive macro generation to Phase 2b or later

### Tests

```rust
#[test]
fn test_schema_registry() {
    let registry = SchemaRegistry::default();
    registry.register(album_schema());
    
    let schema = registry.get("Album").unwrap();
    assert_eq!(schema.fields.len(), 2);
    
    let field = registry.get_field("Album", "name").unwrap();
    assert!(matches!(field.kind, FieldKind::Scalar(ScalarType::String)));
}

#[test]
fn test_structured_navigation() {
    let node = StructuredNode::Struct {
        fields: btreemap! {
            "territory".to_string() => StructuredNode::Scalar(ScalarType::String),
            "expires".to_string() => StructuredNode::Scalar(ScalarType::String),
        },
    };
    
    let child = node.get_child(&StructuredKey::Field("territory".to_string()));
    assert!(matches!(child, Some(StructuredNode::Scalar(ScalarType::String))));
}

#[test]
fn test_structured_path() {
    let node = StructuredNode::Struct {
        fields: btreemap! {
            "licensing".to_string() => StructuredNode::Struct {
                fields: btreemap! {
                    "territory".to_string() => StructuredNode::Scalar(ScalarType::String),
                },
            },
        },
    };
    
    let path = vec![
        StructuredKey::Field("licensing".to_string()),
        StructuredKey::Field("territory".to_string()),
    ];
    let result = node.navigate(&path);
    assert!(matches!(result, Some(StructuredNode::Scalar(ScalarType::String))));
}
```

### Acceptance Criteria

1. Schema types compile and serialize correctly
2. Registry can store and retrieve model schemas
3. Structured property navigation works for nested paths
4. Test models have manual schemas defined

### Files Modified/Created

- `core/src/schema/mod.rs` (new)
- `core/src/schema/registry.rs` (new)
- `core/src/lib.rs` (add schema module)
- `tests/tests/common.rs` (add test schemas)

### Estimated Effort

2-3 days

### Notes

- **Defer derive macro work**: Auto-generating schemas from `#[derive(Model)]` is significant work. Start with manual schemas for testing.
- **PropertyId stability**: For now, manually assign PropertyIds. Later, derive from field order or hashing.

