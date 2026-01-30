# Property Registration Implementation Plan

## Entity Structure

### Model Entity

```
Model {
    name: String,           // "User", "Album", etc.
}
```

Note: Collection mapping is currently 1:1 with model name (lowercased). Future intent is unified collection with materialized views/indexes for queries, but for now we keep the existing derivation.

### Property Entity

```
Property {
    name: String,           // "title", "email", etc.
    model: Ref<Model>,      // Reference to parent Model entity
    backend: BackendKind,   // Conflict resolution strategy
    value_type: ValueType,  // Encoding format (for lww)
    optional: bool,         // Whether None/missing is valid
    // Future: additional flags TBD
}
```

### BackendKind (language-agnostic)

```
enum BackendKind {
    lww,        // Last-write-wins scalar
    yrs_text,   // Collaborative text (Yrs Text)
    yrs_map,    // Collaborative map (future)
    yrs_array,  // Collaborative array (future)
}
```

### ValueType (language-agnostic primitives)

For `lww` backend, determines serialization format:

```
enum ValueType {
    string,
    int32,
    int64,
    float32,
    float64,
    bool,
    bytes,
    json,       // Arbitrary structured data
    ref,        // Entity reference (special case)
}
```

For `yrs_*` backends, the value_type may be implied or have different semantics.

### Rust Type Mappings

| Rust Type | backend | value_type | optional |
|-----------|---------|------------|----------|
| `String` (via YrsString) | `yrs_text` | `string` | false |
| `Option<String>` (via YrsString) | `yrs_text` | `string` | true |
| `LWW<String>` | `lww` | `string` | false |
| `LWW<i32>` | `lww` | `int32` | false |
| `LWW<i64>` | `lww` | `int64` | false |
| `LWW<f32>` | `lww` | `float32` | false |
| `LWW<f64>` | `lww` | `float64` | false |
| `LWW<bool>` | `lww` | `bool` | false |
| `LWW<Json>` | `lww` | `json` | false |
| `Ref<T>` | `lww` | `ref` | false |
| `Option<Ref<T>>` | `lww` | `ref` | true |

## Backend Changes

### Property ID Keying

Backends will store/retrieve using property entity IDs instead of string names:

```rust
// Current
backend.insert("title", value)
backend.get_string("title")

// New  
backend.insert(property_id, value)  // property_id: EntityId
backend.get_string(property_id)
```

### Internal ID Mapping

Backends may use an internal map table to compact entity IDs to smaller integers for storage efficiency (similar to sled's approach). This is an internal optimization detail.

## LWW Native Type Registration

LWW value types correspond to native type registration done with the backend. The backend maintains a registry of supported value types and their serialization logic.

## Ref<T> Special Case

When a property has `value_type: ref`, the Property entity needs an additional field:

```
target_model: Ref<Model>  // Which model this ref points to
```

**Resolution**: Use two-phase registration. Register all Model entities first, then all Property entities can safely reference them.

## System Entity Structure

Model and Property are **system entities** stored as `sys::Item` variants in `_ankurah_system`. They do not use the derive macro - their data contract is implicit and inferred from internal data.

Entity IDs are looked up at runtime by name (not hardcoded). Future: model structs may be annotated with explicit entity IDs for cases requiring determinism across nodes.

## Registration Flow

1. **Triggered on first `trx.create::<Model>()`** - Registration runs when first entity of a model is created
2. **Two-phase registration**:
   - Phase 1: Ensure Model entity exists (upsert by name)
   - Phase 2: Ensure all Property entities exist (upsert by model + name)
3. **Runtime lookup** - Property entity IDs looked up by (model_name, property_name)
4. **Cache entity IDs** - Store property entity IDs for runtime efficiency

## Read Path (with empty string fix)

1. Look up Property entity for `(model, property_name)`
2. If property not registered → `PropertyError::UnknownProperty`
3. Get value from backend using property entity ID
4. If backend returns `None`:
   - If `optional: true` → return `None`
   - If `optional: false` → return type's default (empty string, 0, etc.)
5. If backend returns `Some(value)` → return value

## sys::Item Expansion

```rust
pub enum Item {
    SysRoot,
    Collection { name: String },
    Model { name: String },
    Property {
        name: String,
        model: EntityId,        // Ref to Model entity
        backend: String,        // "lww", "yrs_text", etc.
        value_type: String,     // "string", "int32", etc.
        optional: bool,
        target_model: Option<EntityId>,  // For ref types
    },
    #[serde(other)]
    Other,
}
```

## Resolved Questions

1. **Ref bootstrapping** - Two-phase registration: all Models first, then Properties
2. **Registration trigger** - On first `trx.create::<Model>()` for a given model
3. **Entity ID stability** - Runtime lookup by name for now; future annotation for determinism
4. **System entity structure** - Implicit data contract via sys::Item variants (no derive macro needed)

## Open Questions

1. **Migration path** - How to upgrade existing data without property registration?
2. **Validation timing** - When to validate property access against registration?
3. **Cache invalidation** - How to handle schema changes at runtime?
