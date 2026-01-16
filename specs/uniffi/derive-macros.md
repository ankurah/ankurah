# UniFFI Derive Macro Implementation

How Ankurah's `#[derive(Model)]` macro generates UniFFI bindings.

## Overview

The same `#[derive(Model)]` attribute generates both WASM and UniFFI bindings, controlled by feature flags. When both features are enabled, **wasm takes precedence** (this allows `cargo test --all-features` to work). For UniFFI bindings, enable only the `uniffi` feature without `wasm`.

## Generated Types

For a model like `Message`, the `uniffi` feature generates:

| Type | UniFFI Kind | Purpose |
|------|-------------|---------|
| `MessageView` | Object | Read-only entity view with field accessors |
| `MessageMutable` | Object | Editable handle with field setters |
| `MessageOps` | Object | Singleton with CRUD operations |
| `MessageInput` | Record | Data for creating new entities |
| `MessageRef` | Object | Typed wrapper around `Ref<Message>` |
| `MessageLiveQuery` | Object | Reactive query subscription |
| `MessageResultSet` | Object | Query results collection |
| `MessageChangeSet` | Object | Subscription change notifications |

## Module Organization

| File | Purpose |
|------|---------|
| `derive/src/model/view.rs` | View struct with conditional UniFFI attributes |
| `derive/src/model/mutable.rs` | Mutable struct with conditional UniFFI attributes |
| `derive/src/model/wasm.rs` | WASM-specific wrappers |
| `derive/src/model/uniffi.rs` | UniFFI-specific wrappers |
| `derive/src/model/backend.rs` | Active type wrapper generation (shared) |

## Key Design Decisions

### 1. Singleton Ops Pattern

UniFFI doesn't support static methods. Use a singleton:

```rust
#[derive(uniffi::Object)]
pub struct MessageOps;

#[uniffi::export]
impl MessageOps {
    #[uniffi::constructor]
    pub fn new() -> Self { Self }
    
    pub async fn get(&self, ctx: &Context, id: &EntityId) -> Result<MessageView, RetrievalError> { ... }
    pub async fn fetch(&self, ctx: &Context, selection: String, values: Vec<QueryValue>) -> Result<Vec<Arc<MessageView>>, RetrievalError> { ... }
    pub async fn query(&self, ctx: &Context, selection: String, values: Vec<QueryValue>) -> Result<MessageLiveQuery, RetrievalError> { ... }
    pub async fn create(&self, ctx: &Context, input: MessageInput) -> Result<MessageMutable, MutationError> { ... }
    pub async fn create_one(&self, ctx: &Context, input: MessageInput) -> Result<MessageView, MutationError> { ... }
}
```

TypeScript usage:

```typescript
const messageOps = new MessageOps();
const msg = await messageOps.get(ctx, id);
```

### 2. Method Renaming for Field Accessors

View field accessors use `#[uniffi::method(name = "...")]` to avoid conflicts with the underlying Rust methods:

```rust
#[uniffi::export]
impl MessageView {
    #[uniffi::method(name = "text")]
    pub fn __uniffi_text(&self) -> Result<String, PropertyError> {
        self.text()
    }
}
```

### 3. Input Records Use String for EntityId

`Ref<T>` fields in Input Records become `String` (base64 EntityId) because:
- UniFFI doesn't support generics
- EntityId is an Object, can't be in a Record

```rust
#[derive(uniffi::Record)]
pub struct MessageInput {
    pub room: String,  // Base64 EntityId, not Ref<Room>
    pub text: String,
}
```

### 4. Generated Code Must Not Reference Consuming Crate Features

**Critical**: The derive macro generates code based on its own features at macro expansion time. Generated code must NOT contain `#[cfg(feature = "uniffi")]` because the consuming crate may not have that feature.

```rust
// ❌ WRONG - generates #[cfg] in output
quote! {
    #[cfg(feature = "uniffi")]
    #[derive(uniffi::Object)]
    pub struct FooView { ... }
}

// ✅ CORRECT - conditionally generate at macro expansion time
#[cfg(feature = "uniffi")]
fn uniffi_wrapper() -> TokenStream {
    quote! {
        #[derive(uniffi::Object)]
        pub struct FooView { ... }
    }
}
#[cfg(not(feature = "uniffi"))]
fn uniffi_wrapper() -> TokenStream { quote! {} }
```

### 5. Async Runtime Specification

All async methods need `async_runtime = "tokio"`:

```rust
#[uniffi::export(async_runtime = "tokio")]
impl MessageOps {
    pub async fn query(&self, ...) -> Result<...> { ... }
}
```

## Attribute Comparison: wasm-bindgen vs UniFFI

| Concept | wasm-bindgen | UniFFI |
|---------|--------------|--------|
| Export struct | `#[wasm_bindgen]` | `#[derive(uniffi::Object)]` |
| Export impl | `#[wasm_bindgen]` | `#[uniffi::export]` |
| Constructor | `#[wasm_bindgen(constructor)]` | `#[uniffi::constructor]` |
| Getter | `#[wasm_bindgen(getter)]` | N/A (use method) |
| Async | Native support | `#[uniffi::export(async_runtime = "tokio")]` |
| Errors | `Result<T, JsValue>` | `Result<T, #[uniffi::Error]>` |
| Callbacks | `js_sys::Function` | `#[uniffi::export(callback_interface)]` trait |
| Method rename | `#[wasm_bindgen(js_name = "foo")]` | `#[uniffi::method(name = "foo")]` |
| Static methods | `#[wasm_bindgen(js_class = "Foo")]` | Not supported (use singleton) |
| Variadic args | `#[wasm_bindgen(variadic)]` | Not supported (use arrays) |

## Active Type Wrappers

Generic types like `LWW<T>` need monomorphized wrappers because UniFFI doesn't support generics:

```rust
// Generated for each concrete type
#[derive(uniffi::Object)]
pub struct LWWString(LWW<String>);

#[uniffi::export]
impl LWWString {
    pub fn get(&self) -> Result<String, PropertyError> { ... }
    pub fn set(&self, value: String) -> Result<(), PropertyError> { ... }
}
```

The `backend.rs` file handles this generation, with `uniffi: bool` on each method to control which methods are exposed to UniFFI (some methods like `get_value` returning `serde_json::Value` are UniFFI-incompatible).

## Custom Types

For types like `Json` that wrap UniFFI-incompatible types, use `uniffi::custom_type!`:

```rust
#[cfg(feature = "uniffi")]
::uniffi::custom_type!(Json, String, {
    lower: |obj| serde_json::to_string(&obj.0).expect("Failed to serialize JSON"),
    try_lift: |val| serde_json::from_str(&val).map(Json).map_err(Into::into),
});
```

This converts `Json` to/from `String` at the FFI boundary.

## Dependencies

```toml
# In ankurah/Cargo.toml
[features]
uniffi = [
    "derive",
    "ankurah-derive?/uniffi",
    "ankurah-core/uniffi",
    "ankurah-proto/uniffi",
    "ankurah-signals/uniffi",
    "dep:uniffi",
]
react-native = ["uniffi", "ankurah-signals/react-native"]

[dependencies]
uniffi = { version = "0.29", optional = true }
```

```toml
# In user's model crate
[features]
uniffi = ["ankurah/uniffi", "dep:uniffi"]
react-native = ["ankurah/react-native"]

[dependencies]
uniffi = { version = "0.29", optional = true, features = ["tokio"] }
```

## References

- [UniFFI Procedural Macros](https://mozilla.github.io/uniffi-rs/latest/proc_macro/index.html)
- [UniFFI Supported Types](https://mozilla.github.io/uniffi-rs/latest/types/builtin_types.html)


