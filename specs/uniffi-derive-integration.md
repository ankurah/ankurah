# UniFFI Derive Macro Integration Plan

## Goal

Update Ankurah's derive macros to generate **both** wasm-bindgen and UniFFI bindings from the same `#[derive(Model)]` attribute, with minimal code duplication. The bindings should be as API-compatible as possible between WASM and native (React Native) targets.

## Current State

The `ankurah-derive` crate currently generates:
- **Always**: `Model` impl, `View` struct/impl, `Mutable` struct/impl
- **With `wasm` feature**: WASM wrappers via `model/wasm.rs`

### Current WASM Wrappers Generated

For a model like `Message`, the `wasm` feature generates:

| Type | Purpose |
|------|---------|
| `MessageRef` | Wraps `Ref<Message>` - typed entity reference |
| `MessageResultSet` | Wraps `ResultSet<MessageView>` - query results |
| `MessageChangeSet` | Wraps `ChangeSet<MessageView>` - subscription changes |
| `MessageLiveQuery` | Wraps `LiveQuery<MessageView>` - reactive subscription |
| `NSMessage` | Namespace class with static methods (`get`, `fetch`, `query`, `create`) |
| TypeScript interface | Via vendored tsify - generates TS interface for the POJO |

## Attribute Comparison: wasm-bindgen vs UniFFI

| Concept | wasm-bindgen | UniFFI |
|---------|--------------|--------|
| Export struct | `#[wasm_bindgen]` | `#[derive(uniffi::Object)]` |
| Export impl | `#[wasm_bindgen]` | `#[uniffi::export]` |
| Getter | `#[wasm_bindgen(getter)]` | No direct equivalent (use method) |
| Async | Native support | `#[uniffi::export]` on async fn |
| Errors | `Result<T, JsValue>` | `Result<T, #[uniffi::Error]>` |
| Callbacks | `js_sys::Function` | `#[uniffi::export(callback_interface)]` trait |
| Custom TS | `#[wasm_bindgen(typescript_custom_section)]` | N/A (UniFFI generates its own) |
| Variadic args | `#[wasm_bindgen(variadic)]` | N/A |
| JS name override | `#[wasm_bindgen(js_name = "foo")]` | N/A |

## What Can Be Shared vs Must Diverge

### ✅ Can Share (identical logic)

1. **Wrapper struct definitions** - The newtype pattern is the same:
   ```rust
   pub struct MessageResultSet(ResultSet<MessageView>);
   ```

2. **Simple getters/methods** - Core logic is identical:
   ```rust
   pub fn items(&self) -> Vec<MessageView> {
       use ::ankurah::signals::Get;
       self.0.get()
   }
   ```

3. **`From` trait implementations** - Pure Rust, no FFI:
   ```rust
   impl From<Ref<Message>> for MessageRef { ... }
   ```

4. **Signal trait implementations** - Pure Rust

### ❌ Must Diverge

1. **TypeScript custom sections** - WASM only, UniFFI generates its own TS
2. **Callback types** - `js_sys::Function` vs UniFFI callback traits
3. **Error types** - `JsValue` vs UniFFI error enums
4. **Variadic parameters** - WASM-specific feature
5. **Attribute syntax** - Different macro attributes entirely

### ⚠️ Needs Adaptation

1. **Getters** - wasm-bindgen has `getter` attribute, UniFFI just uses methods
2. **Static methods** - wasm-bindgen uses `js_class`, UniFFI uses different pattern
3. **Async handling** - Both support async but error handling differs

## Proposed Implementation Strategy

### Option A: Unified `ffi.rs` with Conditional Compilation

Refactor `wasm.rs` into `ffi.rs` that generates for both targets:

```rust
enum FfiTarget { Wasm, UniFFI }

fn resultset_wrapper(name: &Ident, view: &Ident, target: FfiTarget) -> TokenStream {
    let (struct_attr, impl_attr) = match target {
        FfiTarget::Wasm => (quote!(#[wasm_bindgen]), quote!(#[wasm_bindgen])),
        FfiTarget::UniFFI => (quote!(#[derive(uniffi::Object)]), quote!(#[uniffi::export])),
    };
    
    quote! {
        #struct_attr
        pub struct #name(ResultSet<#view>);
        
        #impl_attr
        impl #name {
            pub fn items(&self) -> Vec<#view> { ... }
        }
    }
}
```

**Pros**: Single source of truth, easier to keep APIs in sync
**Cons**: Complex conditionals, harder to read

### Option B: Shared Helpers with Separate Modules

Keep `wasm.rs`, create `uniffi.rs`, but extract shared logic:

```rust
// model/shared.rs - struct bodies, method implementations
fn resultset_struct_body(view: &Ident) -> TokenStream { ... }
fn resultset_items_impl(view: &Ident) -> TokenStream { ... }

// model/wasm.rs - WASM attributes + shared bodies
fn wasm_resultset_wrapper(...) -> TokenStream {
    let body = shared::resultset_struct_body(view);
    let items = shared::resultset_items_impl(view);
    quote! {
        #[wasm_bindgen]
        #body
        #[wasm_bindgen]
        impl ... { #items }
    }
}

// model/uniffi.rs - UniFFI attributes + shared bodies
fn uniffi_resultset_wrapper(...) -> TokenStream {
    let body = shared::resultset_struct_body(view);
    let items = shared::resultset_items_impl(view);
    quote! {
        #[derive(uniffi::Object)]
        #body
        #[uniffi::export]
        impl ... { #items }
    }
}
```

**Pros**: Cleaner separation, easier to understand
**Cons**: Some duplication in attribute application

### Option C: Start Minimal, Expand Incrementally

Start with just the essential types for React Native:
1. `View` struct (already exists, just needs UniFFI attributes)
2. `Ref` wrapper
3. Basic CRUD operations

Skip for now:
- `LiveQuery` / `ResultSet` / `ChangeSet` (reactive patterns need callback design)
- TypeScript generation (UniFFI handles this)
- Variadic methods (not supported in UniFFI)

## Recommended Approach

**Option C first, then Option B** - Start minimal to validate the approach:

### Phase 1: Minimal View + Ref
1. Add `uniffi` feature to `ankurah-derive`
2. Add conditional UniFFI attributes to `View` struct in `view.rs`
3. Create simple `uniffi.rs` with just `Ref` wrapper
4. Test in `ankurah-react-native-template`

### Phase 2: CRUD Operations
1. Add `Context` and `Transaction` wrappers to UniFFI
2. Implement `get`, `fetch`, `create` methods
3. Design error handling pattern for UniFFI

### Phase 3: Reactive Patterns
1. Design callback interface for subscriptions
2. Implement `LiveQuery` wrapper with UniFFI callbacks
3. Port `ResultSet` and `ChangeSet`

### Phase 4: Refactor to Shared
1. Once patterns are validated, extract shared logic
2. Refactor `wasm.rs` and `uniffi.rs` to use shared helpers
3. Ensure API parity

## Open Questions

1. **Callback design**: How should `LiveQuery.subscribe()` work in UniFFI?
   - Option: Define `trait LiveQueryCallback { fn on_change(&self, changeset: ChangeSet); }`
   - JS implements this trait, Rust calls it

2. **Error handling**: Should we create a unified error enum?
   - `AnkurahError` that works for both WASM (`Into<JsValue>`) and UniFFI (`#[uniffi::Error]`)

3. **Async runtime**: UniFFI async requires tokio runtime
   - Already handled in `rn-bindings` with global `RUNTIME`
   - Should this be part of the generated code or left to the bindings crate?

4. **Feature interaction**: Can `wasm` and `uniffi` features be enabled simultaneously?
   - Probably not useful, but should we prevent it with `#[cfg(all(feature = "wasm", feature = "uniffi"))] compile_error!(...)`?

## Files to Modify

| File | Changes |
|------|---------|
| `derive/Cargo.toml` | Add `uniffi` feature ✅ |
| `derive/src/lib.rs` | Add conditional `uniffi_impl` generation |
| `derive/src/model/mod.rs` | Add `uniffi` module ✅ |
| `derive/src/model/view.rs` | Add conditional UniFFI attributes |
| `derive/src/model/mutable.rs` | Add conditional UniFFI attributes |
| `derive/src/model/uniffi.rs` | NEW - UniFFI-specific wrappers |
| `ankurah/Cargo.toml` | Add `uniffi` feature that enables derive/uniffi |
| `ankurah/src/lib.rs` | Re-export uniffi deps if feature enabled |

## Dependencies to Add

```toml
# In ankurah/Cargo.toml
[features]
uniffi = ["derive", "ankurah-derive?/uniffi", "dep:uniffi"]

[dependencies]
uniffi = { version = "0.29", optional = true }
```

## Next Steps

1. Review this plan
2. Start with Phase 1: Add UniFFI attributes to `View` struct
3. Create minimal `Ref` wrapper in `uniffi.rs`
4. Test with `ankurah-react-native-template` model
5. Iterate based on what works

## References

- [UniFFI Procedural Macros](https://mozilla.github.io/uniffi-rs/latest/proc_macro/index.html)
- [wasm-bindgen Guide](https://rustwasm.github.io/wasm-bindgen/)
- Current WASM impl: `ankurah/derive/src/model/wasm.rs`
- RN bindings test: `ankurah-react-native-template/rn-bindings/src/lib.rs`

