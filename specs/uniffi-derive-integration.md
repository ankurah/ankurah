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

---

## Understanding the Binding Generation Pipeline

### WASM Pipeline
```
Rust code + #[wasm_bindgen] → wasm-bindgen → .wasm + .js + .d.ts
```
- wasm-bindgen generates TypeScript directly from Rust annotations
- Custom TypeScript sections (`#[wasm_bindgen(typescript_custom_section)]`) override auto-generated types
- The "apology layer" exists because wasm-bindgen's auto-generated TS doesn't always match what we want

### UniFFI Pipeline  
```
Rust code + #[uniffi::export] → uniffi-bindgen → Swift/Kotlin/Python bindings
                              → uniffi-bindgen-react-native → C++ JSI + TypeScript
```
- UniFFI generates **native bindings first** (Swift/Kotlin), then TS is derived from those
- `uniffi-bindgen-react-native` creates JSI C++ that bridges to JS, plus TypeScript types
- No custom TypeScript sections - TS is derived from the Rust type signatures

### Key Insight
Both systems auto-generate TypeScript, but:
- **wasm-bindgen**: Direct Rust→TS, we use custom sections to fix limitations
- **UniFFI**: Rust→Native→TS, the TS is more faithful to Rust types but goes through an extra layer

---

## Concrete UniFFI Capabilities (from our PoC)

Based on `ankurah-react-native-template/react-app/src/generated/ankurah_rn_bindings.ts`:

### ✅ What Works Well

| Feature | Rust | Generated TypeScript |
|---------|------|---------------------|
| Free functions | `#[uniffi::export] fn greet(name: String) -> String` | `export function greet(name: string): string` |
| Async functions | `#[uniffi::export] async fn greet_async(...)` | `export async function greetAsync(...): Promise<string>` |
| Objects | `#[derive(uniffi::Object)] struct Counter` | `export class Counter extends UniffiAbstractObject` |
| Object methods | `#[uniffi::export] impl Counter { fn get(&self) -> u32 }` | `public get(): number` |
| Constructors | `#[uniffi::constructor] fn new() -> Self` | `constructor()` |
| Callback interfaces | `#[uniffi::export(callback_interface)] trait LogCallback` | `export interface LogCallback { onLog(...): void }` |
| Error enums | `#[derive(uniffi::Error, thiserror::Error)]` | `export enum AnkurahError_Tags { ... }` + classes |
| Option types | `Option<String>` | `string \| null` |
| Vec types | `Vec<T>` | `Array<T>` |
| Result types | `Result<T, E>` | Throws on error, returns T on success |

### ⚠️ Limitations / Differences

| Aspect | wasm-bindgen | UniFFI | Impact |
|--------|--------------|--------|--------|
| Generics | Not supported | Not supported | Both need monomorphized wrappers ✓ |
| Getters | `#[wasm_bindgen(getter)]` makes `obj.foo` | Just methods `obj.foo()` | Minor API difference |
| Variadic args | `#[wasm_bindgen(variadic)]` | Not supported | Must use arrays instead |
| Static methods | `#[wasm_bindgen(js_class = "Foo")]` on separate struct | Not supported - use singleton | Different patterns |
| JS name override | `#[wasm_bindgen(js_name = "bar")]` | Not available | Must match Rust names |
| Custom TS | `typescript_custom_section` | Not available | Can't override generated TS |
| Callback args | `js_sys::Function` (untyped) | Trait interface (typed) | UniFFI is actually better here |

### 🔍 Things to Validate

1. **Nested objects**: Can `MessageView` contain `UserRef`? (Probably yes, but verify)
2. **Object in Vec**: `Vec<MessageView>` in return types (PoC shows this works)
3. **Callback with complex args**: `fn on_change(&self, changeset: ChangeSet)` 
4. **Object lifetimes**: UniFFI objects are Arc-wrapped - how does this interact with Ankurah's Entity lifetimes?

---

## Attribute Comparison

| Concept | wasm-bindgen | UniFFI |
|---------|--------------|--------|
| Export struct | `#[wasm_bindgen]` | `#[derive(uniffi::Object)]` |
| Export impl | `#[wasm_bindgen]` | `#[uniffi::export]` |
| Constructor | `#[wasm_bindgen(constructor)]` | `#[uniffi::constructor]` |
| Getter | `#[wasm_bindgen(getter)]` | N/A (use method) |
| Async | Native support | `#[uniffi::export]` on async fn |
| Errors | `Result<T, JsValue>` | `Result<T, #[uniffi::Error]>` |
| Callbacks | `js_sys::Function` | `#[uniffi::export(callback_interface)]` trait |
| Custom TS | `#[wasm_bindgen(typescript_custom_section)]` | N/A |
| Variadic | `#[wasm_bindgen(variadic)]` | N/A |
| JS name | `#[wasm_bindgen(js_name = "foo")]` | N/A |

---

## Decisions Made

### 1. Static Methods → Singleton Ops Object ✅

UniFFI doesn't support static methods. Instead of free functions, we use a **singleton pattern**:

```rust
// WASM (current)
#[wasm_bindgen(js_name = Message)]
pub struct NSMessage {}

#[wasm_bindgen(js_class = Message)]
impl NSMessage {
    pub async fn get(ctx: &Context, id: EntityId) -> Result<MessageView, JsValue> { ... }
}

// UniFFI (new)
#[derive(uniffi::Object)]
pub struct MessageOps;

#[uniffi::export]
impl MessageOps {
    #[uniffi::constructor]
    pub fn new() -> Self { Self }
    
    pub async fn get(&self, ctx: &Context, id: EntityId) -> Result<MessageView, AnkurahError> { ... }
    pub async fn fetch(&self, ctx: &Context, selection: String) -> Result<Vec<MessageView>, AnkurahError> { ... }
    pub fn query(&self, ctx: &Context, selection: String) -> Result<MessageLiveQuery, AnkurahError> { ... }
    pub async fn create(&self, trx: &Transaction, data: Message) -> Result<MessageView, AnkurahError> { ... }
}
```

```typescript
// TypeScript usage
const messageOps = new MessageOps();  // Create once at app init
const msg = await messageOps.get(ctx, id);
const msgs = await messageOps.fetch(ctx, "room = 'general'");
```

### 2. Callback Interfaces → Per-Model Traits ✅

Each model gets its own typed callback trait for full type safety:

```rust
#[uniffi::export(callback_interface)]
pub trait MessageLiveQueryCallback: Send + Sync {
    fn on_change(&self, changeset: MessageChangeSet);
}

#[uniffi::export]
impl MessageLiveQuery {
    pub fn subscribe(&self, callback: Box<dyn MessageLiveQueryCallback>) -> SubscriptionGuard { ... }
}
```

```typescript
// TypeScript usage - fully typed
interface MessageLiveQueryCallback {
    onChange(changeset: MessageChangeSet): void;
}

query.subscribe({
    onChange: (changeset) => {
        console.log(changeset.added);  // MessageView[] - properly typed
    }
});
```

### 3. Error Handling → Minimal Changes, Conditional Derives ✅

No changes to existing error types. Add `#[cfg_attr(feature = "uniffi", derive(uniffi::Error))]` to errors we want to expose:

```rust
#[derive(Debug, thiserror::Error)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Error))]
pub enum AccessDenied {
    #[error("Access denied: {reason}")]
    Denied { reason: String },
}
```

WASM continues to convert errors to `JsValue` via `.to_string()`. UniFFI gets typed error enums.

### 4. Feature Exclusivity → Mutually Exclusive ✅

Bindings features are mutually exclusive to avoid namespace conflicts and confusion:

```rust
#[cfg(all(feature = "wasm", feature = "uniffi"))]
compile_error!("Only one bindings feature can be enabled at a time. Choose 'wasm' OR 'uniffi'.");
```

### 5. Async Runtime → Left to Bindings Crate ✅

UniFFI async functions need a tokio runtime. This is handled in the bindings crate (e.g., `rn-bindings`) with a global `RUNTIME`, not in generated code.

---

## Implementation Strategy

### Module Organization

| File | Current | Proposed |
|------|---------|----------|
| `derive/src/model/view.rs` | WASM-conditional attributes | Add UniFFI-conditional attributes |
| `derive/src/model/mutable.rs` | WASM-conditional attributes | Add UniFFI-conditional attributes |
| `derive/src/model/wasm.rs` | WASM wrappers | Keep as-is |
| `derive/src/model/uniffi.rs` | N/A | **NEW** - UniFFI wrappers |
| `derive/src/lib.rs` | Calls `wasm_impl()` | Add call to `uniffi_impl()` |

### What Goes in Each Module

**`view.rs` / `mutable.rs`** (shared structs, conditional attributes):
- `FooView` struct definition
- `FooMutable` struct definition  
- Core trait implementations (`View`, `Mutable`, `Signal`)
- Field accessor methods

**`wasm.rs`** (WASM-only, keep existing):
- `FooResultSet`, `FooChangeSet`, `FooLiveQuery` wrappers
- `FooRef` wrapper
- `NSFoo` namespace class with static methods
- TypeScript custom sections
- `js_sys::Function` callback handling

**`uniffi.rs`** (NEW, parallel structure):
- `FooResultSet`, `FooChangeSet`, `FooLiveQuery` wrappers
- `FooRef` wrapper
- `FooOps` singleton with CRUD methods
- `FooLiveQueryCallback` trait definition

---

## Implementation Phases

### Phase 3a: Scaffolding
- [x] Add `uniffi` feature to `derive/Cargo.toml`
- [ ] Add `uniffi` feature to `ankurah/Cargo.toml`
- [ ] Add mutual exclusivity `compile_error!`
- [ ] Create empty `derive/src/model/uniffi.rs`
- [ ] Wire up `uniffi_impl()` call in `lib.rs`

### Phase 3b: View + Mutable with UniFFI
- [ ] Update `view.rs` to add `#[derive(uniffi::Object)]` when uniffi feature enabled
- [ ] Update `mutable.rs` similarly
- [ ] Test: Can we export `MessageView` via UniFFI?

### Phase 3c: Ref Wrapper
- [ ] Create `uniffi_ref_wrapper()` in `uniffi.rs`
- [ ] Test: Can we pass `MessageRef` across FFI?

### Phase 3d: CRUD Operations (Ops Singleton)
- [ ] Create `uniffi_ops_wrapper()` for `FooOps` singleton
- [ ] Implement `get`, `fetch`, `create` methods
- [ ] Add conditional `uniffi::Error` derives to relevant error types

### Phase 3e: Reactive Patterns
- [ ] Create `uniffi_livequery_callback_trait()` for per-model callback traits
- [ ] Implement `LiveQuery`, `ResultSet`, `ChangeSet` wrappers
- [ ] Test subscription lifecycle

### Phase 3f: Refactor for Sharing
- [ ] Extract shared method bodies to helper functions
- [ ] Reduce duplication between `wasm.rs` and `uniffi.rs`

---

## Dependencies

```toml
# In ankurah/Cargo.toml
[features]
uniffi = ["derive", "ankurah-derive?/uniffi", "dep:uniffi"]

[dependencies]
uniffi = { version = "0.29", optional = true }
```

```toml
# In ankurah-derive/Cargo.toml  
[features]
uniffi = []  # Already added
```

---

## Testing Strategy

1. **Unit tests in derive crate**: Verify generated TokenStream is valid
2. **Integration test in ankurah-react-native-template**:
   - Enable `uniffi` feature on model crate
   - Export models via `rn-bindings`
   - Verify TypeScript types are generated correctly
   - Test CRUD operations from React Native

---

## References

- [UniFFI Procedural Macros](https://mozilla.github.io/uniffi-rs/latest/proc_macro/index.html)
- [UniFFI Supported Types](https://mozilla.github.io/uniffi-rs/latest/types/builtin_types.html)
- [wasm-bindgen Guide](https://rustwasm.github.io/wasm-bindgen/)
- [uniffi-bindgen-react-native](https://github.com/jhugman/uniffi-bindgen-react-native)
- Current WASM impl: `ankurah/derive/src/model/wasm.rs`
- RN bindings PoC: `ankurah-react-native-template/rn-bindings/src/lib.rs`
- Generated TS example: `ankurah-react-native-template/react-app/src/generated/ankurah_rn_bindings.ts`
