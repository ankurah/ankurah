## Phase 2c: Ref<T> WASM Wrapper Generation

**Status: Implemented** (PR #186, v0.7.9)

### Problem Statement

`wasm-bindgen` doesn't support generic types. Without special handling, `Ref<T>` fields become `any` in TypeScript:

```typescript
// Broken: no type safety
export class ConnectionView {
    readonly user: any;
    readonly session: any;
}
```

### Solution: Monomorphized `ModelRef` Wrappers

The Model macro generates a concrete `{Model}Ref` wrapper for each model (e.g., `SessionRef`, `UserRef`). These wrappers:

- Have no generics (wasm-bindgen compatible)
- Provide typed methods for WASM consumption
- Follow the existing companion type pattern (`SessionView`, `SessionMut`, `SessionRef`)

### Generated Types

For each Model, the macro generates:

| Type | Purpose |
|------|---------|
| `SessionView` | Read-only view |
| `SessionMut` | Mutable handle |
| `SessionRef` | **Typed reference wrapper** |
| `SessionResultSet` | Collection wrapper |
| `SessionLiveQuery` | Subscription wrapper |

### ModelRef API

```typescript
export class SessionRef {
    get(ctx: Context): Promise<SessionView>;  // Fetch the referenced entity
    readonly id: EntityId;                     // Raw EntityId
    static from(view: SessionView): SessionRef; // Create from View
}
```

The `id` getter provides access to the underlying `EntityId`. For string representation, use `ref.id.to_base64()`.

### View Getters

`Ref<T>` and `Option<Ref<T>>` fields return the typed wrapper:

```typescript
export class ConnectionView {
    readonly user: UserRef;                          // Ref<User>
    readonly session: SessionRef;                    // Ref<Session>
    readonly pending_mfa: MfaConfigRef | undefined;  // Option<Ref<MfaConfig>>
    
    r(): ConnectionRef;  // Reference to self
}
```

### Active Type Wrappers

LWW wrappers for `Ref<T>` fields return and accept typed wrappers:

```typescript
export class Connection_LWWRefUser {
    get(): UserRef;
    set(value: UserRef | UserView): void;  // Accepts either type
}
```

The `set()` method uses duck typing on the `.id` property, allowing both `UserRef` and `UserView` to be passed directly.

### Usage Patterns

```typescript
// Reading references
const userRef: UserRef = connection.user;
const user: UserView = await userRef.get(ctx);

// Creating references
const ref1 = connection.r();           // From View's r() method
const ref2 = UserRef.from(user);       // Static constructor

// Setting references (all equivalent)
mutable.assignee.set(user);            // Pass View directly
mutable.assignee.set(user.r());        // Pass Ref from View
mutable.assignee.set(UserRef.from(user)); // Explicit Ref construction
```

### View Implementation Structure

View structs have three `impl` blocks:

1. **Shared** (with `#[wasm_bindgen]`): `id`, `track`, `edit` - used by both Rust and WASM
2. **Rust-only** (no wasm_bindgen): `r()` and field getters returning native types
3. **WASM-only** (with `#[wasm_bindgen]`): Field getters returning WASM wrapper types

This separation ensures:
- Rust code gets native `Ref<T>` types
- WASM code gets `ModelRef` wrapper types
- No symbol collisions between the two

### Model Trait Integration

The `Model` trait has a `RefWrapper` associated type (WASM-only):

```rust
#[cfg(feature = "wasm")]
type RefWrapper: From<Ref<Self>> + Into<Ref<Self>>;
```

This allows generic code to reference the concrete wrapper type without knowing which Model is involved.

---

## Appendix: Why Monomorphization?

### The wasm-bindgen Limitation

wasm-bindgen uses `WasmDescribe::describe()` to emit type information at compile time. This is a static method with no access to generic parameters:

```rust
impl<T> WasmDescribe for Ref<T> {
    fn describe() {
        // Can't access T here - it's erased
        JsValue::describe()  // Falls back to `any`
    }
}
```

### Alternatives Considered

| Approach | Outcome |
|----------|---------|
| Describe as `String` | Loses methods, no `.get(ctx)` |
| Custom TypeScript section | Doesn't fix method signatures |
| Runtime type checking | No compile-time safety |
| Extend wasm-bindgen | Major upstream changes needed |

### Why Monomorphization Works

Concrete types like `SessionRef` have no generics, so wasm-bindgen can describe them fully:

```rust
#[wasm_bindgen]
pub struct SessionRef(Ref<Session>);

// wasm-bindgen can describe this as a class with known methods
```

This is the standard pattern for generic types in WASM FFI - you can't export `Vec<T>`, but you can export `VecString`, `VecI32`, etc.

### Files

- `derive/src/model/wasm.rs` - `ModelRef` wrapper generation
- `derive/src/model/view.rs` - Split Rust/WASM getters
- `derive/src/model/description.rs` - `ref_name()`, `view_wasm_getters()`
- `derive/src/model/backend.rs` - Active type wrapper `get()`/`set()` for Ref types
