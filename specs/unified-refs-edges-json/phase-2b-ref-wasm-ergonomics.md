## Phase 2b: Ref<T> WASM Ergonomics

### Goal

Improve the developer experience for `Ref<T>` in TypeScript/WASM bindings:
1. Typed ref classes (e.g., `UserRef`) with `.get(ctx)` method
2. Flexible setters accepting multiple input types
3. Proper TypeScript types instead of `any`

### Current Limitations

**Problem 1: `Ref<T>` becomes `any` in TypeScript**

The `LWW<Ref<T>>` wrapper's `get()` and `set()` methods show as:
```typescript
export class Session_LWWRefUser {
  get(): any;
  set(value: any): void;
}
```

This is because `Ref<T>::WasmDescribe` delegates to `JsValue::describe()`.

**Problem 2: No `.get(ctx)` on returned refs**

When you call `mutable.user.get()`, you get a string (base64 EntityId), not an object with methods. To fetch the referenced entity, you must:
```typescript
const userId = mutable.user.get();  // string
const user = await User.get(ctx, userId);  // manual lookup
```

Instead of the ergonomic:
```typescript
const user = await mutable.user.get().get(ctx);  // doesn't work today
```

**Problem 3: Setters only accept strings**

Currently `set()` requires a base64 string:
```typescript
mutable.assignee.set(user.id.to_base64());  // verbose
```

Should also accept:
```typescript
mutable.assignee.set(user);       // View
mutable.assignee.set(user.id);    // EntityId
mutable.assignee.set(userRef);    // UserRef
```

### Proposed Solution

#### 1. Generate typed Ref wrapper classes per Model

The derive macro generates `{Model}Ref` alongside `{Model}View`:

```rust
#[wasm_bindgen]
pub struct UserRef(Ref<User>);

#[wasm_bindgen]
impl UserRef {
    #[wasm_bindgen]
    pub async fn get(&self, ctx: &Context) -> Result<UserView, JsValue> {
        self.0.get(ctx).await.map_err(|e| JsValue::from_str(&e.to_string()))
    }
    
    #[wasm_bindgen]
    pub fn to_base64(&self) -> String {
        self.0.id().to_base64()
    }
    
    #[wasm_bindgen(js_name = toString)]
    pub fn to_string(&self) -> String {
        self.0.id().to_base64()
    }
}
```

TypeScript result:
```typescript
export class UserRef {
  get(ctx: Context): Promise<UserView>;
  to_base64(): string;
  toString(): string;
}
```

#### 2. Update LWW wrapper to use typed Ref class

The `LWW<Ref<User>>` wrapper's `get()` returns `UserRef` instead of string:

```rust
// In Session_LWWRefUser
pub fn get(&self) -> Result<UserRef, JsValue> {
    self.0.get().map(UserRef)
}
```

#### 3. Flexible FromWasmAbi for Ref<T>

Update `Ref<T>::from_abi` to accept multiple JS types:

```rust
unsafe fn from_abi(js: Self::Abi) -> Self {
    let js_value = JsValue::from_abi(js);
    
    // String (base64 EntityId)
    if let Some(id_str) = js_value.as_string() {
        if let Ok(id) = EntityId::from_base64(&id_str) {
            return Ref::new(id);
        }
    }
    
    // Object with .id property (View or Ref wrapper)
    if let Ok(id_prop) = js_sys::Reflect::get(&js_value, &JsValue::from_str("id")) {
        // EntityId object
        if let Ok(entity_id) = id_prop.dyn_into::<EntityId>() {
            return Ref::new(entity_id);
        }
        // Nested - id might have to_base64
        if let Ok(to_base64) = js_sys::Reflect::get(&id_prop, &JsValue::from_str("to_base64")) {
            if to_base64.is_function() {
                if let Ok(result) = js_sys::Reflect::apply(
                    to_base64.unchecked_ref(),
                    &id_prop,
                    &js_sys::Array::new()
                ) {
                    if let Some(s) = result.as_string() {
                        if let Ok(id) = EntityId::from_base64(&s) {
                            return Ref::new(id);
                        }
                    }
                }
            }
        }
    }
    
    // EntityId object directly
    if let Ok(entity_id) = js_value.dyn_into::<EntityId>() {
        return Ref::new(entity_id);
    }
    
    // Fallback - panic with helpful message
    panic!("Expected string, EntityId, View, or Ref wrapper");
}
```

#### 4. TypeScript type via custom section

Use `typescript_custom_section` to define proper union types:

```rust
#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND: &'static str = r#"
export type RefInput<T> = string | EntityId | T | { id: EntityId };
"#;
```

And for each wrapper, override the set signature:
```typescript
// Generated for Session_LWWRefUser
interface Session_LWWRefUser {
  set(value: string | EntityId | UserView | UserRef): void;
}
```

### Tasks

- [ ] Add `{Model}Ref` generation to derive macro
- [ ] Update `LWW<Ref<T>>` wrapper `get()` to return typed Ref
- [ ] Update `Ref<T>::from_abi` to accept View/EntityId/string
- [ ] Add TypeScript custom sections for proper typing
- [ ] Update `Ref<T>::describe` to use string (interim improvement)
- [ ] Handle `Option<Ref<T>>` similarly

### Current Workaround

Until this phase is implemented:

```typescript
// Setting a ref field
mutable.user.set(userView.id.to_base64());

// Getting and traversing a ref
const userId = session.user;  // string (base64)
const user = await User.get(ctx, EntityId.from_base64(userId));
```

### Acceptance Criteria

1. `UserRef`, `SessionRef`, etc. classes generated with `.get(ctx)` method
2. `mutable.user.get()` returns `UserRef` with proper TS type
3. `mutable.user.set(view)` works (accepts View, EntityId, string, or Ref)
4. TypeScript shows proper types, not `any`

### Files Modified/Created

- `derive/src/model/mod.rs` - generate `{Model}Ref` wrapper
- `derive/src/model/wasm.rs` - update LWW wrapper generation
- `core/src/property/value/entity_ref.rs` - flexible `from_abi`
- Model crates - regenerate with new derive

### Estimated Effort

2-3 days

### Dependencies

- Phase 2a complete (basic `Ref<T>` type exists)
