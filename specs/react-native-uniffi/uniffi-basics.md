# UniFFI Basics

Core rules for using UniFFI.

## Records vs Objects

UniFFI has two composite types with different tradeoffs:

| Type | Has Methods? | `Vec<T>` | `Option<T>` |
|------|--------------|----------|-------------|
| **Record** | ❌ | ✅ | ✅ |
| **Object** | ✅ | ❌ (use `Vec<Arc<T>>`) | ❌ (use `Option<Arc<T>>`) |

- **Records** (`#[derive(uniffi::Record)]`): Pure data, no methods, value semantics
- **Objects** (`#[derive(uniffi::Object)]`): Have methods via `#[uniffi::export] impl`, reference semantics

### Why Objects Need Arc in Containers

Objects are reference types. UniFFI implements `Lower<UT>` for `Arc<Object>`, not `Object` directly. So containers must hold `Arc<T>`:

```rust
#[derive(uniffi::Object)]
pub struct MessageView { ... }

// ❌ FAILS
pub fn get_messages() -> Vec<MessageView> { ... }

// ✅ WORKS  
pub fn get_messages() -> Vec<Arc<MessageView>> { ... }

// ❌ FAILS
pub fn maybe_message() -> Option<MessageView> { ... }

// ✅ WORKS
pub fn maybe_message() -> Option<Arc<MessageView>> { ... }
```

### For Ankurah Views

Views need methods (`id()`, field accessors, etc.) → must be Objects → use `Vec<Arc<View>>`.

## Object Arguments and Returns

| Pattern | Works? | Notes |
|---------|--------|-------|
| Return `T` | ✅ | Owned return |
| Return `Arc<T>` | ✅ | Also works |
| Arg `&T` | ✅ | Borrowed reference |
| Arg `T` (owned) | ⚠️ | Same-crate only |

## Cross-Crate Considerations

See `cross-crate-uniffi-learnings.md` for details on using types across crate boundaries.

