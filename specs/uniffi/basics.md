# UniFFI Basics

Core rules for using UniFFI with Ankurah.

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

## Error Handling

Use `#[uniffi(flat_error)]` to serialize complex error enums via `ToString`:

```rust
#[derive(Debug, thiserror::Error)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Error))]
#[cfg_attr(feature = "uniffi", uniffi(flat_error))]
pub enum RetrievalError {
    #[error("Not found: {0}")]
    NotFound(String),
    // ...
}
```

## Async Functions

UniFFI async functions default to `async_compat`, which doesn't provide a full tokio runtime. Ankurah requires tokio, so use:

```rust
#[uniffi::export(async_runtime = "tokio")]
impl MessageOps {
    pub async fn query(&self, ctx: &Context, selection: String) -> Result<MessageLiveQuery, RetrievalError> {
        // ...
    }
}
```

And enable the tokio feature:

```toml
uniffi = { version = "0.29", features = ["tokio"] }
```

## Callback Interfaces

For reactive patterns, define a callback trait:

```rust
#[uniffi::export(callback_interface)]
pub trait StoreChangeCallback: Send + Sync {
    fn on_change(&self);
}

#[derive(uniffi::Object)]
pub struct ReactObserver { ... }

#[uniffi::export]
impl ReactObserver {
    pub fn subscribe(&self, callback: Box<dyn StoreChangeCallback>) { ... }
    pub fn unsubscribe(&self) { ... }
}
```

TypeScript usage:

```typescript
observer.subscribe({
    onChange: () => {
        // React re-render trigger
    }
});
```

## Cross-Crate Considerations

See [cross-crate.md](cross-crate.md) for details on using types across crate boundaries.

