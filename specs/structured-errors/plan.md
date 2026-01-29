# Structured Errors - Implementation Plan

## Key Design Decisions

### 1. InternalError is a Marker Struct

`InternalError` is a **fieldless marker struct**, not an enum. The actual error chain is preserved in the `Report` via error-stack:

```rust
#[derive(Debug, Error)]
#[error("internal error")]
pub struct InternalError;

// Failure variant holds the full error chain
Failure(Report<InternalError>)
```

### 2. Internal Code Uses `Report<T>`, Never Public Types

**CRITICAL**: Internal code must NEVER use `RetrievalError`, `MutationError`, or `PropertyError` (the public types). These exist only for the public API boundary.

Internal code uses:
- `Report<StorageError>` - for storage operations
- `Report<LineageError>` - for lineage comparison
- `Report<WithStateError>` - for `EntitySet::with_state`
- `Report<StateError>` - for state serialization
- `ApplyError` / `ApplyErrorCause` - for applying remote updates

### 3. Context Attachment with `#[track_caller]`

Use `.attach("description")` to add context - error-stack captures file:line automatically:

```rust
fn some_operation() -> Result<T, Report<SomeError>> {
    inner_operation()
        .attach("performing some_operation")?  // captures file:line
}
```

Do NOT create custom context structs unless you need to carry data (like EntityId). Simple strings are sufficient.

### 4. Each Operation Has Its Own Error Type

Don't return `Report<LineageError>` from `with_state` - it's not a lineage operation. Create `WithStateError` that can contain lineage errors among others:

```rust
pub enum WithStateError {
    Storage(StorageError),
    Lineage(Report<LineageError>),
}
```

### 5. Conversion at API Boundaries Only

Internal code propagates internal errors. Conversion to public errors happens ONLY at public method boundaries:

```rust
// GOOD: Public method on Context
pub async fn get<R: View>(&self, id: EntityId) -> Result<R, RetrievalError> {
    self.internal_get(id).await?  // ? converts Report<StorageError> -> RetrievalError
}

// BAD: Internal method returning public type
fn internal_helper() -> Result<T, RetrievalError> { /* WRONG */ }

// GOOD: Internal method returning internal type
fn internal_helper() -> Result<T, Report<StorageError>> { /* CORRECT */ }
```

### 6. Generic `From` Implementations for Clean Boundaries

Use generic `From<Report<E>>` implementations to allow `?` at boundaries:

```rust
impl<E: std::error::Error + Send + Sync + 'static> From<Report<E>> for RetrievalError {
    fn from(r: Report<E>) -> Self {
        RetrievalError::Failure(r.change_context(InternalError))
    }
}
```

### 7. Soft Error Extraction at Boundaries

When converting, extract soft errors (NotFound, AccessDenied) as matchable variants:

```rust
impl From<StorageError> for RetrievalError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::EntityNotFound(id) => RetrievalError::NotFound(NotFound::Entity(id)),
            StorageError::CollectionNotFound(id) => RetrievalError::NotFound(NotFound::Collection(id)),
            other => RetrievalError::Failure(Report::new(other).change_context(InternalError)),
        }
    }
}
```

## Key Files

| File | Purpose |
|------|---------|
| `core/src/error/mod.rs` | Public error types |
| `core/src/error/internal.rs` | Internal error types (StorageError, LineageError, WithStateError, etc.) |
| `core/src/storage.rs` | Storage traits (return StorageError) |
| `core/src/context.rs` | Public API boundary - converts internal → public |
| `core/src/lineage.rs` | Internal - returns `Report<LineageError>` |
| `core/src/entity.rs` | Internal - returns `Report<WithStateError>`, `Report<LineageError>` |

## Anti-Patterns to Fix

### Current Problems

1. **`WithStateError::Retrieval(RetrievalError)`** - wraps PUBLIC type in INTERNAL enum
2. **`From<RetrievalError> for StorageError`** - converts PUBLIC to INTERNAL (backwards!)
3. **`PropertyBackend::from_state_buffer() -> RetrievalError`** - internal trait returns public type
4. **`backend_from_string() -> RetrievalError`** - internal function returns public type
5. **`property::PropertyError` wraps `RetrievalError`** - needs splitting into internal/public

### Target State

- Internal traits/functions return `Result<T, Report<InternalType>>` or `Result<T, StorageError>`
- Public methods return `Result<T, RetrievalError>` (etc.)
- No internal code imports or references `RetrievalError`, `MutationError`, or `PropertyError`

## Completed

- [x] error-stack dependency
- [x] Public error types with `Failure(Report<InternalError>)` variants
- [x] Internal error types in `error/internal.rs`
- [x] `StorageEngine` / `StorageCollection` traits use `StorageError`
- [x] SQLite, Sled, Postgres, IndexedDB-wasm storage migrated
- [x] storage-common traits migrated
- [x] Lineage functions return `Report<LineageError>`
- [x] `Entity::apply_state` returns `Report<LineageError>`
- [x] Generic `From<Report<E>>` for public error types

## Remaining

See [tasks.md](./tasks.md) for detailed task list.
