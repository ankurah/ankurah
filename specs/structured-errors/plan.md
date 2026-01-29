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

### 2. Storage Layer Uses StorageError

All storage traits return `StorageError` (internal type), not public errors. `StorageError` is `pub` (not `pub(crate)`) because storage implementation crates need it.

```rust
pub trait StorageEngine: Send + Sync {
    async fn collection(&self, id: &CollectionId) -> Result<..., StorageError>;
}

pub trait StorageCollection: Send + Sync {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, StorageError>;
    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, StorageError>;
}
```

### 3. Conversion at API Boundaries

Internal code uses internal error types exclusively. Conversion to public errors happens at API boundaries:

```rust
fn storage_to_retrieval(e: StorageError) -> RetrievalError {
    match e {
        StorageError::EntityNotFound(id) => RetrievalError::NotFound(NotFound::Entity(id)),
        StorageError::CollectionNotFound(id) => RetrievalError::NotFound(NotFound::Collection(id)),
        other => RetrievalError::Failure(Report::new(other).change_context(InternalError)),
    }
}
```

### 4. No Cross-Wrapping

Error types don't contain each other. Each type is for a specific concern.

## Key Files

| File | Purpose |
|------|---------|
| `core/src/error/mod.rs` | Public error types |
| `core/src/error/internal.rs` | Internal error types (StorageError, StateError, etc.) |
| `core/src/storage.rs` | Storage traits (return StorageError) |
| `core/src/context.rs` | API boundary conversions |

## Completed

- [x] error-stack dependency
- [x] Public error types with `Failure(Report<InternalError>)` variants
- [x] Internal error types in `error/internal.rs`
- [x] `StorageEngine` / `StorageCollection` traits use `StorageError`
- [x] SQLite storage migrated
- [x] Sled storage migrated (engine, streams, index)
- [x] Postgres storage migrated
- [x] IndexedDB-wasm storage migrated
- [x] storage-common traits migrated
- [x] All tests pass

## Future: Context Attachments

Enrich error reports with context structs for better diagnostics:

```rust
struct EntityContext {
    entity_id: EntityId,
    collection: CollectionId,
    method: &'static str,
}
```

Attached at each layer using `report.attach(context)`.
