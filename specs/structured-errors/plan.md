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

This was chosen over an enum because:
- error-stack preserves the full chain via `.change_context()`
- Introspection happens via Report's iterator, not pattern matching
- Keeps the public API surface minimal

### 2. Storage Layer Uses StorageError

The `StorageEngine` and `StorageCollection` traits return `StorageError` (internal type), not public errors:

```rust
pub trait StorageEngine: Send + Sync {
    async fn collection(&self, id: &CollectionId) -> Result<..., StorageError>;
    async fn delete_all_collections(&self) -> Result<bool, StorageError>;
}

pub trait StorageCollection: Send + Sync {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, StorageError>;
    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, StorageError>;
    // ... etc
}
```

`StorageError` is publicly exported (not `pub(crate)`) because storage implementation crates need it.

### 3. Conversion at API Boundaries

Internal code uses internal error types exclusively. Conversion to public errors happens at API boundaries using helper functions:

```rust
// In context.rs
fn storage_to_retrieval(e: StorageError) -> RetrievalError {
    match e {
        StorageError::EntityNotFound(id) => RetrievalError::NotFound(NotFound::Entity(id)),
        StorageError::CollectionNotFound(id) => RetrievalError::NotFound(NotFound::Collection(id)),
        other => RetrievalError::Failure(Report::new(other).change_context(InternalError)),
    }
}

// Usage
let collection = self.collections.get(id).await.map_err(storage_to_retrieval)?;
```

Similar helpers exist in `node.rs` and `node_applier.rs`.

### 4. No Cross-Wrapping

Error types don't contain each other:
- `RetrievalError` doesn't contain `PropertyError`
- `PropertyError` doesn't contain `RetrievalError`
- `MutationError` doesn't contain `RetrievalError`

Each type is for a specific concern.

## Key Files

| File | Purpose |
|------|---------|
| `core/src/error/mod.rs` | Public error types |
| `core/src/error/internal.rs` | Internal error types |
| `core/src/storage.rs` | Storage traits (return StorageError) |
| `core/src/context.rs` | API boundary - `storage_to_retrieval()` |
| `core/src/node.rs` | API boundary - `storage_to_mutation()`, `retrieval_to_mutation()` |
| `core/src/node_applier.rs` | `retrieval_to_cause()`, `mutation_to_cause()` |
| `core/src/collectionset.rs` | Returns StorageError |

## Migration Status

### Completed
- [x] error-stack dependency added
- [x] Public error types with `Failure(Report<InternalError>)` variants
- [x] Internal error types in `error/internal.rs`
- [x] `StorageEngine` / `StorageCollection` traits use `StorageError`
- [x] `CollectionSet` returns `StorageError`
- [x] SQLite storage uses `StorageError`
- [x] Sled storage collection uses `StorageError`
- [x] `ankurah-core` compiles

### Remaining
- [ ] Sled internal streams (scan_index.rs, entity.rs, etc.)
- [ ] Postgres storage
- [ ] storage-common traits (`EntityIdStream`, etc.)
- [ ] IndexedDB-wasm storage
- [ ] Context attachments (LineageContext, EntityContext, IntentContext)

## Future: Context Attachments

After storage compiles, add rich context to error reports:

```rust
struct LineageContext {
    subject_frontier: BTreeSet<EventId>,
    other_frontier: BTreeSet<EventId>,
    budget_remaining: usize,
}

struct EntityContext {
    entity_id: EntityId,
    collection: CollectionId,
    method: &'static str,
}

struct IntentContext {
    operation: &'static str,
}
```

Attached at each layer using `report.attach(context)`.
