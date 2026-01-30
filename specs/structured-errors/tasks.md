# Structured Errors - Tasks

## Current State

```bash
cargo check  # passes
cargo test   # passes
```

## Completed

- [x] Storage traits use `StorageError`
- [x] SQLite, Sled, Postgres, IndexedDB-wasm all migrated
- [x] storage-common stream traits migrated
- [x] All tests pass

## Remaining

### 1. Improve `.diagnostic()` output

The `Failure` variants have `.diagnostic()` methods, but the output could be more human-readable. Review and improve formatting of error chains.

**Files to check:**
- `core/src/error/mod.rs` - `.diagnostic()` implementations
- Anywhere `Report::new(...).change_context(InternalError)` is used

### 2. Add Context Attachments (optional enhancement)

Attach structured context at error sites for richer diagnostics:

```rust
struct EntityContext {
    entity_id: EntityId,
    collection: CollectionId,
    method: &'static str,
}

// Usage
report.attach(EntityContext { ... })
```

See plan.md for full design.

## Reference

- **Pattern to follow:** `storage/sqlite/src/error.rs` - local error enum with `From<LocalError> for StorageError`
- **Core principle:** Internal code uses `StorageError`, conversion to public errors at API boundaries only
