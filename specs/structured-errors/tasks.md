# Structured Errors - Remaining Tasks

## First: Check Current State

```bash
cargo check 2>&1 | grep "^error\[" | wc -l
```

## Core Principle

**Internal code NEVER uses public error types.** Storage, streaming, internal operations all use `StorageError`. Conversion to `RetrievalError`/`MutationError` happens only at API boundaries.

## Reference: SQLite is Complete

Use `storage/sqlite/src/engine.rs` and `storage/sqlite/src/error.rs` as the pattern for other storage backends.

## Tasks (in order)

### 1. storage-common traits (DO FIRST)
File: `storage/common/src/traits.rs`

The stream traits define the error type that all implementations must match:
```rust
// Change from:
type Item = Result<EntityId, RetrievalError>;
// To:
type Item = Result<EntityId, StorageError>;
```

### 2. Sled internal streams
Files:
- `storage/sled/src/scan_index.rs`
- `storage/sled/src/scan_collection.rs`
- `storage/sled/src/entity.rs`
- `storage/sled/src/index.rs`
- `storage/sled/src/property.rs`

Pattern:
```rust
// Change from:
RetrievalError::StorageError(...)
// To:
StorageError::BackendError(...)
```

### 3. Postgres storage
File: `storage/postgres/src/lib.rs`

Same pattern as SQLite - import `StorageError`, update signatures and conversions.

### 4. IndexedDB-wasm storage
File: `storage/indexeddb-wasm/src/*.rs`

Same pattern.

## Done When

- [ ] `cargo check` passes (0 errors)
- [ ] `cargo test` passes
- [ ] `.diagnostic()` output is human-readable for failures

## Later: Context Attachments

After everything compiles, enrich error reports with context structs (LineageContext, EntityContext, IntentContext). See plan.md.
