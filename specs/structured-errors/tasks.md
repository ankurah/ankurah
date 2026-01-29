# Structured Errors - Tasks

## Current State

```bash
cargo check  # passes (but with incorrect error types)
cargo test   # passes
```

## Completed

- [x] Storage traits use `StorageError`
- [x] SQLite, Sled, Postgres, IndexedDB-wasm all migrated
- [x] storage-common stream traits migrated
- [x] Lineage functions return `Report<LineageError>`
- [x] `Entity::apply_state` returns `Report<LineageError>`
- [x] Generic `From<Report<E>>` for public error types
- [x] `.diagnostic()` and `diagnostic_debug()` on public error types

## Remaining Tasks

### Phase 1: Fix Internal Error Types

#### 1.1 Fix `WithStateError` (internal.rs)

**Problem**: `WithStateError::Retrieval` wraps `RetrievalError` (public type)

**Fix**: Remove `Retrieval` variant, add `Storage` variant properly

```rust
// BEFORE (wrong)
pub enum WithStateError {
    Storage(#[from] StorageError),
    Retrieval(#[from] crate::error::RetrievalError),  // PUBLIC TYPE!
    Lineage(Report<LineageError>),
}

// AFTER (correct)
pub enum WithStateError {
    Storage(StorageError),
    Lineage(Report<LineageError>),
}
```

Also remove:
- `From<RetrievalError> for Report<WithStateError>` impl
- `From<RetrievalError> for StorageError` impl (this is backwards!)

#### 1.2 Fix `PropertyBackend` trait (property/backend/mod.rs)

**Problem**: `from_state_buffer` returns `RetrievalError`

**Fix**: Return `StorageError` instead

```rust
// BEFORE
fn from_state_buffer(state_buffer: &Vec<u8>) -> Result<Self, RetrievalError>;

// AFTER
fn from_state_buffer(state_buffer: &Vec<u8>) -> Result<Self, StorageError>;
```

Update implementations in:
- `property/backend/lww.rs`
- `property/backend/yrs.rs`
- `property/backend/pn_counter.rs` (if used)

#### 1.3 Fix `backend_from_string` (property/backend/mod.rs)

**Problem**: Returns `RetrievalError`

**Fix**: Return `StorageError`

```rust
// BEFORE
pub fn backend_from_string(name: &str, buffer: Option<&Vec<u8>>) -> Result<Arc<dyn PropertyBackend>, RetrievalError>

// AFTER
pub fn backend_from_string(name: &str, buffer: Option<&Vec<u8>>) -> Result<Arc<dyn PropertyBackend>, StorageError>
```

#### 1.4 Consolidate `PropertyError` types

**Problem**: TWO `PropertyError` types exist:
- `error/mod.rs::PropertyError` - public with uniffi, has `Failure` (but unused!)
- `property/traits.rs::PropertyError` - used everywhere, has `RetrievalError` variant (wrong!)

**Fix**:
1. Use `error/mod.rs::PropertyError` as THE canonical public type
2. Remove `property/traits.rs::PropertyError` entirely
3. Update all internal code to use `crate::error::PropertyError`
4. In `error/mod.rs::PropertyError`, convert `SerializeError`/`DeserializeError` to `Failure` (internal details shouldn't be separate variants)

The public `PropertyError` should only have user-facing variants:
- `Missing` - property not set
- `InvalidVariant { given, ty }` - wrong type
- `InvalidValue { value, ty }` - can't parse
- `TransactionClosed` - tried to mutate after transaction ended
- `Failure(Report<InternalError>)` - all internal errors

Note: Property access IS a public operation (View/Mutable types are public), so property errors go directly to users.

### Phase 2: Fix Entity Module

#### 2.1 Fix `Entity::from_state` (entity.rs:144)

**Problem**: Returns `RetrievalError`

**Fix**: Return `StorageError` (it's only doing deserialization)

```rust
// BEFORE
fn from_state(id: EntityId, collection: CollectionId, state: &State) -> Result<Self, RetrievalError>

// AFTER
fn from_state(id: EntityId, collection: CollectionId, state: &State) -> Result<Self, StorageError>
```

#### 2.2 Fix `EntitySet` methods (entity.rs)

Several methods return `RetrievalError`:
- `private_get` - should return `Option<Entity>` or `Result<Option<Entity>, StorageError>`
- `private_get_or_create` - should return `Result<(bool, Entity), StorageError>`
- `get` - should return `Result<Option<Entity>, StorageError>`
- `get_retrieve_or_create` - should return `Result<Entity, Report<WithStateError>>`

#### 2.3 Fix `Entity::get_backend` (entity.rs:361)

**Problem**: Returns `RetrievalError`

**Fix**: Return `StorageError`

### Phase 3: Fix Retrieval Module

#### 3.1 Fix `LocalRetriever::store_used_events` (retrieval.rs:113)

**Problem**: Returns `RetrievalError`

**Fix**: Return `StorageError` (it's just storage operations)

### Phase 4: Fix Reactor Module

#### 4.1 Fix `GapFetcher` trait (reactor/fetch_gap.rs)

**Problem**: `fetch_gap` returns `RetrievalError`

**Fix**: `GapFetcher` is internal plumbing for `LiveQuery` - users never touch it. Change to return `Report<StorageError>` or similar internal type. The `LiveQuery` boundary converts to `RetrievalError`.

### Phase 5: Fix Node Module

#### 5.1 Audit `node.rs` for internal `RetrievalError` usage

Many internal methods return `RetrievalError`. Need to:
1. Identify which are true API boundaries (keep `RetrievalError`)
2. Convert internal helpers to return `Report<StorageError>` or similar

Methods to review:
- `request_events` (line 737) - internal, should return internal type
- `fetch_entities_remote` (line 837) - internal, should return internal type

### Phase 6: Fix Remaining Files

#### 6.1 `livequery.rs`

Review all `RetrievalError` usages - `LiveQuery` public methods can use it, but internal helpers should not.

#### 6.2 `peer_subscription/client_relay.rs`

Review - most conversions here are at boundaries (good), but verify.

#### 6.3 `util/expand_states.rs`

**Problem**: Returns `RetrievalError`

**Fix**: Return `StorageError` or `Report<StorageError>`

#### 6.4 `value/cast_predicate.rs`

**Problem**: Returns `RetrievalError` but this is query validation/casting, not retrieval

Functions:
- `cast_predicate_types` - casts literals in predicate based on schema
- `cast_expr_types` - casts literals in expressions
- `cast_literal_to_type` - actual cast, calls `value.cast_to()` which returns `CastError`

**Fix**: Return `QueryError` (which can hold field lookup and cast errors). Caller at API boundary wraps in `RetrievalError::InvalidQuery` if needed.

### Phase 7: Verification

- [ ] Run `cargo check` with no warnings about error type misuse
- [ ] Run `cargo test` - all tests pass
- [ ] Run `rt_missing_events` test - verify error output shows file:line at each level
- [ ] Grep for `RetrievalError` - should only appear in:
  - `core/src/error/mod.rs` (definition)
  - Public method return types
  - Conversion helpers at API boundaries
- [ ] No internal code should import `RetrievalError` except at boundaries

## Files to Modify (Summary)

| File | Changes |
|------|---------|
| `core/src/error/internal.rs` | Remove `WithStateError::Retrieval`, remove `From<RetrievalError> for StorageError` |
| `core/src/error/mod.rs` | Update `PropertyError` - remove `SerializeError`/`DeserializeError` (use `Failure`) |
| `core/src/property/traits.rs` | DELETE `PropertyError` enum, use `crate::error::PropertyError` instead |
| `core/src/property/backend/mod.rs` | `from_state_buffer` → `StorageError`, `backend_from_string` → `StorageError` |
| `core/src/property/backend/lww.rs` | Update `from_state_buffer` return type |
| `core/src/property/backend/yrs.rs` | Update `from_state_buffer` return type |
| `core/src/entity.rs` | `from_state` → `StorageError`, `get_backend` → `StorageError`, other internal methods |
| `core/src/retrieval.rs` | `store_used_events` → `StorageError` |
| `core/src/reactor/fetch_gap.rs` | `GapFetcher::fetch_gap` → `Report<StorageError>` |
| `core/src/node.rs` | Internal methods → internal error types |
| `core/src/util/expand_states.rs` | Return `StorageError` |
| `core/src/value/cast_predicate.rs` | Return `QueryError` |

## Reference

- **Pattern to follow:** `storage/sqlite/src/error.rs` - local error enum with `From<LocalError> for StorageError`
- **Core principle:** Internal code uses `Report<InternalType>`, conversion to public errors at API boundaries only
