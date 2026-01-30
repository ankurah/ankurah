# Structured Errors Migration - Current State

## Goal

Public error types (`RetrievalError`, `MutationError`, `PropertyError`) should ONLY be returned from true public API methods. Internal methods must use internal error types (`StorageError`, `StateError`, `Report<_>`, etc.).

## Current Approach

We added `ChangeMe` suffix to all Result error types. Public API methods have been restored. Remaining `ChangeMe` suffixes mark internal code that needs fixing.

```bash
grep -rn "ChangeMe" core/src --include="*.rs"
```

## Public API Methods (DONE)

These return public error types - no changes needed:

| Method | Error Type |
|--------|------------|
| `Context::get`, `get_cached`, `fetch`, `query`, `collection` | `RetrievalError` |
| `Transaction::create` | `MutationError` |
| `Transaction::get` | `RetrievalError` |
| `Transaction::commit`, `uniffi_commit` | `MutationError` |
| `LWW::set`, `LWW::get` | `PropertyError` |
| `YrsString::insert`, `delete`, `overwrite`, `replace` | `PropertyError` |
| `Ref::get` | `RetrievalError` |
| `LiveQuery::update_selection`, `update_selection_wait` | `RetrievalError` |
| `View::to_model` | `PropertyError` |

## Internal Methods - By Target Error Type

### → StorageError
Storage/retrieval operations. Already exists in `error/internal.rs`.

- `storage.rs` - `StorageEngine`, `StorageCollection` trait methods
- `collectionset.rs` - `CollectionSet::get`, `list_collections`, `delete_all_collections`
- `retrieval.rs` - `GetEvents::retrieve_event`, `Retrieve::get_state`, `store_used_events`
- `lineage.rs` - test mock `retrieve_event`
- `entity.rs` - `Entity::from_state`, `private_get_or_create`
- `property/backend/*.rs` - `from_state_buffer`
- `property/backend/mod.rs` - `backend_from_string`
- `util/expand_states.rs` - `expand_states`

### → StateError
Serialization operations. Already exists in `error/internal.rs`.

- `entity.rs` - `to_state`, `to_entity_state`
- `property/backend/*.rs` - `to_state_buffer`

### → StorageError (backend mutations)
Backend operation errors. Use `StorageError::BackendError` variant.

- `property/backend/*.rs` - `to_operations`, `apply_operations`, `insert`, `delete`, `apply_update`
- `entity.rs` - `apply_operations`, `generate_commit_event`, `apply_event`, `get_backend`

### → PropertyError (keep the traits.rs version for now)
**Note**: There are TWO `PropertyError` types:
- `error/mod.rs::PropertyError` - public, has `Failure` variant
- `property/traits.rs::PropertyError` - internal, used everywhere

For now, keep using `property/traits.rs::PropertyError`. Consolidation is a separate task.

- `property/mod.rs` - `Property` trait (`into_value`, `from_value`)
- `property/traits.rs` - `FromActiveType` trait
- `property/value/*.rs` - `FromActiveType` impls
- `schema.rs` - `CollectionSchema::field_type`

### → RequestError / SendError
Networking errors. Already exist in `error/internal.rs`.

- `node.rs` - `request`, pending channels
- `connector.rs` - `PeerSender::send_message`

### → SubscriptionError
Subscription errors. Already exists in `error/internal.rs`.

- `reactor.rs` - `unsubscribe`, `remove_query`
- `reactor/subscription.rs` - `remove_predicate`
- `peer_subscription/server.rs` - `remove_predicate`

### → ApplyError / ApplyErrorCause
Already correct internal types in `error/internal.rs`.

- `node_applier.rs` - all methods (just remove `ChangeMe` suffix)

### → Needs Decision
These are internal but unclear what error type to use:

- `node.rs` - `relay_to_required_peers`, `commit_remote_transaction`, `get_from_peer`, `fetch_entities_from_local`
- `system.rs` - `collection`, `join_system`
- `changes.rs` - `EntityChange::new`
- `peer_subscription/client_relay.rs` - subscription methods
- `reactor/fetch_gap.rs` - `GapFetcher` trait (internal plumbing for LiveQuery)
- `context.rs` - `TContext::commit_local_trx` (internal trait)
- `value/cast_predicate.rs` - cast functions (should probably be `QueryError`)
- `value/cast.rs` - `Value::cast_to` (should probably be `CastError`)

## How to Fix Each Category

1. **Simple cases** (StorageError, StateError, etc.): Just remove `ChangeMe` suffix
2. **Need From impls**: Some conversions may need `From` impls added to `error/internal.rs`
3. **"Needs Decision"**: Read the code, understand what errors can occur, pick appropriate internal type

## Key Files

- `core/src/error/mod.rs` - Public error types (RetrievalError, MutationError, PropertyError)
- `core/src/error/internal.rs` - Internal error types (StorageError, StateError, etc.)
- `specs/structured-errors/*.md` - Background (some details stale, but principles are correct)

## Key Principle

```rust
// INTERNAL method - uses internal error type
fn internal_helper() -> Result<T, StorageError> { ... }

// PUBLIC method - converts at boundary via From impl
pub fn public_api() -> Result<T, RetrievalError> {
    self.internal_helper()?  // ? uses From<StorageError> for RetrievalError
}
```

The `From` impls in `error/mod.rs` extract soft errors (NotFound, AccessDenied) as matchable variants and wrap everything else in `Failure(Report<InternalError>)`.
