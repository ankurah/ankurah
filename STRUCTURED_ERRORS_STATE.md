# Structured Errors Migration - Current State

## Goal

Remove public error types (`RetrievalError`, `MutationError`, `PropertyError`) from internal code. Only true public API methods should return these types. Internal methods should use internal error types like `StorageError`, `StateError`, `Report<_>`, etc.

## What's Been Done

1. Added `ChangeMe` suffix to ALL Result error types in `core/src/` (except `Report<_>`)
2. Restored proper public error types for PUBLIC API methods (checked off)
3. Remaining `ChangeMe` suffixes mark INTERNAL methods that need proper internal error types

## How to See What's Left

```bash
grep -rn "ChangeMe" core/src --include="*.rs"
```

## Public API Methods (DONE - use public error types)

These have been restored to use proper public error types:

- `Context::get`, `get_cached`, `fetch`, `query`, `collection` → `RetrievalError`
- `Transaction::create` → `MutationError`
- `Transaction::get` → `RetrievalError`
- `Transaction::commit`, `uniffi_commit` → `MutationError`
- `LWW::set`, `LWW::get` → `PropertyError`
- `YrsString::insert`, `delete`, `overwrite`, `replace` → `PropertyError`
- `Ref::get` → `RetrievalError`
- `LiveQuery::update_selection`, `update_selection_wait` → `RetrievalError`
- `View::to_model` → `PropertyError`

## Internal Methods - Categorized by Target Error Type

### StorageError (storage operations)
```
storage.rs:17,19,24,25,28,30,37,51,54,57  - StorageEngine/StorageCollection traits
collectionset.rs:28,48,54                 - CollectionSet methods
retrieval.rs:83,96,133,187,248,341        - GetEvents/Retrieve traits
retrieval.rs:113,220                      - store_used_events
lineage.rs:532                            - test mock retrieve_event
entity.rs:144,551                         - from_state, private_get_or_create
property/backend/mod.rs:38,61             - from_state_buffer, backend_from_string
property/backend/lww.rs:99                - from_state_buffer
property/backend/yrs.rs:143               - from_state_buffer
util/expand_states.rs:23                  - expand_states
```

### StateError (serialization)
```
entity.rs:116,127                         - to_state, to_entity_state
property/backend/mod.rs:36                - to_state_buffer trait
property/backend/lww.rs:93                - to_state_buffer
property/backend/yrs.rs:135               - to_state_buffer
```

### Report<BackendError> or new internal type (backend mutations)
```
property/backend/mod.rs:42,44             - to_operations, apply_operations trait
property/backend/lww.rs:106,126           - to_operations, apply_operations
property/backend/yrs.rs:44,51,58,157,172  - insert, delete, apply_update, to_operations, apply_operations
property/backend/pn_counter.rs:91,105     - to_operations, apply_operations
entity.rs:46,163,217,271,361              - apply_operations, generate_commit_event, apply_event, get_backend
```

### PropertyError (internal, from traits.rs) - keep but consolidate with error/mod.rs
```
property/mod.rs:15,16,22,28,40,41,65,67   - Property trait
property/traits.rs:65,72                  - FromActiveType trait
property/value/yrs.rs:79,89,98            - FromActiveType impls
property/value/json.rs:155,157            - Property impl
property/value/pn_counter.rs:56           - FromActiveType impl
schema.rs:8                               - CollectionSchema trait
value/cast_predicate.rs:89                - test schema impl
```

### RequestError/SendError (networking)
```
node.rs:69,70,74,303,307,327              - request channels, send_message
connector.rs:13                           - PeerSender trait
```

### SubscriptionError (subscriptions)
```
reactor.rs:104,135                        - unsubscribe, remove_query
reactor/subscription.rs:65                - remove_predicate
peer_subscription/server.rs:57            - remove_predicate
```

### ApplyError/ApplyErrorCause (already correct internal types)
```
node_applier.rs:44,96,158,180,201,264     - all apply methods
```

### Needs Decision (internal plumbing)
```
node.rs:544,574,737,837,859,893           - relay_to_peers, commit_remote, get_from_peer, fetch_entities
system.rs:99,168                          - collection, join_system
changes.rs:22                             - EntityChange::new
peer_subscription/client_relay.rs:531,553,668 - subscription methods
reactor/fetch_gap.rs:39,73                - GapFetcher trait
context.rs:60,80,262                      - TContext::commit_local_trx (internal trait)
value/cast_predicate.rs:13,51,70          - cast functions
value/cast.rs:41                          - Value::cast_to
```

## Next Steps

1. For each category above, remove the `ChangeMe` suffix and use the proper internal error type
2. Update the `From` impls in `error/mod.rs` and `error/internal.rs` as needed
3. Some internal error types may need new variants or new types created
4. Run `cargo check` after each batch to verify the changes compile
5. The goal is zero `ChangeMe` suffixes remaining

## Key Principle

Internal code uses `Report<InternalType>` or direct internal error types. Conversion to public errors (`RetrievalError`, `MutationError`, `PropertyError`) happens ONLY at public API method boundaries via `From` impls.

## Reference Files

- `specs/structured-errors/spec.md` - Overall spec
- `specs/structured-errors/plan.md` - Design decisions
- `specs/structured-errors/tasks.md` - Detailed task list
- `core/src/error/mod.rs` - Public error types
- `core/src/error/internal.rs` - Internal error types
