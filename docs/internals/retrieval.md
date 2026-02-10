# Retrieval and Storage Layer

This document covers the traits and concrete types in `core/src/retrieval.rs`
that mediate between the event DAG subsystem and persistent storage.


## The Three Traits

### `GetEvents`

```rust
#[async_trait]
pub trait GetEvents {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError>;
    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError>;
    fn storage_is_definitive(&self) -> bool { false }
}
```

The read-only event interface. This is what `compare()` and `apply_event`
accept.

- **`get_event`** returns the union of staging and permanent storage. On
  `CachedEventGetter`, it additionally falls back to a remote peer request.
  This is the method the BFS algorithm calls at every traversal step.
- **`event_stored`** checks permanent storage only, skipping the staging map.
  Used by the creation-event guard where the question is "has this event been
  previously committed?" not "is this event currently discoverable?"
- **`storage_is_definitive`** returns whether `event_stored() == false` is
  authoritative. On durable nodes, all events backing the entity state are in
  local storage, so a `false` from `event_stored` proves the event has never
  been seen. On ephemeral nodes (where `StateSnapshot` can establish entity
  state without storing individual events), this defaults to `false`.

A blanket `impl GetEvents for &R` exists because nearly every call site passes
getters by reference.

### `GetState`

```rust
#[async_trait]
pub trait GetState {
    async fn get_state(&self, entity_id: EntityId)
        -> Result<Option<Attested<EntityState>>, RetrievalError>;
}
```

Retrieves entity state snapshots from local storage. Separated from `GetEvents`
because state retrieval has different caching characteristics and is not needed
by the BFS comparison algorithm. Also has a blanket `&R` impl.

### `SuspenseEvents`

```rust
#[async_trait]
pub trait SuspenseEvents: GetEvents {
    fn stage_event(&self, event: Event);
    async fn commit_event(&self, attested: &Attested<Event>) -> Result<(), MutationError>;
}
```

Extends `GetEvents` with mutation capabilities. Only the outermost call site
(e.g., `NodeApplier::apply_update`) holds a `SuspenseEvents` reference. It is
deliberately **not** passed into `apply_event` to prevent accidental staging or
committing inside the comparison/application logic.

- **`stage_event`** uses interior mutability (`&self`, not `&mut self`) via
  `Arc<RwLock<HashMap>>`. After staging, the event is discoverable by
  `get_event` (and therefore by BFS), but not by `event_stored`.
- **`commit_event`** takes `&Attested<Event>` (not `&EventId`) because the
  storage layer requires the full attested event. It persists to storage and
  removes the event from the staging map.


## Concrete Types

### `LocalEventGetter`

Implements `GetEvents + SuspenseEvents`. Used by durable nodes.

| Method | Behavior |
|--------|----------|
| `get_event` | Check staging map, then local storage |
| `event_stored` | Check local storage only |
| `storage_is_definitive` | Returns the `durable` flag passed at construction |
| `stage_event` | Insert into `Arc<RwLock<HashMap>>` |
| `commit_event` | `collection.add_event(attested)`, then remove from staging |

Constructed with a `durable` flag. On durable nodes (`durable: true`),
`storage_is_definitive()` returns `true`, enabling the cheap creation-event
guard in `apply_event`.

### `CachedEventGetter`

Implements `GetEvents + SuspenseEvents`. Used by ephemeral nodes.

| Method | Behavior |
|--------|----------|
| `get_event` | Check staging, then local storage, then request from remote peer |
| `event_stored` | Check local storage only |
| `storage_is_definitive` | Returns `false` (default) |
| `stage_event` | Insert into `Arc<RwLock<HashMap>>` |
| `commit_event` | `collection.add_event(attested)`, then remove from staging |

The remote-peer fallback in `get_event` requests the event from a random durable
peer, stores the response locally for future access, then returns it. This is
what allows BFS to succeed on ephemeral nodes for events they have never locally
committed: the accumulator calls `get_event`, which transparently fetches from
the durable peer.

`CachedEventGetter` is parameterized over the storage engine, policy agent, and
context data types, and borrows the `Node` and context data by reference.

### `LocalStateGetter`

Implements `GetState`. Shared by both durable and ephemeral paths.

Wraps a `StorageCollectionWrapper` and translates `EntityNotFound` errors into
`Ok(None)`.


## The Staging Lifecycle

A typical flow in `NodeApplier::apply_update` for `EventOnly` content:

```
for each event_fragment:
    validate_received_event(...)
    event_getter.stage_event(event.clone())        // (1) stage

entity = get_retrieve_or_create(...)

for each attested_event:
    entity.apply_event(event_getter, &event)?      // (2) compare + apply (in memory)
    event_getter.commit_event(&attested_event)?     // (3) commit to disk

save_state(node, &entity, &collection)?             // (4) persist entity state
```

Steps 2 and 3 are per-event. Step 4 runs once after all events are applied.

For `StateAndEvent` content, the flow first attempts `apply_state`. If the state
is strictly newer, it commits all staged events and saves state. If the state
diverges or is older, it falls back to per-event `apply_event` followed by
`commit_event`, then saves state.

For `EventBridge` (in `apply_delta`), all events are staged upfront, then
applied and committed in causal order (oldest first), and finally the entity
state is saved.


## Crash Safety Properties

The ordering invariant (`commit_event` before `set_state`) provides these
guarantees:

**Crash after `commit_event`, before `set_state`:** The event is in permanent
storage but the entity state on disk still references the old head. On recovery,
the entity loads the old state. The next time this event is delivered (or any
descendant of it), BFS will find it in storage and integrate it correctly. No
data loss, no corruption.

**Crash after `stage_event`, before `commit_event`:** Neither the event nor the
updated entity state are persisted. The staging map is in-memory only and lost
on crash. This is a clean rollback to the last persisted state.

**Crash after `set_state`:** The fully consistent state is on disk. Normal
operation.

**No crash, but concurrent `apply_event` on the same entity:** The `try_mutate`
helper in `Entity` acquires the write lock and checks that the head has not
changed since the BFS comparison. If it has, the caller retries with the updated
head (up to 5 attempts). This prevents TOCTOU races between comparison and head
mutation.
