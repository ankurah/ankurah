# Entity Lifecycle

## Overview

An entity in ankurah moves through a well-defined lifecycle: creation, local
mutation within transactions, commit to storage and peers, and remote
application of events arriving from other nodes. At every stage, the entity's
**head clock** records which events have been integrated into its state, and
the **[event DAG](event-dag.md)** determines whether an incoming update extends, duplicates,
or conflicts with that state.

This document traces the full lifecycle from creation through concurrent
updates to state persistence, with emphasis on the code paths in
`core/src/entity.rs`, `core/src/context.rs`, and `core/src/node_applier.rs`.


## Data Structures

**Entity** (`entity.rs`) -- An `Arc<EntityInner>` providing shared ownership.
`EntityInner` holds an `EntityId`, a `CollectionId`, a broadcast channel, an
`EntityKind`, and an `RwLock<EntityInnerState>`. The `EntityInnerState` bundles
the head clock and [backend](property-backends.md) map into a single lock so that head and property
state are always updated atomically.

**EntityKind** -- Either `Primary` (a resident entity on the node) or
`Transacted { trx_alive, upstream }` (a snapshot fork owned by a live
transaction, with a reference back to the primary entity it was forked from).

**State** (`proto::State`) -- A serialized snapshot containing a `head` clock
and a `StateBuffers` map (`BTreeMap<String, Vec<u8>>`). Each key is a backend
name (e.g. `"lww"`, `"yrs"`); each value is the backend's serialized state.
See [The StateBuffers Format](property-backends.md#the-statebuffers-format).

**EntityState** (`proto::EntityState`) -- Wraps a `State` with an `EntityId`
and `CollectionId`, forming the unit of persistence to storage.

**WeakEntitySet** (`entity.rs`) -- A node-wide registry of weak references to
live entities, keyed by `EntityId`. Guarantees that at most one `Entity`
instance exists per entity ID on a given node. All entity construction flows
through this set.

**StateApplyResult** (`entity.rs`) -- The return type of `apply_state`:

| Variant | Meaning |
|---------|---------|
| `Applied` | Incoming state was strictly newer; backends replaced |
| `AlreadyApplied` | Incoming head equals current head |
| `Older` | Incoming state is strictly older |
| `DivergedRequiresEvents` | True concurrency; events needed for merge |


## Entity Creation

### Local creation (transaction path)

A user begins a transaction via `Context::begin()`, then calls
`Transaction::create::<MyModel>()`. The transaction delegates to
`TContext::create_entity`, which does two things:

1. Calls `WeakEntitySet::create` to mint a fresh `EntityId`, construct a
   `Primary` entity with an empty head and empty backends, and register it in
   the weak set.
2. Calls `Entity::snapshot(trx_alive)` on that primary entity. The snapshot
   forks every backend (via [`PropertyBackend::fork()`](property-backends.md#the-propertybackend-trait)), copies the current
   head, and wraps the result as `EntityKind::Transacted` with a back-pointer
   to the primary.

The transaction holds the `Transacted` snapshot. User mutations (setting
properties) go through the snapshot's backends. The primary entity is
read-only; its `is_writable()` returns `false` because its kind is `Primary`.

### System root creation

`SystemManager::create` follows a different path because there is no
transaction. It calls `WeakEntitySet::create` directly, sets [LWW](property-backends.md#lww-backend) properties
on the resulting entity, then calls `generate_commit_event` to produce the
creation event. The event is [staged](event-dag.md#the-staging-pattern), applied via `apply_event`, committed to
storage, and the entity state is persisted. This is the only place that
applies a creation event to an entity that it also created locally.


## Local Transaction Commit

`commit_local_trx` in `context.rs` is the core commit path for user
transactions. It proceeds in five phases.

### Phase 1: Generate events

For each entity in the transaction, `generate_commit_event` collects pending
operations from all backends (via [`PropertyBackend::to_operations()`](property-backends.md#mutation-methods)) and
packages them into an `Event` whose `parent` is the snapshot's current head.
If a backend has no pending operations, it is skipped. If no backends have
operations, the entity is skipped entirely (no event generated).

A validation check ensures that any event with an empty parent (a creation
event) corresponds to an entity whose ID is in the transaction's
`created_entity_ids` set. This prevents "phantom entities" -- entities that
were inserted into the weak set outside of `Transaction::create` -- from
producing illegitimate creation events.

### Phase 2: Fork-based validation

For each `(entity, event)` pair, the commit path creates a second fork of the
transacted entity (`entity.snapshot(trx_alive)`) to serve as a validation
sandbox. The event is [staged](event-dag.md#the-staging-pattern) into a [`LocalEventGetter`](retrieval.md#localeventgetter) and applied to this
fork via `apply_event`. The fork's post-application state is then passed to
the policy agent's `check_event`, which compares the before-state (the
upstream primary entity) with the after-state (the validated fork) and decides
whether to attest the event. The attested event is committed to storage via
`event_getter.commit_event`.

### Phase 3: Update heads

After all events are validated and stored, the commit path updates heads on
the transacted entities by calling `entity.commit_head(Clock::new([event_id]))`.
This replaces the snapshot's head with a single-element clock containing the
new event's ID.

Heads are updated **before** relaying to peers. This ensures that if the
relay target echoes the event back (e.g., a durable peer's subscription
update), the local entity already has the correct head and the echo is
recognized as `Equal` or `StrictAscends`.

### Phase 4: Relay to peers

`relay_to_required_peers` sends the attested events to [durable peers](node-architecture.md#durable-vs-ephemeral-nodes). The
commit waits for peer confirmation before proceeding to persistence.

### Phase 5: Persist state

For each entity, the commit path resolves the canonical entity (the upstream
primary for `Transacted` forks, or the entity itself for `Primary`). For
transacted forks, a fresh `LocalEventGetter` is created and the event is
applied to the upstream primary via `apply_event`. This brings the primary
up to date. The entity's state is serialized via `to_state()` and persisted
to storage via `collection.set_state`. Finally, `EntityChange` notifications
are emitted to the reactor.


## Remote Event Application

Remote events arrive via two paths, both handled by `NodeApplier` in
`node_applier.rs`. See also [Node Architecture and Replication](node-architecture.md) for the
full replication protocol.

### Subscription updates (`apply_updates`)

Streaming subscription updates arrive as `SubscriptionUpdateItem` values with
one of two content types:

**EventOnly** -- The common case for incremental updates. Events are validated
against the policy agent, [staged](event-dag.md#the-staging-pattern), then the entity is retrieved (or created if
not resident). Each event is applied via [`apply_event`](#apply_event-in-detail); successfully applied
events are committed to storage. State is saved if any events were applied.

**StateAndEvent** -- Used for initial [subscription delivery](node-architecture.md#subscription-propagation), fetch responses,
and reactor-driven subscription updates. Events are validated, staged. The
state is validated and applied via `WeakEntitySet::with_state`, which calls
[`apply_state`](#apply_state-in-detail) internally. If the state was applied successfully (`Applied` or
new entity), all staged events are committed and the entity state is
persisted. If the state was not applied (divergence or older), the code falls
back to event-by-event application via [`apply_event`](#apply_event-in-detail), handling each event
individually. This fallback is critical for the `DivergedSince` case where
state snapshots cannot be merged without event-level [conflict resolution](lww-merge.md).

### Delta application (`apply_deltas`)

Fetch and QuerySubscribed responses arrive as `EntityDelta` values. Two
content types are handled:

**StateSnapshot** -- The state is validated and applied via `with_state`. The
entity state is persisted. No events are involved.

**EventBridge** -- Events are [staged](event-dag.md#the-staging-pattern) for [BFS](event-dag.md#bfs-traversal) discovery, the entity is
retrieved or created, then events are applied in causal (oldest-first) order
via [`apply_event`](#apply_event-in-detail). Each event is committed after successful application. The
entity state is persisted after all events are processed.


## `apply_event` in Detail

`Entity::apply_event` (`entity.rs`) is the central event integration method.
It is used by both local commit and remote application paths.

### Guard ordering

Three guards execute before the retry loop:

1. **Creation event on non-empty head.** On [durable nodes](node-architecture.md#durable-vs-ephemeral-nodes) where
   [`storage_is_definitive()`](retrieval.md#getevents) is true, `event_stored() == false` proves the
   event is from a different genesis -- reject as `Disjoint`. On [ephemeral
   nodes](node-architecture.md#durable-vs-ephemeral-nodes), fall through to [BFS](event-dag.md#bfs-traversal) which distinguishes re-delivery
   (`StrictAscends`) from different genesis (`Disjoint`).

2. **Creation event on empty head.** Acquires the state write lock, re-checks
   that the head is still empty, then applies the event's operations directly
   and sets the head to the event's ID. This mutex-protected check avoids the
   TOCTOU race between reading `head.is_empty()` and writing the head.

3. **Non-creation event on empty head.** The entity was never properly
   created. Reject with `InvalidEvent` rather than allowing BFS to produce a
   spurious `DivergedSince(meet=[])`.

### The retry loop

After guards pass, `apply_event` enters a loop of up to `MAX_RETRIES` (5)
attempts. Each attempt:

1. Reads the current head (outside the lock).
2. Calls [`compare(getter, &event_id_clock, &head, budget)`](event-dag.md#the-comparison-algorithm). The event is
   already [staged](event-dag.md#the-staging-pattern), so BFS can discover it.
3. Matches on the [`AbstractCausalRelation`](event-dag.md#core-concepts):

**Equal** -- The event is already integrated. Return `Ok(false)`.

**StrictDescends** -- The event is a direct descendant. Apply operations via
`try_mutate`, which acquires the write lock and checks that the head has not
moved since comparison. Set the head to the event's ID. If `try_mutate`
returns `Ok(false)` (head moved), `continue` to retry.

**StrictAscends** -- The event is older than the current state. Return
`Ok(false)`.

**DivergedSince** -- True concurrency. Decompose the [`ComparisonResult`](event-dag.md#core-concepts) to
obtain the [`EventAccumulator`](event-dag.md#core-concepts), compute [`EventLayers`](event-dag.md#core-concepts) from the meet point,
collect all layers, then apply under the write lock. Under the lock, re-check
that the head has not moved. For each layer, check whether any `to_apply`
events reference backends not yet in the state map; if so, [create the backend
and replay earlier layers](property-backends.md#backend-registration-and-late-creation) against it. Apply each layer to all backends via
[`backend.apply_layer`](property-backends.md#the-propertybackend-trait). After all layers, update the head: remove each meet
ID, insert the new event's ID. If the head moved during [layer computation](lww-merge.md#layer-computation),
`continue` to retry.

**Disjoint** -- Return `Err(LineageError::Disjoint)`.

**BudgetExceeded** -- Return `Err(LineageError::BudgetExceeded)`.

If all retries are exhausted, return `Err(MutationError::TOCTOUAttemptsExhausted)`.


## `apply_state` in Detail

`Entity::apply_state` (`entity.rs`) applies a `State` snapshot to the entity.
It cannot merge divergent state -- merging requires event-level operations
that state snapshots do not carry. See [LWW Merge Resolution](lww-merge.md) for how
event-level merging works.

Like `apply_event`, it runs a retry loop with TOCTOU protection:

1. Call [`compare(getter, &new_head, &current_head, budget)`](event-dag.md#the-comparison-algorithm).
2. Match on the relation:

**Equal** -- Return `AlreadyApplied`.

**StrictDescends** -- The incoming state is newer. Under [`try_mutate`](#the-try_mutate-toctou-protection),
replace all backends by deserializing from the state's [`StateBuffers`](property-backends.md#the-statebuffers-format) and set
the head to the incoming clock. Return `Applied`.

**StrictAscends** -- Incoming is older. Return `Older`.

**DivergedSince** -- Cannot merge without events. Return
`DivergedRequiresEvents`. The caller is responsible for falling back to
event-by-event application.

**Disjoint** -- Return `Err(LineageError::Disjoint)`.

**BudgetExceeded** -- Return `Err(LineageError::BudgetExceeded)`.


## `apply_state` vs `apply_event`

| Aspect | `apply_state` | `apply_event` |
|--------|---------------|---------------|
| Input | Full `State` snapshot (head + serialized backends) | Single `Event` (parent + operations) |
| Linear extension | Replaces all backends from snapshot | Applies operations incrementally |
| Divergence | Returns `DivergedRequiresEvents` | Computes layers, merges per-property |
| When used | Initial entity loading, `StateAndEvent` delivery, `StateSnapshot` deltas | Local commit, `EventOnly` delivery, `EventBridge` deltas, `StateAndEvent` fallback |

The `StateAndEvent` flow in `NodeApplier` first tries `apply_state` (fast
path). If the state diverges, it falls back to applying the accompanying
events one by one via `apply_event`. This two-phase approach was introduced
to fix the StateAndEvent divergence bug where events were saved to storage
but never applied to the in-memory entity on divergence.


## The `try_mutate` TOCTOU Protection

`Entity::try_mutate` is a private helper that serializes the
compare-then-mutate sequence:

```rust
fn try_mutate<F, E>(&self, expected_head: &mut Clock, body: F) -> Result<bool, E>
where F: FnOnce(&mut EntityInnerState) -> Result<(), E>
{
    let mut state = self.state.write().unwrap();
    if &state.head != expected_head {
        *expected_head = state.head.clone();
        return Ok(false);
    }
    body(&mut state)?;
    Ok(true)
}
```

The caller performs an async `compare()` without holding any lock, then calls
`try_mutate` with the head it used for comparison. If the head moved between
comparison and mutation (another thread applied an event concurrently),
`try_mutate` updates the caller's head variable to the new value and returns
`Ok(false)`. The caller's retry loop then re-runs comparison against the
fresh head.

For `StrictDescends`, `try_mutate` is called directly. For `DivergedSince`,
the lock is acquired manually with the same head-check pattern, because the
mutation involves iterating layers rather than a single closure.


## Head Management

The head clock evolves through three patterns:

### Linear extension

The most common case. The entity's head is `[A]`; a new event `B` arrives
with `parent=[A]`. Comparison yields `StrictDescends`. The head becomes `[B]`.
This is a single-element clock throughout.

### Divergence (multi-head)

Two events `B` and `C` are created concurrently, both with `parent=[A]`. The
entity applies `B` first (head becomes `[B]`), then `C` arrives. Comparison
yields `DivergedSince { meet=[A] }`. After layer-based merge, the head update
removes the meet ID `A` from the current head `[B]` (no-op, `A` is not in
the head) and inserts `C`. The resulting head is `[B, C]` -- a multi-element
clock indicating two concurrent tips.

### Merge

A later event `D` arrives with `parent=[B, C]`. Since `D`'s parent matches
the current head exactly, the quick-check in `compare` fires immediately
(all head members appear in the event's parent set). The head becomes `[D]`,
collapsing the multi-head back to a single element.


## Entity State Persistence

### `to_state` and `to_entity_state`

`Entity::to_state` serializes the current in-memory state. Under the read
lock, it iterates all backends, calls [`to_state_buffer()`](property-backends.md#serialization-methods) on each, and
collects the results into a `StateBuffers` map. Combined with the current
head clock, this forms a `State`. `to_entity_state` wraps the `State` with
the entity's ID and collection to form an `EntityState`.

### `set_state` (storage persistence)

After events are committed to storage and the in-memory entity state is
updated, the caller serializes the entity via `to_entity_state`, wraps it
in an `Attested<EntityState>` (with optional policy attestation), and calls
`collection.set_state(attested)`. This is the final durability boundary.

The ordering invariant is: **commit events to storage before persisting
state** (see [The Staging Pattern](event-dag.md#the-staging-pattern) and [Crash Safety Properties](retrieval.md#crash-safety-properties)).
If the process crashes after `commit_event` but before `set_state`,
recovery loads the old state and the event is re-applied on next delivery.
If the process crashes before `commit_event`, neither the event nor the
updated state are persisted -- a clean rollback.

### `with_state` (WeakEntitySet)

`WeakEntitySet::with_state` is the primary entry point for applying a state
snapshot to a possibly-nonexistent entity. It checks:

1. Is the entity already resident in the weak set? If so, call
   `apply_state` on it.
2. Is the entity in local storage? If so, construct it from the stored state
   via `private_get_or_create`, then call `apply_state` with the incoming
   state.
3. Neither? Create the entity directly from the incoming state via
   `private_get_or_create`. If no other thread raced to create it, this is
   an early return -- the entity is already at the incoming state.

Returns `(Option<bool>, Entity)` where `None` means the entity was newly
created from the incoming state, `Some(true)` means the state was applied
(strictly descended), and `Some(false)` means the state was not applied
(equal, older, or diverged).


## Invariants

1. **Atomic head and backend updates.** Head clock and backend state live
   under a single `RwLock<EntityInnerState>`. Every mutation updates both
   under the same write lock acquisition.

2. **TOCTOU protection on every mutation path.** Both `apply_event` and
   `apply_state` verify the head has not moved between async comparison and
   synchronous mutation. Retries are bounded to 5 attempts.

3. **Creation event idempotency.** Re-delivery of a creation event is
   detected either by the durable fast path (`event_stored() == true`) or by
   BFS (`StrictAscends`). Neither path corrupts state.

4. **Transaction snapshot isolation.** User mutations operate on a forked
   snapshot. The primary entity is not modified until commit phase 5, when
   the event is applied to the upstream via `apply_event`.

5. **Staging before comparison; commit before persistence.** An event must
   be staged (and thus discoverable by BFS) before `apply_event` is called.
   An event must be committed to permanent storage before the entity state
   referencing it is persisted.

6. **StateAndEvent divergence fallback.** When `apply_state` returns
   `DivergedRequiresEvents`, the `NodeApplier` falls back to event-by-event
   application. Events are never silently dropped on divergence.
