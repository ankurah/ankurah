# Node Architecture and Replication Protocol

## Overview

An Ankurah deployment consists of **nodes** that replicate entity state across a
network. Nodes come in two flavors -- **durable** and **ephemeral** -- that
differ in what they store, how they retrieve events, and how they participate in
the replication protocol. This document describes both node types, the message
formats that carry data between them, and the subscription machinery that keeps
ephemeral nodes synchronized with their durable peers.

The primary source files are:

- `core/src/node.rs` -- `Node`, `NodeInner`, peer management
- `core/src/node_applier.rs` -- `NodeApplier`, inbound update/delta application
- `core/src/retrieval.rs` -- [`LocalEventGetter`, `CachedEventGetter`, `LocalStateGetter`](retrieval.md)
- `core/src/peer_subscription/client_relay.rs` -- `SubscriptionRelay`
- `core/src/peer_subscription/server.rs` -- `SubscriptionHandler`
- `proto/src/update.rs` -- `UpdateContent` variants (`EventOnly`, `StateAndEvent`)
- `proto/src/request.rs` -- `DeltaContent` variants (`StateSnapshot`, `EventBridge`, `StateAndRelation`)


## Durable vs Ephemeral Nodes

### Construction

Durable nodes are created with `Node::new_durable(engine, policy_agent)`.
Ephemeral nodes are created with `Node::new(engine, policy_agent)`. Both
constructors set `NodeInner::durable` accordingly; the critical downstream
effect is which [event getter](retrieval.md#concrete-types) is used and whether [`storage_is_definitive()`](retrieval.md#getevents)
returns `true`.

Durable nodes do **not** have a `SubscriptionRelay` (`subscription_relay: None`
in `new_durable`). Ephemeral nodes always have one (`subscription_relay:
Some(SubscriptionRelay::new())` in `new`).

### What each stores

**Durable nodes** are the authoritative source of truth. They persist every
event they accept (via `collection.add_event`) and every entity state snapshot
(via `collection.set_state`). Because every event backing an entity's head clock
is guaranteed to be in local storage, `storage_is_definitive()` returns `true`.

**Ephemeral nodes** also persist events and state to a local storage engine (the
same `StorageEngine` trait), but they may receive entity state without receiving
the underlying events. This happens when a `StateSnapshot` delta establishes an
entity's state directly -- the head clock references events that were never
individually committed to the ephemeral node's event storage. As a result,
`storage_is_definitive()` returns `false`.

### Event handling differences

When a durable node receives events (via `commit_remote_transaction`), it
creates a [`LocalEventGetter`](retrieval.md#localeventgetter) with `durable: true`. The event getter's
`get_event` checks the [staging map](event-dag.md#the-staging-pattern) and then local storage -- no remote fallback.
If an event is not found locally, that is a hard error.

When an ephemeral node receives streaming updates (via `NodeApplier::apply_updates`),
it creates a [`CachedEventGetter`](retrieval.md#cachedeventgetter) that adds a third lookup tier: if the event is
not in staging or local storage, it requests the event from a random durable
peer via `NodeRequestBody::GetEvents`, caches the response locally, and returns
it. This transparent remote fallback is what allows [BFS traversal](event-dag.md#bfs-traversal) to succeed on
ephemeral nodes that lack historical events.


## The `storage_is_definitive()` Distinction

The [`GetEvents`](retrieval.md#getevents) trait defines `storage_is_definitive()` with a default of
`false`. [`LocalEventGetter`](retrieval.md#localeventgetter) returns the `durable` flag passed at construction.
[`CachedEventGetter`](retrieval.md#cachedeventgetter) inherits the default `false`.

This method matters for the [creation-event guard](entity-lifecycle.md#guard-ordering) in [`Entity::apply_event`](entity-lifecycle.md#apply_event-in-detail). When
a creation event arrives for an entity that already has a non-empty head:

- **Durable node** (`storage_is_definitive() == true`): `event_stored()` is
  authoritative. If it returns `true`, the creation event was already committed
  -- this is re-delivery, a no-op. If it returns `false`, the creation event has
  genuinely never been seen, which means a different genesis -- reject as
  `Disjoint` without BFS.

- **Ephemeral node** (`storage_is_definitive() == false`): `event_stored()`
  returning `false` is not conclusive -- the entity might have been established
  via `StateSnapshot` without the creation event being individually stored. The
  guard falls through to [BFS](event-dag.md#bfs-traversal), which correctly determines `StrictAscends`
  (re-delivery) or `Disjoint` (different genesis).

This optimization avoids unnecessary BFS traversal on durable nodes for the
common case of redundant event delivery.


## `LocalEventGetter` vs `CachedEventGetter`

Both implement [`GetEvents`](retrieval.md#getevents) + [`SuspenseEvents`](retrieval.md#suspenseevents). Both use the same [staging
mechanism](event-dag.md#the-staging-pattern) (`Arc<RwLock<HashMap<EventId, Event>>>`). They differ only in
`get_event` fallback behavior and the `storage_is_definitive` flag.

### `LocalEventGetter`

Used in three contexts:

1. **`commit_remote_transaction`** (in `node.rs`): When a durable or ephemeral
   node receives a `CommitTransaction` request from a peer, it creates a
   `LocalEventGetter` with the node's own `durable` flag.

2. **`fetch_entities_from_local`** (in `node.rs`): When loading entities from
   local storage for queries or subscription setup.

3. **`collect_event_bridge`** (in `node.rs`): When building an event bridge for
   a peer, uses local-only event access since all bridge events must be in local
   storage.

### `CachedEventGetter`

Used in two contexts:

1. **`NodeApplier::apply_updates`** (in `node_applier.rs`): When an ephemeral
   node receives `SubscriptionUpdate` messages from a durable peer. The
   `CachedEventGetter` is constructed with the collection, the node reference,
   and the context data set from the subscription relay.

2. **`remote_subscribe`** (in `client_relay.rs`): When an ephemeral node
   processes the initial `EntityDelta` batch returned by a `SubscribeQuery`
   response. The same `CachedEventGetter` is used for `apply_deltas`.

In both cases, the remote fallback in `get_event` contacts a random durable peer
(`node.get_durable_peer_random()`) and sends a `GetEvents` request. Fetched
events are stored locally via `collection.add_event` before being returned, so
subsequent accesses find them in local storage.


## The Replication Protocol

Data flows between nodes using two protocol-level envelopes that carry different
content types depending on context.

### Streaming updates: `UpdateContent`

Defined in `proto/src/update.rs`. Carried inside `SubscriptionUpdateItem`,
which is sent from a durable node's `SubscriptionHandler` to connected peers
whenever the reactor fires a change notification.

| Variant | Contents | When used |
|---------|----------|-----------|
| `EventOnly(Vec<EventFragment>)` | Events only, no state | When the receiver already has the entity state |
| `StateAndEvent(StateFragment, Vec<EventFragment>)` | Full state + events | When the receiver may need the entity state |

In practice, the server-side `convert_item` function in `server.rs` always
produces `StateAndEvent` -- it attests the entity state and includes all events
from the reactor update. The `EventOnly` variant is defined in the protocol but
is not currently emitted by the server; it is handled on the receiver side in
`NodeApplier::apply_update` for forward compatibility.

**Receiver behavior for `EventOnly`:** Events are validated, [staged](event-dag.md#the-staging-pattern), the entity
is loaded from local storage (or created), and each event is applied via
[`entity.apply_event`](entity-lifecycle.md#apply_event-in-detail), then committed. This path expects the entity to already
exist locally.

**Receiver behavior for `StateAndEvent`:** Events are validated and [staged](event-dag.md#the-staging-pattern), the
state is validated, and the entity is initialized or updated via
[`entities.with_state`](entity-lifecycle.md#with_state-weakentityset). If the state applies cleanly (new entity or strict
descendant), all staged events are committed and the state is saved. If the
state diverges or is older, the code falls back to per-event [`apply_event`](entity-lifecycle.md#apply_event-in-detail)
(which handles divergence via [LWW merge resolution](lww-merge.md)) and commits only the
events that were actually applied.

### Request/response deltas: `DeltaContent`

Defined in `proto/src/request.rs`. Carried inside `EntityDelta`, which is
returned in `Fetch` and `QuerySubscribed` responses. These are produced by
`Node::generate_entity_delta` on the server side.

| Variant | Contents | When used |
|---------|----------|-----------|
| `StateSnapshot { state }` | Full state snapshot | Entity is unknown to the requester, or event bridge construction failed |
| `EventBridge { events }` | Ordered event sequence | Entity is in `known_matches` and the server can build a forward chain |
| `StateAndRelation { state, relation }` | State + causal assertion | Large event gap (not yet implemented in application code) |

### How `generate_entity_delta` chooses a variant

The server inspects the `known_matches` map sent by the requester:

1. **Entity in `known_matches`, heads equal:** Return `None` (omit entity from
   response -- the client already has the current state).

2. **Entity in `known_matches`, heads differ:** Call `collect_event_bridge` to
   attempt building an `EventBridge`. If the current head strictly descends from
   the known head, the bridge contains the events between the two. If the
   relationship is anything else (diverged, disjoint, etc.) or the bridge is
   empty, fall through to `StateSnapshot`.

3. **Entity not in `known_matches`:** Send `StateSnapshot`.

The `StateAndRelation` variant is defined in the protocol but returns
`MutationError::InvalidUpdate` if received by `apply_delta_inner` -- it is not
yet implemented on the application side.

### The event bridge mechanism

`Node::collect_event_bridge` (in `node.rs`) builds the set of events needed to
advance a peer from `known_head` to `current_head`:

1. Compare `current_head` against `known_head` using [`event_dag::compare`](event-dag.md#the-comparison-algorithm) with a
   [`LocalEventGetter`](retrieval.md#localeventgetter).

2. If the result is `StrictDescends`, walk backward from `current_head` through
   parent pointers, collecting events until all frontier members are in
   `known_head`. Reverse the collected events to produce oldest-first causal
   order.

3. For `Equal`, return an empty vec. For any other relationship (diverged,
   disjoint, budget exceeded), return an empty vec, causing the caller to fall
   back to `StateSnapshot`.

The bridge avoids sending full state when only a few events separate the peer's
known state from the current state. It is particularly useful for subscription
re-establishment after brief disconnections.

**Current limitation:** `collect_event_bridge` has no traversal limit on the
backward walk (noted as a security concern in the implementation resume). A
malicious or stale `known_head` could cause unbounded event collection.


## Subscription Propagation

Ephemeral nodes use a `SubscriptionRelay` to manage query subscriptions with
durable peers. The relay lives on the ephemeral node and coordinates the
lifecycle of remote subscriptions.

### Subscription lifecycle

1. **Local subscription created:** When application code creates a live query,
   `Node::subscribe_remote_query` calls `relay.subscribe_query(...)`, registering
   the query ID, collection, selection, context data, and version. The relay
   stores this as `Status::PendingRemote`.

2. **Peer connects:** `Node::register_peer` calls `relay.notify_peer_connected`.
   The relay iterates all `PendingRemote` subscriptions, marks them as
   `Status::Requested`, and spawns async tasks to establish them on the peer.

3. **Remote subscription established:** Each task calls `TNode::remote_subscribe`
   (implemented on `WeakNode`), which:
   - Pre-fetches `known_matches` from local storage via
     `fetch_entities_from_local`.
   - Sends a `SubscribeQuery` request to the durable peer with the known matches.
   - Receives `EntityDelta` items in the `QuerySubscribed` response.
   - Applies all deltas via `NodeApplier::apply_deltas` using a
     `CachedEventGetter`.
   - On success, the relay marks the subscription as `Status::Established`.

4. **Streaming updates flow:** After subscription establishment, the durable
   node's `SubscriptionHandler` monitors the reactor. When entities matching the
   subscribed query change, it sends `SubscriptionUpdate` messages containing
   `StateAndEvent` items. The ephemeral node receives these in
   `Node::handle_update`, which delegates to `NodeApplier::apply_updates`.

5. **Peer disconnects:** `Node::deregister_peer` calls
   `relay.notify_peer_disconnected`. All subscriptions associated with that peer
   revert to `PendingRemote` and are automatically re-established when a new
   durable peer connects.

6. **Subscription removed:** `Node::unsubscribe_remote_predicate` calls
   `relay.unsubscribe_predicate`, which removes the subscription from tracking
   and sends an `UnsubscribeQuery` message to the established peer.

### Retry and error handling

The relay runs a background task that periodically (every 5 seconds) calls
`setup_remote_subscriptions`, re-attempting any `PendingRemote` subscriptions.
Errors are classified as retryable (e.g., `PeerNotConnected`, `ConnectionLost`)
or permanent (e.g., `ServerError`, `AccessDenied`). Retryable failures revert
to `PendingRemote`; permanent failures move to `Status::Failed`.

### Context data propagation

The relay stores `ContextData` for each subscription. When the ephemeral node
receives a `SubscriptionUpdate` from a peer, `apply_updates` retrieves the
relevant context data via `relay.get_contexts_for_peer(from_peer_id)`, which
scans all subscriptions established with that peer and collects their unique
context values. This set is used for policy validation of incoming events and
states.


## Commit Paths

### Ephemeral node commits locally

When an ephemeral node commits a local transaction ([`context.rs:commit_local_trx`](entity-lifecycle.md#local-transaction-commit)),
it creates events referencing the entity's current head. These events are sent
to durable peers via `relay_to_required_peers`, which sends a
`CommitTransaction` request and waits for `CommitComplete` responses.

The durable peer receives the events in `commit_remote_transaction`, validates
them via the policy agent, [applies them to the entity](entity-lifecycle.md#apply_event-in-detail), and persists the result.
The reactor then fires, sending `SubscriptionUpdate` messages to all other
connected peers (including potentially back to the originating ephemeral node,
which will see them as re-delivery and no-op via `StrictAscends`).

### Durable node commits locally

A durable node committing a local transaction follows the same
[`commit_local_trx`](entity-lifecycle.md#local-transaction-commit) path but does not relay to durable peers (it is itself
durable). The reactor fires and distributes `SubscriptionUpdate` messages to
connected ephemeral peers.

### Durable node receives remote commit

`commit_remote_transaction` in `node.rs`:

1. For each event, create a `LocalEventGetter` with `durable: true`.
2. Stage the event.
3. For creation events on empty entities: apply directly.
4. For updates: fork the entity, apply to the fork for policy validation.
5. Run policy check (`check_event`) comparing before/after states.
6. Commit the event to permanent storage.
7. Apply to the real entity (for updates; creations already applied).
8. [Persist entity state](entity-lifecycle.md#entity-state-persistence) and collect changes.
9. Notify the reactor, which distributes to other peers.


## Integration Test Patterns

See also the [Testing Strategy](testing.md#durableephemeral-interaction) document for the
full test matrix.

The `durable_ephemeral` tests (`tests/tests/durable_ephemeral.rs`) exercise the
four core scenarios:

- **Ephemeral writes, durable receives:** Two concurrent transactions on the
  ephemeral node produce a DAG fork (events B and C both parented to A). The
  durable node receives both and verifies a two-element head `[B, C]`.

- **Durable writes, ephemeral observes:** Concurrent writes on the durable node.
  The ephemeral node fetches and verifies convergence via `StateSnapshot` or
  `EventBridge`.

- **Cross-node concurrent write:** Both durable and ephemeral write concurrently
  from the same head. Both converge to the same merged state after propagation.

- **Late-arriving branch:** A 20-event linear history is built on the durable
  node. The ephemeral node receives the state, then creates a concurrent branch.
  The durable node merges correctly without budget exhaustion.

The `multi_ephemeral` tests (`tests/tests/multi_ephemeral.rs`) extend this to
topologies with multiple ephemeral nodes connected to a single durable node,
verifying that independent writes, [same-property conflicts](lww-merge.md#per-property-independence), and three-way races
all converge deterministically across all participants.
