# Offline-First Entity Subscriptions V1

## Goal

Make ephemeral-node `get` behave like an offline-first read that can stay fresh after reconnect, without yet expanding the same implicit entity-subscription behavior to `fetch` or `query`.

This is a focused follow-on to the cached-root / cached-livequery work. The intent is to improve the semantics of single-entity reads first, then revisit broader read APIs in a later phase.

## Motivation

Today:

- `get` on an ephemeral node is effectively remote-first.
- `get_cached` exists as a semver-preserving escape hatch.
- `fetch(cached = true)` now falls back to local cache when no durable peer is available.
- Holding an entity locally does **not** imply that the entity stays subscribed to updates from a durable peer.

This leads to unintuitive behavior:

- `get` may fail or block on a weak connection even when a usable local entity already exists.
- a locally resident entity can silently go stale after the query/livequery that originally hydrated it is dropped.

The first problem can be improved with broader cached fallback. The second requires an explicit entity-subscription protocol.

## V1 Scope

V1 adds implicit **entity-level remote subscriptions for `get` only**.

Included:

- new remote protocol for entity subscription
- node-level tracking of resident entities on ephemeral nodes
- buffered batched `UnsubscribeEntities`
- per-entity reference counting so a remote entity subscription is held while the entity remains resident

Explicitly out of scope:

- implicit entity subscriptions for `fetch`
- implicit entity subscriptions for `query` / `SubscribeQuery`
- changing the public `get` / `get_cached` API shape
- altering `query_wait` / cached livequery semantics
- protocol compression beyond unsubscribe range batching

## Protocol

### Subscribe

Add a request/response pair for entity subscription.

```rust
NodeRequestBody::SubscribeEntity {
    collection: CollectionId,
    ids: Vec<EntityId>,
    known_heads: Vec<KnownEntity>,
}
```

Response shape should mirror the existing known-heads delta model used by `Fetch` / `SubscribeQuery`, so the server can return either:

- nothing, if the client already has the current head
- an event bridge
- a full state snapshot

Possible response:

```rust
NodeResponseBody::EntitiesSubscribed {
    deltas: Vec<EntityDelta>,
}
```

### Unsubscribe

Add a best-effort one-way message for teardown.

```rust
NodeMessage::UnsubscribeEntities {
    from: EntityId,
    collection: CollectionId,
    ranges: Vec<EntityIdRange>,
}
```

Where:

```rust
struct EntityIdRange {
    start: EntityId,
    end: EntityId,
}
```

`ranges` are a compression format over the peer-local subscribed resident set, not a claim about dense global entity-id allocation.

Server behavior:

- for this peer and collection, remove any currently subscribed entity IDs that fall within the provided inclusive ranges
- missing IDs are a no-op

## Lifetime Model

Entity subscription ownership should live in `NodeInner`, not in `Entity` itself.

Rationale:

- `NodeInner` already owns peer connectivity, request lifecycles, and subscription relay concerns
- `Entity` is a thin state handle and is a poor place to hang network teardown logic directly
- node-level ownership makes batching, debouncing, and refcount-based cancellation straightforward

### Node-owned state

For ephemeral nodes, maintain:

- a resident/refcount map keyed by `(CollectionId, EntityId)`
- an ordered subscribed-entity set per durable peer (or a single active durable peer in V1 if the implementation assumes one)
- a pending-unsubscribe ordered set

### Reference-count semantics

On first resident reference (`0 -> 1`):

- ensure the entity is remotely subscribed

On last resident reference (`1 -> 0`):

- enqueue entity into pending-unsubscribe set
- do not immediately send teardown

If the entity becomes resident again before the debounce fires:

- remove it from pending-unsubscribe

## Buffering / Batching

Default debounce: `100ms`

Rationale:

- small enough to avoid visibly stale teardown
- large enough to coalesce common drop bursts from UI churn and query/view lifecycle changes

At flush time:

1. take pending-unsubscribe ordered set
2. coalesce adjacent entries in that ordered set into inclusive `EntityIdRange`s
3. send one `UnsubscribeEntities` message per collection

Range adjacency is defined over the ordered subscribed resident set for that node/peer, not over a requirement that every raw ULID between endpoints exists globally.

## Server-side handling

`SubscriptionHandler` already has local entity-subscription support through the reactor subscription:

- `add_entity_subscriptions(...)`
- `remove_entity_subscriptions(...)`

V1 should reuse that machinery rather than inventing a second update path.

Expected flow:

- `SubscribeEntity` applies initial deltas and then registers entity subscriptions on the peer's `SubscriptionHandler.subscription()`
- subsequent entity changes are delivered through the existing `SubscriptionUpdate` stream
- `UnsubscribeEntities` removes those entity subscriptions

## Client-side `get` semantics after V1

Desired behavior for an ephemeral node:

- if local entity exists, return it immediately
- if durable peer is available, ensure entity subscription is established in the background
- if local entity does not exist, fetch/subscribe from durable peer
- if network is weak but local entity exists, return local entity and allow background retry/recovery

This should make `get` substantially more DWIM for an offline-first system even before the public API is cleaned up in a later semver bump.

## Deferred Work

Later phases may:

- make `fetch` implicitly entity-subscribe its result entities
- make `SubscribeQuery` also retain entity freshness after query drop
- collapse `get_cached` into a cleaner public API in a minor semver bump
- broaden cached fallback for transient request failures beyond `NoDurablePeers`
- tune or expose the unsubscribe debounce

## Tests To Add With V1

- `get` on ephemeral node hydrates an entity and then continues receiving updates after the originating livequery is dropped
- dropping the last resident local holder eventually causes remote unsubscribe
- unsubscribe debounce cancels correctly if the entity becomes resident again before flush
- `UnsubscribeEntities` range batching removes the correct set of subscribed IDs and ignores missing IDs
