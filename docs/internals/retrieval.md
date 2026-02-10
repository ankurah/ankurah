# Retrieval and Storage Layer

The retrieval layer is the bridge between the [event DAG](event-dag.md) and
persistent storage. Its job is to answer two questions during event
processing: "give me this event's data" and "has this event already been
durably stored?" These sound similar but have deliberately different
semantics, and the layer's design follows from keeping them separate.


## Why Three Traits Instead of One

The system originally had a monolithic `Retrieve` trait that bundled event
reading, state reading, and staging/committing into a single interface. The
problem was that [`apply_event`](entity-lifecycle.md#how-events-are-applied)
-- the core integration function -- only needs read access to events, but
receiving a `Retrieve` reference gave it the *ability* to stage or commit
events. That is the caller's responsibility, not `apply_event`'s.

The split enforces staging discipline at the type level:

- **`GetEvents`** -- Read-only event access. This is what the
  [BFS comparison algorithm](event-dag.md#bfs-traversal) and `apply_event`
  accept. It exposes `get_event` (the union of staging and permanent
  storage), `event_stored` (permanent storage only), and
  `storage_is_definitive` (whether a negative `event_stored` result is
  authoritative).

- **`GetState`** -- Entity state snapshot retrieval. Separated because state
  has different caching characteristics and is not needed by BFS.

- **`SuspenseEvents`** -- Extends `GetEvents` with `stage_event` and
  `commit_event`. Only the outermost caller (e.g., `NodeApplier`) holds
  this; it is deliberately *not* passed into `apply_event`.


## Staging vs Permanent Storage

The retrieval layer maintains two tiers of event storage:

**The staging map** is an in-memory `HashMap` behind an `Arc<RwLock>`. When
an event is staged, it becomes visible to `get_event` and therefore to BFS
traversal, but `event_stored` will still return `false`. Staging is what
makes the [comparison algorithm](event-dag.md#comparing-two-clocks) work
on incoming events: the event is staged first, then `compare` uses the
event's own ID as the subject clock, and BFS discovers the event body
through the staging map.

**Permanent storage** is the durable backend. After `apply_event` succeeds,
the caller commits the event -- writing it to permanent storage and removing
it from the staging map. From this point, `event_stored` returns `true`.

The distinction between `get_event` (union view) and `event_stored`
(permanent-only) matters for the
[creation-event guard](entity-lifecycle.md#guard-ordering): on
[durable nodes](node-architecture.md#durable-vs-ephemeral-nodes) where
`storage_is_definitive()` is `true`, a `false` from `event_stored` for a
creation event proves it has never been seen, enabling a cheap rejection
without BFS. On [ephemeral nodes](node-architecture.md#durable-vs-ephemeral-nodes),
this shortcut is unsafe because entities can arrive via `StateSnapshot`
without their individual events being stored.


## Durable vs Ephemeral Lookup Strategies

The two concrete implementations of the retrieval traits differ in how far
they search for a missing event:

**`LocalEventGetter`** -- Used by [durable nodes](node-architecture.md#durable-vs-ephemeral-nodes).
Checks the staging map, then local storage. If the event is not found
locally, that is a hard error. Sets `storage_is_definitive` based on the
`durable` flag passed at construction.

**`CachedEventGetter`** -- Used by [ephemeral nodes](node-architecture.md#durable-vs-ephemeral-nodes).
Adds a third lookup tier: if the event is not in staging or local storage,
it requests the event from a random durable peer, caches the response
locally, and returns it. This transparent remote fallback is what allows
[BFS](event-dag.md#bfs-traversal) to succeed on ephemeral nodes that lack
historical events. `storage_is_definitive` is always `false`.

**`LocalStateGetter`** -- Shared by both paths. Wraps storage to retrieve
entity state snapshots, translating "not found" into `Ok(None)`.


## The Event Lifecycle: Stage, Apply, Commit, Persist

A typical flow in [`NodeApplier::apply_update`](node-architecture.md#streaming-updates-updatecontent)
for `EventOnly` content:

```text
for each event:
    validate(event)
    event_getter.stage_event(event)            // (1) stage

entity = get_or_create(...)

for each attested_event:
    entity.apply_event(event_getter, &event)   // (2) compare + apply in memory
    event_getter.commit_event(&attested_event)  // (3) commit to disk

save_state(entity)                             // (4) persist entity state
```

For `StateAndEvent` content, the flow first tries
[`apply_state`](entity-lifecycle.md#how-state-snapshots-are-applied). If the state is
strictly newer, all staged events are committed and state is saved. If the
state diverges, it falls back to per-event
[`apply_event`](entity-lifecycle.md#how-events-are-applied) followed by
`commit_event`, then saves state.

For `EventBridge` (in `apply_delta`), all events are staged upfront, then
applied and committed in causal order (oldest first), and finally the entity
state is saved.


## Crash Safety

The ordering invariant -- commit events before persisting state -- provides
clean recovery in every failure scenario:

- **Crash after commit, before state save:** The event is in storage but the
  entity state still references the old head. On recovery, the next delivery
  of the same event (or any descendant) integrates it via BFS. No data loss.

- **Crash after stage, before commit:** The staging map is in-memory only
  and lost on crash. Neither the event nor the updated state are persisted.
  Clean rollback.

- **Crash after state save:** Fully consistent. Normal operation.

- **Concurrent `apply_event` on the same entity:** The
  [`try_mutate`](entity-lifecycle.md#toctou-protection) helper
  checks that the head has not changed since comparison. If it has, the
  caller retries (up to 5 attempts), preventing TOCTOU races.
