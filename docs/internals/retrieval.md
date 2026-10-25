# Event Retrieval and Staging

The retrieval layer is the bridge between the [event DAG](event-dag.md) and
persistent storage. Its job is to answer two questions during event
processing: "give me this event's data" and "has this event already been
durably stored?" These sound similar but have deliberately different
semantics, and the layer's design follows from keeping them separate. The
durable backends it sits on top of -- the traits, the engine matrix, and the
predicate fetch path -- are documented separately in the
[Storage Engine Layer](storage-engines.md).


## Why Three Traits Instead of One

The system originally had a monolithic `Retrieve` trait that bundled event
reading, state reading, and staging/committing into a single interface. The
problem was that [`apply_events`](entity-lifecycle.md#how-events-are-applied)
-- the core integration function -- only needs read access to events, but
receiving a `Retrieve` reference gave it the *ability* to stage or commit
events. That is the caller's responsibility, not `apply_events`'s.

The split enforces staging discipline at the type level:

- **`GetEvents`** -- Read-only event access. This is what the
  [comparison algorithm](event-dag.md#generation-guided-traversal) and `apply_events`
  accept. It exposes `get_event` (the union of staging and permanent
  storage), `event_stored` (permanent storage only), and
  `storage_is_definitive` (whether a negative `event_stored` result is
  authoritative).

- **`GetState`** -- Entity state snapshot retrieval. Separated because state
  has different caching characteristics and is not needed by comparison.

- **`SuspenseEvents`** -- Extends `GetEvents` with `stage_event`. Only the
  outermost caller (e.g., `NodeApplier`) holds this; it is deliberately *not*
  passed into `apply_events`. Durable event writes go through `StorageTransaction`.


## Staging vs Permanent Storage

The retrieval layer maintains two tiers of event storage:

**The staging map** is an in-memory `HashMap` behind an `Arc<RwLock>`. When
an event is staged, it becomes visible to `get_event` and therefore to comparison
traversal, but `event_stored` will still return `false`. Staging is what
makes the [comparison algorithm](event-dag.md#comparing-two-clocks) work
on incoming events: the event is staged first, then `compare` uses the
event's own ID as the subject clock, and comparison discovers the event body
through the staging map.

**Permanent storage** is the durable backend. After validating and attesting
the complete logical event set, the caller includes it in the canonical state
CAS batch. After that batch commits, `event_stored` returns `true`. The
short-lived getter and its staging map are dropped when that ingress operation
finishes.

The distinction between `get_event` (union view) and `event_stored`
(permanent-only) matters for the
[creation-event guard](entity-lifecycle.md#guard-ordering): on
[durable nodes](node-architecture.md#durable-vs-ephemeral-nodes) where
`storage_is_definitive()` is `true`, a `false` from `event_stored` for a
creation event proves it has never been seen, enabling a cheap rejection
without comparison. On [ephemeral nodes](node-architecture.md#durable-vs-ephemeral-nodes),
this shortcut is unsafe because entities can arrive via `StateSnapshot`
without their individual events being stored.


## Durable vs Ephemeral Lookup Strategies

The two concrete implementations of the retrieval traits differ in how far
they search for a missing event:

**`LocalEventGetter`** -- Used for local commits on all node types.
Checks the staging map, then local storage. If the event is not found
locally, that is a hard error. Sets `storage_is_definitive` based on the
`durable` flag passed at construction.

**`CachedEventGetter`** -- Used by [ephemeral nodes](node-architecture.md#durable-vs-ephemeral-nodes).
Adds a third lookup tier: if the event is not in staging or local storage,
it requests the event from a random durable peer, caches the response
locally, and returns it. This transparent remote fallback is what allows
[comparison](event-dag.md#generation-guided-traversal) to succeed on ephemeral nodes that lack
historical events. `storage_is_definitive` is always `false`.

**`LocalStateGetter`** -- Shared by both paths. Wraps storage to retrieve
entity state snapshots, translating "not found" into `Ok(None)`.


## The Event Lifecycle: Stage, Apply, Commit

A typical flow in [`NodeApplier::apply_update`](node-architecture.md#streaming-updates-updatecontent)
for `EventOnly` content:

```text
for each event:
    validate(event)
    event_getter.stage_event(event)            // (1) stage

events = topological_sort(events)
entity = get_or_create(...)

entity.apply_events(event_getter, &mut events) // (2) compare once, then apply and check each event

storage_trx = storage.transaction()
storage_trx.add_events(events)
storage_trx.set_state(expected_head, entity_state)
storage_trx.commit()                           // (3) atomic history and state
publish(entity)
```

For an existing entity, `StateAndEvent` delegates to `apply_and_commit_state`.
On each attempt, that coordinator forks the resident and either adopts an
extending snapshot or applies the accompanying events. It then persists the
resulting state and every supplied event before publication. A storage conflict
repeats the decision from a fresh fork. A bare snapshot that is not adopted
requires no write.

The same parent-first rule applies to every multi-event wire shape --
`EventOnly` and `StateAndEvent` alike: the
receiver stages the whole batch upfront, topologically sorts it by in-batch
parent edges (`event_dag/ordering.rs`), and only then applies and submits
canonical state. The producer also sorts what it sends, but wire
order is untrusted. Preparation independently rejects any batch in which a
supplied parent follows its child, before applying operations.


## Crash Safety

Transaction events and state share one persistence boundary:

- **Crash before commit:** The staging map is lost; neither the new events nor
  updated states persist.
- **Crash during `StorageTransaction::commit`:** Events, canonical states, associations,
  materializations, and indexes commit together or not at all.
- **Crash after commit, before acknowledgement:** The complete batch is durable;
  re-delivery is idempotent.

- **Concurrent `apply_events` on the same entity:** The
  [`try_mutate`](entity-lifecycle.md#toctou-protection) helper
  checks that the head has not changed since comparison. If it has, the
  caller retries (up to 5 attempts), preventing TOCTOU races.
