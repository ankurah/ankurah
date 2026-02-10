# Event DAG Subsystem

## Overview

Ankurah entities are replicated across multiple nodes that may concurrently apply
mutations. Each mutation produces an **event** whose `parent` clock records the
entity's head at the time the mutation was created. The set of all events for an
entity forms a **directed acyclic graph** (DAG) in which edges point from child
events to their parents. The event DAG subsystem is responsible for:

1. Determining the causal relationship between any two points in this DAG.
2. When two branches have diverged, producing a topologically ordered sequence
   of **layers** so that [property backends](property-backends.md) can merge
   concurrent operations.
3. Providing the caching and retrieval infrastructure that the comparison
   algorithm needs to traverse the DAG efficiently.

The subsystem lives in `core/src/event_dag/` and is consumed primarily by
[`Entity::apply_event`](entity-lifecycle.md#apply_event-in-detail) and
[`Entity::apply_state`](entity-lifecycle.md#apply_state-in-detail) in
`core/src/entity.rs`.


## Core Concepts

**Event** (`ankurah_proto::Event`) -- A mutation to a single entity. Carries an
`entity_id`, a `parent` clock (the entity head when the event was created), and
an `OperationSet` of backend-specific operations. An event with an empty parent
clock is a **creation event** (genesis).

**Clock** (`ankurah_proto::Clock`) -- An ordered set of `EventId`s representing a
frontier in the DAG. An entity's **head** is a clock: normally a single event ID
(linear history) but potentially multiple IDs when concurrent branches have been
merged.

**Head** -- The current clock of an entity. Updated in memory by `apply_event`
and persisted to disk by `set_state`. A multi-element head means the entity
has integrated concurrent branches.

**Frontier** (`event_dag::frontier::Frontier`) -- A `BTreeSet<EventId>` used
during BFS traversal. The comparison algorithm maintains two frontiers (subject
and comparison), expanding them backward through parent pointers.

**Meet point** -- The greatest common ancestor(s) of two diverged clocks,
identified during comparison. The meet is itself a frontier: no meet member is
an ancestor of another. Events above the meet are partitioned into layers for
merge application.

**EventAccumulator** (`event_dag::accumulator::EventAccumulator`) -- Owns a
[`GetEvents`](retrieval.md#getevents) implementation and accumulates DAG
structure (event-id-to-parent-ids mapping) plus an LRU cache of full `Event`
bodies during BFS. After comparison, the accumulated DAG is consumed to produce
`EventLayers`.

**EventLayers** (`event_dag::accumulator::EventLayers`) -- An async iterator
over [`EventLayer`](property-backends.md#eventlayer-helpers) values, computed by
forward topological expansion from the meet. Each layer is a set of events with
no unprocessed in-DAG ancestors relative to earlier layers.

**ComparisonResult** (`event_dag::accumulator::ComparisonResult`) -- The return
type of `compare()`. Bundles an `AbstractCausalRelation` with the
`EventAccumulator` that produced it, so callers can call `into_layers()` on
divergence results without re-traversing the DAG.

**AbstractCausalRelation** (`event_dag::relation::AbstractCausalRelation`) --
The five possible outcomes of comparing two clocks:

| Variant | Meaning | Typical action |
|---------|---------|----------------|
| `Equal` | Same lattice point | No-op |
| `StrictDescends` | Subject is strictly newer | Apply subject's ops (fast path) |
| `StrictAscends` | Subject is strictly older | No-op |
| `DivergedSince { meet, .. }` | True concurrency since meet | Merge via layers |
| `Disjoint` | Different genesis events | Reject (lineage error) |
| `BudgetExceeded` | Traversal budget exhausted | Error after internal escalation |


## The Comparison Algorithm

Entry point: `event_dag::comparison::compare()`.

```rust
pub async fn compare<E: GetEvents>(
    event_getter: E,
    subject: &Clock,
    comparison: &Clock,
    budget: usize,
) -> Result<ComparisonResult<E>, RetrievalError>
```

### Quick-check optimization

Before starting BFS, `compare` fetches the subject's events and collects their
parent IDs. If every comparison-clock member appears in that parent set, the
subject is exactly one step ahead of the comparison -- the common case of a
**linear extension**. This returns `StrictDescends` immediately and, crucially,
avoids fetching the comparison events at all. This is what makes ephemeral-node
commits work: the comparison head events may not exist in local storage, but the
quick-check never needs to fetch them.

### BFS traversal

If the quick-check does not fire, `compare` enters a loop driven by a
`Comparison` state machine:

1. **Initialize** two frontiers from the subject and comparison clocks.
2. **Step**: for each ID on both frontiers, fetch the event via
   `EventAccumulator::get_event`, call `accumulate` to record its DAG
   structure, then call `process_event` which:
   - Removes the ID from whichever frontier(s) contained it.
   - Records which traversal direction(s) have seen it (`seen_from_subject`,
     `seen_from_comparison`).
   - Adds its parents to the appropriate frontier, extending the backward walk.
   - If the event is now seen from both directions, marks it as a meet
     candidate and increments `common_child_count` on its parents.
   - Tracks genesis events (empty parents) for `Disjoint` detection.
3. **Check result**: after each step, `check_result` examines the state:
   - If all comparison heads have been seen by the subject traversal:
     `StrictDescends`.
   - If all subject heads have been seen by the comparison traversal:
     `StrictAscends`.
   - If both frontiers are empty: compute the minimal meet (candidates with
     `common_child_count == 0`), then return `DivergedSince` or `Disjoint`.
   - If the budget is zero: `BudgetExceeded`.
   - Otherwise: continue.

### Unfetchable events and both-frontiers meet

When `get_event` returns `EventNotFound`, the algorithm checks whether the event
ID appears on **both** frontiers simultaneously. If so, it is a common ancestor
that is unreachable from local storage (typical on ephemeral nodes where
historical events live on the durable peer). The algorithm processes it with
empty parents, correctly terminating the traversal at that point. If the event
is on only one frontier, `EventNotFound` is returned as a hard error -- the DAG
is genuinely incomplete.

### Budget escalation

The initial budget (default 1000) limits the number of events fetched per BFS
attempt. If exhausted, `compare` internally retries with 4x budget (up to
`initial * 4`), reusing the same `EventAccumulator` so that its DAG structure
and LRU cache provide a warm start. The `E: GetEvents` does not need to be
`Clone`; it stays owned by the accumulator throughout.


## The Staging Pattern

### Lifecycle of an event

An incoming event passes through four stages:

1. **`stage_event(event)`** -- Places the event in an in-memory staging map
   (`Arc<RwLock<HashMap<EventId, Event>>>`). From this point, `get_event` will
   find it, so BFS can discover and traverse it.

2. **`apply_event(getter, event)`** -- Compares the event's clock against the
   entity's head. On success, updates the entity's head and backend state **in
   memory**. The staged event is what makes this work: `compare` uses the
   event's own ID as the subject clock, and BFS finds the event body via
   staging.

3. **`commit_event(attested)`** -- Writes the attested event to permanent
   storage and removes it from the staging map. After this call,
   `event_stored()` returns `true` for this event.

4. **`set_state(attested_state)`** -- Persists the entity's state (head clock +
   backend buffers) to disk. This is the final durability boundary.

### Why this ordering matters

The critical ordering invariant is:

> **`stage_event` before head update (in memory); `commit_event` before
> `set_state` (to disk).**

The first half ensures that when `apply_event` updates the head to include the
new event, any subsequent BFS traversal starting from that head can fetch the
event (from staging or storage). Without this, a concurrent `apply_event` could
see a head referencing an unfetchable event.

The second half ensures crash safety. If the process crashes after
`commit_event` but before `set_state`, recovery loads the old entity state --
the event is in storage but the head does not reference it, so the next
`apply_event` for the same event will integrate it normally via BFS. If the
process crashes before `commit_event`, neither the event nor the updated state
are persisted, which is a clean rollback.

### The trait split

The monolithic `Retrieve` trait was split into three focused traits to enforce
the staging protocol at the type level (see [Retrieval and Storage Layer](retrieval.md)
for full API documentation):

- **[`GetEvents`](retrieval.md#getevents)** -- `get_event` (union of staging + storage) and
  `event_stored` (permanent storage only). This is what [`apply_event`](entity-lifecycle.md#apply_event-in-detail) and the
  comparison algorithm accept: read-only event access.
- **[`GetState`](retrieval.md#getstate)** -- `get_state` for entity state snapshots. Separated because
  state retrieval has different caching and lifetime requirements than event
  retrieval.
- **[`SuspenseEvents`](retrieval.md#suspenseevents)** -- Extends `GetEvents` with `stage_event` and
  `commit_event`. Only the caller (e.g., `node_applier`) holds this; it is
  **not** passed into `apply_event`, ensuring that `apply_event` cannot
  accidentally commit or stage.

### `get_event` vs `event_stored`

`get_event` is the **union view**: it checks the staging map first, then falls
back to permanent storage (and on [`CachedEventGetter`](retrieval.md#cachedeventgetter), to a remote peer).
`event_stored` checks **permanent storage only**. This distinction is used by
the [creation-event guard](entity-lifecycle.md#guard-ordering) in `apply_event`: on [durable nodes](node-architecture.md#durable-vs-ephemeral-nodes) where
`storage_is_definitive()` returns `true`, `event_stored() == false` for a
creation event proves it has never been seen, enabling a cheap `Disjoint`
rejection without BFS.


## Event Application

[`Entity::apply_event`](entity-lifecycle.md#apply_event-in-detail) in `core/src/entity.rs` is the main entry point for
integrating a new event into an entity. See [Entity Lifecycle](entity-lifecycle.md)
for the full lifecycle context.

### Guard ordering

Three guards execute **before** the retry loop, in this order:

1. **Creation event on non-empty head (durable fast path):** If the event is a
   creation event and the head is non-empty, check `event_stored`. On durable
   nodes, `true` means re-delivery (no-op), `false` means a different genesis
   (reject as `Disjoint`). On ephemeral nodes, fall through to BFS.

2. **Creation event on empty head (mutex-protected):** Acquires the state write
   lock, re-checks that the head is still empty, and if so, applies the
   creation event directly. This avoids a TOCTOU race between checking
   `head.is_empty()` and modifying the head.

3. **Non-creation event on empty head:** The entity was never created. Reject
   with `InvalidEvent` rather than allowing BFS to produce a spurious
   `DivergedSince(meet=[])`.

### The retry loop

After guards pass, `apply_event` enters a retry loop (up to 5 attempts):

1. Call `compare(getter, &event_id_as_clock, &head, budget)`.
2. Match on `ComparisonResult::relation`:

| Result | Action |
|--------|--------|
| `Equal` | No-op, return `Ok(false)` |
| `StrictDescends` | Apply operations, replace head with event ID, under `try_mutate` |
| `StrictAscends` | Incoming is older, return `Ok(false)` |
| `DivergedSince` | Decompose result, compute [layers](lww-merge.md#layer-computation), apply all layers under write lock, update head by removing meet IDs and inserting event ID |
| `Disjoint` | Return `Err(LineageError::Disjoint)` |
| `BudgetExceeded` | Return `Err(LineageError::BudgetExceeded)` |

The `try_mutate` helper provides TOCTOU protection: it acquires the write lock,
verifies the head has not changed since comparison, then executes the mutation.
If the head moved, it updates the caller's `head` variable and returns
`Ok(false)`, causing the loop to retry with fresh lineage info.

### DivergedSince head update

When merging concurrent branches, the head update removes the meet IDs from the
current head and inserts the new event's ID. The `common_child_count == 0`
filter on meet candidates guarantees that every head tip the new event descends
from appears in the meet set. Deep common ancestors that are not head tips may
also appear in the meet, but removing them from the head is a harmless no-op
(they are not there).


## Invariants

### Algorithmic (BFS correctness)

1. **No `E: Clone`.** Budget escalation is internal to `Comparison`. The event
   getter stays owned by the accumulator, never cloned.

2. **Both-frontiers = common ancestor.** If an event ID appears on both the
   subject and comparison frontiers during BFS, it is a meet point. Process it
   with empty parents; do not require fetching it. This handles ephemeral nodes
   where the meet event is not in local storage.

3. **Unfetchable single-frontier events are errors.** `EventNotFound` for an
   event on only one frontier must return `Err`, not silently `continue`. The
   old "dead end" `continue` behavior caused infinite busyloops by leaving
   unfetchable IDs permanently on the frontier.

4. **Never call `into_layers()` on `BudgetExceeded`.** The accumulated DAG is
   incomplete; layer computation would produce incorrect results.

5. **Meet filter guarantees correctness.** The `common_child_count == 0` filter
   on meet candidates ensures that head tips always appear in the meet (head
   tips have no descendants in the comparison clock, so no child of a head tip
   can be "common," so head tips always pass the filter). Deep ancestors produce
   harmless no-op removals.

### Integration contracts (staging/storage protocol)

6. **`stage_event` before head update (in memory); `commit_event` before
   `set_state` (to disk).** The event must be discoverable by BFS at the time
   the head is updated. The event must be in permanent storage before the
   entity state is persisted.

7. **`get_event` is the union view (staging + storage). `event_stored` is
   permanent-only.** Confusing these breaks creation-uniqueness semantics.

8. **Blanket `&R` impls are required.** Nearly every call site passes event
   getters by reference. Without `impl GetEvents for &R`, the code does not
   compile.

9. **Idempotency and creation guards go before the retry loop** in
   `apply_event`, not inside it. They are properties of the event, not of
   the current head, and must not be re-evaluated on retry.

10. **Disjoint genesis is detected by `compare()`, not by early guards.** Two
    different creation events (both with `parent=[]`) produce `Disjoint` from
    BFS. Re-delivery of the same creation event produces `StrictAscends`. The
    `DuplicateCreation` early guard was removed because `event_stored()` is
    unreliable on ephemeral nodes that receive entities via `StateSnapshot`.

11. **`storage_is_definitive()` enables cheap disjoint rejection on durable
    nodes.** When `true`, `event_stored() == false` definitively means the
    event does not exist, so a creation event on a non-empty head can be
    rejected immediately as `Disjoint` without BFS. Default is `false` (safe
    for ephemeral nodes).


## Design Decisions

### Eliminating `compare_unstored_event`

The original algorithm compared an incoming event by starting BFS from its
**parent** clock, then applied a transform to infer the event's own
relationship. This transform assumed the event was genuinely novel. Re-delivery
of a historical event violated this assumption, causing head corruption: e.g.,
chain A-B-C with head=[C], re-deliver B, BFS from parent(B)=[A] vs head=[C]
yields `StrictAscends`, transform produces `DivergedSince(meet=[A])`, handler
inserts B into head, producing the invalid head [C, B].

The fix was to stage the event first, then call `compare` with the event's own
ID as the subject clock. BFS finds the staged event, traverses its parents
naturally, and produces the correct result (`StrictAscends` for re-delivery)
with no special-case transform.

### Removing the `DuplicateCreation` guard

An early guard in [`apply_event`](entity-lifecycle.md#apply_event-in-detail) used `event_stored()` to detect duplicate
creation events. On [durable nodes](node-architecture.md#durable-vs-ephemeral-nodes) this works: if the creation event is in
storage, it is a re-delivery; if not, it is a different genesis. On [ephemeral
nodes](node-architecture.md#durable-vs-ephemeral-nodes) that receive entities via `StateSnapshot`, the creation event is never
individually committed to event storage -- only the entity state (including the
head clock referencing the creation event) is persisted. When the creation event
later arrives via subscription, `event_stored()` returns `false`, causing a
legitimate re-delivery to be misclassified as an attack.

The guard was removed. `compare()` already handles both cases: re-delivery
yields `StrictAscends` (no-op); different genesis yields `Disjoint` (error).
On durable nodes, `storage_is_definitive()` restores the cheap fast path.

### Splitting `Retrieve` into focused traits

The original monolithic `Retrieve` trait combined event access, state access,
and staging in a single interface. This made it impossible to express "read-only
event access" at the type level, which matters because `apply_event` must not
commit or stage events (the caller manages staging). The split into [`GetEvents`](retrieval.md#getevents),
[`GetState`](retrieval.md#getstate), and [`SuspenseEvents`](retrieval.md#suspenseevents) enforces this: `apply_event` takes
`E: GetEvents`; the caller holds `E: SuspenseEvents` and calls `stage_event` /
`commit_event` around the `apply_event` call.

### The `storage_is_definitive()` method

[Ephemeral nodes](node-architecture.md#durable-vs-ephemeral-nodes) may have entity state (head clock) without having the underlying
events in storage, because `StateSnapshot` delivery establishes state without
storing individual events. On such nodes, `event_stored()` returning `false`
does not mean the event has never been seen -- it might have been integrated via
state snapshot. `storage_is_definitive()` lets [durable nodes](node-architecture.md#durable-vs-ephemeral-nodes) (which always have
events backing their state) opt into the cheaper check, while ephemeral nodes
default to the safe BFS path.
