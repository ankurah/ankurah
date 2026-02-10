# Event DAG Subsystem

## What is the Event DAG?

Every entity in Ankurah can be mutated concurrently by multiple nodes. Each
mutation produces an **event** whose parent pointer records what the node
believed the entity's latest state to be. Over time these events form a
**directed acyclic graph** -- a history that branches when nodes mutate in
parallel and reconverges when branches are merged.

```text
        A          linear history
        |
        B          B's parent is A
       / \
      C   D        C and D were created concurrently (both parent B)
       \ /
        E          E merges the two branches
```

The event DAG subsystem answers two questions:

1. **How do two points in history relate?** Given the entity's current head and
   an incoming event, are they linearly ordered, or have they diverged?
2. **If they diverged, what happened since they last agreed?** It produces a
   topologically sorted sequence of event layers that
   [property backends](property-backends.md) use to merge concurrent operations.

The implementation lives in `core/src/event_dag/` and is consumed primarily by
[`Entity::apply_event`](entity-lifecycle.md#apply_event-in-detail).


## Key Concepts

**Event** -- A single mutation to an entity. Carries a parent clock (the
entity's head when the event was created) and a set of backend-specific
operations. An event with an empty parent clock is a **creation event**
(genesis).

**Clock** -- An ordered set of event IDs representing a frontier in the DAG. An
entity's **head** is a clock: usually a single event ID (linear history), but
multiple IDs when concurrent branches coexist.

**Meet point** -- The greatest common ancestor(s) of two diverged clocks. The
meet is itself a frontier: no member is an ancestor of another. Everything
between the meet and the branch tips needs to be merged.

```text
       A
       |
       B  <-- meet point
      / \
     C   D
      \ /
       E
```

**Event layers** -- After finding the meet, events above it are partitioned into
topological layers for merge. Each layer contains events whose dependencies are
satisfied by earlier layers. See [LWW merge](lww-merge.md#layer-computation)
for how property backends consume these layers.


## Comparing Two Clocks

The comparison algorithm (`core/src/event_dag/comparison.rs`) determines the
causal relationship between a subject clock and a comparison clock. There are
five possible outcomes:

| Outcome | Meaning | Action |
|---------|---------|--------|
| Equal | Same point in history | No-op |
| StrictDescends | Subject is strictly newer | Fast-forward apply |
| StrictAscends | Subject is strictly older | No-op (already integrated) |
| DivergedSince | Concurrent branches since a meet | Merge via layers |
| Disjoint | Unrelated histories (different genesis) | Reject |

If the traversal budget is exhausted before reaching a conclusion, the result is
`BudgetExceeded` (see [Budget escalation](#budget-escalation) below).

### Quick-check: the linear-extension fast path

The overwhelmingly common case is that an incoming event extends the current
head by exactly one step. Before launching a full traversal, the algorithm
checks whether every member of the comparison clock appears directly in the
subject event's parent set. If so, it returns `StrictDescends` immediately
**without fetching the comparison events at all**.

This matters for [ephemeral nodes](node-architecture.md#durable-vs-ephemeral-nodes),
where the comparison head events may not exist in local storage -- the
quick-check never needs them.

### BFS traversal

When the quick-check does not apply, the algorithm walks backward through the
DAG from both clocks simultaneously:

1. Initialize two **frontiers**, one from the subject clock and one from the
   comparison clock.
2. At each step, fetch every event on both frontiers, record its DAG structure,
   and extend the frontiers backward through parent pointers.
3. When an event is reached from both directions, it becomes a **meet
   candidate**.
4. After each step, check whether a conclusion can be drawn:
   - All comparison heads seen by the subject traversal: `StrictDescends`.
   - All subject heads seen by the comparison traversal: `StrictAscends`.
   - Both frontiers empty: compute the minimal meet (candidates with no common
     descendants in the traversal), return `DivergedSince` or `Disjoint`.

### Unfetchable events on both frontiers

On ephemeral nodes, historical events may live only on the durable peer. If an
event ID appears on **both** frontiers but cannot be fetched, it is a common
ancestor beyond local storage. The algorithm processes it with empty parents,
correctly terminating traversal at that point. An unfetchable event on only
**one** frontier is a genuine error -- the DAG is incomplete.

### Budget escalation

The initial budget (default 1000 events) caps each BFS attempt. If exhausted,
the algorithm internally retries with 4x budget (up to `initial * 4`), reusing
the accumulated DAG structure and LRU cache as a warm start. This avoids
re-fetching events on retry and keeps the public API simple -- callers do not
need to manage retry logic.


## The Staging Pattern

An incoming event must be **discoverable by BFS** before the entity's head is
updated to reference it. Otherwise, a concurrent traversal starting from the
new head would encounter an unfetchable event. The staging pattern solves this
with a four-phase lifecycle:

```text
  stage_event -----> apply_event -----> commit_event -----> set_state
  (in-memory map)   (head update)      (durable storage)   (durable state)
```

1. **Stage** -- Place the event in an in-memory map so that `get_event` can
   find it during BFS.
2. **Apply** -- Compare the event against the entity head and update in-memory
   state on success.
3. **Commit** -- Write the event to permanent storage and remove it from the
   staging map.
4. **Persist state** -- Write the entity's head clock and backend buffers to
   disk.

### The ordering invariant

> **Stage before head update (in memory); commit before state persist (to disk).**

The first half ensures BFS reachability. The second half ensures crash safety:

- Crash after commit but before state persist: recovery loads the old entity
  state; the event is in storage but unreferenced by the head, so the next
  `apply_event` integrates it normally via BFS.
- Crash before commit: neither event nor updated state are persisted -- a clean
  rollback.

### Trait separation enforces the protocol

The [retrieval layer](retrieval.md) splits event access into distinct traits so
that `apply_event` (which takes [`GetEvents`](retrieval.md#why-three-traits-instead-of-one)) cannot
accidentally stage or commit events. Only the outer caller holds
[`SuspenseEvents`](retrieval.md#why-three-traits-instead-of-one), which adds `stage_event` and
`commit_event`. This makes the staging protocol a compile-time guarantee rather
than a convention.

The distinction between `get_event` (union of staging + storage) and
`event_stored` (permanent storage only) enables the
[creation-event guard](entity-lifecycle.md#guard-ordering): on
[durable nodes](node-architecture.md#durable-vs-ephemeral-nodes) where storage
is definitive, `event_stored() == false` for a creation event proves it has
never been seen, enabling a cheap rejection without BFS.


## Invariants

### BFS correctness

1. **Both-frontiers = common ancestor.** If an event appears on both frontiers,
   treat it as a meet point with empty parents. Do not require fetching it. This
   is essential for ephemeral nodes where the meet event is not in local storage.

2. **Single-frontier unfetchable events are hard errors.** The old "dead end"
   `continue` behavior left unfetchable IDs permanently on the frontier, causing
   infinite loops.

3. **Never compute layers from an incomplete traversal.** `BudgetExceeded` means
   the accumulated DAG is partial; layer computation would produce incorrect
   merge results.

4. **Meet filter: `common_child_count == 0`.** This ensures head tips always
   appear in the meet set (they have no descendants in the comparison, so they
   always pass the filter). Deep ancestors that also pass produce harmless no-op
   removals from the head.

### Staging protocol

5. **Stage before head update; commit before state persist.** Violating the
   first causes BFS failures; violating the second breaks crash recovery.

6. **`get_event` is the union view; `event_stored` is permanent-only.** Mixing
   these up breaks creation-uniqueness semantics.

7. **Creation guards execute before the retry loop** in `apply_event`. They are
   properties of the event, not of the current head, and must not be
   re-evaluated on retry.


## Design Decisions

### Why `compare` uses the event's own ID, not its parent clock

The original algorithm started BFS from the incoming event's **parent** clock,
then applied a transform to infer the event's causal relationship. This assumed
the event was genuinely novel. Re-delivery of a historical event violated the
assumption, causing head corruption:

```text
Chain: A -> B -> C   head=[C]
Re-deliver B:
  Old algorithm: BFS from parent(B)=[A] vs head=[C]
    -> StrictAscends, transform -> DivergedSince(meet=[A])
    -> Inserts B into head -> invalid head [C, B]
```

The fix: stage the event first, then pass the event's own ID as the subject
clock. BFS discovers the staged event, traverses its parents naturally, and
produces the correct `StrictAscends` for re-delivery with no special-case logic.

### Why the `DuplicateCreation` guard was removed

An early guard used `event_stored()` to detect duplicate creation events. On
[durable nodes](node-architecture.md#durable-vs-ephemeral-nodes) this works, but
on [ephemeral nodes](node-architecture.md#durable-vs-ephemeral-nodes) that
receive entities via `StateSnapshot`, the creation event is never individually
committed -- only the entity state referencing it is stored. When the creation
event later arrives via subscription, `event_stored()` returns `false`,
misclassifying a legitimate re-delivery as a different genesis.

The guard was removed. `compare()` already handles both cases correctly:
re-delivery yields `StrictAscends`; different genesis yields `Disjoint`. On
durable nodes, [`storage_is_definitive()`](retrieval.md#why-three-traits-instead-of-one) restores the
cheap fast path.

### Why the retrieval traits were split

The original monolithic `Retrieve` trait combined event access, state access,
and staging. This made it impossible to express "read-only event access" at the
type level. Since `apply_event` must not stage or commit events (the caller
manages that), the split into [`GetEvents`](retrieval.md#why-three-traits-instead-of-one),
[`GetState`](retrieval.md#why-three-traits-instead-of-one), and
[`SuspenseEvents`](retrieval.md#why-three-traits-instead-of-one) turns the staging protocol into
a compile-time constraint. See the
[Retrieval and Storage Layer](retrieval.md) documentation for details.
