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
[`Entity::apply_event`](entity-lifecycle.md#how-events-are-applied).


## Key Concepts

**Event** -- A single mutation to an entity. Carries a parent clock (the
entity's head when the event was created), a set of backend-specific
operations, and a mandatory **generation**: its topological depth,
`1 + max(parent generations)`, exactly `1` for a creation event. The event id
is a content hash over `(entity_id, operations, parent, generation)`, so the
generation is sealed inside the event's identity: storage or wire corruption
cannot change it without changing the id. An event with an empty parent clock
is a **creation event** (genesis).

**Clock** -- An ordered set of event IDs representing a frontier in the DAG. An
entity's **head** is a clock: usually a single event ID (linear history), but
multiple IDs when concurrent branches coexist.

**Generation** -- The depth number above, stamped by the committer and
verified at admission wherever the parents are locally resolvable (a mismatch
is a typed rejection; a genesis must claim exactly 1). Being hash-sealed makes
a generation *authentic*, not automatically *correct* -- a buggy or hostile
writer seals its wrong value just as faithfully -- so the comparison consumes
generations only as accelerations that can never affect an outcome (see
[Generation accelerations](#generation-accelerations)). Heads are materialized
with one generation per tip (`GClock`), which is how commit stamping and most
admission verification run without reading event payloads.

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

**Event layers** -- After finding the meet, the history is replayed in
topological generations for merge. See [Event Layers](#event-layers) for the
precise definition and its guarantees, and
[LWW merge](lww-merge.md#the-three-stage-pipeline) for how property backends
consume the layers.


## Comparing Two Clocks

The comparison algorithm (`core/src/event_dag/comparison.rs`) determines the
causal relationship between a subject clock and a comparison clock. There are
six possible outcomes:

| Outcome | Meaning | Action |
|---------|---------|--------|
| Equal | Same point in history | No-op |
| StrictDescends | Subject is strictly newer | Fast-forward apply |
| StrictAscends | Subject is strictly older | No-op (already integrated) |
| DivergedSince | Concurrent branches since a meet | Merge via layers |
| Disjoint | Unrelated histories (different genesis) | Reject |
| BudgetExceeded | Traversal budget exhausted before a conclusion | Error (see [Budget escalation](#budget-escalation)) |

`StrictDescends` carries a `chain` of the events the subject traversal
visited. Its contents are duplicate-free and emitted in **canonical
topological order** (the chain set re-sorted through the shared topological
sorter with sorted input, so the emitted order is a function of the set, not
of the schedule that visited it; pinned by permutation tests). Batch
application paths still sort independently (see
[batch ordering](retrieval.md#the-event-lifecycle-stage-apply-commit-persist)).

### Quick-check: the linear-extension fast path

The overwhelmingly common case is that an incoming event extends the current
head by exactly one step. Before launching a full traversal, the algorithm
checks whether every member of the comparison clock appears directly in the
subject event's parent set. If so, it returns `StrictDescends` immediately
**without fetching the comparison events at all**.

This matters for [ephemeral nodes](node-architecture.md#durable-vs-ephemeral-nodes),
where the comparison head events may not exist in local storage -- the
quick-check never needs them.

When the caller supplies per-tip generation operands, two rejection
prechecks (P1/P2) may prove `StrictDescends` impossible before any fetch and
**suppress** this quick-check attempt -- suppress only, never conclude; see
[Generation accelerations](#generation-accelerations).

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
   - The subject traversal has seen every comparison head **and** its boundary
     is clean (see below): `StrictDescends`.
   - All subject heads seen by the comparison traversal: `StrictAscends`.
   - Both frontiers empty: compute the minimal meet (candidates with no common
     descendants in the traversal), return `DivergedSince` or `Disjoint`.

### Why coverage alone is not enough

Seeing every comparison head proves the subject's cover *contains* the
comparison clock -- but not that the subject introduces nothing else. A
subject clock can smuggle in a foreign lineage two ways: as an extra head
(`[B, X]` versus `[A]`, where `B` descends `A` but `X` has an independent
genesis), or through a single graft event whose parent clock joins a
legitimate ancestor with an unrelated root. Fast-forwarding either shape
would adopt the foreign line wholesale.

The guard is the **clean-boundary check**: before declaring `StrictDescends`,
every genesis root the subject traversal has discovered, and every id still
on its unexplored frontier, must lie within the comparison's ancestry
(computed over the accumulated DAG, including parent ids referenced by
explored events but not yet fetched). Honest shapes pass exactly as before --
a deep linear extension's unexplored remainder sits below the comparison
surface, and a sibling tip bottoms out at a shared ancestor. Smuggled shapes
fail the check, the traversal runs to exhaustion, and the foreign line goes
through the diverged merge or reject paths instead of being adopted.

### Unfetchable events on both frontiers

On ephemeral nodes, historical events may live only on the durable peer. If an
event ID appears on **both** frontiers but cannot be fetched, it is a common
ancestor beyond local storage. The algorithm processes it with empty parents,
correctly terminating traversal at that point. An unfetchable event on only
**one** frontier is a genuine error -- the DAG is incomplete.

### Budget escalation

The initial budget (default 1000 events) caps each BFS attempt. If exhausted,
the algorithm internally retries with 4x budget (up to `initial * 4`). The
traversal itself restarts from the original clocks -- only the accumulator
(recorded DAG structure and LRU event cache) survives the retry, so re-walked
steps avoid storage round-trips but still spend budget. The internal retry
keeps the public API simple -- callers do not need to manage retry logic.

### Generation accelerations

Since D2 (#266), the comparison consumes event generations in exactly three
places, all governed by one discipline: a generation may order work and gate
optional shortcuts, but it can never select, route, or conclude a verdict.
Verdicts, meets, and layer partitions are byte-identical under arbitrary
corruption of the values (pinned at scale by the generation-corruption
immunity oracle in `core/src/event_dag/tests.rs`).

1. **Rejection prechecks (P1/P2)** (`prechecks.rs`) -- given eligible per-tip
   operands for both clocks, sound rules can prove the `StrictDescends`
   hypothesis false before any fetch. A rejection only *suppresses* the
   quick-check attempt; the BFS still decides everything, so a wrong operand
   costs time, never an outcome.
2. **Schedule keying** (`comparison.rs`) -- each BFS level drains in
   max-eligible-generation-first order (EventId tiebreak, unknowns last).
   A pure schedule choice: level membership and every conclusion are
   order-independent.
3. **Walk-time edge checks** (`accumulator.rs`) -- wherever the walk already
   holds a child and all its parents, it re-checks the stamp equation for
   free. A violation warns, increments a counter, and demotes that event's
   value to per-comparison ineligibility; it never rejects committed history
   (that escalation question is recorded as issue #335). The warning cannot
   fire under honest, correctly implemented operation -- an honest stamp
   satisfies the equation by construction and admission rejects mismatches
   wherever parents are resolvable -- so a production occurrence is a
   high-signal incident (defective client build, hostile writer, or storage
   serving doctored payloads).

Eligibility is resolved at consumption: a value is consulted only if it is
not the `u32::MAX` saturation sentinel, not admitted-unverified (events
adopted below a state snapshot's horizon), and not demoted by an edge check.
A node-level kill-switch (`Node::set_generation_accelerations_disabled`)
makes all three consumers dormant at runtime -- comparisons then run exactly
as pre-D2 -- while commit stamping and admission verification stay on.


## Event Layers

When comparison returns `DivergedSince`, the merge machinery walks the
accumulated DAG *forward* and hands [property backends](property-backends.md)
the history as a sequence of **event layers**. A layer is one generation of a
topological sort:

> A layer is the set of events whose parents have all been emitted by earlier
> layers -- where the meet itself, and any parent outside the accumulated DAG,
> counts as already emitted.

Equivalently, an event's layer number is its longest-path distance from the
meet: an event whose parents sit at depths 1 and 3 lands at depth 4, waiting
for its deepest parent.

```text
        M              meet (never emitted)
       / \
     X1   Y1           layer 1: {X1, Y1}
      |
     X2                layer 2: {X2}
      |
     X3                layer 3: {X3}
```

Layers group by causal depth, **not by branch**. With head `[X3]` and
incoming `Y1`, X1 and Y1 share layer 1 even though they sit on opposite sides
of the divergence, and the local tip X3 flows through in layer 3. Two
guarantees make per-layer merging sound:

1. **Parents precede children.** Every causal predecessor of an event above
   the meet appears in a strictly earlier layer, so applying layers in order
   respects causality.

2. **Divergent-region layers are antichains.** Above the meet, no event in a
   layer is an ancestor of another event in the same layer, even transitively.
   This holds because `DivergedSince` is only produced by an exhaustive
   traversal (see [Invariants](#invariants)): every event between the meet and
   the tips is in the accumulated DAG, so a causal path cannot hide behind an
   unfetched event. Within a layer, concurrency is genuine.

Orthogonal to layering, each layer partitions its events by whether the local
replica has already incorporated them: events in the current head's ancestry
are **already-applied**, the rest are **to-apply**. In the diagram, X1..X3
are already-applied and Y1 is to-apply. Already-applied events participate in
[merge resolution](lww-merge.md#the-three-stage-pipeline) as context -- they
can defeat an incoming write -- but only to-apply winners mutate state.

Order *within* a layer is deliberately meaningless: backends receive the
layer as a set. [Yrs](property-backends.md#yrs-backend) ignores layer
boundaries entirely (CRDT operations commute), while
[LWW](property-backends.md#lww-backend) treats each layer as an election
round whose incumbent is re-seeded from stored state.

Two scope notes. First, the layer sweep covers the *entire* accumulated DAG,
and an exhaustive traversal accumulates the common history **below** the meet
as well -- so when the meet sits above genesis, early layers also carry
below-meet events (genesis itself surfaces in layer 1). These are always
already-applied (they are ancestors of the current head by definition), so
they never mutate state; they are inert electoral context. Second, the
antichain guarantee is scoped to the divergent region: a below-meet straggler
can share a layer with an event that descends it through the meet. This is
harmless for the same reason -- causal comparisons consult the full
accumulated DAG, never layer membership.


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

### Why the `DuplicateCreation` guard was refined

An early guard used `event_stored()` unconditionally to detect duplicate
creation events. On
[durable nodes](node-architecture.md#durable-vs-ephemeral-nodes) this works, but
on [ephemeral nodes](node-architecture.md#durable-vs-ephemeral-nodes) that
receive entities via `StateSnapshot`, the creation event is never individually
committed -- only the entity state referencing it is stored. When the creation
event later arrives via subscription, `event_stored()` returns `false`,
misclassifying a legitimate re-delivery as a different genesis.

The unconditional guard was replaced with conditional logic. Where storage is
definitive ([`storage_is_definitive()`](retrieval.md#why-three-traits-instead-of-one)),
`event_stored() == true` short-circuits a re-delivery as a no-op and a missing
event is rejected as a different genesis. Ephemeral nodes fall through to
`compare()`, which handles both cases correctly: re-delivery yields
`StrictAscends`; different genesis yields `Disjoint`.

### Why the retrieval traits were split

The original monolithic `Retrieve` trait combined event access, state access,
and staging. This made it impossible to express "read-only event access" at the
type level. Since `apply_event` must not stage or commit events (the caller
manages that), the split into [`GetEvents`](retrieval.md#why-three-traits-instead-of-one),
[`GetState`](retrieval.md#why-three-traits-instead-of-one), and
[`SuspenseEvents`](retrieval.md#why-three-traits-instead-of-one) turns the staging protocol into
a compile-time constraint. See the
[Event Retrieval and Staging](retrieval.md) documentation for details.
