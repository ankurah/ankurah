# LWW Property Resolution During Concurrent Updates

## The Problem

Two users concurrently edit the same entity. User A sets `title = "Alpha"` on
one [replica](node-architecture.md); User B sets `title = "Beta"` on another. Both writes happen
independently -- neither knows about the other. When the replicas sync, the
system must pick one winner for `title` and guarantee that **every replica picks
the same one**, no matter which write it saw first.

LWW (Last-Writer-Wins) resolution answers this question with two rules applied
in order:

1. **Causal dominance.** If one write causally follows another (the writer had
   already seen the earlier value), the later write wins.
2. **Deterministic tiebreak.** If neither write has seen the other (they are
   truly concurrent), the write with the lexicographically greater `EventId`
   wins. Since EventIds are SHA-256 hashes, this is an arbitrary but
   consistent total order that every replica can compute independently.


## The Three-Stage Pipeline

When the [event DAG](event-dag.md) detects that branches have diverged, the
merge proceeds in three conceptual stages:

### 1. Layer computation

All events between the [meet point](event-dag.md#core-concepts) (the last
common ancestor of the two branches) and the branch tips are partitioned into
**concurrency layers** -- groups of events at the same causal depth. Within a
layer, events are either **already applied** (the replica has already
incorporated them) or **to-apply** (new to this replica). Layers are produced
in topological order so that earlier causal history is resolved before later
history. See [EventLayers](event-dag.md#core-concepts) for details on the
iterator.

### 2. Per-layer resolution

For each layer, every LWW property is resolved independently. The algorithm
starts with the property's current stored value as the incumbent, then
considers every event in the layer -- both already-applied and to-apply -- as
challengers. The [resolution rules](#resolution-rules) below determine whether
a challenger displaces the incumbent.

### 3. State mutation

Only winners that originate from **to-apply** events actually mutate the
backend. If the winning write was already in the replica's state (from an
already-applied event or from the stored seed), nothing changes. After all
mutations, subscribers are notified for each changed property.


## Resolution Rules

When a challenger event competes against the current incumbent for a property,
three rules are applied in priority order:

1. **Older-than-meet rule.** If the incumbent value was written by an event
   that predates the [meet point](event-dag.md#core-concepts), the challenger
   wins unconditionally. Rationale: every event in the layer descends from the
   meet, which itself descends from (or equals) the old event. The challenger
   is strictly newer.

2. **Causal dominance.** If one event is an ancestor of the other in the
   [accumulated DAG](event-dag.md#core-concepts), the descendant wins. This
   respects user intent: a write made *after* seeing a prior value should
   supersede it.

3. **Lexicographic tiebreak.** If the two events are truly concurrent (neither
   descends from the other), the one with the greater `EventId` wins. This is
   an arbitrary but deterministic rule that ensures every replica reaches the
   same conclusion.

If no incumbent exists for a property, the first event to write it wins by
default.


## Per-Property Independence

Each property is resolved independently. In a diamond DAG:

```text
      A (genesis, title="Init", artist="Init")
     / \
    B    C
```

Event B writes `title = "B-title"`. Event C writes `artist = "C-artist"`.

- **title**: Only B touched it. B's value wins.
- **artist**: Only C touched it. C's value wins.

Final state: `title = "B-title"`, `artist = "C-artist"`. When both branches
write the *same* property, the resolution rules above determine the winner.
Different properties may have different winning events from the same layer.


## Determinism

The system guarantees that the same set of concurrent events always resolves to
the same property values, regardless of event delivery order.

### Worked example

Replicas R1 and R2 both hold an entity at head \[G\] (genesis). Three
concurrent events A, B, C -- all parented on \[G\] -- arrive in different
orders. Assume `EventId(A) < EventId(B) < EventId(C)`.

**R1 receives A, then B, then C:**

| Step | Arrives | Layer (already / to-apply) | Winner for each property |
|------|---------|----------------------------|--------------------------|
| 1 | A | -- (direct apply) | A's values |
| 2 | B | already=\[A\], to-apply=\[B\] | max(A, B) = B by tiebreak |
| 3 | C | already=\[A, B\], to-apply=\[C\] | max(B, C) = C by tiebreak |

**R2 receives C, then A, then B:**

| Step | Arrives | Layer (already / to-apply) | Winner for each property |
|------|---------|----------------------------|--------------------------|
| 1 | C | -- (direct apply) | C's values |
| 2 | A | already=\[C\], to-apply=\[A\] | max(C, A) = C by tiebreak |
| 3 | B | already=\[A, C\], to-apply=\[B\] | max(C, B) = C by tiebreak |

Both replicas converge to the same head \[A, B, C\] and the same winner (C)
for every property that all three events wrote. The key insight: pairwise
`max()` over EventIds is **commutative and associative**, so the evaluation
order does not matter.

### The ingredients of determinism

1. **Lexicographic tiebreak is a total order.** EventIds are SHA-256 hashes of
   `(entity_id, operations, parent_clock)`. The byte-level ordering is
   consistent across all replicas.

2. **`max()` is commutative and associative.** Pairwise competition produces
   the same result regardless of evaluation order.

3. **Causal dominance is objective.** The DAG structure is the same on every
   replica, so ancestor/descendant queries return the same answers everywhere.

4. **The older-than-meet rule is deterministic.** Whether a stored event falls
   inside or outside the accumulated DAG depends only on the DAG contents,
   which converge across replicas.

5. **Stored state tracks winners.** Each layer updates the stored `event_id`
   for mutated properties, so subsequent layers seed from the correct
   incumbent.

### Prerequisite: causal delivery

Determinism relies on **causal delivery**: a child event must not arrive before
its parents. If event D has parent \[C\], a [replica](node-architecture.md)
must have already integrated C before D arrives.


## Integration With Entity::apply_event

The `DivergedSince` handler in
[`Entity::apply_event`](entity-lifecycle.md#apply_event-in-detail) ties the
pipeline together:

1. Decompose the [comparison result](event-dag.md#core-concepts) into the
   causal relation and the accumulated DAG.
2. Build the [layer iterator](#the-three-stage-pipeline) from the meet point
   and current head.
3. Collect all layers (async; may hit [storage](retrieval.md)).
4. Acquire the entity write lock; re-check the head for
   [TOCTOU safety](entity-lifecycle.md#the-try_mutate-toctou-protection).
5. For each layer in topological order, call the resolution algorithm on every
   [property backend](property-backends.md#the-propertybackend-trait). If a
   to-apply event introduces a
   [new backend type](property-backends.md#backend-registration-and-late-creation),
   create it and replay earlier layers first.
6. Update the head, release the lock, and broadcast change notifications.


## Test Coverage

See [Testing Strategy](testing.md#determinism) for the full test matrix. Key
test files:

- **`tests/tests/lww_resolution.rs`** -- deeper-branch-wins, sequential last
  write, lexicographic tiebreak, per-property independence, order independence.
- **`tests/tests/determinism.rs`** -- two-event same-property determinism (the
  most critical test: two [nodes](node-architecture.md#durable-vs-ephemeral-nodes)
  apply events in opposite order and assert identical final state), deep
  diamond, multi-property convergence, three-way fork.
