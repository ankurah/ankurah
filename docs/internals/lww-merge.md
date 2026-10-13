# LWW Property Resolution During Concurrent Updates

## The Problem

Two users concurrently edit the same entity. User A sets `title = "Alpha"` on
one [replica](node-architecture.md); User B sets `title = "Beta"` on another. Both writes happen
independently -- neither knows about the other. When the replicas sync, the
system must pick one winner for `title` and guarantee that **every replica picks
the same one**, no matter which write it saw first.

LWW (Last-Writer-Wins) ranks writes by the writing event's generation, then
its event ID. The stored value carries that rank so replicas can choose the
same winner without retrieving the stored writer's ancestry. Saturated
generations retain the previous causal rule; see [Resolution Rules](#resolution-rules).


## The Three-Stage Pipeline

Every applied event reaches the backend through `apply_layer`. An immediate
descendant produces one layer with the previous head as its meet; genesis uses
an empty meet. Divergent histories may require several layers. All three use
the same comparison-to-layer pipeline:

### 1. Layer computation

All events between the [meet point](event-dag.md#key-concepts) (the last
common ancestor of the two branches) and the branch tips are partitioned into
**concurrency layers** -- groups of events at the same causal depth. Within a
layer, events are either **already applied** (the replica has already
incorporated them) or **to-apply** (new to this replica). Layers are produced
in topological order so that earlier causal history is resolved before later
history. See [Event Layers](event-dag.md#event-layers) for the precise
definition of a layer and the guarantees it provides.

### 2. Per-layer resolution

Only properties mentioned by the layer's operations participate. The algorithm
looks up each touched property's stored rank as its incumbent, then
considers every event in the layer -- both already-applied and to-apply -- as
challengers. The [resolution rules](#resolution-rules) below determine whether
a challenger displaces the incumbent.

### 3. State mutation

Only winners that originate from **to-apply** events actually mutate the
backend. If the winning write was already in the replica's state (from an
already-applied event or from the stored seed), nothing changes. After all
mutations, subscribers are notified for each changed property.


## Resolution Rules

Among concurrent writes to one LWW property, the write whose event has the
greater [generation](event-dag.md#generations) wins. Between writes of equal
generation, the write whose event has the greater `EventId` wins. `EventId`s
are SHA-256 hashes, so this second order is arbitrary, but every replica
computes it the same way.

A write's **rank** is the pair (generation of the writing event, `EventId` of
the writing event), compared in that order. The LWW backend keeps one entry
per property: the value and the rank of the write that set it. A write
replaces the entry when its rank is greater. If no write holds the property
yet, the first one does. Two writes of equal rank come from the same event,
since no two events share an `EventId`, and the later of them applies.

### Why one entry suffices before saturation

1. **The rank order extends causal order.** An update's generation is one
   more than the greatest generation among its parents, so an event's
   generation is greater than that of every event it descends from. A write
   made after seeing another write therefore outranks it, and no ancestry
   query is needed before the generation reaches `u32::MAX`.

2. **The rank order is total.** Two distinct writes come from events with
   different `EventId`s, so one of them outranks the other.

3. **The winner is a maximum.** Each property ends with the write of greatest
   rank among the writes the replica has applied. A maximum is commutative and
   associative, so it depends only on which writes were applied, not on their
   order or on how they were grouped into layers. The stored entry holds the
   maximum so far, which is everything the next comparison needs.

### Saturation

Generations saturate at `u32::MAX`, so a descendant can have the same generation
as its ancestor. Applying the ID tiebreak in that case could undo a later
causal write. When both writes have saturated generations, the backend keeps
the existing causal rule: descendants win, and concurrent writes use event
ID. The layer must retain enough ancestry to order these writes; comparison
must not discard shared history merely because generations match.

This fallback preserves existing behavior at saturation. It does not provide
the delivery-order independence proved above: that limitation of the previous
causal/ID rule remains for saturated histories.


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

Before generation saturation, the same set of events resolves to the same property values, whatever
order the events are delivered in: each property ends with the write of
greatest rank among them, and a maximum over a total order does not depend on
the order in which the candidates are compared.

### Worked example

Every event writes the property `p`. X1 and Y branch from the genesis G, and
X2 extends X1. The ids happen to order `EventId(X1) > EventId(Y) > EventId(X2)`.

```text
      G           p = "G"               generation 1
     / \
    X1  Y         p = "X1", p = "Y"     generation 2
    |
    X2            p = "X2"              generation 3
```

The ranks order X2 above X1 (greater generation), X1 above Y (equal
generation, greater id), and Y above G. A replica can receive these events in
three causal orders. The stored value of `p` after each arrival:

| Delivery order | 1st | 2nd | 3rd | 4th |
|----------------|-----|-----|-----|-----|
| G, X1, X2, Y | G | X1 | X2 | X2 |
| G, X1, Y, X2 | G | X1 | X1 | X2 |
| G, Y, X1, X2 | G | Y | X1 | X2 |

Every order ends with X2. Y never displaces X1 or X2: it ties X1 on
generation with a smaller id, and its generation is below X2's.

### History

The previous rule preferred the causally later write and broke ties between
concurrent writes by the greater `EventId`. That preference is not
transitive: in the example, X2 beats X1 because it descends from X1, X1 beats
Y by id, and Y beats X2 by id because the two are concurrent. Its outcome
depended on which writes were compared with which, so it could not be computed
from one stored entry per property: a replica that received G, X1, X2, Y ended
with Y, and one that received G, X1, Y, X2 ended with X2.

### Prerequisite: causal application order

Determinism relies on events being **applied** parents-first: if event D has
parent \[C\], a [replica](node-architecture.md) must integrate C before D.
This is an application-order invariant, not a transport guarantee -- the
receiver enforces it itself by staging each multi-event batch and
topologically sorting it by in-batch parent edges (`event_dag/ordering.rs`)
before applying. Sender order is never trusted.


## Integration With EntityState::apply_events

[`EventLayers::prepare`](entity-lifecycle.md#how-events-are-applied)
and the layer applier tie the pipeline together:

1. Validate the batch and compare its final subject clock against the current head.
2. Load the required history and group its layers by the input event whose policy
   check they must precede, before any mutation. `EventLayers` retains those
   boundaries alongside one flat layer sequence; already-applied inputs have
   empty slices.
3. Acquire the entity write lock and re-check the head before applying layers.
   The transaction path applies one input's slice before checking its policy;
   resident reconciliation applies the full sequence.
4. Apply each layer to the [property backends](property-backends.md#the-propertybackend-trait).
   A newly introduced backend receives preceding layers first. Advance the
   head from the applied events only after the slice succeeds, then release
   the lock and broadcast the change. A failed transaction fork is discarded.


## Test Coverage

See [Testing Strategy](testing.md#determinism) for the full test matrix. Key
test files:

- **`core/src/event_dag/tests.rs`** (module `lww_delivery_order`): the example
  above and histories of unequal branches, delivered to fresh replicas in
  every causal order; every order must end with one value, and it must be the
  deepest branch's tip.
- **`core/src/property/backend/conformance.rs`**: the backend laws, including
  restored ranks with missing writer history, and causal overwrite at saturated
  generations through comparison-built and explicit layers.
- **`tests/tests/lww_resolution.rs`**: deeper-branch-wins, sequential last
  write, lexicographic tiebreak, per-property independence, order independence.
- **`tests/tests/determinism.rs`**: two-event same-property determinism (two
  [nodes](node-architecture.md#durable-vs-ephemeral-nodes) apply events in
  opposite order and assert identical final state), deep diamond,
  multi-property convergence, three-way fork.
