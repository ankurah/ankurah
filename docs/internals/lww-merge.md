# LWW Property Resolution During Concurrent Updates

## Overview

When two or more branches of an entity's [event DAG](event-dag.md) diverge, each branch may
independently write to the same LWW (Last-Writer-Wins) property. At merge time,
the system must pick a single winner for every property and guarantee that every
replica converges on the same answer regardless of event delivery order.

The resolution pipeline has three stages:

1. **Layer computation** -- the [`EventAccumulator`](event-dag.md#core-concepts) and [`EventLayers`](event-dag.md#core-concepts) iterator
   partition all events between the [meet point](event-dag.md#core-concepts) and the branch tips into
   topologically ordered concurrency layers.
2. **Per-layer resolution** -- [`LWWBackend::apply_layer`](property-backends.md#conflict-resolution-via-apply_layer) determines, for each
   property, which event's write wins within a layer.
3. **State mutation** -- only winning values that originate from `to_apply`
   events (i.e., events not yet in the entity's state) are written to the
   backend.

All relevant code lives in three files:

| File | Role |
|------|------|
| `core/src/event_dag/accumulator.rs` | `EventAccumulator`, `EventLayers`, `EventLayer`, `ComparisonResult` |
| `core/src/property/backend/lww.rs` | `LWWBackend::apply_layer` |
| `core/src/entity.rs` | [`Entity::apply_event`](entity-lifecycle.md#apply_event-in-detail) DivergedSince handler |


## Layer Computation

After [`compare()`](event-dag.md#the-comparison-algorithm) returns `DivergedSince`, the caller decomposes the
[`ComparisonResult`](event-dag.md#core-concepts) into its relation and [`EventAccumulator`](event-dag.md#core-concepts), then calls
`accumulator.into_layers(meet, current_head)`.

```rust
let (_relation, accumulator) = comparison_result.into_parts();
let mut layers = accumulator.into_layers(meet.clone(), head.as_slice().to_vec());
```

`into_layers` constructs an `EventLayers` iterator. Construction performs three
preparatory steps:

1. **Parent-to-children index.** Iterates over the accumulated DAG once and
   builds a `BTreeMap<EventId, Vec<EventId>>` mapping each event to its
   children. All subsequent child lookups are O(1).

2. **Head ancestry.** Calls `compute_ancestry_from_dag` to walk backward from
   `current_head` through the DAG's parent pointers, collecting every reachable
   event ID into a `BTreeSet`. This set determines which events are
   `already_applied` vs `to_apply`.

3. **Initial frontier.** Seeds the frontier with every event in the DAG whose
   in-DAG parents are all in the `processed` set (initially just the meet).
   Parents that fall outside the accumulated DAG (i.e., below the meet) are
   treated as already processed. This is a generalized topological-sort seed
   that handles multi-parent (merge) events correctly.

### Frontier expansion

Each call to `layers.next().await` advances one layer:

1. **Partition** the current frontier into `already_applied` (in head ancestry)
   and `to_apply` (not in head ancestry). Fetch full event bodies via
   `accumulator.get_event` (LRU-cached, falls back to [storage](retrieval.md)).
2. **Advance** the frontier to children whose in-DAG parents are all processed.
3. **Yield** an `EventLayer` if the frontier was non-empty.

> **Spec discrepancy:** The Phase 3 spec proposes skipping layers with empty
> `to_apply`. The code does not skip them. This has no correctness impact
> because `apply_layer` only mutates state for `to_apply` winners.


## The EventLayer Structure

```rust
pub struct EventLayer {
    pub already_applied: Vec<Event>,
    pub to_apply: Vec<Event>,
    dag: Arc<BTreeMap<EventId, Vec<EventId>>>,
}
```

**`already_applied`** -- Events at this causal depth that are already in the
entity's head ancestry. Their property writes are in the current backend state.
They participate in winner comparison so the backend knows what the "incumbent"
values are.

**`to_apply`** -- Events at this causal depth that are new. Only winning values
from these events will be written to state.

**`dag`** -- An `Arc`-shared snapshot of the full accumulated DAG (event-id to
parent-ids mapping). Every layer shares the same DAG, so `compare()` can
traverse ancestry across layer boundaries. This is critical: a stored value's
event may be in a different (earlier or later) layer than the current one, and
the comparison still works because all parent pointers are available.

### `dag_contains(id)` -- the "older than meet" check

Returns whether the given event ID has an entry in the accumulated DAG. The
LWW backend uses this to implement the "older than meet" rule: if a stored
value's `event_id` is not in the DAG, the event predates the meet point and
any layer candidate automatically beats it.

```rust
pub fn dag_contains(&self, id: &EventId) -> bool {
    self.dag.contains_key(id)
}
```

### `compare(a, b)` -- causal relation between two events

Determines the causal relationship between two event IDs using only parent
pointers in the accumulated DAG. The method is **infallible** -- missing entries
are treated as dead ends (below the meet), not errors.

```rust
pub fn compare(&self, a: &EventId, b: &EventId) -> CausalRelation {
    if a == b { return CausalRelation::Descends; }
    if is_descendant_dag(&self.dag, a, b) { return CausalRelation::Descends; }
    if is_descendant_dag(&self.dag, b, a) { return CausalRelation::Ascends; }
    CausalRelation::Concurrent
}
```

`is_descendant_dag` walks backward from `descendant` through parent pointers
looking for `ancestor`. Missing DAG entries terminate that path silently.

| Result | Meaning |
|--------|---------|
| `Descends` | `a` causally follows `b` (or `a == b`) |
| `Ascends` | `b` causally follows `a` |
| `Concurrent` | Neither is an ancestor of the other |

> **Note:** `a == b` returns `Descends`, not a separate `Equal` variant.
> When the stored value and a layer candidate refer to the same event, the
> incumbent stays -- same behavior as `Descends`.

> **Spec discrepancy:** The causal-fix spec proposed `compare` returning
> `Result<CausalRelation, RetrievalError>`. The implementation is infallible:
> missing entries are dead ends, and the "older than meet" rule handles the
> case where the stored value's event is entirely absent from the DAG.


## LWW Resolution Rules

`LWWBackend::apply_layer` (`core/src/property/backend/lww.rs`, line 169)
implements the resolution algorithm. It operates in three phases.

### Phase 1: Seed from stored state

Read every committed property value from the backend and insert it into a
`winners: BTreeMap<PropertyName, Candidate>` map. Each candidate carries:

```rust
struct Candidate {
    value: Option<Value>,
    event_id: EventId,
    from_to_apply: bool,
    older_than_meet: bool,
}
```

The `older_than_meet` flag is set when `layer.dag_contains(&event_id)` returns
false -- the stored value's event predates the accumulated DAG context.
Properties without a committed `event_id` (i.e., in `Uncommitted` or `Pending`
state) cause an immediate error. This never fires in normal operation because
`apply_layer` runs on primary entities, which only hold `Committed` values.

### Phase 2: Compete layer events

Iterate through `already_applied` then `to_apply` events. For each event, try
to deserialize its LWW operations. For each property written by the event,
build a new `Candidate` and compete it against the current winner:

```text
if current winner is older_than_meet:
    new candidate wins unconditionally
else:
    match layer.compare(candidate.event_id, current.event_id):
        Descends  => candidate wins (it causally follows the incumbent)
        Ascends   => incumbent stays (it causally follows the candidate)
        Concurrent => higher EventId wins (lexicographic tiebreak)
```

If no winner exists yet for the property, the candidate is inserted directly.

The "older than meet" rule is the key fix for the scenario where a property was
last written by an event deep in history (below the [meet point](event-dag.md#core-concepts)). Since that
event is not in the [accumulated DAG](event-dag.md#core-concepts), `compare()` cannot determine causality.
But we know every event in the layer is a descendant of the meet, which is
itself a descendant of (or equal to) the stored event. Therefore any layer
candidate trivially beats it.

### Phase 3: Write winners from `to_apply`

Only candidates whose `from_to_apply` flag is `true` are written back to
the backend. If the winner came from `already_applied` or from the stored
seed, no mutation occurs -- the backend state already reflects that value.

```rust
for (prop, candidate) in winners {
    if candidate.from_to_apply {
        values.insert(prop, ValueEntry::Committed {
            value: candidate.value,
            event_id: candidate.event_id,
        });
    }
}
```

After mutations, subscribers are notified for each changed field.


## Determinism

The system guarantees that the same set of concurrent events always resolves to
the same property values, regardless of the order in which events are delivered
to different replicas.

### Why it works

Consider replicas R1 and R2 that both hold the same entity at head [G]
(genesis) and receive three concurrent events A, B, C (all with parent [G])
in different orders.

**R1 receives A, then B, then C:**

1. A applied directly (`StrictDescends`). Head = [A].
2. B arrives. `DivergedSince(meet=[G])`. Layer: already=[A], to_apply=[B].
   Winner per property = max(A, B) by EventId. Head = [A, B].
3. C arrives. `DivergedSince(meet=[G])`. Layer: already=[A, B], to_apply=[C].
   Winner per property = max(max(A, B), C) = max(A, B, C). Head = [A, B, C].

**R2 receives C, then A, then B:**

1. C applied directly. Head = [C].
2. A arrives. Layer: already=[C], to_apply=[A]. Winner = max(A, C).
   Head = [A, C].
3. B arrives. Layer: already=[A, C], to_apply=[B]. Winner = max(max(A, C), B)
   = max(A, B, C). Head = [A, B, C].

Both replicas converge to the same head and the same winner for every property.

### The ingredients of determinism

1. **Lexicographic EventId tiebreak is a total order.** EventIds are SHA-256
   hashes of `(entity_id, operations, parent_clock)`. The byte-level ordering
   is deterministic and consistent across all replicas.

2. **`max()` over EventIds is commutative and associative.** Pairwise
   competition with `>` as tiebreak produces the same result regardless of
   evaluation order.

3. **Causal dominance is objective.** When one event descends from another,
   [`compare()`](property-backends.md#compare) returns `Descends`/`Ascends` consistently on every replica --
   the DAG structure is the same everywhere.

4. **The "older than meet" rule is deterministic.** Whether a stored event is
   in the DAG depends only on the DAG contents (which converge across
   replicas), not on delivery order. See [`dag_contains`](property-backends.md#dag_contains).

5. **Stored state reflects prior winners.** Each `apply_layer` call updates
   the stored `event_id` for mutated properties, so subsequent layers (or
   subsequent event arrivals) seed from the correct incumbent.

### What determinism requires

Causal delivery. A child event must not arrive before its parents. If event D
has parent [C], [replica](node-architecture.md) R must have already integrated C before D arrives. The
system does not reorder events internally; if causal delivery is violated,
`compare()` may fail with `EventNotFound` for the missing parent.


## Per-Property Independence

Each LWW property is resolved independently within a layer. The `winners` map
is keyed by `PropertyName`, so different properties can have different winning
events.

Example with a diamond DAG:

```text
      A (genesis, title="Init", artist="Init")
     / \
    B    C
```

Event B writes `title="B-title"`. Event C writes `artist="C-artist"`.

Layer 0: already=[B], to_apply=[C] (or vice versa depending on delivery order).

- For `title`: B wrote it, C did not. Winner = B (from seed). No mutation (B is
  already applied).
- For `artist`: C wrote it, B did not. Winner = C (from to_apply). Mutation
  occurs.

Final state: `title="B-title"`, `artist="C-artist"`. Each property picked its
winner independently.

When both branches write the same property, the competition uses `compare()` and
the lexicographic tiebreak as described above. Different properties may still
have different winning events from the same layer.


## Integration: Entity::apply_event

The DivergedSince handler in [`Entity::apply_event`](entity-lifecycle.md#apply_event-in-detail) (`core/src/entity.rs`,
starting at the `DivergedSince` match arm) ties the pipeline together:

1. Decompose [`ComparisonResult`](event-dag.md#core-concepts) into `(relation, accumulator)`.
2. Call `accumulator.into_layers(meet, current_head)` to get the [layer
   iterator](#layer-computation).
3. Collect all layers eagerly (async iteration, may hit storage).
4. Acquire the entity write lock. Re-check the head for [TOCTOU safety](entity-lifecycle.md#the-try_mutate-toctou-protection); if it
   moved, retry.
5. For each layer, in topological order:
   a. Check whether any `to_apply` event introduces a [new backend type](property-backends.md#backend-registration-and-late-creation). If
      so, create the backend and replay all earlier layers on it.
   b. Call [`backend.apply_layer(&layer)`](property-backends.md#the-propertybackend-trait) on every backend.
6. [Update the head](entity-lifecycle.md#head-management): remove meet IDs, insert the new event ID.
7. Release lock. Broadcast change notification.

The "replay earlier layers for new backends" step (5a) ensures that a backend
type first encountered in layer N does not miss operations from layers 0
through N-1. Earlier layers are stored in an `applied_layers` vector for this
purpose.


## Test Coverage

See [Testing Strategy](testing.md#determinism) for the full test matrix. Two test files
exercise LWW resolution and determinism:

| Test | File | What it verifies |
|------|------|------------------|
| `test_deeper_branch_wins` | `tests/tests/lww_resolution.rs` | A merge event that descends from both branches wins over a shallower branch |
| `test_sequential_writes_last_wins` | same | Linear chain: latest write always wins |
| `test_lexicographic_tiebreak` | same | Two concurrent events at same depth: higher EventId wins |
| `test_per_property_concurrent_writes` | same | Different properties resolved independently |
| `test_lww_order_independence` | same | Repeated trials confirm consistent winner selection |
| `test_two_event_determinism_same_property` | `tests/tests/determinism.rs` | Two nodes apply events A,B vs B,A -- same final value |
| `test_deep_diamond_determinism` | same | Deep linear chain preserves last-write semantics |
| `test_multi_property_determinism` | same | Concurrent branches writing different properties converge |
| `test_three_way_concurrent_determinism` | same | Three-way fork: highest EventId wins, head = [B, C, D] |

The determinism test (`test_two_event_determinism_same_property`) is the most
critical: it creates two separate [durable nodes](node-architecture.md#durable-vs-ephemeral-nodes), applies the same events in
different orders, and asserts identical final state.
