# Concurrent Updates Specification

> **Status:** Implemented in `concurrent-updates-event-dag` branch

## Motivation

Ephemeral and durable peers observe edits at different times. When a peer receives updates for an entity, it must determine if those updates descend from its current head, are ancestors, or represent true concurrency. The system must converge deterministically without regressions or spurious multi-heads.

## Core Concepts

### Causal Relationships

The `AbstractCausalRelation<Id>` enum represents all possible relationships between two clocks:

| Variant | Meaning | Action |
|---------|---------|--------|
| `Equal` | Identical lattice points | No-op |
| `StrictDescends { chain }` | Subject strictly after other | Apply forward chain |
| `StrictAscends` | Subject strictly before other | No-op (incoming is older) |
| `DivergedSince { meet, subject, other, subject_chain, other_chain }` | True concurrency | Per-property LWW merge |
| `Disjoint { gca, subject_root, other_root }` | Different genesis events | Reject per policy |
| `BudgetExceeded { subject, other }` | Traversal budget exhausted | Resume with frontiers |

### Forward Chains

Forward chains are event sequences in causal order (oldest to newest) from the meet point to each head. They enable:
- **Replay**: Apply events in correct order for `StrictDescends`
- **Merge**: Compute per-property depths for `DivergedSince` resolution

Chains are accumulated during backward BFS traversal and reversed before return.

## Algorithm: Backward Breadth-First Search

The comparison algorithm walks **backward** from two clock heads toward their common ancestors:

```
Timeline:   Root → A → B → C (head)
                   ↑   ↑   ↑
Search:       older ← ← ← newer (we walk this direction)
```

### Process

1. **Initialize**: Two frontiers = {subject_head} and {other_head}
2. **Expand**: Fetch events at current frontier positions
3. **Process**: For each event:
   - Remove from frontier, add parents
   - Track which frontier(s) have seen it
   - Accumulate in visited list for chain building
4. **Detect**: Check termination conditions
5. **Repeat** until: relationship determined, frontiers empty, or budget exhausted

### Termination Conditions

- `unseen_comparison_heads == 0` → `StrictDescends` (subject has seen all comparison heads)
- `unseen_subject_heads == 0` → `StrictAscends` (comparison has seen all subject heads)
- Frontiers empty + common ancestors found → `DivergedSince` (true concurrency)
- Frontiers empty + different genesis roots → `Disjoint` (incompatible histories)
- Frontiers empty + no common, can't prove disjoint → `DivergedSince` with empty meet
- Budget exhausted → `BudgetExceeded` (return frontiers for resumption)

## Conflict Resolution: Layered Event Application

For `DivergedSince`, the system uses a **layered event application model** with uniform backend interface.

### The Layer Model

Events are applied in **layers** - sets of concurrent events at the same causal depth from the meet point:

```rust
struct EventLayer<'a> {
    already_applied: Vec<&'a Event>,  // Context: events already in our state
    to_apply: Vec<&'a Event>,         // New: events to process
}
```

**Key insight:** Since all events in a layer are mutually concurrent (same causal depth), winner determination is simple: **lexicographic EventId**. No explicit depth comparison needed - depth is implicit in which layer you're at.

### Resolution Process

1. **Navigate backward** from both heads to find meet point (common ancestors)
2. **Accumulate full events** during traversal (via AccumulatingNavigator)
3. **Compute layers** via frontier expansion from meet
4. **Apply layers in causal order**, calling `backend.apply_layer()` for each

### Backend Interface

All backends implement the same `apply_layer` method:

```rust
trait PropertyBackend {
    fn apply_layer(
        &self,
        already_applied: &[&Event],  // Events at this depth already in state
        to_apply: &[&Event],         // Events at this depth to process
    ) -> Result<(), MutationError>;
}
```

### LWW Resolution Within Layer

For LWW backends, each layer is resolved by:
1. Examine ALL events (already_applied + to_apply)
2. Per-property winner = highest lexicographic EventId
3. Only mutate state for winners from to_apply set
4. Track event_id per property for future conflict resolution

### Example

```
        A (meet)
       /|\
      B C D      ← Layer 1: {already: [B], to_apply: [C, D]}
      | | |
      E F G      ← Layer 2: {already: [E], to_apply: [F, G]}
      |   |
      H   I      ← Layer 3: {already: [H], to_apply: [I]}

If current head is [H] (via A→B→E→H) and events [C,D,F,G,I] arrive:
- Layer 1: B already applied, C and D are new, all concurrent
- Layer 2: E already applied, F and G are new, all concurrent
- Layer 3: H already applied, I is new, concurrent
```

### Backend-Specific Behavior

- **LWW backends**: Determine winner by lexicographic EventId across layer; only apply winners from to_apply
- **Yrs backends**: Apply all operations from to_apply; CRDT handles idempotency internally; already_applied ignored

## Key Design Decisions

### 1. Multi-Head StrictAscends Transformation

**Problem**: When event D with parent `[C]` arrives at entity head `[B, C]`:
- `compare([C], [B, C])` returns `StrictAscends` (parent is subset of head)
- But D extends tip C and is concurrent with B

**Solution**: In `compare_unstored_event`, transform `StrictAscends` to `DivergedSince`:
- Meet = event's parent clock (the divergence point)
- Other = head tips not in parent
- Empty `other_chain` triggers conservative resolution (current value wins)

This correctly identifies "event extending one concurrent tip" as concurrency, not redundancy.

### 2. Empty Other-Chain Conservative Resolution

When `other_chain` is empty (StrictAscends transformation case):
- We can't compute depths for the other branch
- Conservative choice: keep current value if it has a tracked `event_id`
- Rationale: Current value was set by a more recent event on the head's branch

### 3. TOCTOU Retry Pattern

Event application is non-atomic (comparison is async, then write lock):

```rust
for attempt in 0..MAX_RETRIES {
    let head = entity.head();
    let relation = compare(event, head).await;  // No lock held

    let mut state = entity.state.write();       // Take lock
    if state.head != head {                     // Head changed during comparison
        continue;                               // Retry
    }
    // Apply under lock
    break;
}
```

### 4. Per-Property Event Tracking

LWW backend stores `event_id` per property for future conflict resolution:

```rust
struct ValueEntry {
    value: Option<Value>,
    event_id: Option<EventId>,  // Which event set this value
    committed: bool,
}
```

This enables correct resolution when concurrent events arrive at different times.

## Invariants

1. **No regression**: Never skip required intermediate events
2. **Single head for linear history**: Multi-heads only for true concurrency
3. **Deterministic convergence**: Same events → same final state
4. **Idempotent application**: Redundant delivery is a no-op

## API Reference

### Compare Functions

```rust
/// Compare two clocks to determine their causal relationship
pub async fn compare<N, C>(
    navigator: &N,
    subject: &C,
    comparison: &C,
    budget: usize,
) -> Result<AbstractCausalRelation<N::EID>, RetrievalError>

/// Compare an unstored event against a stored clock
pub async fn compare_unstored_event<N, E>(
    navigator: &N,
    event: &E,
    comparison: &E::Parent,
    budget: usize,
) -> Result<AbstractCausalRelation<N::EID>, RetrievalError>
```

### PropertyBackend Trait

```rust
/// Apply operations with event tracking for conflict resolution
fn apply_operations_with_event(
    &self,
    operations: &[Operation],
    event_id: EventId,
) -> Result<(), MutationError>

/// Apply a layer of concurrent events (unified interface for all backends)
fn apply_layer(
    &self,
    already_applied: &[&Event],  // Events at this depth already in state
    to_apply: &[&Event],         // Events at this depth to process
) -> Result<(), MutationError>
```

### Layer Computation

```rust
/// Compute event layers from meet point for layered application
fn compute_layers<'a>(
    events: &'a BTreeMap<EventId, Event>,
    meet: &[EventId],
    current_head_ancestry: &BTreeSet<EventId>,
) -> Vec<EventLayer<'a>>
```

## Test Coverage

The implementation includes comprehensive tests for:

- Multi-head extension (event extends one tip of multi-head)
- Deep diamond concurrency (symmetric and asymmetric branches)
- Short branch from deep point (meet is not genesis)
- Per-property LWW resolution (different properties, different winners)
- Same-depth lexicographic tiebreak
- Idempotency (redundant event delivery)
- Chain ordering verification
- Disjoint detection (different genesis events)
- Layer computation (two-branch, multi-way, diamonds)
- LWW apply_layer (winner determination, already_applied vs to_apply)
- Yrs apply_layer (CRDT idempotency, concurrent text edits)

## Future Work

- **Attested lineage shortcuts**: Server-provided causal assertions (see `specs/lineage-attestation/`)
- **Forward chain caching**: Avoid recomputation for repeated comparisons
- **Budget resumption**: Continue interrupted traversals
