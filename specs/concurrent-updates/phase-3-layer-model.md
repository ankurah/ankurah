# Phase 3: Layered Event Application Model

> **Status:** Implemented (3a, 3b, 3c complete; 3e tests in progress)
> **Depends on:** Phase 2 (forward chains, LWW resolution) - complete
> **Goal:** Unify event application interface, eliminate backend-specific resolution code

## Motivation

The current implementation has diverged from the original design intent:

**Current state:**
- `apply_operations(operations)` - no event tracking
- `apply_operations_with_event(operations, event_id)` - tracks event per property
- `apply_operations_with_resolution(operations, event_id, depth, other_chain, ...)` - LWW-specific

**Problems:**
1. Backend-specific code in entity layer (`if backend_name == "lww"`)
2. Resolution logic split between entity and backend
3. Forward chains are `Vec<EventId>`, requiring separate concepts of "depth from meet"
4. Doesn't naturally handle multi-way concurrency or batch arrivals

**Original design intent** (from implementation-status.md):
> "The entity walks through subject's chain, oldest to newest. For each event, for each property it modifies, checks if that property was also modified in other's chain..."

This spec refines that intent into a cleaner model: **layered event application**.

## Core Concept: Concurrency Layers

A **layer** is a set of events that are:
1. **Mutually concurrent** - no event in the layer causally precedes another
2. **At the same causal depth** - same distance from the meet point
3. **Partitioned by origin** - which events we've already applied vs which are new

```rust
struct EventLayer<'a> {
    /// Events at this layer already in our state (for context)
    already_applied: Vec<&'a Event>,
    /// Events at this layer we need to process
    to_apply: Vec<&'a Event>,
}
```

**Key insight:** Since all events in a layer are concurrent (same causal depth), winner determination is simple: **lexicographic EventId**. No depth comparison needed - depth is implicit in which layer you're at.

### Visual Example

```
        A (meet)
       /|\
      B C D      ← Layer 1: {already: [B], to_apply: [C, D]}
      | | |
      E F G      ← Layer 2: {already: [E], to_apply: [F, G]}
      |   |
      H   I      ← Layer 3: {already: [H], to_apply: [I]}
```

If current head is [H] (via A→B→E→H) and events [C,D,F,G,I] arrive:
- Layer 1: B is already applied, C and D are new, all concurrent
- Layer 2: E is already applied, F and G are new, all concurrent
- Layer 3: H is already applied, I is new, concurrent

## Unified Backend Interface

```rust
trait PropertyBackend {
    // ... existing methods ...

    /// Apply a layer of concurrent events.
    ///
    /// All events in `already_applied` and `to_apply` are mutually concurrent
    /// (same causal depth from meet). The backend receives ALL events for
    /// complete context, but only needs to mutate state for `to_apply` events.
    ///
    /// # Contract
    /// - All events in the layer are mutually concurrent (no causal relationship)
    /// - `already_applied` events are in the current state (for context/comparison)
    /// - `to_apply` events are new and need processing
    /// - Backend MUST implement this method (no default)
    ///
    /// # For LWW backends
    /// Determine per-property winner by lexicographic EventId across all events
    /// in the layer. Only apply values where winner is in `to_apply`.
    ///
    /// # For CRDT backends (Yrs)
    /// Apply all operations from `to_apply` events. Order within layer doesn't
    /// matter (CRDTs are commutative). Can ignore `already_applied`.
    fn apply_layer(
        &self,
        already_applied: &[&Event],
        to_apply: &[&Event],
    ) -> Result<(), MutationError>;
}
```

### LWW Implementation

```rust
impl PropertyBackend for LWWBackend {
    fn apply_layer(
        &self,
        already_applied: &[&Event],
        to_apply: &[&Event],
    ) -> Result<(), MutationError> {
        // All events in this layer are concurrent - determine per-property winners
        let mut winners: BTreeMap<PropertyName, (Option<Value>, EventId)> = BTreeMap::new();

        // Consider ALL events (both already_applied and to_apply)
        for event in already_applied.iter().chain(to_apply.iter()) {
            for (prop, value) in self.extract_lww_changes(event)? {
                let dominated = winners.get(&prop)
                    .map(|(_, existing_id)| event.id() > *existing_id)
                    .unwrap_or(true);
                if dominated {
                    winners.insert(prop.clone(), (value, event.id()));
                }
            }
        }

        // Apply values where winner is from to_apply
        let to_apply_ids: BTreeSet<_> = to_apply.iter().map(|e| e.id()).collect();
        let mut changed_fields = Vec::new();

        {
            let mut values = self.values.write().unwrap();
            for (prop, (value, winner_id)) in winners {
                if to_apply_ids.contains(&winner_id) {
                    values.insert(prop.clone(), ValueEntry {
                        value,
                        committed: true,
                        event_id: Some(winner_id),
                    });
                    changed_fields.push(prop);
                }
                // Winner from already_applied → already in state, no mutation needed
            }
        }

        // Notify subscribers
        self.notify_changed_fields(&changed_fields);
        Ok(())
    }
}
```

### Yrs Implementation

```rust
impl PropertyBackend for YrsBackend {
    fn apply_layer(
        &self,
        _already_applied: &[&Event],  // Ignored - CRDT handles idempotency
        to_apply: &[&Event],
    ) -> Result<(), MutationError> {
        // Order within layer doesn't matter for CRDTs
        for event in to_apply {
            for (_, operations) in event.operations.iter() {
                self.apply_operations(operations)?;
            }
        }
        Ok(())
    }
}
```

## Layer Computation Algorithm

Given a merged DAG and current head, compute layers via frontier expansion:

```rust
fn compute_layers<'a>(
    events: &'a BTreeMap<EventId, Event>,
    meet: &[EventId],
    current_head_ancestry: &BTreeSet<EventId>,
) -> Vec<EventLayer<'a>> {
    let mut layers = Vec::new();
    let mut processed: BTreeSet<EventId> = meet.iter().cloned().collect();

    // Start with immediate children of meet
    let mut frontier: BTreeSet<EventId> = meet.iter()
        .flat_map(|m| children_of(events, m))
        .collect();

    while !frontier.is_empty() {
        // Partition frontier into already_applied vs to_apply
        let mut already_applied = Vec::new();
        let mut to_apply = Vec::new();

        for event_id in &frontier {
            let event = events.get(event_id).unwrap();
            if current_head_ancestry.contains(event_id) {
                already_applied.push(event);
            } else {
                to_apply.push(event);
            }
        }

        // Only create layer if there's something to apply
        if !to_apply.is_empty() {
            layers.push(EventLayer { already_applied, to_apply });
        }

        // Mark frontier as processed
        processed.extend(frontier.iter().cloned());

        // Advance to next frontier: children whose parents are all processed
        frontier = frontier.iter()
            .flat_map(|id| children_of(events, id))
            .filter(|id| !processed.contains(id))
            .filter(|id| parents_of(events, id).iter().all(|p| processed.contains(p)))
            .collect();
    }

    layers
}

fn children_of(events: &BTreeMap<EventId, Event>, parent: &EventId) -> Vec<EventId> {
    events.iter()
        .filter(|(_, e)| e.parent.contains(parent))
        .map(|(id, _)| id.clone())
        .collect()
}

fn parents_of(events: &BTreeMap<EventId, Event>, child: &EventId) -> Vec<EventId> {
    events.get(child)
        .map(|e| e.parent.iter().cloned().collect())
        .unwrap_or_default()
}
```

## Entity Layer Changes

### Current Flow (to be replaced)

```rust
// entity.rs - CURRENT
match relation {
    StrictDescends { chain } => {
        // Apply single event, ignore chain
        state.apply_operations_from_event(...)?;
    }
    DivergedSince { subject_chain, other_chain, .. } => {
        if backend_name == "lww" {
            lww.apply_operations_with_resolution(
                operations, event_id, subject_depth, &other_chain, ...
            )?;
        } else {
            state.apply_operations_from_event(...)?;
        }
    }
}
```

### New Flow

```rust
// entity.rs - PROPOSED
match relation {
    Equal | StrictAscends => {
        // No-op
        return Ok(false);
    }

    StrictDescends { chain } | DivergedSince { subject_chain: chain, .. } => {
        // Events already accumulated during comparison BFS - get from navigator
        let events: &BTreeMap<EventId, Event> = navigator.traversed_events();

        // Current head's ancestry determines "already_applied" partition
        // (events we've already processed vs events that are new)
        let current_ancestry: BTreeSet<EventId> = head.iter()
            .flat_map(|id| ancestors_of(events, id))
            .collect();

        // Compute layers from meet point
        let meet = match &relation {
            DivergedSince { meet, .. } => meet.clone(),
            StrictDescends { .. } => head.iter().cloned().collect(), // Meet is current head
        };
        let layers = compute_layers(events, &meet, &current_ancestry);

        // Apply layers in order
        let mut state = self.state.write().unwrap();
        for layer in &layers {
            for (backend_name, backend) in state.backends.iter() {
                backend.apply_layer(&layer.already_applied, &layer.to_apply)?;
            }
        }

        // Update head
        state.head = new_head;
    }

    Disjoint { .. } => return Err(LineageError::Disjoint.into()),
    BudgetExceeded { .. } => return Err(LineageError::BudgetExceeded { .. }.into()),
}
```

**Key difference from current:** No separate event fetching step. The navigator accumulated events during the comparison BFS. We just use them directly.

## What Gets Removed

1. **`apply_operations_with_resolution`** on LWWBackend - replaced by `apply_layer`
2. **Backend-specific branching** in entity.rs - all backends use same `apply_layer` call
3. **Depth calculation** in entity.rs - implicit in layer structure
4. **`other_chain_set` construction** - not needed, layer computation handles partitioning

## What Gets Added

1. **`apply_layer`** method on PropertyBackend trait (required, no default impl)
2. **`compute_layers`** function in event_dag module
3. **Event accumulation** in navigator during BFS traversal
4. **`traversed_events()`** accessor on navigator (or wrapper)

## Comparison Algorithm Changes

The comparison algorithm currently returns `Vec<EventId>` for chains.

**Decision: Accumulate full events during reverse BFS.**

The navigator already loads events via `expand_frontier` during traversal. Instead of discarding them and keeping only IDs, we accumulate the full `Event`s. This avoids a separate fetch pass.

**Key insight:** Events are either:
- **Staged** (from EventBridge) - already in memory, zero fetch cost
- **In local storage** (from our applied history) - cheap to load

The staging mechanism exists precisely so the reverse BFS can access incoming events without remote fetches. We're just retaining what we already load.

**Memory consideration:** Accumulating full events uses more memory than IDs during traversal. For typical chain lengths (tens of events), this is acceptable. Issue #200 tracks future optimization for very long chains.

### Preserving Test Ergonomics

`AbstractCausalRelation<Id>` remains generic so unit tests can use simple integer IDs without constructing full `Event` objects. The navigator trait is already generic over event type.

For layer computation and backend testing:
- **Comparison tests:** Mock navigator with integer IDs, verify relationship detection
- **Layer computation tests:** Mock events with minimal structure (id, parents, properties)
- **Backend tests:** Real `Event` objects, verify resolution logic
- **Integration tests:** End-to-end with real everything

```rust
// Mock event for layer computation tests
struct MockEvent {
    id: u64,
    parents: Vec<u64>,
    properties: BTreeMap<String, i32>,  // Simple LWW properties
}
```

### Navigator Event Accumulation

The `CausalNavigator` trait (or a wrapper) accumulates events during traversal:

```rust
// During comparison, navigator accumulates traversed events
let relation = compare(navigator, subject, other, budget).await?;

// Events are available from navigator after traversal
let events: &BTreeMap<EventId, Event> = navigator.traversed_events();

// Compute layers using relation structure + full events
let layers = compute_layers(&relation, events, &current_head_ancestry);
```

This keeps the comparison algorithm focused on relationship detection while making events available for layer computation.

## Invariants

1. **Causal ordering**: Layer N events are causally after layer N-1 events
2. **Concurrency within layer**: All events in a layer are mutually concurrent
3. **Completeness**: `already_applied ∪ to_apply` contains all events at that causal depth
4. **Determinism**: Same events → same layers → same final state
5. **Idempotency**: Re-applying a layer with same events is a no-op

## Edge Cases

### StrictDescends with Internal Concurrency

```
    A (current head)
   / \
  B   C
   \ /
    D (incoming)
```

D strictly descends from A, but the path has concurrency:
- Layer 1: {already: [], to_apply: [B, C]} - both concurrent, both new
- Layer 2: {already: [], to_apply: [D]}

The layer model handles this naturally. `already_applied` is empty because nothing in [B,C,D] is in current head's ancestry.

### Multi-Head Current State

```
Current head: [H, G] (already concurrent)
Incoming: I (descends from G only)
```

- Meet with I is G
- Layer 1: {already: [], to_apply: [I]}
- New head: [H, I]

The H branch is unaffected. Layer computation only considers the path from meet to incoming.

### Empty Layers

If a layer has only `already_applied` events and no `to_apply`:
- Skip the layer (nothing to apply)
- This happens when our branch is "ahead" at that depth

---

## Implementation Plan

### Phase 3a: Navigator & Layer Infrastructure

1. Add event accumulation to navigator (or wrapper) during BFS traversal
2. Add `traversed_events()` accessor to retrieve accumulated events
3. Add `compute_layers` function in event_dag module
4. Unit tests for layer computation with mock events

### Phase 3b: Backend Interface

1. Add `apply_layer` to PropertyBackend trait (required, no default impl)
2. Implement `apply_layer` for LWWBackend
3. Implement `apply_layer` for YrsBackend
4. Unit tests for backend layer application

### Phase 3c: Entity Integration

1. Update entity.rs to use layer-based application
2. Use navigator's accumulated events (no separate fetch)
3. Remove `apply_operations_with_resolution`
4. Remove backend-specific branching
5. Integration tests

### Phase 3d: Cleanup

1. Remove dead code (`apply_operations_with_resolution`, depth parameters)
2. Update documentation
3. Update spec.md to reflect new model

### Phase 3e: Test Cycle

Comprehensive testing per test-design.md:

1. **Layer computation tests**
   - Simple two-branch
   - Multi-way concurrency (3+ branches)
   - Diamonds (reconvergence)
   - Nested concurrency
   - Empty layers (skip correctly)

2. **LWW resolution within layer**
   - Single property, multiple concurrent setters
   - Multiple properties, different winners
   - Lexicographic tiebreak verification
   - already_applied winner (no mutation)
   - to_apply winner (mutation occurs)

3. **Yrs layer application**
   - Concurrent text edits merge correctly
   - Order within layer doesn't affect result
   - already_applied correctly ignored

4. **Integration scenarios**
   - Deep diamond with late branch arrival
   - EventBridge batch with internal concurrency
   - Multi-head extension
   - Rapid concurrent updates

5. **Determinism verification**
   - Same events, different arrival order → same final state
   - Multiple peers converge to identical state

6. **DAG structure verification** (per assert_dag! design)
   - Verify layer structure matches expected
   - Verify head evolution

---

## Design Decisions (Resolved)

### Event Accumulation Strategy
**Decision:** Accumulate full events during reverse BFS traversal.

Events are loaded via navigator during traversal anyway (from staged or local storage). We retain them instead of discarding after extracting IDs. No separate fetch step needed.

### Layer Caching
**Decision:** No caching. Build forward chain in memory during each application.

Layer computation is O(events) which is typically small. Issue #200 tracks future optimization if profiling shows need.

### Event Fetching
**Decision:** Events accumulated during BFS - no separate fetch.

Staged events (EventBridge) are already in memory. Local events are cheap to load. The navigator accumulates during traversal.

### Test Abstraction
**Decision:** Keep `AbstractCausalRelation<Id>` generic. No `AbstractLayer` trait needed initially.

Tests use mock navigators with integer IDs for comparison testing. Layer computation and backend tests use constructed `Event` objects (simple to create for test scenarios).

---

## Success Criteria

1. **Uniform interface**: All backends use `apply_layer`, no special cases
2. **No backend knowledge in entity**: Entity layer doesn't know about LWW vs Yrs
3. **Correct resolution**: All existing tests pass
4. **Determinism**: New determinism tests pass
5. **Clean code**: `apply_operations_with_resolution` removed

## Migration Notes

- This is a refactor, not a behavior change
- Existing persisted state remains valid
- Wire protocol unchanged
- Only internal application logic changes
