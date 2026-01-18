# apply_state / apply_event Factorization Analysis

## Current State

### The Problem

When `apply_state` encounters divergence (`DivergedSince`), it returns `Ok(false)` and the caller must fall back to `apply_event`. This pattern is repeated in multiple places and is error-prone.

Additionally, there's redundant work happening:
1. `compare()` fetches full events during DAG traversal
2. It discards them, keeping only event IDs in `subject_chain` / `other_chain`
3. Callers then re-fetch or re-accumulate these same events to apply them

### Two Separate Issues

**Issue 1: Ambiguous `bool` return from `apply_state`**

`apply_state` returns `bool` where `false` means one of:
- `Equal` - already at this state (no-op)
- `StrictAscends` - incoming state is older (no-op)
- `DivergedSince` - can't apply state, need events

Callers can't distinguish "nothing to do" from "need fallback to events".

**Issue 2: Wasteful event handling in comparison**

The `compare()` function in `event_dag/comparison.rs`:
- Fetches full `Event` objects via `navigator.expand_frontier()` (line 280)
- Extracts only `id` and `parent`, discards the rest
- Returns `DivergedSince { subject_chain: Vec<Id>, other_chain: Vec<Id>, ... }`
- Callers must re-fetch events by ID or use `AccumulatingNavigator` wrapper

Current workaround in `apply_event`:
```rust
let acc_navigator = AccumulatingNavigator::new(getter);
match compare_unstored_event(&acc_navigator, event, &head, budget).await? {
    DivergedSince { meet, .. } => {
        let events = acc_navigator.get_events();  // Side-channel to get events
        let layers = compute_layers(&events, &meet, ...);
        // apply layers...
    }
}
```

## Proposed Improvements

### Option A: Richer Return Type for `apply_state`

Replace `bool` with an explicit enum:

```rust
pub enum StateApplyResult {
    Applied,                    // State was applied (StrictDescends)
    AlreadyApplied,             // Already at this state (Equal)
    Older,                      // Incoming state is older (StrictAscends)
    Diverged { meet: Clock },   // Can't merge without events (DivergedSince)
}
```

**Pros:**
- Self-documenting return type
- Caller knows exactly what happened
- Type-safe handling of all cases

**Cons:**
- Breaking change to API (~7 call sites)
- Doesn't solve the event re-fetching problem

### Option B: `apply_state` Takes Optional Events

```rust
pub async fn apply_state<G>(
    &self,
    getter: &G,
    state: &State,
    fallback_events: Option<&[Event]>,
) -> Result<StateApplyResult, MutationError>
```

On `DivergedSince`, automatically apply the provided events.

**Pros:**
- Single method handles both state and event application
- No duplicated fallback logic in callers
- Still allows callers without events to get `Diverged` and handle themselves

**Cons:**
- Mixes state and event concerns in one method
- Events still need to be accumulated separately by caller

### Option C: `DivergedSince` Includes Full Events

Modify `AbstractCausalRelation::DivergedSince` to include the actual events:

```rust
DivergedSince {
    meet: Vec<Id>,
    subject_chain: Vec<Event>,   // Full events, not just IDs
    other_chain: Vec<Event>,     // Full events, not just IDs
    // ... or pre-computed layers
}
```

**Pros:**
- No re-fetching needed
- Events already fetched during comparison, just keep them
- Could even pre-compute layers during comparison

**Cons:**
- Increases memory usage of comparison result
- Changes the generic `AbstractCausalRelation<Id>` to need event type
- May not always need the events (e.g., if just checking relationship)

### Option D: `compare` Always Accumulates (Configurable)

Add accumulation mode to comparison:

```rust
pub async fn compare_with_events<N, C>(
    navigator: &N,
    subject: &C,
    comparison: &C,
    budget: usize,
) -> Result<(AbstractCausalRelation<N::EID>, HashMap<N::EID, N::Event>), RetrievalError>
```

Or use a builder pattern:
```rust
Comparison::new(navigator, subject, comparison, budget)
    .accumulate_events(true)
    .execute()
    .await
```

**Pros:**
- Opt-in accumulation for callers that need it
- Keeps `compare` simple for callers that don't need events
- No re-fetching when accumulation is enabled

**Cons:**
- Two code paths to maintain
- Still separate from layer computation

### Option E: Return Pre-computed Layers in DivergedSince

The most aggressive optimization - compute layers during comparison:

```rust
DivergedSince {
    meet: Vec<Id>,
    layers: Vec<Layer>,  // Ready to apply
}

struct Layer {
    already_applied: Vec<Event>,
    to_apply: Vec<Event>,
}
```

**Pros:**
- Zero post-processing needed
- Caller just iterates layers and applies
- All computation done in single pass

**Cons:**
- Tight coupling between comparison and application
- Layers depend on "current head" which is caller-specific
- May compute layers that aren't needed

## Recommendation

A phased approach:

### Phase 1: Immediate (Low Risk)
- **Option A**: Add `StateApplyResult` enum for clearer semantics
- Update `apply_state` and `with_state` to return it
- Update ~7 call sites

### Phase 2: Short Term
- **Option B**: Add `fallback_events` parameter to `apply_state`
- Callers with events can pass them for automatic handling
- Eliminates duplicated fallback logic

### Phase 3: Medium Term
- **Option D**: Add `compare_with_events` variant
- `apply_state` uses this when it has no events but needs to handle divergence
- Eliminates the `AccumulatingNavigator` wrapper pattern

### Phase 4: Long Term (If Needed)
- **Option E**: Evaluate if pre-computed layers provide meaningful perf benefit
- Only if profiling shows layer computation is a bottleneck

## Files Involved

- `core/src/entity.rs` - `apply_state`, `apply_event`, `with_state`
- `core/src/event_dag/comparison.rs` - `compare`, `Comparison` struct
- `core/src/event_dag/relation.rs` - `AbstractCausalRelation` enum
- `core/src/event_dag/navigator.rs` - `AccumulatingNavigator`
- `core/src/node_applier.rs` - Uses `with_state`, has fallback logic
- `core/src/context.rs` - Uses `with_state`
- `core/src/system.rs` - Uses `with_state`
- `core/src/node.rs` - Uses `with_state`

## Related Work

- PR #201: Concurrent updates with event DAG
- `specs/concurrent-updates/stateandvent-divergence-bug.md` - Bug that led to this analysis
- `tests/tests/stateandvent_divergence.rs` - Tests for the divergence fix
