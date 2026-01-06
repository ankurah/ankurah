# Concurrent Updates: Implementation Status

**Branch:** `trusted_causal_assertions`
**Date:** 2026-01-05

## Current State

The `event_dag` module has been added to provide a cleaner abstraction for causal relationship comparison, replacing the inline lineage logic. The rebase onto main is complete.

### What Exists

1. **`event_dag` module** (`core/src/event_dag/`)
   - `comparison.rs`: Backward BFS algorithm comparing two clocks
   - `navigator.rs`: `CausalNavigator` trait with `expand_frontier` method
   - `relation.rs`: `AbstractCausalRelation` enum
   - `frontier.rs`: Frontier state machine with tainting for assertions

2. **`AbstractCausalRelation` enum** (current implementation):
   - `Equal`
   - `StrictDescends`
   - `NotDescends { meet }`
   - `Incomparable`
   - `PartiallyDescends { meet }`
   - `BudgetExceeded { subject_frontier, other_frontier }`

3. **`CausalNavigator` trait**:
   - `expand_frontier(frontier_ids, budget) -> NavigationStep`
   - Implemented for `LocalRetriever` and `EphemeralNodeRetriever`

4. **Entity integration**:
   - `entity.apply_event` uses `compare_unstored_event` from event_dag
   - `entity.apply_state` uses `compare` from event_dag

## Architectural Questions

### 1. StrictAscends vs NotDescends

**Spec says:**
```
- StrictAscends: ignore (incoming is older than current head)
- DivergedSince { meet, subject, other }: true concurrency, merge
```

**Implementation has:**
```rust
NotDescends { meet } // Conflates both cases
```

**The problem:** In `entity.apply_event`, `NotDescends` always augments the head (treats as concurrent). This is **wrong for StrictAscends cases** where the incoming event is an ancestor of the current head.

**Current behavior:**
```
Current head: [C]  (history: A → B → C)
Incoming event: B  (with parent A)

compare_unstored_event returns: NotDescends { meet: [A] }
Current code: Augments head to [C, B]  ← WRONG! Should be no-op
```

**Options:**
1. Add `StrictAscends` variant to `AbstractCausalRelation`
2. Distinguish at application time by checking if meet equals incoming event's parent
3. Return direction information in `NotDescends`

### 2. Event Replay Gap

When `StrictDescends` is detected, the current code only applies the incoming event's operations. But if the incoming event is multiple hops ahead of the current head, intermediate events need to be replayed.

**Current behavior:**
```
Current head: [A]
Incoming event: D  (with parent C, history: A → B → C → D)

compare returns: StrictDescends
Current code: Applies only D's operations ← WRONG! Should replay B, C, D
```

**Options:**
1. Have `StrictDescends` return the connecting chain
2. Make entity fetch the chain from storage after determining descent
3. Use staged events to capture the chain during comparison

### 3. DivergedSince Forward Chain Semantics

The spec describes applying "from the meet, the subject's forward chain in causal order." The current algorithm finds the meet but doesn't return the forward chains.

**Needed for proper merge:**
- From meet, replay subject events (oldest → newest)
- Apply LWW resolution per-backend
- Prune subsumed descendants
- Bubble remaining concurrent heads

## Build Status

~25 compile errors remain after rebase. Main issues:
- `Retrieve` trait bound changes (no longer has associated types)
- `stage_events`/`mark_event_used` moved from trait to inherent methods
- `deltas` → `initial` field rename in proto
- `crate::lineage` module removed (should use `event_dag`)

## Remaining Work

### Phase 1: Make It Compile
- [ ] Fix trait bounds in `node_applier.rs`
- [ ] Update `client_relay.rs` field names
- [ ] Remove `crate::lineage` imports
- [ ] Ensure staging methods are called correctly

### Phase 2: Fix StrictAscends
- [ ] Decide: new variant vs runtime detection
- [ ] Update `AbstractCausalRelation` if adding variant
- [ ] Update `entity.apply_event` to handle ascends as no-op
- [ ] Add tests for older-event delivery

### Phase 3: Event Replay
- [ ] Implement forward chain retrieval for multi-hop descends
- [ ] Update `entity.apply_event` to replay intermediate events
- [ ] Add tests for multi-hop descent scenarios

### Phase 4: True Concurrency
- [ ] Implement forward chain extraction for DivergedSince
- [ ] Implement LWW resolution with lexicographic tiebreak
- [ ] Implement descendant pruning
- [ ] Add tests for concurrent update scenarios

## Test Status

2 event_dag assertion tests + 1 reactor test were failing before rebase. These need investigation after build passes.
