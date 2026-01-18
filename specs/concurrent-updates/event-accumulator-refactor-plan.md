# Event Accumulator Refactor Plan

## Summary

Refactor the comparison and event accumulation architecture to:
1. Eliminate the awkward `AccumulatingNavigator` side-channel pattern
2. Have `Comparison` accumulate events directly via `EventAccumulator`
3. Return layer iteration capability in `DivergedSince` results
4. Establish patterns that enable future streaming (issue #200)

## Current State (Problems)

1. **`Comparison`** fetches full events during BFS, extracts id+parent, **discards events**
2. **`AccumulatingNavigator`** wraps navigator to intercept and collect events as side effect
3. **After comparison**, caller manually extracts events and wires to relation
4. **`apply_state`** doesn't use accumulator, can't handle divergence
5. **Retriever** already handles storage + peer fallback + stores fetched events

## Proposed Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Comparison                               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   EventAccumulator                       │    │
│  │  ┌─────────────────┐  ┌─────────────────────────────┐   │    │
│  │  │ DAG Structure   │  │ LRU Cache (bounded)         │   │    │
│  │  │ id → parents    │  │ EventId → Event             │   │    │
│  │  │ (always in mem) │  │ (hot events only)           │   │    │
│  │  └─────────────────┘  └────────────┬────────────────┘   │    │
│  │                                    │ cache miss         │    │
│  │                                    ▼                    │    │
│  │                          ┌─────────────────┐            │    │
│  │                          │    Retriever    │            │    │
│  │                          │ (storage+peer)  │            │    │
│  │                          └─────────────────┘            │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ returns
              ┌───────────────────────────────────┐
              │      ComparisonResult             │
              │  - relation: AbstractCausalRelation
              │  - For DivergedSince: EventLayers │
              └───────────────────────────────────┘
```

## New Types

### EventAccumulator

```rust
/// Accumulates event DAG structure during comparison.
/// Caches hot events in memory, falls back to retriever for misses.
pub struct EventAccumulator<R: Retrieve> {
    /// DAG structure: event id → parent ids (always in memory, cheap)
    dag: BTreeMap<EventId, Vec<EventId>>,

    /// LRU cache of actual Event objects (bounded size)
    cache: LruCache<EventId, Event>,

    /// Fallback for cache misses (handles storage + peer)
    retriever: R,
}

impl<R: Retrieve> EventAccumulator<R> {
    /// Called during BFS traversal
    pub fn accumulate(&mut self, event: Event) {
        let id = event.id();
        let parents = event.parent().members().to_vec();
        self.dag.insert(id, parents);
        self.cache.put(id, event);
    }

    /// Get event by id (cache → retriever fallback)
    pub async fn get_event(&mut self, id: &EventId) -> Result<Event, RetrievalError> {
        if let Some(event) = self.cache.get(id) {
            return Ok(event.clone());
        }
        let event = self.retriever.get_event(id).await?;
        self.cache.put(id.clone(), event.clone());
        Ok(event)
    }

    /// Produce layer iterator for merge (consumes self)
    pub fn into_layers(
        self,
        meet: Vec<EventId>,
        current_head: Vec<EventId>,
    ) -> EventLayers<R> {
        EventLayers::new(self, meet, current_head)
    }
}
```

### EventLayers (async iterator)

```rust
/// Async iterator over EventLayer for merge application.
/// Computes layers lazily, fetches events from cache or retriever.
pub struct EventLayers<R: Retrieve> {
    accumulator: EventAccumulator<R>,
    meet: Vec<EventId>,
    current_head_ancestry: BTreeSet<EventId>,

    // Iteration state
    processed: BTreeSet<EventId>,
    frontier: BTreeSet<EventId>,
}

impl<R: Retrieve> EventLayers<R> {
    /// Get next layer (async - may fetch from storage)
    pub async fn next(&mut self) -> Result<Option<EventLayer<Event>>, RetrievalError> {
        // Compute next frontier layer
        // Fetch events (from cache or retriever)
        // Partition into already_applied / to_apply
        // Advance frontier
    }
}
```

## Changes to Existing Code

### 1. Comparison (comparison.rs)

- Add `EventAccumulator` field (or embed its logic)
- In `process_event()`, store full event not just id+parents
- Return `ComparisonResult` instead of just `AbstractCausalRelation`

```rust
struct Comparison<'a, R: Retrieve> {
    accumulator: EventAccumulator<R>,
    // ... existing fields (frontiers, states, etc.)
}

pub struct ComparisonResult<R: Retrieve> {
    pub relation: AbstractCausalRelation<EventId>,
    accumulator: EventAccumulator<R>,  // private
}

impl<R: Retrieve> ComparisonResult<R> {
    /// For DivergedSince, get layer iterator
    pub fn into_layers(
        self,
        current_head: Vec<EventId>,
    ) -> Option<EventLayers<R>> {
        match &self.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                Some(self.accumulator.into_layers(meet.clone(), current_head))
            }
            _ => None
        }
    }
}
```

### 2. compare() function

```rust
pub async fn compare<R: Retrieve>(
    retriever: R,
    subject: &Clock,
    comparison: &Clock,
    budget: usize,
) -> Result<ComparisonResult<R>, RetrievalError>
```

Note: No longer takes `navigator: &N`, takes `retriever: R` directly.

### 3. AccumulatingNavigator

- **Deprecate** - no longer needed
- Comparison handles accumulation internally

### 4. apply_event (entity.rs)

```rust
// Before:
let acc_navigator = AccumulatingNavigator::new(getter);
match compare_unstored_event(&acc_navigator, event, &head, budget).await? {
    DivergedSince { meet, .. } => {
        let events = acc_navigator.get_events();
        let ancestry = compute_ancestry(&events, head);
        let layers = compute_layers(&events, &meet, &ancestry);
        for layer in layers { ... }
    }
}

// After:
let result = compare_unstored_event(retriever, event, &head, budget).await?;
match result.relation {
    DivergedSince { .. } => {
        let mut layers = result.into_layers(head.as_slice().to_vec()).unwrap();
        while let Some(layer) = layers.next().await? {
            // Apply layer
        }
    }
}
```

### 5. apply_state (entity.rs)

Can now handle divergence internally:

```rust
pub async fn apply_state<R: Retrieve>(
    &self,
    retriever: R,
    state: &State,
) -> Result<StateApplyResult, MutationError> {
    let result = compare(retriever, &state.head, &self.head(), budget).await?;

    match result.relation {
        DivergedSince { .. } => {
            // Can now handle divergence!
            let mut layers = result.into_layers(self.head().to_vec()).unwrap();
            while let Some(layer) = layers.next().await? {
                self.apply_layer(&layer)?;
            }
            Ok(StateApplyResult::AppliedViaLayers)
        }
        // ... other cases
    }
}
```

### 6. CausalNavigator trait

- May be simplified or removed
- The `expand_frontier` concept merges into `EventAccumulator` + `Retriever`

## Migration Path

### Phase 1: Add new types (non-breaking)
- [ ] Add `EventAccumulator` struct
- [ ] Add `EventLayers` struct
- [ ] Add `ComparisonResult` struct
- [ ] Add tests for new types

### Phase 2: Modify Comparison internals
- [ ] Add `EventAccumulator` to `Comparison` struct
- [ ] Store full events in `process_event()`
- [ ] Return `ComparisonResult` from `compare()`
- [ ] Update `compare_unstored_event()` similarly

### Phase 3: Update callers
- [ ] Update `apply_event` to use new pattern
- [ ] Update `apply_state` to handle divergence
- [ ] Update any other `compare()` callers

### Phase 4: Cleanup
- [ ] Deprecate `AccumulatingNavigator`
- [ ] Simplify or remove `CausalNavigator` trait if no longer needed
- [ ] Remove `compute_layers` / `compute_ancestry` standalone functions (logic moves to `EventLayers`)

## Open Questions

1. **LRU cache size** - What's a reasonable default? Configurable?

2. **Retriever trait** - Does existing `Retrieve` trait have what we need, or needs extension?

3. **Async iterator ergonomics** - Use `futures::Stream`? Custom trait? `async fn next()`?

4. **Error handling in layers** - If retriever fails mid-iteration, how to handle?

5. **StateApplyResult** - Still want the richer enum? What variants?
   - `Applied` - state applied directly (StrictDescends)
   - `AppliedViaLayers` - divergence handled via layer merge
   - `AlreadyApplied` - Equal, no-op
   - `Older` - StrictAscends, no-op

6. **compare_unstored_event** - How does this fit? It compares an event not yet in storage.

## Related Issues

- #200 - Streaming layer iteration (this plan enables it)
- Current PR - StateAndEvent divergence fix (this plan supersedes the quick fix)

## Files to Modify

- `core/src/event_dag/comparison.rs` - Major changes
- `core/src/event_dag/mod.rs` - New exports
- `core/src/event_dag/navigator.rs` - Deprecate AccumulatingNavigator
- `core/src/event_dag/layers.rs` - May merge into EventLayers
- `core/src/entity.rs` - Update apply_event, apply_state
- `core/src/retrieval.rs` - May need Retrieve trait extensions
- `core/src/node_applier.rs` - Update StateAndEvent handling
