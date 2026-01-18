# Event Accumulator Refactor Plan

## Summary

Refactor the comparison and event accumulation architecture to:
1. Eliminate the awkward `AccumulatingNavigator` side-channel pattern
2. Eliminate the `CausalNavigator` trait entirely (it was an abstraction over recursion that never happened)
3. Have `Comparison` accumulate events directly via `EventAccumulator`
4. Return layer iteration capability in `DivergedSince` results
5. Establish patterns that enable future streaming (issue #200)

## Current State (Problems)

1. **`Comparison`** fetches full events during BFS, extracts id+parent, **discards events**
2. **`AccumulatingNavigator`** wraps navigator to intercept and collect events as side effect
3. **`CausalNavigator` trait** exists for "assertions" feature that returns empty - over-abstraction
4. **After comparison**, caller manually extracts events and wires to relation
5. **`apply_state`** doesn't use accumulator, can't handle divergence
6. **`EphemeralNodeRetriever.staged_events`** is awkward separate staging mechanism

## Proposed Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Comparison                               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   EventAccumulator<R>                    │    │
│  │  ┌─────────────────┐  ┌─────────────────────────────┐   │    │
│  │  │ DAG Structure   │  │  Seeded Events              │   │    │
│  │  │ id → parents    │  │  (from peer, not stored)    │   │    │
│  │  │ (always in mem) │  └─────────────────────────────┘   │    │
│  │  └─────────────────┘  ┌─────────────────────────────┐   │    │
│  │                       │ LRU Cache (bounded)         │   │    │
│  │                       │ EventId → Event (hot)       │   │    │
│  │                       └────────────┬────────────────┘   │    │
│  │                                    │ cache miss         │    │
│  │                                    ▼                    │    │
│  │                          ┌─────────────────┐            │    │
│  │                          │    Retriever    │            │    │
│  │                          │ (has storage)   │            │    │
│  │                          └─────────────────┘            │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ returns
              ┌───────────────────────────────────┐
              │      ComparisonResult<R>          │
              │  - relation: AbstractCausalRelation
              │  - accumulator (private)          │
              │  - into_layers() for DivergedSince│
              └───────────────────────────────────┘
```

## Key Design Decisions

1. **Retriever owns storage access** - No separate storage parameter needed
2. **Seeded events** - Incoming peer events are seeded before comparison starts
3. **Store during layer iteration** - Events stored as they're applied, not before
4. **Nodes/contexts are cheap to clone** - No complex borrowing needed
5. **CausalNavigator eliminated** - Assertions feature was unused (returns empty)

## New Types

### EventAccumulator

```rust
/// Accumulates event DAG structure during comparison.
/// Caches hot events in memory, falls back to retriever for misses.
/// Handles seeded events (from peer) that need to be stored during iteration.
pub struct EventAccumulator<R: Retrieve> {
    /// DAG structure: event id → parent ids (always in memory, cheap)
    dag: BTreeMap<EventId, Vec<EventId>>,

    /// Seeded events from peer (not yet stored in local storage)
    seeded: HashMap<EventId, Event>,

    /// LRU cache of actual Event objects (bounded size)
    cache: LruCache<EventId, Event>,

    /// Retriever with storage access
    retriever: R,
}

impl<R: Retrieve> EventAccumulator<R> {
    pub fn new(retriever: R) -> Self {
        Self {
            dag: BTreeMap::new(),
            seeded: HashMap::new(),
            cache: LruCache::new(NonZeroUsize::new(1000).unwrap()), // configurable
            retriever,
        }
    }

    /// Seed with events from peer (before comparison)
    pub fn seed(&mut self, events: impl IntoIterator<Item = Event>) {
        for event in events {
            let id = event.id();
            let parents = event.parent().members().to_vec();
            self.dag.insert(id, parents);
            self.seeded.insert(id, event);
        }
    }

    /// Called during BFS traversal (for events fetched during comparison)
    pub fn accumulate(&mut self, event: Event) {
        let id = event.id();
        let parents = event.parent().members().to_vec();
        self.dag.insert(id, parents);
        self.cache.put(id, event);
    }

    /// Get event by id, storing to storage if from seeded
    /// Order: seeded → cache → retriever
    pub async fn get_and_store_event(&mut self, id: &EventId) -> Result<Event, RetrievalError> {
        // 1. Check seeded (peer events not yet stored)
        if let Some(event) = self.seeded.remove(id) {
            // Store to local storage via retriever
            self.retriever.store_event(&event).await?;
            self.cache.put(*id, event.clone());
            return Ok(event);
        }

        // 2. Check LRU cache
        if let Some(event) = self.cache.get(id) {
            return Ok(event.clone());
        }

        // 3. Fetch from retriever (already in storage or from peer)
        let event = self.retriever.get_event(id).await?;
        self.cache.put(*id, event.clone());
        Ok(event)
    }

    /// Get event without storing (for comparison traversal)
    pub async fn get_event(&mut self, id: &EventId) -> Result<Event, RetrievalError> {
        if let Some(event) = self.seeded.get(id) {
            return Ok(event.clone());
        }
        if let Some(event) = self.cache.get(id) {
            return Ok(event.clone());
        }
        let event = self.retriever.get_event(id).await?;
        self.cache.put(*id, event.clone());
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
/// Computes layers lazily, stores events during iteration.
/// Events are stored to local storage as they are fetched for application.
pub struct EventLayers<R: Retrieve> {
    accumulator: EventAccumulator<R>,
    meet: Vec<EventId>,
    current_head_ancestry: BTreeSet<EventId>,

    // Iteration state
    processed: BTreeSet<EventId>,
    frontier: BTreeSet<EventId>,
}

impl<R: Retrieve> EventLayers<R> {
    /// Get next layer (async - stores events during fetch)
    pub async fn next(&mut self) -> Result<Option<EventLayer<Event>>, RetrievalError> {
        if self.frontier.is_empty() {
            return Ok(None);
        }

        let mut to_apply = Vec::new();
        let mut already_applied = Vec::new();
        let mut next_frontier = BTreeSet::new();

        for id in std::mem::take(&mut self.frontier) {
            if self.processed.contains(&id) || self.meet.contains(&id) {
                continue;
            }
            self.processed.insert(id);

            // get_and_store_event: stores seeded events during iteration
            let event = self.accumulator.get_and_store_event(&id).await?;

            // Partition based on whether already in local head ancestry
            if self.current_head_ancestry.contains(&id) {
                already_applied.push(event.clone());
            } else {
                to_apply.push(event.clone());
            }

            // Add parents to next frontier
            for parent in event.parent().members() {
                if !self.processed.contains(parent) {
                    next_frontier.insert(*parent);
                }
            }
        }

        self.frontier = next_frontier;

        if to_apply.is_empty() && already_applied.is_empty() {
            return Ok(None);
        }

        Ok(Some(EventLayer { to_apply, already_applied }))
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

- **Remove entirely** - was only abstraction over recursion that never happened
- The `expand_frontier` concept replaced by `EventAccumulator.get_event()` calls
- Assertions feature was unused (always returned empty)
- If peer-aided shortcuts needed in future, add as optional callback

### 7. EphemeralNodeRetriever

- Remove `staged_events` field - replaced by `EventAccumulator.seeded`
- Remove `EventStaging` trait methods if no longer needed

## Migration Path

### Phase 1: Add new types (non-breaking)
- [ ] Add `store_event` method to `Retrieve` trait (or create new trait)
- [ ] Add `EventAccumulator` struct in new `event_dag/accumulator.rs`
- [ ] Add `EventLayers` struct (can extend existing `layers.rs`)
- [ ] Add `ComparisonResult` struct
- [ ] Add unit tests for `EventAccumulator` and `EventLayers`

### Phase 2: Modify Comparison internals
- [ ] Replace `CausalNavigator` usage with direct `Retrieve` calls
- [ ] Add `EventAccumulator` to `Comparison` struct
- [ ] Store full events in `process_event()` via `accumulator.accumulate()`
- [ ] Return `ComparisonResult` from `compare()`
- [ ] Update `compare_unstored_event()` to seed accumulator with unstored event

### Phase 3: Update callers
- [ ] Update `apply_event` to use `result.into_layers()`
- [ ] Update `apply_state` to handle divergence internally
- [ ] Update `node_applier.rs` StateAndEvent handling
- [ ] Update any other `compare()` callers

### Phase 4: Cleanup
- [ ] Remove `AccumulatingNavigator`
- [ ] Remove `CausalNavigator` trait
- [ ] Remove `staged_events` from `EphemeralNodeRetriever`
- [ ] Remove standalone `compute_layers` / `compute_ancestry` functions
- [ ] Remove `EventStaging` trait if no longer needed

## Resolved Design Questions

1. **LRU cache size** - Default to 1000, make configurable via builder pattern

2. **Retriever trait** - Needs `store_event(&Event)` method added for storing seeded events

3. **Async iterator ergonomics** - Use `async fn next()` for simplicity, avoid `futures::Stream` complexity

4. **Error handling in layers** - Propagate errors, caller handles (keep it simple)

5. **Storage timing** - Events stored during `EventLayers` iteration via `get_and_store_event()`
   - Seeded events are stored exactly when they're needed for layer application
   - This is the natural filter - only events that are actually applied get stored

6. **compare_unstored_event** - Seed the accumulator with the unstored event before comparison
   - `accumulator.seed(vec![event.clone()])`
   - Comparison can then find and use the event normally

7. **CausalNavigator trait** - Eliminated entirely
   - Assertions feature was never used (always returns empty)
   - If needed in future, can be added back as optional callback

8. **staged_events in EphemeralNodeRetriever** - Replaced by EventAccumulator.seeded
   - Cleaner separation: seeded = not-yet-stored, cache = hot events

## Open Questions

1. **StateApplyResult** - Still want the richer enum? What variants?
   - `Applied` - state applied directly (StrictDescends)
   - `AppliedViaLayers` - divergence handled via layer merge
   - `AlreadyApplied` - Equal, no-op
   - `Older` - StrictAscends, no-op

2. **Backward compatibility** - Deprecate old APIs with warnings, or breaking change?

## Related Issues

- #200 - Streaming layer iteration (this plan enables it)
- Current PR - StateAndEvent divergence fix (this plan supersedes the quick fix)

## Files to Modify

### New Files
- `core/src/event_dag/accumulator.rs` - New `EventAccumulator` type

### Major Changes
- `core/src/event_dag/comparison.rs` - Embed `EventAccumulator`, return `ComparisonResult`
- `core/src/event_dag/layers.rs` - Update `EventLayers` to use accumulator
- `core/src/entity.rs` - Update `apply_event`, `apply_state` to use new pattern
- `core/src/retrieval.rs` - Add `store_event` to `Retrieve` trait

### Minor Changes
- `core/src/event_dag/mod.rs` - New exports, remove old ones
- `core/src/node_applier.rs` - Update StateAndEvent handling (may simplify)

### Files to Remove/Deprecate
- `core/src/event_dag/navigator.rs` - `CausalNavigator` trait, `AccumulatingNavigator`
- `core/src/retrieval.rs` - `EventStaging` trait, `staged_events` from `EphemeralNodeRetriever`
