# Event Accumulator Refactor - Research & Analysis

## Findings

### 1. Trait Structure Discovery

The current retrieval architecture has three traits:
- **`Retrieve`** - minimal, only `get_state()`
- **`CausalNavigator`** - `expand_frontier()` for batch event fetching
- **`EventStaging`** - `stage_events()`, `mark_event_used()` for deferred storage

`CausalNavigator::expand_frontier()` is ONLY called from `Comparison::step()`. The trait exists to:
1. Abstract over local vs remote retrieval
2. Support future **assertions** (peer-aided shortcuts) - currently returns empty

### 2. Assertions Feature

`NavigationStep` includes `assertions: Vec<AssertionResult>` for future optimization:
```rust
pub enum AssertionRelation<Id> {
    Descends,                    // Shortcut - skip ahead
    NotDescends { meet },        // Terminate path
    PartiallyDescends { meet },  // Taint but continue
    Incomparable,                // Terminate path
}
```

**Current state**: Both `LocalRetriever` and `EphemeralNodeRetriever` return empty assertions.
**Future**: Peer attestations could provide shortcuts.

**Risk**: Removing `CausalNavigator` loses this extension point.

### 3. compare_unstored_event Complexity

`compare_unstored_event` handles incoming events not yet in storage:

```rust
// Compares event.parent() against comparison clock
let result = compare(navigator, event.parent(), comparison, budget).await?;

// Transforms result - e.g., StrictAscends becomes DivergedSince
// because the event itself is new, creating a concurrent branch
```

Key issue at lines 135-147: When `StrictAscends` detected, returns `DivergedSince` with **empty `other_chain`**.

Current workaround in `apply_event`:
```rust
let mut events = acc_navigator.get_events();
events.insert(event.id(), event.clone());  // Manually add unstored event!
```

**Impact on plan**: `EventAccumulator` won't automatically have the unstored event. Need explicit handling.

### 4. Lifetime Complexity

`EphemeralNodeRetriever` has complex lifetimes:
```rust
pub struct EphemeralNodeRetriever<'a, SE, PA, C> {
    pub node: &'a Node<SE, PA>,
    pub cdata: &'a C,
    staged_events: Mutex<Option<HashMap<EventId, (Attested<Event>, bool)>>>,
}
```

If `EventAccumulator<R>` owns the retriever:
- Lifetimes propagate: `EventAccumulator<'a, ...>`
- Then to: `EventLayers<'a, ...>`, `ComparisonResult<'a, ...>`

**Alternative**: `EventAccumulator` borrows retriever with explicit lifetime.

### 5. Staged Events Duplication

Both would hold events:
- `EventAccumulator`: LRU cache for hot events
- `EphemeralNodeRetriever.staged_events`: Events pending storage

Questions:
- Are these redundant?
- Different purposes? (cache vs pending-storage)
- Should `EventAccumulator` replace `staged_events`?

### 6. Budget Tracking

`expand_frontier` returns `consumed_budget` for `BudgetExceeded` detection.
`Comparison` already tracks `remaining_budget`.

With new design: budget tracking stays in `Comparison`, `EventAccumulator` just fetches.

## Potential Holes in Plan

### Hole 1: Assertion Extensibility

**Problem**: Removing `CausalNavigator` loses the assertion extension point.

**Options**:
1. Keep `CausalNavigator` but move accumulation into `Comparison`
2. Design `EventAccumulator` to support assertions
3. Accept loss, add back later if needed

**Recommendation**: Option 1 - keep trait, change where accumulation happens.

### Hole 2: Unstored Event Handling

**Problem**: `compare_unstored_event` deals with events not in storage. How does unstored event get into `EventAccumulator`?

**Options**:
1. `compare_unstored_event` takes event, adds to accumulator explicitly
2. Return type indicates caller must provide event for layers
3. `EventAccumulator::add_unstored(event)` method

**Recommendation**: Option 1 - `compare_unstored_event` handles it.

### Hole 3: Lifetime Propagation

**Problem**: Borrowing retriever creates lifetime complexity.

**Options**:
1. Own the retriever (requires `'static` or cloning)
2. Borrow with explicit lifetime (propagates everywhere)
3. Use `Arc` for shared ownership

**Recommendation**: Investigate if retrievers can be `Clone` or wrapped in `Arc`.

### Hole 4: Staged Events Integration

**Problem**: `EphemeralNodeRetriever` has `staged_events`, `EventAccumulator` has LRU cache.

**Options**:
1. `EventAccumulator` replaces `staged_events` entirely
2. Keep both, clear separation (cache vs pending-storage)
3. `EventAccumulator` delegates storage to retriever's staging

**Recommendation**: Option 3 - `EventAccumulator` uses retriever's staging under the hood.

### Hole 5: Error Recovery Mid-Iteration

**Problem**: If retriever fails during `EventLayers` iteration, what happens?

**Options**:
1. Propagate error, caller handles
2. Retry logic in `EventLayers`
3. Pre-validate all events exist before iteration

**Recommendation**: Option 1 - keep it simple, propagate errors.

### Hole 6: Layer Computation Needs Current Head

**Problem**: `compute_layers` needs `current_head_ancestry` to partition `already_applied` vs `to_apply`. But head can change between comparison and layer application (TOCTOU).

**Current handling**: `apply_event` has retry loop, re-checks head under lock.

**Impact**: `EventLayers` needs to either:
1. Take head snapshot at creation (may be stale)
2. Re-compute ancestry on each `next()` (expensive)
3. Accept head as parameter, caller ensures consistency

**Recommendation**: Option 3 - caller responsibility, matches current pattern.

## Revised Architecture Thoughts

Given the findings, a more conservative refactor:

1. **Keep `CausalNavigator` trait** - for assertions extensibility
2. **Move accumulation into `Comparison`** - not a wrapper navigator
3. **`ComparisonResult`** contains relation + borrowed accumulator access
4. **Caller manages retriever lifetime** - avoid ownership complexity
5. **`EventLayers`** takes references, caller holds retriever/accumulator

```rust
// Comparison accumulates internally
let mut comparison = Comparison::new(&retriever, subject, other, budget);
let result = comparison.run().await?;

// Result borrows from comparison
match result.relation() {
    DivergedSince { meet, .. } => {
        // layers() borrows accumulated events
        let layers = result.layers(meet, &current_head);
        for layer in layers {
            // layer.already_applied, layer.to_apply available
        }
    }
}
```

This avoids:
- Removing `CausalNavigator`
- Complex ownership transfer
- Lifetime propagation to return types

But still achieves:
- No `AccumulatingNavigator` wrapper
- Events accumulated during comparison
- Clean layer access

## Resolved Questions

1. **Should we keep `CausalNavigator` trait for assertion extensibility?**
   - **No.** Navigator was an abstraction over recursion that never happened. Assertions always return empty. Eliminate the trait entirely; if needed in future, add as optional callback.

2. **How important is ownership transfer vs borrowing for `EventLayers`?**
   - **Ownership.** Nodes and contexts are cheap to clone, so no complex borrowing needed. `EventLayers` owns the `EventAccumulator`.

3. **Should `EventAccumulator` replace or complement `staged_events`?**
   - **Replace.** `EventAccumulator.seeded` replaces `staged_events` with cleaner semantics.

4. **Is the conservative refactor sufficient, or do we need the full restructure?**
   - **Full restructure.** The conservative approach kept too much complexity. Eliminating Navigator and moving accumulation into Comparison is cleaner.

5. **When should events be stored to the storage engine?**
   - **During EventLayers iteration.** Events stored via `get_and_store_event()` exactly when needed for layer application. This is the natural filter - only events actually applied get stored.

6. **How to handle unstored events in compare_unstored_event?**
   - **Seed the accumulator.** Call `accumulator.seed(vec![event])` before comparison starts.

## Conclusion

The refactor eliminates three layers of complexity:
1. `CausalNavigator` trait - replaced by direct `Retrieve` calls
2. `AccumulatingNavigator` wrapper - accumulation moves into `Comparison`
3. `staged_events` in retriever - replaced by `EventAccumulator.seeded`

The new architecture is simpler and enables future streaming (#200) by having all event handling flow through `EventAccumulator`.
