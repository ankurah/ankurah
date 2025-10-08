### Reactor Hot Path Serialization - Implementation Summary

**Completed:** October 2025

## Goals Achieved

- Minimized time under global locks on the hot path (`notify_change`)
- Batched per-subscription work at the subscription level
- Parallelized predicate evaluation across subscriptions
- Enabled future gap filling serialization by making `Subscription` clonable with interior mutability
- Gap filling now consolidated into single `ReactorUpdate` per invocation

## Architecture Changes

### Subscription Internal State

- Renamed `SubscriptionState` to `Subscription` and made it clonable via `Arc<Inner>` wrapper
- Changed from exterior mutability (accessed via reactor lock) to interior mutability (`std::sync::Mutex<State>`)
- Kept `Subscriptions: Mutex<HashMap<ReactorSubscriptionId, Subscription<E, Ev>>>` unchanged (only for insert/remove/fetch)
- Added `notify_lock: tokio::sync::Mutex<()>` to `ReactorInner` to serialize `notify_change` invocations

### Change Batching (`CandidateChanges`)

Implemented in `core/src/reactor/candidate_changes.rs`:

```rust
pub struct CandidateChanges<C> {
    changes: Arc<Vec<C>>,
    query_offsets: BTreeMap<QueryId, HashSet<usize>>,
    entity_offsets: HashSet<usize>,
}
```

- Wraps changes in `Arc<Vec<C>>` to avoid cloning per subscription
- Tracks offsets separately for query-specific and entity-level candidates
- Provides `query_iter()` and `entity_iter()` for efficient iteration

### notify_change Flow (Implemented)

1. **Acquire notify_lock** (async mutex, serializes invocations)
2. **Under watcher_set lock** (std mutex, no await):
   - Scan watchers for all changed entities
   - Build `HashMap<ReactorSubscriptionId, CandidateChanges<C>>`
   - Drop lock
3. **Parallel evaluation**:
   - Lock subscriptions map, collect evaluation futures (cloning `Subscription` Arc)
   - Drop subscriptions lock
   - `join_all` on `subscription.evaluate_changes(candidates)` across all subscriptions
4. **Apply watcher changes**:
   - Re-lock watcher_set
   - Apply all returned `WatcherChange`s
   - Drop lock

### Subscription::evaluate_changes (async)

- **Signature**: `async fn evaluate_changes(self, candidates: CandidateChanges<C>) -> Vec<WatcherChange>`
- Takes ownership of `self` (cheap Arc clone)
- Locks internal state once for entire evaluation
- For each query candidate:
  - Evaluates predicate
  - Detects membership changes (Add/Remove)
  - Updates resultset
  - Accumulates `WatcherChange`s to return
- Handles entity-level subscriptions
- Collects gap fill data while holding lock
- Drops state lock before spawning gap fill task
- Returns `WatcherChange`s for reactor to apply globally

### Gap Filling

- Integrated into `fill_gaps_and_notify` method
- Spawned as background task after dropping state lock
- Processes multiple gap fills in parallel using `join_all`
- Emits single consolidated `ReactorUpdate` after all gaps filled

### Watcher Management (`WatcherSet`)

Extracted to `core/src/reactor/watcherset.rs`:

- Encapsulates index watchers, wildcard watchers, and entity watchers
- Key method: `accumulate_interested_watchers()` - identifies subscriptions interested in each change
- Critical fix: Distinguish `EntityWatcherId::Predicate` from `EntityWatcherId::Subscription`:
  - Predicate watchers → route to query evaluation loop (for membership change detection)
  - Subscription watchers → route to entity-only updates

### Locking Discipline

- `notify_lock`: Held for entire `notify_change` execution (async)
- `watcher_set`: Two brief std mutex locks per `notify_change` (accumulate, then apply)
- `subscriptions`: Brief std mutex lock to fetch subscriptions (no await)
- `Subscription.state`: Std mutex lock for evaluation, dropped before any await
- No std mutex held across await points

## Key Implementation Details

### Removed Functionality

- Deleted `update_query_matching_entities` (logic inlined into `evaluate_changes`)
- Deleted standalone `pause_query` (only used internally by `update_query`)

### Refactored Methods

- `add_query`: Sets up watchers, sends initialization update
- `update_query`: Updates predicates, handles adds/removes, updates watchers atomically
- `remove_query`: Cleans up watchers for removed query
- `unsubscribe`: Cleans up all watchers for subscription
- `system_reset`: Clears entity watchers and notifies all subscriptions

### Critical Bug Fixed

In `WatcherSet::accumulate_interested_watchers`: Entity watchers for predicates (`EntityWatcherId::Predicate`) must call `add_query()` not `add_entity()` to route changes through query evaluation loop where membership changes are detected. This was the root cause of gap filling test failures.

## Testing

All tests pass including:

- Basic reactor tests
- Gap filling tests (single and multi-node, ascending and descending)
- Inter-node tests
- Property backend tests

## Future Considerations

- **Gap filling serialization**: Currently gap fill tasks can be spawned concurrently if `notify_change` is called again before previous gap fill completes. Architecture now supports adding per-subscription serialization (e.g., mutex or pending flag) to prevent concurrent gap fills
- Possible merge of `reactor::subscription_state::Subscription` and `reactor::subscription::ReactorSubscription` (would require weak ref to break cycle)
- Coalescing semantics for batching multiple change notifications when one change notification is in progress and another one is received
