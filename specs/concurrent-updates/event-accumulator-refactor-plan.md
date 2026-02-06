# Event Accumulator Refactor Plan

## Summary

Refactor the comparison and event accumulation architecture to:
1. Eliminate the awkward `AccumulatingNavigator` side-channel pattern
2. Eliminate the `CausalNavigator` trait entirely (it was an abstraction over recursion that never happened)
3. Have `Comparison` accumulate events directly via `EventAccumulator`
4. Return layer iteration capability in `DivergedSince` results
5. Establish patterns that enable future streaming (issue #200)
6. Fix correctness bugs identified by compartmentalized review (InsufficientCausalInfo, idempotency, creation race, budget, backend layer gaps)

## Current State (Problems)

1. **`Comparison`** fetches full events during BFS, extracts id+parent, **discards events**
2. **`AccumulatingNavigator`** wraps navigator to intercept and collect events as side effect
3. **`CausalNavigator` trait** exists for "assertions" feature that returns empty - over-abstraction
4. **After comparison**, caller manually extracts events and wires to relation
5. **`apply_state`** doesn't use accumulator, can't handle divergence
6. **`EphemeralNodeRetriever.staged_events`** is awkward separate staging mechanism
7. **Stored property `event_id`s below the meet point** are absent from the accumulated DAG — LWW `apply_layer` must detect these and treat them as "older than meet" rather than attempting causal comparison (which would find no path and return `Concurrent` incorrectly)
8. **No idempotency guard** — re-delivery of historical events creates spurious multi-head states
9. **O(n²) `children_of`** — linear scan over all events for each parent lookup during layer computation
10. **Budget of 100** — too small for real-world DAGs, no escalation on exhaustion

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
                              │
                              ▼ into_layers()
              ┌───────────────────────────────────┐
              │         EventLayers<R>            │
              │  - children_index (O(1) lookup)   │
              │  - accumulator (owns retriever)   │
              │  - forward frontier expansion     │
              └───────────────────────────────────┘
```

## Key Design Decisions

1. **Retriever owns storage access** - No separate storage parameter needed
2. **Seeded events** - Incoming peer events are seeded before comparison starts; comparison must treat them as first-class in traversal (they may be the only source for a required parent in batch application)
3. **Store during layer iteration** - Events stored as they're applied, not before
4. **No Clone requirement on retrievers** - Budget escalation is handled internally by `Comparison` (retry loop with fresh state, same retriever instance), so callers and `R` do not need `Clone`. This avoids incompatibility with lifetime-borrowing retrievers like `EphemeralNodeRetriever`
5. **CausalNavigator eliminated** - Assertions feature was unused (returns empty)
6. **Missing stored event_id rule** - If a stored value's `event_id` is not in the accumulated event set for a layer, it is strictly older than the meet and must lose to any layer candidate (see §Behavioral Rules)
7. **Children index** - `EventLayers` pre-builds a parent→children index at construction for O(1) forward traversal
8. **EventLayer carries DAG structure, not full events** - `EventLayer` holds `dag: Arc<BTreeMap<EventId, Vec<EventId>>>` (parent pointers only), not cloned full events. `is_descendant` only needs parent lookups; `dag.contains_key()` is sufficient for the older-than-meet check. This avoids cloning events into a second map.

## Behavioral Rules

### LWW Resolution When Stored event_id Is Older Than Meet

The BFS walks backward from both heads to the meet. Every event above the meet is accumulated. Therefore:

- If a stored value's `event_id` is **not present** in the accumulated event set for a layer, it must be treated as **strictly older than the meet** and **must lose** to any candidate in the layer for the same property.
- If a *candidate's* `event_id` (from a layer event) is missing from the accumulated event set, this is an error (`InsufficientCausalInfo`) because the system cannot validate the candidate's lineage.

This preserves safety (don't accept unknown candidates) while allowing legitimate updates to overwrite stale stored values whose lineage is known to be older than the divergence point.

### Idempotency Contract

`apply_event` must reject events already present in storage before invoking comparison. Without this check, re-delivery of historical events produces spurious multi-head states where an ancestor and its descendant coexist as tips — a corrupted DAG.

### StrictDescends Assumes Causal Delivery

`StrictDescends` handling assumes **causal delivery** (all parents already applied). The forward chain in the result is informational. Implementations may optionally replay the chain, but are not required to if causal delivery is guaranteed by the protocol.

### Entity Creation Uniqueness

An entity may have at most one creation event (empty parent clock). If a second creation event arrives after the entity already has a non-empty head, it must be rejected. Otherwise, the entity ends up with two genesis events and all future comparisons may return `Disjoint`.

### Mixed-Parent Events Spanning the Meet Boundary

A merge event may have parents from multiple branches, where only some branches descend from the meet. For example, event E with parents [C, D] where D is the meet but C descends from an independent lineage (C ← A ← G). The forward layer expansion must not assume all events above the meet are reachable by forward expansion from the meet alone.

**Rule:** During layer computation, parents that are not present in the accumulated DAG are below the meet and treated as implicitly processed. The initial frontier is seeded from ALL events in the DAG whose in-DAG parents are all processed (a generalized topological-sort seed), not just children of the meet. This ensures merge events that bring in non-meet branches are correctly reached and their operations are applied.

Without this rule, events with mixed-parent lineages stall forever in the frontier and their operations are silently dropped.

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

    /// Seed with events from peer (before comparison).
    /// Seeded events are first-class during traversal — if a required parent
    /// exists only in the seed set, comparison must still succeed.
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

    /// Check whether an event_id is present in the accumulated DAG structure.
    /// Used by LWW resolution to distinguish "known older" from "unknown".
    pub fn contains(&self, id: &EventId) -> bool {
        self.dag.contains_key(id)
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
/// Computes layers lazily using forward expansion from the meet point.
/// Pre-builds a parent→children index at construction for O(1) lookups.
/// Events are stored to local storage as they are fetched for application.
pub struct EventLayers<R: Retrieve> {
    accumulator: EventAccumulator<R>,
    meet: Vec<EventId>,
    current_head_ancestry: BTreeSet<EventId>,

    /// Parent→children index, built once at construction: O(N)
    children_index: BTreeMap<EventId, Vec<EventId>>,

    /// Shared DAG structure reference, passed to each yielded EventLayer
    dag: Arc<BTreeMap<EventId, Vec<EventId>>>,

    // Iteration state
    processed: BTreeSet<EventId>,
    frontier: BTreeSet<EventId>,
}

impl<R: Retrieve> EventLayers<R> {
    /// Precondition: `accumulator.dag` contains all events on paths from both
    /// heads to the meet. This is guaranteed by construction — the backward BFS
    /// in Comparison is the only pathway that populates the accumulator, and it
    /// walks both frontiers to convergence before returning DivergedSince.
    /// (Does not hold for BudgetExceeded results — those never reach this path.)
    fn new(
        accumulator: EventAccumulator<R>,
        meet: Vec<EventId>,
        current_head: Vec<EventId>,
    ) -> Self {
        // Build parent→children index from accumulated DAG: O(N)
        let mut children_index: BTreeMap<EventId, Vec<EventId>> = BTreeMap::new();
        for (id, parents) in &accumulator.dag {
            for parent in parents {
                children_index.entry(parent.clone()).or_default().push(id.clone());
            }
        }

        // Compute current head ancestry for partitioning.
        // Walks backward from current_head through the dag's parent pointers,
        // collecting all ancestor event_ids into a BTreeSet.
        let current_head_ancestry = compute_ancestry_from_dag(&accumulator.dag, &current_head);

        // Snapshot the DAG structure into an Arc for sharing with EventLayers
        let dag = Arc::new(accumulator.dag.clone());

        // Initialize frontier: all events in the DAG whose in-DAG parents are
        // all processed. This is a generalized topological-sort seed — not just
        // children of the meet — because merge events can bring in branches that
        // are NOT descendants of the meet (e.g., event E with parents [C, D]
        // where D is the meet but C descends from a separate lineage). Parents
        // outside the DAG are below the meet and treated as implicitly processed.
        // This may introduce layers that are not descendants of the meet; those
        // represent concurrently introduced branches and must still be ordered
        // causally. The O(N) scan is a one-time cost, comparable to building the
        // children_index above.
        //
        // Note: meet is assumed to be a frontier — no meet member is an ancestor
        // of another. The comparison algorithm guarantees this (meet candidates
        // are the closest common ancestors, not arbitrary common ancestors).
        let processed: BTreeSet<_> = meet.iter().cloned().collect();
        let frontier: BTreeSet<EventId> = accumulator.dag.keys()
            .filter(|id| !processed.contains(id))
            .filter(|id| {
                accumulator.dag.get(id)
                    .map(|ps| ps.iter().all(|p|
                        processed.contains(p) || !accumulator.dag.contains_key(p)
                    ))
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        Self {
            accumulator,
            meet,
            current_head_ancestry,
            children_index,
            dag,
            processed: meet.iter().cloned().collect(),
            frontier,
        }
    }

    /// Get next layer (async - stores events during fetch).
    /// Returns layers in topological order (earliest layer first, given the
    /// DAG slice). Each layer is a frontier of events with no unprocessed
    /// in-DAG parents.
    pub async fn next(&mut self) -> Result<Option<EventLayer<EventId, Event>>, RetrievalError> {
        if self.frontier.is_empty() {
            return Ok(None);
        }

        let mut to_apply = Vec::new();
        let mut already_applied = Vec::new();
        let layer_frontier: Vec<EventId> = std::mem::take(&mut self.frontier)
            .into_iter().collect();

        for id in &layer_frontier {
            if self.processed.contains(id) {
                continue;
            }
            self.processed.insert(*id);

            // get_and_store_event: stores seeded events during iteration
            let event = self.accumulator.get_and_store_event(id).await?;

            // Partition based on whether already in local head ancestry
            if self.current_head_ancestry.contains(id) {
                already_applied.push(event);
            } else {
                to_apply.push(event);
            }
        }

        // Advance to next frontier: expand only from nodes processed THIS step.
        // For each node just processed, check its children via the pre-built index.
        // A child is ready when ALL its in-DAG parents have been processed.
        // Parents outside the DAG are below the meet and implicitly satisfied.
        // Cost: O(|layer_frontier| × max_children) per step, not O(|processed|).
        let mut next_frontier = BTreeSet::new();
        for id in &layer_frontier {
            if let Some(children) = self.children_index.get(id) {
                for child in children {
                    if !self.processed.contains(child) && !next_frontier.contains(child) {
                        let all_parents_done = self.accumulator.dag.get(child)
                            .map(|ps| ps.iter().all(|p|
                                self.processed.contains(p) || !self.accumulator.dag.contains_key(p)
                            ))
                            .unwrap_or(false);
                        if all_parents_done {
                            next_frontier.insert(child.clone());
                        }
                    }
                }
            }
        }
        self.frontier = next_frontier;

        if to_apply.is_empty() && already_applied.is_empty() {
            return Ok(None);
        }

        Ok(Some(EventLayer {
            to_apply,
            already_applied,
            dag: Arc::clone(&self.dag),
        }))
    }
}

/// Compute ancestry set by walking backward through DAG parent pointers.
/// Returns all event IDs reachable from `head` (inclusive).
///
/// Scoped to the comparison DAG by design — events below the meet are not
/// present in `dag` and are correctly excluded. When a parent ID is absent
/// from `dag`, traversal stops at that branch (the `if let Some` guard),
/// which is the correct behavior: those events are below the meet and
/// outside the scope of layer computation.
fn compute_ancestry_from_dag(
    dag: &BTreeMap<EventId, Vec<EventId>>,
    head: &[EventId],
) -> BTreeSet<EventId> {
    let mut ancestry = BTreeSet::new();
    let mut frontier: Vec<EventId> = head.to_vec();
    while let Some(id) = frontier.pop() {
        if !ancestry.insert(id) { continue; }
        if let Some(parents) = dag.get(&id) {
            for parent in parents {
                if !ancestry.contains(parent) {
                    frontier.push(parent.clone());
                }
            }
        }
    }
    ancestry
}
```

### EventLayer with causal comparison

```rust
/// A layer of concurrent events for unified backend application.
/// All events in a layer have no in-DAG ancestor/descendant relation with each
/// other — they form a topological wave. (With the generalized frontier seed,
/// a layer may include events from different branches at varying distances from
/// the meet; "same depth" is not strictly guaranteed.)
///
/// Carries a shared reference to the accumulated DAG structure (parent pointers
/// only, not full events) for causal comparison within the layer. This avoids
/// cloning full events into a second map — is_descendant only needs parent lookups.
#[derive(Debug, Clone)]
pub struct EventLayer<Id, E> {
    pub already_applied: Vec<E>,
    pub to_apply: Vec<E>,
    /// Shared DAG structure for causal comparison: event_id → parent_ids.
    /// Built from the EventAccumulator's dag and shared across all layers.
    dag: Arc<BTreeMap<Id, Vec<Id>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CausalRelation {
    Descends,
    Ascends,
    Concurrent,
}

impl<Id: EventId, E> EventLayer<Id, E> {
    /// Check whether an event_id is present in the accumulated DAG.
    /// Used by LWW to implement the "older than meet" rule: if a stored
    /// value's event_id is absent, it predates the meet and always loses.
    pub fn dag_contains(&self, id: &Id) -> bool {
        self.dag.contains_key(id)
    }

    /// Compare two event IDs using accumulated DAG context.
    ///
    /// Precondition: both `a` and `b` should be present in the DAG for
    /// meaningful results. For stored values, use `dag_contains()` first;
    /// absent IDs are provably below the meet and should not reach this
    /// method (see §Behavioral Rules, "older than meet" rule).
    ///
    /// Returns the causal relation between `a` and `b`. If either ID is
    /// not in the DAG, traversal treats missing entries as dead ends and
    /// the result will be Concurrent (no path found).
    pub fn compare(&self, a: &Id, b: &Id) -> CausalRelation {
        if a == b {
            return CausalRelation::Descends;
        }
        if is_descendant_dag(&self.dag, a, b) {
            return CausalRelation::Descends;
        }
        if is_descendant_dag(&self.dag, b, a) {
            return CausalRelation::Ascends;
        }
        CausalRelation::Concurrent
    }
}

/// Walk backward from `descendant` through parent pointers looking for `ancestor`.
/// Operates on the DAG structure map (id → parent_ids), not full events.
/// Missing entries are treated as dead ends (below the meet), not errors —
/// same pattern as `compute_ancestry_from_dag`. This is correct because the
/// DAG only contains events accumulated during comparison (above the meet);
/// parents outside the DAG are below the meet and cannot contain the ancestor.
fn is_descendant_dag<Id: EventId>(
    dag: &BTreeMap<Id, Vec<Id>>,
    descendant: &Id,
    ancestor: &Id,
) -> bool {
    let mut visited = BTreeSet::new();
    let mut frontier = vec![descendant.clone()];
    while let Some(id) = frontier.pop() {
        if !visited.insert(id.clone()) { continue; }
        if &id == ancestor { return true; }
        let Some(parents) = dag.get(&id) else { continue; };
        for parent in parents {
            if !visited.contains(parent) {
                frontier.push(parent.clone());
            }
        }
    }
    false
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
Budget escalation is handled internally by `Comparison`: on `BudgetExceeded`,
it resets its internal state (frontiers, visited sets, meet candidates) and
retries with 4× budget, up to a configurable max. The retry uses the same
`EventAccumulator` instance (which retains its DAG structure and cache from
the first attempt — these are still valid). The retriever does NOT need to
be `Clone`; it stays owned by the accumulator throughout.

### 3. AccumulatingNavigator

- **Deprecate** - no longer needed
- Comparison handles accumulation internally

### 4. apply_event (entity.rs)

```rust
const DEFAULT_BUDGET: usize = 1000;

// Idempotency guard: reject events already in storage
if retriever.event_exists(&event.id()).await? {
    return Ok(false);
}

// Entity creation uniqueness guard
if event.is_entity_create() && !self.head().is_empty() {
    return Err(MutationError::DuplicateCreation);
}

// Budget escalation is handled internally by compare_unstored_event —
// no Clone needed on the retriever. If the initial budget is exhausted,
// Comparison retries internally with 4× budget before returning BudgetExceeded.
let result = compare_unstored_event(retriever, event, &head, DEFAULT_BUDGET).await?;
match result.relation {
    DivergedSince { .. } => {
        let mut layers = result.into_layers(head.as_slice().to_vec()).unwrap();
        let mut applied_layers: Vec<EventLayer<EventId, Event>> = Vec::new();

        while let Some(layer) = layers.next().await? {
            // Check for backends that first appear in this layer
            for evt in &layer.to_apply {
                for (backend_name, _) in evt.operations.iter() {
                    if !state.backends.contains_key(backend_name) {
                        let backend = backend_from_string(backend_name, None)?;
                        // Replay earlier layers for this backend
                        for earlier in &applied_layers {
                            backend.apply_layer(earlier)?;
                        }
                        state.backends.insert(backend_name.clone(), backend);
                    }
                }
            }

            // Apply to all backends
            for backend in state.backends.values() {
                backend.apply_layer(&layer)?;
            }
            applied_layers.push(layer);
        }
    }
    BudgetExceeded { .. } => {
        // Internal escalation already attempted and failed.
        return Err(LineageError::BudgetExceeded { .. }.into());
    }
    // ... other cases unchanged
}
```

### 5. apply_state (entity.rs)

Returns a richer result enum instead of `bool`:

```rust
pub enum StateApplyResult {
    Applied,              // StrictDescends — state applied directly
    AppliedViaLayers,     // DivergedSince — divergence handled via layer merge
    AlreadyApplied,       // Equal — no-op
    Older,                // StrictAscends — incoming state is older, no-op
    DivergedRequiresEvents, // DivergedSince but no events available for merge
}

pub async fn apply_state<R: Retrieve>(
    &self,
    retriever: R,
    state: &State,
) -> Result<StateApplyResult, MutationError> {
    let result = compare(retriever, &state.head, &self.head(), DEFAULT_BUDGET).await?;

    match result.relation {
        Equal => Ok(StateApplyResult::AlreadyApplied),
        StrictDescends { .. } => {
            // Apply state buffers directly (causal delivery assumed)
            // ...
            Ok(StateApplyResult::Applied)
        }
        StrictAscends => Ok(StateApplyResult::Older),
        DivergedSince { .. } => {
            // State snapshots cannot be merged without events.
            // Signal caller to request event history.
            Ok(StateApplyResult::DivergedRequiresEvents)
        }
        // ...
    }
}
```

### 6. LWW apply_layer — missing-event rule

```rust
fn apply_layer(&self, layer: &EventLayer<EventId, Event>) -> Result<(), MutationError> {
    let mut winners: BTreeMap<PropertyName, Candidate> = BTreeMap::new();

    // Seed with stored last-write candidates
    {
        let values = self.values.read().unwrap();
        for (prop, entry) in values.iter() {
            let Some(event_id) = entry.event_id() else {
                // Pre-migration LWW values may lack event_id. This is a hard
                // error — data must be migrated before this code path runs.
                // If pre-migration data coexistence is needed, a fallback
                // (e.g., treat as older_than_meet) should be added here.
                return Err(MutationError::UpdateFailed(
                    anyhow::anyhow!("LWW candidate missing event_id for property {}", prop).into(),
                ));
            };

            // KEY RULE: If stored event_id is NOT in the accumulated DAG,
            // it is strictly older than the meet. Any layer candidate wins.
            // We still seed it so it participates if no layer event touches
            // this property, but mark it as auto-losable.
            let known_in_dag = layer.dag_contains(&event_id);
            winners.insert(prop.clone(), Candidate {
                value: entry.value(),
                event_id,
                from_to_apply: false,
                older_than_meet: !known_in_dag,
            });
        }
    }

    // Add candidates from events in this layer
    for (event, from_to_apply) in /* ... */ {
        // ...
        if let Some(current) = winners.get_mut(&prop) {
            if current.older_than_meet {
                // Stored value is below meet — any layer candidate wins
                *current = candidate;
            } else {
                // Both in accumulated set — use causal comparison
                let relation = layer.compare(&candidate.event_id, &current.event_id);
                match relation {
                    CausalRelation::Descends => { *current = candidate; }
                    CausalRelation::Ascends => {}
                    CausalRelation::Concurrent => {
                        if candidate.event_id > current.event_id {
                            *current = candidate;
                        }
                    }
                }
            }
        } else {
            winners.insert(prop, candidate);
        }
    }

    // Apply winners from to_apply events...
}
```

### 7. CausalNavigator trait

- **Remove entirely** - was only abstraction over recursion that never happened
- The `expand_frontier` concept replaced by `EventAccumulator.get_event()` calls
- Assertions feature was unused (always returned empty)
- If peer-aided shortcuts needed in future, add as optional callback

### 8. EphemeralNodeRetriever

- Remove `staged_events` field - replaced by `EventAccumulator.seeded`
- Remove `EventStaging` trait methods if no longer needed
- Does NOT need `Clone` — budget escalation is internal to `Comparison`, so retrievers with lifetime borrows remain compatible

## Migration Path

### Phase 1: Add new types (non-breaking)
- [ ] Add `get_event` method to `Retrieve` trait (replaces `CausalNavigator` fetch)
- [ ] Add `store_event` method to `Retrieve` trait (or create new trait)
- [ ] Add `event_exists` method to `Retrieve` trait (for idempotency guard; can delegate to `get_event` + match on `NotFound`, or add a dedicated `has_event` to the storage layer)
- [ ] Add `lru` crate dependency to `core/Cargo.toml`
- [ ] Add `EventAccumulator` struct in new `event_dag/accumulator.rs`
- [ ] Add `EventLayers` struct with children index (can extend existing `layers.rs`)
- [ ] Add `ComparisonResult` struct
- [ ] Add `StateApplyResult` enum
- [ ] Add `MutationError::DuplicateCreation` variant
- [ ] Add unit tests for `EventAccumulator` and `EventLayers`

### Phase 2: Modify Comparison internals + budget
- [ ] Replace `CausalNavigator` usage with direct `Retrieve` calls
- [ ] Add `EventAccumulator` to `Comparison` struct
- [ ] Store full events in `process_event()` via `accumulator.accumulate()`
- [ ] Return `ComparisonResult` from `compare()`
- [ ] Update `compare_unstored_event()` to seed accumulator with unstored event
- [ ] Increase default budget from 100 to 1000
- [ ] Add internal budget escalation in `Comparison`: on `BudgetExceeded`, reset traversal state (retain accumulator cache), retry with 4× budget up to configurable max

### Phase 3: Update callers + correctness fixes
- [ ] Add idempotency guard to `apply_event`: check `event_exists` before comparison
- [ ] Add entity creation uniqueness guard to `apply_event`
- [ ] Update `apply_event` to use `result.into_layers()`
- [ ] Implement backend-replay for late-created backends across layer boundaries
- [ ] Update LWW `apply_layer` with the missing-stored-event rule (older_than_meet)
- [ ] Update `apply_state` to return `StateApplyResult` enum
- [ ] Update `apply_state` to handle divergence via `DivergedRequiresEvents`
- [ ] Update `node_applier.rs` StateAndEvent handling
- [ ] Update any other `compare()` callers

### Phase 4: Cleanup + tests
- [ ] Remove `AccumulatingNavigator`
- [ ] Remove `CausalNavigator` trait
- [ ] Remove `staged_events` from `EphemeralNodeRetriever`
- [ ] Remove standalone `compute_layers` / `compute_ancestry` functions
- [ ] Remove `EventStaging` trait if no longer needed
- [ ] Add test: stored event_id below meet loses to layer candidate
- [ ] Add test: re-delivery of historical event is rejected (idempotency)
- [ ] Add test: second creation event for same entity is rejected
- [ ] Add test: backend first seen at Layer N gets Layers 1..N-1 replayed
- [ ] Add test: budget escalation succeeds where initial budget fails
- [ ] Add test: TOCTOU retry exhaustion produces clean error
- [ ] Add test: merge event with parent from non-meet branch is correctly layered (mixed-parent spanning meet)
- [ ] Add test: seeded event is visible during comparison traversal (validates seed mechanism)
- [ ] Cleanup: assess `Retrieve` trait scope — it now spans state retrieval (`get_state`), event access (`get_event`, `event_exists`), and event storage (`store_event`), collapsing the original local-only vs network-fallback separation that `CausalNavigator` provided. Consider renaming or splitting the trait to reflect its expanded role. The original design had `Retrieve` as local-only state access and `CausalNavigator` as the (optionally network-aware) event traversal layer; that bifurcation is lost. Low priority but worth addressing for API clarity

## Resolved Design Questions

1. **LRU cache size** - Default to 1000, make configurable via builder pattern

2. **Retriever trait** - Needs `get_event(&EventId)`, `store_event(&Event)`, and `event_exists(&EventId)` methods added. Does NOT require `Clone` — budget escalation is internal to `Comparison`. `event_exists` can be implemented as a thin wrapper over `get_event` returning `Ok(false)` on `NotFound`, or as a dedicated storage-level `has_event(id)` method for efficiency

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

9. **InsufficientCausalInfo for stored values** - Resolved by the "older than meet" rule
   - Stored event_id absent from accumulated DAG → provably below meet → always loses
   - Layer candidate event_id absent from accumulated DAG → error (should never happen)
   - No async fallback needed; `EventLayer.compare()` stays sync
   - LWW calls `layer.dag_contains(&event_id)` to check; `layer.compare()` only called when both IDs are known in the DAG

10. **StateApplyResult** - Resolved: use a rich enum
    - `Applied` - state applied directly (StrictDescends)
    - `AppliedViaLayers` - divergence handled via layer merge
    - `AlreadyApplied` - Equal, no-op
    - `Older` - StrictAscends, no-op
    - `DivergedRequiresEvents` - divergence, but no events for merge

11. **BudgetExceeded handling** - Budget escalation is internal to `Comparison`. On first `BudgetExceeded`, `Comparison` resets its traversal state (frontiers, visited sets, meet candidates, counters) but retains the `EventAccumulator` (DAG structure and cache are still valid), then retries with 4× budget. If the escalated attempt also exceeds budget, `BudgetExceeded` is returned to the caller as a hard error. True resumption (continuing from partial state) is not feasible — the returned frontiers are insufficient without full internal state. The accumulator's DAG cache provides a warm start for the retry, avoiding redundant fetches

12. **Backward compatibility** - Breaking change. The old APIs (`AccumulatingNavigator`, `CausalNavigator`, `compute_layers`) are removed in Phase 4, not deprecated.

## Related Issues

- #200 - Streaming layer iteration (this plan enables it)
- Current PR - StateAndEvent divergence fix (this plan supersedes the quick fix)

## Files to Modify

### New Files
- `core/src/event_dag/accumulator.rs` - New `EventAccumulator` type

### Major Changes
- `core/src/event_dag/comparison.rs` - Embed `EventAccumulator`, return `ComparisonResult`
- `core/src/event_dag/layers.rs` - Update `EventLayers` to use accumulator + children index
- `core/src/entity.rs` - Update `apply_event` (idempotency, creation guard, backend replay, budget), `apply_state` (rich return type)
- `core/src/retrieval.rs` - Add `store_event`, `event_exists` to `Retrieve` trait
- `core/src/property/backend/lww.rs` - Implement older_than_meet rule in `apply_layer`

### Minor Changes
- `core/src/event_dag/mod.rs` - New exports, remove old ones
- `core/src/error.rs` - Add `DuplicateCreation` variant to `MutationError`
- `core/src/node_applier.rs` - Update StateAndEvent handling (may simplify)

### Files to Remove/Deprecate
- `core/src/event_dag/navigator.rs` - `CausalNavigator` trait, `AccumulatingNavigator`
- `core/src/retrieval.rs` - `EventStaging` trait, `staged_events` from `EphemeralNodeRetriever`

## Spec Alignment Notes

The following spec sections should be updated to match this plan:

- **"LWW Resolution Within Layer"** - Add the older-than-meet rule
- **"Idempotency / Apply Event"** - Document the storage pre-check requirement
- **"BudgetExceeded"** - Remove "resume with frontiers" claim; document as hard error with escalation
- **"StrictDescends Action Table"** - Clarify causal delivery assumption; chain is informational
- **"apply_state Semantics"** - Document `StateApplyResult` enum replacing `bool`
- **"API Reference"** - Update `apply_layer` signature, `ValueEntry` states, `compare()` return type
- **"Performance Notes"** - Document children index requirement for layer computation
