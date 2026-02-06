# EventAccumulator Refactor — Implementation Resume

**Worktree:** `/Users/daniel/ak/ankurah-201` (branch: `concurrent-updates-event-dag`)
**Date:** 2026-02-06
**Status:** Phases 1–2 complete. Phases 3–4 remain.

---

## Your Role

You are the supervisor agent. You coordinate implementation across 4 sequential phases, dispatch code work to sub-agents (use background Task agents), run validation gates between phases, and preserve your own context for decision-making. You do not write code directly — dispatch each phase to a sub-agent.

## The Plan

`specs/concurrent-updates/event-accumulator-refactor-plan.md` — this is the authoritative implementation spec. Read it first. It contains full code sketches, type definitions, behavioral rules, migration phases with checklists, and resolved design questions.

The implementation prompt at `specs/concurrent-updates/implementation-prompt.md` has additional coordination guidance (phase gates, invariants, parallelization rules).

---

## What Has Been Completed (Phase 1)

Phase 1 was "Add new types (non-breaking, additive)". All items are done and validated:

### Files Modified (6 tracked + 1 new untracked)

| File | Change | Lines |
|------|--------|-------|
| `core/Cargo.toml` | Added `lru = "0.12"` dependency | +1 |
| `core/src/retrieval.rs` | Added `get_event()` and `event_exists()` to `Retrieve` trait; implemented for `LocalRetriever` (line 126) and `EphemeralNodeRetriever` (line 305) | +62 |
| `core/src/error.rs` | Added `MutationError::DuplicateCreation` variant (line 170) | +2 |
| `core/src/entity.rs` | Added `StateApplyResult` enum (line 18) | +12 |
| `core/src/event_dag/mod.rs` | Added `pub mod accumulator;` and re-exports for `ComparisonResult`, `EventAccumulator`, `EventLayers` | +6 |
| `core/src/event_dag/accumulator.rs` | **NEW FILE** (463 lines) — `EventAccumulator<R>`, `ComparisonResult<R>`, `EventLayers<R>`, new `EventLayer` (with `dag: Arc<BTreeMap<EventId, Vec<EventId>>>`), `compute_ancestry_from_dag()`, `is_descendant_dag()`, 4 unit tests | +463 |
| `Cargo.lock` | Updated for lru dependency | +33 |

### Phase 1 Gate Results
- `cargo check` — passes (only pre-existing warnings)
- `cargo test` — all pass except 2 pre-existing failures in `nonexistent_entity.rs`
- No existing function signatures modified
- No existing call sites modified
- 116 insertions, 0 deletions in tracked files

### Phase 1 Checklist (all done)
- [x] `get_event` method on `Retrieve` trait
- [x] `event_exists` method on `Retrieve` trait (with default impl)
- [x] `lru` crate dependency
- [x] `EventAccumulator` struct in `event_dag/accumulator.rs`
- [x] `EventLayers` struct with children index
- [x] `ComparisonResult` struct
- [x] `StateApplyResult` enum
- [x] `MutationError::DuplicateCreation` variant
- [x] Unit tests for accumulator helpers and new EventLayer

---

## What Remains

### Phase 2: Comparison Rewrite (NEXT — single agent, do NOT split)

**Key structural change:** The current BFS in `comparison.rs` calls `navigator.expand_frontier(&all_frontier_ids, budget)` which returns multiple events at once via the `CausalNavigator` trait. The new code calls `accumulator.get_event(id)` per frontier node via the `Retrieve` trait. The assertions system is dropped entirely (always returned empty).

**Files to modify:**
- `core/src/event_dag/comparison.rs` (614 lines) — the main rewrite
- `core/src/event_dag/tests.rs` (1802 lines) — tests must be adapted to new signatures

**What changes in comparison.rs:**

1. **Comparison struct**: Change from `Comparison<'a, N: CausalNavigator>` to `Comparison<R: Retrieve>`. Replace `navigator: &'a N` with `accumulator: EventAccumulator<R>`. All `N::EID` becomes `EventId`, all `N::Event` becomes `Event`.

2. **step() method**: Instead of batch `navigator.expand_frontier(&all_ids, budget)`, iterate frontier IDs individually and call `accumulator.get_event(&id).await` for each. Call `accumulator.accumulate(&event)` for each fetched event. Each fetch costs 1 budget. Drop all assertion processing (the `process_assertion()` method, `AssertionResult` handling).

3. **DO NOT alter BFS traversal logic.** The core algorithm (frontier expansion, meet detection, state tracking in `process_event()`, `check_result()`, `build_forward_chain()`, `collect_immediate_children()`) was validated correct by 8/8 reviewers. You're changing the retrieval plumbing, not the algorithm.

4. **compare() signature**: `pub async fn compare<R: Retrieve>(retriever: R, subject: &Clock, comparison: &Clock, budget: usize) -> Result<ComparisonResult<R>, RetrievalError>`. Takes `retriever: R` directly (not `&N`). Returns `ComparisonResult<R>`.

5. **compare_unstored_event() signature**: `pub async fn compare_unstored_event<R: Retrieve>(retriever: R, event: &Event, comparison: &Clock, budget: usize) -> Result<ComparisonResult<R>, RetrievalError>`. Same pattern. The transformation logic (StrictAscends → DivergedSince for unstored events, etc.) stays the same but wraps results in `ComparisonResult`.

6. **Budget escalation**: On `BudgetExceeded`, reset traversal state (frontiers, visited, meet candidates, counters) but retain the `EventAccumulator` (cache is still valid). Retry with 4× budget up to a configurable max. Default budget increases from 100 to 1000.

7. **No `R: Clone` requirement.** Budget escalation reuses the same accumulator instance. The retriever stays owned by the accumulator throughout.

**What changes in tests.rs:**

The existing tests use generic mock types (`TestId = u32`, `TestEvent`, `MockEventStore` implementing `CausalNavigator`). Since the new `compare()` takes `R: Retrieve` and works with concrete `ankurah_proto::EventId`/`Event`, the tests need adaptation:

- Create a `MockRetriever` that implements `Retrieve` (stores `HashMap<EventId, Event>`)
- Create events using `ankurah_proto::Event` with varying `entity_id` seeds for unique content-hashed IDs
- Helper: `fn make_test_event(seed: u8, parent_ids: &[EventId]) -> Event`
- Build DAGs bottom-up: create genesis, capture its `.id()`, use that as parent for children
- Test assertions check the same causal relations, but can't compare against literal numeric IDs — instead compare structural properties (meet length, chain length, relation variant)
- **Remove all assertion-related tests** (the `test_assertion_*` tests) — assertions are being dropped
- Keep all non-assertion comparison tests, all multi-head tests, all chain ordering tests, all idempotency tests
- The LWW/Yrs layer tests at the bottom of tests.rs use the OLD `EventLayer<Id, E>` from layers.rs — leave those unchanged for now (they'll be updated in Phase 3)

**Gate:** `cargo check && cargo test` with comparison tests adapted. Verify `ComparisonResult.relation` matches old return values for every non-assertion test case.

### Phase 3: Update Callers + Correctness Fixes

**Files to modify:**
- `core/src/entity.rs` — `apply_event()` and `apply_state()`
- `core/src/property/backend/lww.rs` — `apply_layer()` with older_than_meet rule
- `core/src/node_applier.rs` — StateAndEvent handling
- Any other `compare()` callers (check with grep)

**Key changes:**
- `apply_event`: Add idempotency guard (`event_exists` before comparison), creation uniqueness guard (`DuplicateCreation`), use `result.into_layers()` instead of manual `compute_layers()`, increase budget to 1000, backend replay for late-created backends
- `apply_state`: Return `StateApplyResult` enum instead of `bool`, handle `DivergedRequiresEvents`
- `lww.rs apply_layer`: Must accept the NEW `accumulator::EventLayer` type (not the old `layers::EventLayer<Id, E>`). Add `older_than_meet` rule: if stored value's `event_id` is NOT in the accumulated DAG (`layer.dag_contains()`), it is strictly older than the meet and any layer candidate wins
- Guards go BEFORE the retry loop in `apply_event`, not inside it

**Parallelization:** entity.rs and lww.rs changes are somewhat independent — can dispatch 2 agents.

**Gate:** `cargo check && cargo test` (the 2 pre-existing failures in `nonexistent_entity.rs` should NOW PASS after this phase). Then launch a spec alignment agent to verify each behavioral rule maps to exact file:line.

### Phase 4: Cleanup + Tests

**Files to modify/remove:**
- Remove `core/src/event_dag/navigator.rs` entirely
- Remove `AccumulatingNavigator`, `CausalNavigator` trait, `NavigationStep`, `AssertionResult`, `AssertionRelation` exports from `mod.rs`
- Remove `staged_events` from `EphemeralNodeRetriever` and `LocalRetriever`
- Remove `EventStaging` trait
- Remove standalone `compute_layers()`, `compute_ancestry()`, `children_of()` from `layers.rs` (replaced by accumulator)
- Remove old `EventLayer<Id, E>` from `layers.rs` (replaced by `accumulator::EventLayer`)

**New tests to add (8 from the plan):**
1. Stored event_id below meet loses to layer candidate
2. Re-delivery of historical event is rejected (idempotency)
3. Second creation event for same entity is rejected
4. Backend first seen at Layer N gets Layers 1..N-1 replayed
5. Budget escalation succeeds where initial budget fails
6. TOCTOU retry exhaustion produces clean error
7. Merge event with parent from non-meet branch is correctly layered
8. Eagerly stored peer events are discoverable by BFS during comparison

**Gate:** `cargo check && cargo test` all green. Grep to confirm no references to removed types. Then launch an adversarial agent that reads `specs/concurrent-updates/claude-metareview.md` and verifies each P0/P1/P2 finding is addressed.

---

## Critical Invariants (copy from implementation-prompt.md)

1. **No `R: Clone`.** Budget escalation is internal to `Comparison` — the retriever stays owned by the accumulator.
2. **Don't alter BFS traversal logic.** You're changing retrieval plumbing, not the algorithm.
3. **Out-of-DAG parents are dead ends, not errors.** Use `if let Some` / `else { continue }`. Never panic or return error for missing parents.
4. **Idempotency and creation guards go before the retry loop** in `apply_event`, not inside it.
5. **Never call `into_layers()` on `BudgetExceeded`.** The DAG is incomplete.
6. **Follow the plan's code sketches when stuck.** They handle edge cases you might not anticipate.

---

## Key Type Signatures (current state after Phase 1)

### Retrieve trait (retrieval.rs:20-38)
```rust
#[async_trait]
pub trait Retrieve {
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError>;
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError>;
    async fn event_exists(&self, event_id: &EventId) -> Result<bool, RetrievalError> { /* default impl */ }
}
```

### EventAccumulator (accumulator.rs:27-93)
```rust
pub struct EventAccumulator<R: Retrieve> {
    dag: BTreeMap<EventId, Vec<EventId>>,
    cache: LruCache<EventId, Event>,
    retriever: R,
}
// Key methods: new(), accumulate(&Event), get_event(&EventId), contains(&EventId), dag(), into_layers()
```

### ComparisonResult (accumulator.rs:96-124)
```rust
pub struct ComparisonResult<R: Retrieve> {
    pub relation: AbstractCausalRelation<EventId>,
    accumulator: EventAccumulator<R>,  // private
}
// Key methods: new(), into_layers(current_head) -> Option<EventLayers<R>>, accumulator()
```

### New EventLayer (accumulator.rs:274-310)
```rust
pub struct EventLayer {
    pub already_applied: Vec<Event>,
    pub to_apply: Vec<Event>,
    dag: Arc<BTreeMap<EventId, Vec<EventId>>>,
}
// Key methods: dag_contains(&EventId), compare(&EventId, &EventId) -> CausalRelation, has_work()
```

### Old EventLayer (layers.rs:18-24) — still exists, remove in Phase 4
```rust
pub struct EventLayer<Id, E> {
    pub already_applied: Vec<E>,
    pub to_apply: Vec<E>,
    events: Arc<BTreeMap<Id, E>>,
}
// Key methods: compare() -> Result<CausalRelation, RetrievalError>  (fallible, unlike new one)
```

### Current compare() signatures (comparison.rs — TO BE REWRITTEN in Phase 2)
```rust
pub async fn compare<N, C>(navigator: &N, subject: &C, comparison: &C, budget: usize)
    -> Result<AbstractCausalRelation<N::EID>, RetrievalError>

pub async fn compare_unstored_event<N, E>(navigator: &N, event: &E, comparison: &E::Parent, budget: usize)
    -> Result<AbstractCausalRelation<N::EID>, RetrievalError>
```

### Target compare() signatures (Phase 2)
```rust
pub async fn compare<R: Retrieve>(retriever: R, subject: &Clock, comparison: &Clock, budget: usize)
    -> Result<ComparisonResult<R>, RetrievalError>

pub async fn compare_unstored_event<R: Retrieve>(retriever: R, event: &Event, comparison: &Clock, budget: usize)
    -> Result<ComparisonResult<R>, RetrievalError>
```

---

## Known Test State

- `cargo test` has exactly 2 pre-existing failures (expected to be fixed by Phase 3):
  - `server_rejects_update_for_nonexistent`
  - `server_rejects_create_for_existing`
- All other tests pass (including 4 new accumulator unit tests from Phase 1)

## File Sizes for Reference

| File | Lines | Purpose |
|------|-------|---------|
| `core/src/event_dag/comparison.rs` | 614 | BFS comparison — Phase 2 rewrites this |
| `core/src/event_dag/tests.rs` | 1802 | Comparison + layer tests — Phase 2 adapts comparison tests |
| `core/src/event_dag/accumulator.rs` | 463 | NEW in Phase 1 — EventAccumulator, EventLayers, new EventLayer |
| `core/src/event_dag/layers.rs` | 466 | Old compute_layers — Phase 4 removes |
| `core/src/event_dag/navigator.rs` | 149 | CausalNavigator — Phase 4 removes entirely |
| `core/src/entity.rs` | 712 | apply_event, apply_state — Phase 3 rewrites |
| `core/src/retrieval.rs` | 359 | Retrieve trait, LocalRetriever, EphemeralNodeRetriever |
| `core/src/property/backend/lww.rs` | 316 | LWW apply_layer — Phase 3 updates |
| `core/src/error.rs` | 345 | Error types |
