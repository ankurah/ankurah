# EventAccumulator Refactor — Implementation Resume

**Worktree:** `/Users/daniel/ak/ankurah-201` (branch: `concurrent-updates-event-dag`)
**Date:** 2026-02-06
**Status:** All 4 phases complete. Post-implementation review in progress.

---

## Active Discussion: Event Storage Ordering & Idempotency

**This is the most important section. Read this first when resuming.**

### The Problem

The adversarial review (post Phase 4) found a correctness gap: re-delivery of a non-head historical event can corrupt the entity's head into a spurious multi-head state.

**Concrete scenario:** Chain A→B→C, head=[C]. Event B is re-delivered.
1. `compare_unstored_event(B, head=[C])` compares parent(B)=[A] vs head=[C]
2. A is an ancestor of C → `StrictAscends`
3. `compare_unstored_event` transforms this to `DivergedSince(meet=[A])` — it assumes B is new
4. DivergedSince handler: removes meet [A] from head (no-op, head is [C]), inserts B → head becomes [C, B]
5. **Corrupted:** B and C coexist as tips, but B is an ancestor of C

### Why It Happens

`compare_unstored_event` was designed for genuinely unstored events. It compares the event's PARENT clock and infers the event's relationship via a transform. The transform assumes the event is new. When the event is already stored (because of eager storage in `node_applier.rs`), this assumption is violated, and the StrictAscends→DivergedSince transform is wrong for re-delivered historical events.

### The Two Storage Paths (current code)

**Local commit** (`context.rs:commit_local_trx_impl`):
- Line 303: `forked.apply_event()` — event is NOT stored yet (vetting the fork) ✓
- Line 314: `collection.add_event()` — stored AFTER vetting ✓
- Line 331: `upstream.apply_event()` — event IS stored at this point
- Local commits generate fresh EventIds, so re-delivery is structurally impossible here

**Remote delivery** (`node_applier.rs:save_events`):
- Line 169: `collection.add_event()` — stored EAGERLY before apply_event ✗
- This is where re-delivery corruption can happen
- There is an existing TODO at line 166-168 about "suspense set" — Daniel already identified this concern

### Consensus Direction (Daniel + Codex + subagent analysis)

**Hold the incoming event in suspense during vetting, store only on acceptance:**

1. `compare_unstored_event` works correctly when the event is genuinely unstored — the parent-comparison + transform gives the right answer for all cases
2. For single-event delivery: vet → accept → store → update head. Clean and correct.
3. For batch delivery (multiple events for same entity): events need a "suspense set" that the retriever can draw from during BFS, so later events in the batch can discover earlier ones. Store to the collection only on acceptance. (This matches the existing TODO in `node_applier.rs:166-168`)
4. The event MUST be conclusively stored BEFORE updating EntityKind::Primary to contain a reference to it
5. `has_event_local(id)` (local-only, no network fallback) can be an optional perf optimization in the caller to skip needless vetting of known re-deliveries, but correctness does NOT depend on it

**Key insight from Codex:** For elegance, all correctness should live in `apply_event` / `compare_unstored_event`. The caller-level check is a performance optimization, not a correctness requirement. `compare_unstored_event` is not literally "the event is unstored" — it's a vetting/admission function. It works correctly when the event isn't pre-stored.

### What Needs to Change

1. **`node_applier.rs`**: Move `collection.add_event()` to AFTER `apply_event` succeeds (or implement the suspense set from the TODO for batch scenarios)
2. **`entity.rs`**: The `apply_event` DivergedSince handler should store the event before updating the head (ensure event is persisted before head references it)
3. **`compare_unstored_event`**: No changes needed — it already works correctly for unstored events
4. **Optional**: Add `has_event_local(id)` to storage layer as a perf optimization for the ingestion path

### Remaining Review Items

- [ ] **Implement suspense-then-store pattern** in `node_applier.rs` (fixes the idempotency gap)
- [ ] **Remove 2 empty `#[ignore]`d test stubs** (`test_backend_first_seen_at_layer_n...` and `test_toctou_retry_exhaustion...`) — empty bodies, covered elsewhere or too complex for unit test
- [ ] **`test_sequential_text_operations`**: Valid test, blocked by known Yrs empty-string-as-null bug (PR #236). Keep `#[ignore]` with TODO referencing PR #236.
- [ ] **Missing `AppliedViaLayers` variant** in `StateApplyResult` — benign, no code path uses it
- [ ] **`build_forward_chain` multi-meet bug** (comparison.rs:399-412) — P2 finding, chains are informational only, nil impact

---

## Your Role

You are the supervisor agent. You coordinate implementation across phases, dispatch code work to sub-agents (use background Task agents), run validation gates between phases, and preserve your own context for decision-making. You do not write code directly — dispatch each phase to a sub-agent.

## The Plan

`specs/concurrent-updates/event-accumulator-refactor-plan.md` — this is the authoritative implementation spec. Read it first. It contains full code sketches, type definitions, behavioral rules, migration phases with checklists, and resolved design questions.

The implementation prompt at `specs/concurrent-updates/implementation-prompt.md` has additional coordination guidance (phase gates, invariants, parallelization rules).

---

## What Has Been Completed

### Phase 1: Add new types (non-breaking, additive) — DONE

All items done and validated. Commit `d9669f9c`.

- [x] `get_event` and `event_exists` methods on `Retrieve` trait
- [x] `lru` crate dependency
- [x] `EventAccumulator` struct in `event_dag/accumulator.rs`
- [x] `EventLayers` struct with children index
- [x] `ComparisonResult` struct
- [x] `StateApplyResult` enum
- [x] `MutationError::DuplicateCreation` variant
- [x] Unit tests for accumulator helpers and new EventLayer

### Phase 2: Comparison Rewrite — DONE

All items done and validated. Commit `d9669f9c`.

- [x] `Comparison` struct uses `R: Retrieve` (replaces `CausalNavigator`)
- [x] `compare()` and `compare_unstored_event()` return `ComparisonResult<R>`
- [x] Budget escalation with 4x retry, warm accumulator cache
- [x] All comparison tests adapted to `MockRetriever` with concrete Event types
- [x] Assertion-related tests removed
- [x] `CausalNavigator` trait bounds removed from `node.rs` and `node_applier.rs`

### Phase 3: Update Callers + Correctness Fixes — DONE

All items done. Commit `5cbc255a`.

**Files modified:**
| File | Change |
|------|--------|
| `core/src/entity.rs` | Creation uniqueness guard (`DuplicateCreation`), `into_layers()` replaces manual `compute_layers()`, budget increased to 1000, backend replay for late-created backends, `apply_state` returns `StateApplyResult` |
| `core/src/property/backend/lww.rs` | Accepts new `accumulator::EventLayer`, `older_than_meet` rule via `dag_contains()`, infallible `compare()` |
| `core/src/property/backend/mod.rs` | `PropertyBackend::apply_layer` takes new `EventLayer` type |
| `core/src/property/backend/yrs.rs` | Signature updated for new `EventLayer` type |
| `core/src/event_dag/accumulator.rs` | Added `EventLayer::new()` constructor |
| `core/src/event_dag/tests.rs` | LWW/Yrs layer tests updated to use new `accumulator::EventLayer` |

### Phase 4: Cleanup + Tests — DONE

All items done. Commit `87447a9b`.

**Cleanup:**
- Removed `navigator.rs` entirely (CausalNavigator, AccumulatingNavigator, assertions types)
- Removed old `EventLayer<Id, E>`, `compute_layers()`, `compute_ancestry()`, `children_of()`, `is_descendant()` from `layers.rs`
- Removed `EventStaging` trait and `staged_events` from retrievers
- Replaced staged event flow with eager `collection.add_event()` in `node_applier.rs`
- Added `InvalidEvent` guard for non-creation events on empty-head entities
- `layers.rs` now contains only the `CausalRelation` enum

**Tests added (6 passing, 2 `#[ignore]`d stubs):**
1. Stored event_id below meet loses to layer candidate ✓
2. Re-delivery of historical event is idempotent no-op ✓
3. Second creation event rejected (DuplicateCreation) ✓
4. Backend replay for late-created backends — `#[ignore]` (needs integration test)
5. Budget escalation succeeds where initial budget fails ✓
6. TOCTOU retry exhaustion — `#[ignore]` (needs timing control)
7. Mixed-parent merge event correctly layered ✓
8. Eagerly stored peer events discoverable by BFS ✓

### Post-Implementation: Adversarial Review — DONE

Spec compliance review completed. See "Active Discussion" section at top for the key finding (idempotency gap) and consensus direction.

---

## Critical Invariants

1. **No `R: Clone`.** Budget escalation is internal to `Comparison` — the retriever stays owned by the accumulator.
2. **Don't alter BFS traversal logic.** You're changing retrieval plumbing, not the algorithm.
3. **Out-of-DAG parents are dead ends, not errors.** Use `if let Some` / `else { continue }`. Never panic or return error for missing parents.
4. **Idempotency and creation guards go before the retry loop** in `apply_event`, not inside it.
5. **Never call `into_layers()` on `BudgetExceeded`.** The DAG is incomplete.
6. **Event must be stored BEFORE head is updated** to reference it.

---

## Known Test State

- `cargo test` — all pass, 3 `#[ignore]`d:
  - `test_sequential_text_operations` — valid test, blocked by Yrs empty-string bug (PR #236)
  - `test_backend_first_seen_at_layer_n...` — empty stub, recommend remove
  - `test_toctou_retry_exhaustion...` — empty stub, recommend remove
- The 2 previously-failing nonexistent_entity tests now pass

## Key Commits

| Commit | Content |
|--------|---------|
| `d9669f9c` | Phase 1-2: EventAccumulator types + comparison rewrite |
| `5cbc255a` | Phase 3: caller updates, correctness guards |
| `87447a9b` | Phase 4: cleanup, remove old types, add tests |
