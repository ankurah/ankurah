# EventAccumulator Refactor — Implementation Resume

**Worktree:** `/Users/daniel/ak/ankurah-201` (branch: `concurrent-updates-event-dag`)
**Date:** 2026-02-10
**Status:** Phases 1-8 complete. All tests pass. Ready for merge.

---

## Current State

- `cargo check` / `cargo test` — all pass
- 176 core unit tests, 1 `#[ignore]`d (`test_toctou_retry_exhaustion`)
- 4/4 `durable_ephemeral` integration tests pass
- Full internals documentation in `docs/internals/` (7 files, mdbook-structured)
- Spec compliance: 91 compliant, 3 deviated-with-justification, 0 non-compliant

---

## What This Branch Does

Replaces the `CausalNavigator` + `compare_unstored_event` algorithm with a new
BFS-based `Comparison` state machine and `EventAccumulator`. Key changes:

1. **New `event_dag/` module** — `Comparison` state machine, `EventAccumulator`,
   `EventLayers`, `EventLayer` with DAG-aware `compare()` and `dag_contains()`.
2. **Staging pattern** — events are staged in-memory before `apply_event`, making
   them discoverable by BFS. Committed to storage after successful application.
3. **`Retrieve` trait split** → `GetEvents`, `GetState`, `SuspenseEvents` with
   clear separation of concerns.
4. **LWW `older_than_meet` rule** — stored values whose event ID is outside the
   accumulated DAG auto-lose to layer candidates.
5. **BFS fixes for ephemeral nodes** — quick-check for linear extension,
   both-frontiers meet detection, `EventNotFound` → error (not busyloop).
6. **`DuplicateCreation` guard removed** — `compare()` handles both re-delivery
   (`StrictAscends`) and disjoint genesis (`Disjoint`). Added
   `storage_is_definitive()` for durable-node fast path.

---

## Critical Invariants

1. **`stage_event` before head update (in memory); `commit_event` before `set_state` (to disk).**
2. **`get_event` = staging + storage (union view). `event_stored` = permanent storage only.**
3. **Both-frontiers = common ancestor.** Process with empty parents, no fetch needed.
4. **Unfetchable single-frontier events are errors.** Must return `Err`, not `continue`.
5. **Meet filter correctness.** `common_child_count == 0` ensures head tips always appear in meet.
6. **Disjoint genesis detected by BFS, not early guards.** `event_stored()` is unreliable on ephemeral nodes.
7. **No `E: Clone`.** Budget escalation (4x retry) is internal to `Comparison`.
8. **Never call `into_layers()` on `BudgetExceeded`.** DAG is incomplete.

---

## Deferred Items (GitHub Issues)

| Issue | Description | Priority |
|-------|-------------|----------|
| #242 | Fork/snapshot for `EntityKind::Transacted` in `commit_local_trx` | Pre-existing |
| #243 | Creation events bypass fork-based policy validation | Pre-existing |
| #244 | Remote event fetching bypasses `validate_received_event` | Pre-existing |
| #245 | `build_forward_chain` multi-meet bug (dead data, never read by production code) | P2 |
| #246 | No size/count limits on peer event messages | Enhancement |
| #247 | No traversal limit on `collect_event_bridge` | Enhancement |

---

## Known `#[ignore]`d Tests

| Test | Reason | Reference |
|------|--------|-----------|
| `test_toctou_retry_exhaustion` | Needs mock that modifies entity head during `get_event` | Branch-introduced stub |
| `test_sequential_text_operations` | Yrs empty-string = null, no creation event generated | #175, PR #236 |
| 3x sled multi-column ordering | i64 lexicographic sorting | #210 (pre-existing) |

---

## Review Findings (latest round, 2026-02-10)

### Spec Compliance
91 requirements compliant. 3 justified deviations:
- `apply_layer` takes `&EventLayer` (not separate slices) — DAG context needed for causal comparison
- Idempotency via BFS `StrictAscends` instead of `event_stored()` pre-check
- `DuplicateCreation` removed — `compare()` handles both cases

### Correctness Deep-Dive
11 items confirmed correct. 1 low-severity finding:
- **Quick-check false positive for multi-head subjects** — `apply_state` with multi-element subject clock could get false `StrictDescends` if one head has unrelated ancestry. Cannot occur in single-genesis systems. `apply_event` always uses single-event clocks.

### Error Handling
- Staging `RwLock` uses `.unwrap()` (6 sites) — should use `.unwrap_or_else(|e| e.into_inner())` for poison recovery. Trivial fix.
- Entity state `RwLock` poisoning cascade is pre-existing pattern used throughout codebase.

### Code Elegance
- `pub mod` in event_dag should be `pub(crate) mod` (API surface too wide)
- `DEFAULT_BUDGET` duplicated in `apply_event` and `apply_state`
- `SimpleRetriever` is duplicate of `MockRetriever` (branch-introduced, can remove)
- Test helpers duplicated across inner test modules (~120 lines, branch-introduced)

---

## Remaining TODO (diff reduction)

- [ ] `pub mod` → `pub(crate) mod` for event_dag submodules
- [ ] Deduplicate `DEFAULT_BUDGET` constant
- [ ] Remove `SimpleRetriever` (identical to `MockRetriever`)
- [ ] Deduplicate test helpers across inner test modules
- [ ] Remove unused `EventLayers::meet` field (`#[allow(dead_code)]`)
- [ ] Remove unused `Frontier::insert` method
- [ ] Staging `RwLock` `.unwrap()` → `.unwrap_or_else(|e| e.into_inner())`

---

## Documentation

Internals documentation in `docs/internals/` (mdbook-structured with cross-links):

| File | Content |
|------|---------|
| `event-dag.md` | BFS algorithm, staging pattern, invariants, design decisions |
| `retrieval.md` | GetEvents/GetState/SuspenseEvents traits, concrete types, crash safety |
| `entity-lifecycle.md` | Creation, apply_event, apply_state, persistence |
| `node-architecture.md` | Durable/ephemeral, replication protocol, subscriptions |
| `property-backends.md` | LWW/Yrs backends, EventLayer helpers, StateBuffers |
| `lww-merge.md` | Layer computation, resolution rules, determinism |
| `testing.md` | Test strategy, infrastructure, coverage gaps |

---

## Key Commits

| Commit | Content |
|--------|---------|
| `d9669f9c` | Phase 1-2: EventAccumulator types + comparison rewrite |
| `5cbc255a` | Phase 3: caller updates, correctness guards |
| `87447a9b` | Phase 4: cleanup, remove old types, add tests |
| `07e0b146` | Phase 5: Retrieve trait split, staging pattern, idempotency fix |
| `b9220663`..`15da302f` | Phase 6: BFS fixes for ephemeral nodes |
| `93b0c68a`..`1021fa70` | Phase 7: DuplicateCreation removal, trace cleanup |
| `34b8894a` | Phase 8: dead code removal, tests, documentation |
| `455f29a1` | Diff golf + test annotation fix |
| `7eb7af88` | cargo fmt |
| `9d383f02` | Internals documentation |
