# EventAccumulator Refactor — Implementation Resume

**Worktree:** `/Users/daniel/ak/ankurah-201` (branch: `concurrent-updates-event-dag`)
**Date:** 2026-02-10
**Status:** All code complete. All tests pass. Doc validation done. Code review matrix COMPLETE (8/8 reviewers).

---

## Current State

- `cargo check` / `cargo test` — all pass
- 176 core unit tests, 1 `#[ignore]`d (`test_toctou_retry_exhaustion`)
- All integration tests pass (concurrent_transactions, determinism, lww_resolution, edge_cases, durable_ephemeral, multi_ephemeral)
- Only flaky failure: `test_websocket_bidirectional_subscription` (pre-existing set-ordering issue, unrelated to this branch)
- Full internals documentation in `docs/internals/` (7 files, mdbook-structured, concept-focused rewrite done)
- Spec compliance: 91 compliant, 3 deviated-with-justification, 0 non-compliant

---

## What This Branch Does

Replaces the `CausalNavigator` + `compare_unstored_event` algorithm with a new
BFS-based `Comparison` state machine and `EventAccumulator`. Key changes:

1. **New `event_dag/` module** — `Comparison` state machine, `EventAccumulator`,
   `EventLayers`, `EventLayer` with DAG-aware `compare()` and `dag_contains()`.
   All types are `pub(crate)`.
2. **Staging pattern** — events are staged in-memory before `apply_event`, making
   them discoverable by BFS. Committed to storage after successful application.
3. **`Retrieve` trait split** → `GetEvents`, `GetState`, `SuspenseEvents` with
   clear separation of concerns.
4. **LWW `older_than_meet` rule** — stored values whose event ID is outside the
   accumulated DAG auto-lose to layer candidates.
5. **BFS fixes for ephemeral nodes** — quick-check for linear extension,
   both-frontiers meet detection, `EventNotFound` → error (not busyloop).
6. **`DuplicateCreation` guard refined** — `compare()` handles both re-delivery
   (`StrictAscends`) and disjoint genesis (`Disjoint`). Added
   `storage_is_definitive()` for durable-node fast path. Guard still exists
   in `entity.rs:252-260` but is conditional (durable vs ephemeral).

---

## Critical Invariants

1. **`stage_event` before head update (in memory); `commit_event` before `set_state` (to disk).**
2. **`get_event` = staging + storage (union view). `event_stored` = permanent storage only.**
3. **Both-frontiers = common ancestor.** Process with empty parents, no fetch needed.
4. **Unfetchable single-frontier events are errors.** Must return `Err`, not `continue`.
5. **Meet filter correctness.** `common_child_count == 0` ensures head tips always appear in meet.
6. **Disjoint genesis detected by BFS, not early guards.** `event_stored()` is unreliable on ephemeral nodes.
7. **No `E: Clone`.** Budget escalation (single 1x→4x jump) is internal to `Comparison`. Only LRU cache survives retry; BFS restarts from original clocks.
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

## Completed Cleanup (this session)

All 7 diff-reduction TODO items from the previous session are done:

- [x] `pub mod` → `pub(crate) mod` for all event_dag submodules and types (including `lib.rs`)
- [x] `DEFAULT_BUDGET` consolidated into `event_dag::DEFAULT_BUDGET`
- [x] `SimpleRetriever` / `StagingOnlyRetriever` removed (use `MockRetriever`)
- [x] Test helpers deduplicated (`make_lww_event`, `layer_from_refs`, `layer_from_refs_with_context`) — ~126 lines saved
- [x] Unused `EventLayers::meet` field removed (was `#[allow(dead_code)]`)
- [x] Unused `Frontier::insert` method removed
- [x] Staging `RwLock` `.unwrap()` → `.unwrap_or_else(|e| e.into_inner())` (6 sites in `retrieval.rs`)

---

## Documentation Validation Results

7 docs in `docs/internals/` were rewritten for conceptual clarity (code-translation → concept-focused),
then validated against source code by 7 independent agents. Summary:

| Document | Verdict | Key Issues |
|----------|---------|------------|
| `testing.md` | **Clean** | No material inaccuracies |
| `lww-merge.md` | **Clean** | 2 minor imprecisions (provenance vs value-equality; head growth mechanism) |
| `retrieval.md` | **2 fixes needed** | Pseudocode omits conditional commit/save; "used by durable nodes" misleading for LocalEventGetter |
| `property-backends.md` | **1 fix needed** | "batch of events in causal order" → events within a layer are concurrent, layers are in topological order |
| `entity-lifecycle.md` | **3 minor fixes** | Guard 1 omits event_stored no-op path; head update mechanism simplified; StateAndEvent fallback includes Older |
| `node-architecture.md` | **1 medium + 4 low** | Omits EventOnly variant; "LWW merge resolution" imprecise; omits StateAndRelation; traversal budget nuance; subscription re-establishment timing |
| `event-dag.md` | **3 medium + 5 low** | Budget "warm start" misleading (only cache survives); staging diagram doesn't match context.rs; "guard removed" should be "guard refined"; outcome count (5 vs 6); empty clock handling |

### Doc Fixes Still TODO

- [ ] `retrieval.md`: Add conditional logic to pseudocode (commit only when apply_event returns true; save_state only when events applied)
- [ ] `retrieval.md`: Change "Used by durable nodes" to "Used for local commits on all node types"
- [ ] `property-backends.md`: Fix "batch of events in causal order" → "sequence of layers in topological order; each layer contains concurrent events"
- [ ] `entity-lifecycle.md`: Add event_stored() == true no-op path to Guard 1 description
- [ ] `entity-lifecycle.md`: Clarify head update is "remove meet ancestors + insert event ID"
- [ ] `entity-lifecycle.md`: Note StateAndEvent fallback triggers on Older too, not only divergence
- [ ] `node-architecture.md`: Mention EventOnly variant (valid wire format, handled by receiver)
- [ ] `node-architecture.md`: Fix "LWW merge resolution" → "event-by-event apply_event with BFS comparison"
- [ ] `event-dag.md`: Fix "warm start" → only LRU cache survives, BFS restarts from original clocks
- [ ] `event-dag.md`: Fix "five possible outcomes" → six (including BudgetExceeded)
- [ ] `event-dag.md`: Change "DuplicateCreation guard removed" → "guard refined/replaced with conditional logic"

---

## Code Review Matrix (COMPLETE)

8 code reviewers dispatched (4 dimensions × 2 tracks: informed vs cold). All complete.

| Dimension | Informed | Cold |
|---|---|---|
| **Correctness** | 6 RESOLVED, 3 CONFIRMED, 8 NEW | 7 findings (2 P1, 3 P2, 2 P3) |
| **Security** | 15 findings (5 RESOLVED, 2 HIGH, 5 MEDIUM, 2 LOW, 1 IMPROVED) | 12 findings (1 Critical, 3 High, 6 Medium, 2 Low) |
| **Elegance** | 1 P1, 10 P2, 6 P3 | 1 P1, 5 P2, 7 P3 |
| **Spec Compliance** | 22 COMPLIANT, 11 DEVIATED (all justified), 0 NON-COMPLIANT | 19 COMPLIANT, ~8 DEVIATED (all justified), 0 NON-COMPLIANT |

---

## Resolved by This Branch (confirmed across reviewers)

1. **InsufficientCausalInfo crash** (was CRITICAL, 7/8 prior agreement) — fixed by `older_than_meet` rule + infallible `is_descendant_dag`
2. **Idempotency violation for non-head events** (was P0) — fixed by staging + BFS `StrictAscends`
3. **BudgetExceeded no resumption** (was P1) — fixed by budget escalation (1000 → 4000)
4. **O(n²) children_of** (was P1, 7/8 agreement) — fixed by pre-built `children_index`
5. **Entity creation race** (was P1) — fixed by `Disjoint` detection + creation guards
6. **Backend misses earlier layers** (was P2) — fixed by replay loop (`entity.rs:351-363`)

---

## Review Findings (consolidated from 8 reviewers)

### CRITICAL

- **EventBridge skips `validate_received_event`** — `node_applier.rs:271-288` `apply_delta_inner` for `DeltaContent::EventBridge` never calls `validate_received_event` or `validate_received_state`. Compare to `apply_update` lines 81/109 which do validate. A malicious peer can inject events bypassing all attestation checks via the Fetch/SubscribeQuery delta path. **Pre-existing (not introduced by this branch). Tracked as #244.**

### HIGH / P1

| # | Finding | Source | Confidence | Notes |
|---|---------|--------|------------|-------|
| H1 | LWW stale event_id across layers | Cold correctness F12 | Medium-High | `apply_layer` doesn't write back winners from `already_applied` (from_to_apply=false), leaving stored event_id stale for next layer's comparison. Cross-layer multi-property scenario required. |
| H2 | `Clock::from(Vec<EventId>)` doesn't sort | Cold correctness F19, Informed security F4 | Medium | `contains()` uses `binary_search` on potentially unsorted vec. Deserialization path may produce unsorted clocks. |
| H3 | Unbounded `collect_event_bridge` | Both security reviewers | High | No limit on backward walk depth. Deferred #247. |
| H4 | No size/count limits on peer messages | Both security reviewers | High | No cardinality limit on items/events. Deferred #246. |
| H5 | Unchecked bincode deser sizes | Cold security F4 | Medium | Crafted length prefix → memory exhaustion. |
| H6 | `apply_event` caller contract fragility | Informed correctness N1 | High | `apply_event` relies on caller to stage event. No type-level enforcement. |

### MEDIUM / P2

| # | Finding | Source | Notes |
|---|---------|--------|-------|
| M1 | Entity RwLock `.unwrap()` | Both security, both elegance | 4+ sites in entity.rs. Pattern exists in retrieval.rs already. |
| M2 | Partial layer application on deser error | Informed correctness N5 | Backends mutated by layers 1..N-1 if layer N fails. Head not updated. |
| M3 | Creation events bypass fork-based validation | Both security | Deferred #243. |
| M4 | TOCTOU retry no backoff | Both security | MAX_RETRIES=5, no exponential backoff. |
| M5 | CausalRelation naming confusion | Cold elegance P1, Informed elegance F8 | `CausalRelation` (3-variant) vs `AbstractCausalRelation` (6-variant). Names are backwards. |
| M6 | DivergedSince path doesn't use try_mutate | Both elegance | Manual TOCTOU reimplementation at entity.rs:340-347. |
| M7 | apply_event/apply_state duplication | Informed elegance F5 | Match arms for Equal/StrictAscends/Disjoint/BudgetExceeded are identical. |
| M8 | DAG clone in EventLayers::new | Both elegance | `accumulator.dag.clone()` at accumulator.rs:142 could be `mem::take`. |
| M9 | Self-referential parent → budget exhaustion | Cold correctness F18 | Cyclic parent causes repeated traversal until budget hits. |
| M10 | Frontier struct adds no value over BTreeSet | Both elegance | `ids` is pub(crate), methods are 1-line delegations. |

### LOW / P3

| # | Finding | Source | Notes |
|---|---------|--------|-------|
| L1 | `build_forward_chain` multi-meet bug | Confirmed (both correctness) | Dead data. Deferred #245. |
| L2 | `saturating_sub` masks cycle double-decrement | Both security | Defense-in-depth, correct. |
| L3 | Staging shared across entities in batch | Informed security F10 | By design for correctness, minor info leak risk. |
| L4 | Redundant `#[cfg(test)]` on inner test modules | Informed elegance F6 | Parent module already test-only. |
| L5 | `ComparisonResult::into_parts` only used once | Both elegance | Consider unifying with `into_layers`. |
| L6 | `layers.rs` is vestigial (8 lines, 1 enum) | Informed elegance F11 | Move `CausalRelation` into accumulator.rs. |
| L7 | `make_test_event` uses different entity_id per seed | Cold elegance F16 | Misleading but comparison ignores entity_id. |
| L8 | Quick-check doesn't decrement budget | Informed correctness N4 | Benign — quick-check is bounded by clock size. |

### Spec Compliance

Both reviewers agree: **0 non-compliant requirements**. All deviations are justified:
1. `apply_layer` takes `&EventLayer` (not separate slices) — DAG context needed
2. Idempotency via BFS `StrictAscends` instead of `event_stored()` pre-check
3. `DuplicateCreation` removed — `compare()` handles both cases
4. Budget escalation (not in spec) — strictly beneficial
5. `ValueEntry` enum vs struct — richer type model
6. `compare()` signature differences (async, budget param, generic return) — implementation necessity
7. Forward chains informational only (not consumed by replay or merge) — acceptable

Additional: spec's "conservative resolution" language (lines 147-150) is now inaccurate — `older_than_meet` rule means incoming event WINS against sub-meet stored values, not "keep current."

---

## Documentation

Internals documentation in `docs/internals/` (mdbook-structured, concept-focused):

| File | Content |
|------|---------|
| `SUMMARY.md` | mdbook chapter index |
| `event-dag.md` | BFS algorithm, staging pattern, invariants, design decisions |
| `retrieval.md` | GetEvents/GetState/SuspenseEvents traits, staging lifecycle, crash safety |
| `entity-lifecycle.md` | Creation, apply_event, apply_state, persistence |
| `node-architecture.md` | Durable/ephemeral, replication protocol, subscriptions |
| `property-backends.md` | LWW/Yrs backends, conflict resolution, StateBuffers |
| `lww-merge.md` | Layer computation, resolution rules, determinism |
| `testing.md` | Test philosophy, scenario coverage, known gaps |

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
| `9d383f02` | Internals documentation (initial) |
| `73b7ebb5` | Prune implementation resume |
| `0a97241a` | Diff reduction: deduplicate, remove dead code, tighten visibility |
| `173ef97d` | Rewrite internals docs: concept-focused |

---

## Agent Best Practices (for future sessions)

- **Review agents should write findings to files** (e.g. `specs/concurrent-updates/reviews/`) rather than returning large results inline. Inline results explode the parent context window when multiple agents complete. The parent agent can then read the files on demand.
- When dispatching multiple review agents in parallel, give each a distinct output file path.
- Keep inline agent results to a short summary (≤20 lines). Full findings go in the file.
