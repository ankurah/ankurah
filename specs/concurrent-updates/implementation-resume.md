# EventAccumulator Refactor — Implementation Resume

**Worktree:** `/Users/daniel/ak/ankurah-201` (branch: `concurrent-updates-event-dag`)
**Date:** 2026-02-10
**Status:** Phases 1-7 complete. All 4 durable_ephemeral tests pass. Cleanup in progress.

---

## Current State

- `cargo check` — passes
- `cargo test` — all tests pass, 1 `#[ignore]`d (`test_toctou_retry_exhaustion`)
- Integration tests (`durable_ephemeral`) — 4 of 4 pass
- 52 event_dag unit tests pass, 1 `#[ignore]`d (`test_toctou_retry_exhaustion`)
- Debug trace logging removed (Phase 7)
- `unimplemented!()` on `StateAndRelation` converted to error return

---

## Comprehensive Review Results (2026-02-09)

13 review agents dispatched across 7 angles. Key findings below.

### P0 Head Update Bug — FALSE ALARM

The clean-room correctness review flagged `entity.rs:370-375` (DivergedSince removes `meet` IDs from head) as P0. A targeted BFS trace proved this is **correct**. The meet is filtered by `common_child_count == 0`, which guarantees that every head tip the new event descends from will always appear in the meet. Deep ancestors that aren't head tips result in harmless no-op removals.

**Proof:** Head tips have no descendants in the comparison clock → BFS never visits children of head tips → no child of a head tip can be "common" → head tips always have `common_child_count == 0` → they pass the meet filter.

Full trace: `/private/tmp/claude-501/-Users-daniel-ak/tasks/a611039.output`

### Invariant #6 — Overly Strict, Code Is Correct

The stated invariant "`commit_event` BEFORE head update" is violated in 6 of 8 call sites. However, analysis shows the **actual safety property** is weaker and correctly maintained:

> **`stage_event` before head update (in memory); `commit_event` before `set_state` (to disk).**

This is sufficient because: BFS uses `get_event` which checks staging first; `set_state` always runs after `commit_event`; a crash before `commit_event` reverts the entity to a consistent prior state.

**Action:** Update invariant #6 wording to reflect the actual property.

Full analysis: `/private/tmp/claude-501/-Users-daniel-ak/tasks/aa7f648.output`
Head mutation audit: `/private/tmp/claude-501/-Users-daniel-ak/tasks/aa834c7.output`

### Default `event_stored` — Semantic Trap (P1)

The default implementation of `GetEvents::event_stored` falls through to `get_event()`, which includes staging. This contradicts the documented "permanent storage only" semantics. All concrete impls override correctly, but any future implementor using the default gets broken creation-uniqueness semantics. `MockRetriever` and `SimpleRetriever` in tests both use the broken default.

**Action:** Either remove the default impl (force all implementors to be explicit) or change the default to return an error/unimplemented.

### Creation Event Applied Before Policy Check (P1)

In `node.rs:commit_remote_transaction`, creation events are applied to the real entity (not a fork) before the policy check. If a non-permissive policy agent rejects, the entity is left in a poisoned state (non-empty head referencing uncommitted event, blocking all future operations). With `PermissiveAgent` (current default), this can never trigger.

**Action:** Use fork/snapshot for creation events, same as the update path.

Full analysis: `/private/tmp/claude-501/-Users-daniel-ak/tasks/a99ec55.output`

### CachedEventGetter Bypasses Validation (P1, Security)

`CachedEventGetter::get_event` stores events fetched from remote peers directly to permanent storage during BFS, with zero validation (no `validate_received_event`, no attestation check, no response filtering). The staging bypass is defensible (these are historical context, not mutations), but the missing validation creates a hole in the security boundary with a real policy agent.

**Action:** Add `validate_received_event()` to the BFS fetch path; filter responses to only store events matching requested IDs.

Full analysis: `/private/tmp/claude-501/-Users-daniel-ak/tasks/a24fd38.output`

### Old Reviews Cross-Check — 83% Resolved

125 findings across 16 prior review documents: 104 resolved (83%), 10 partially resolved (8%), 8 deferred (6%), 3 unresolved (2%). All P0/Critical findings from all reviews are resolved.

Full report: `/private/tmp/claude-501/-Users-daniel-ak/tasks/a45b1e5.output`

### Dead Code and Diff Cleanup

Minimum diff review identified ~226 lines of removable diff. Golf review (scoped to diff) found additional efficiency wins.

**Dead code to remove:**
- `traits.rs` (~55 lines) — abstract traits never used
- `FrontierState`/`TaintReason` in `frontier.rs` (~22 lines) — never read
- `InsufficientCausalInfo` error variant — never constructed
- `EventLayer::has_work()`, `event_getter_mut()`, `ComparisonResult::into_layers()` — never called

**Dev artifacts to remove:**
- 7x `info!("[TRACE-AE]"...)` debug traces at info level (~25 lines)

**Unrelated cleanups (consider splitting to separate commit):**
- `property_backend_name() -> &'static str`, `&Vec<Op>` → `&[Op]`, `deltas` → `initial` rename

Full reports:
- Minimum diff: `/private/tmp/claude-501/-Users-daniel-ak/tasks/a225c5c.output`
- Golf v2: `/private/tmp/claude-501/-Users-daniel-ak/tasks/a39860a.output`

### Test Coverage Gaps

The Phase 5 staging pattern is the least-tested part:
1. No entity-level idempotency test (the exact Phase 5 bug scenario)
2. Zero unit tests for `LocalEventGetter`/`CachedEventGetter`
3. `MockRetriever` doesn't distinguish staged vs stored
4. TOCTOU retry loop completely untested (only `#[ignore]`d stub)
5. `InvalidEvent` guard (non-creation on empty head) — no test

Full report: `/private/tmp/claude-501/-Users-daniel-ak/tasks/a3076b6.output`

### Security Review

Top findings:
- `unimplemented!()` on `StateAndRelation` (`node_applier.rs:316`) — peer can crash the node
- No size/count limits on peer events — trivial DoS
- `collect_event_bridge` has no traversal limit (`node.rs:680-702`)
- `std::sync::RwLock` poisoning cascade on any panic

Full report: `/private/tmp/claude-501/-Users-daniel-ak/tasks/afdb6e2.output`

---

## Phase 5: Retrieve Trait Split + Event Staging — DONE

### Background: The Idempotency Bug

The adversarial review (post Phase 4) found a correctness gap: re-delivery of a non-head historical event corrupts the entity's head into a spurious multi-head state.

**Concrete scenario:** Chain A→B→C, head=[C]. Event B is re-delivered.
1. `compare_unstored_event(B, head=[C])` compares parent(B)=[A] vs head=[C]
2. A is an ancestor of C → `StrictAscends`
3. `compare_unstored_event` transforms this to `DivergedSince(meet=[A])` — it assumes B is new
4. DivergedSince handler: removes meet [A] from head (no-op, head is [C]), inserts B → head becomes [C, B]
5. **Corrupted:** B and C coexist as tips, but B is an ancestor of C

**Root cause:** `compare_unstored_event` compares the event's PARENT clock and infers the event's relationship via a transform. The transform assumes the event is genuinely novel. This precondition is violated by eager storage in `node_applier.rs` and by re-delivery of previously accepted events.

### What Was Implemented

**Eliminated `compare_unstored_event` entirely.** Incoming events are staged in a staging set on the event getter, then `compare` is called directly with the event's ID as the subject clock. BFS finds the event in the staging set and traverses its parents naturally — no parent-comparison-plus-transform needed.

**Split the monolithic `Retrieve` trait** into focused traits with clear semantics:

```rust
#[async_trait]
pub trait GetEvents {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError>;
    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError>;
}

#[async_trait]
pub trait GetState {
    async fn get_state(&self, entity_id: EntityId)
        -> Result<Option<Attested<EntityState>>, RetrievalError>;
}

#[async_trait]
pub trait SuspenseEvents: GetEvents {
    fn stage_event(&self, event: Event);  // interior mutability (RwLock on staging map)
    async fn commit_event(&self, attested: &Attested<Event>) -> Result<(), MutationError>;
}
```

**Design decisions made during implementation:**

- `apply_event` takes `E: GetEvents` only (not `SuspenseEvents`). The caller manages staging and committing. This avoids needing a "dry-run" wrapper for the fork in `context.rs:commit_local_trx_impl` where `commit_event` must NOT fire.
- `commit_event` takes `&Attested<Event>` (not `&EventId`), because `collection.add_event()` requires the attested version.
- `stage_event` uses interior mutability (`&self` not `&mut self`) via `Arc<RwLock<HashMap>>`.
- Blanket `&R` impls for `GetEvents` and `GetState` are needed — nearly every call site passes `&event_getter` or `&state_getter` by reference.

**Implementors:**

| Struct | Implements | Replaces | Behavior |
|--------|-----------|----------|----------|
| `LocalEventGetter` | `GetEvents + SuspenseEvents` | `LocalRetriever` (event half) | Local storage + staging map |
| `CachedEventGetter` | `GetEvents + SuspenseEvents` | `EphemeralNodeRetriever` (event half) | Remote fetch with local cache + staging map |
| `LocalStateGetter` | `GetState` | `LocalRetriever` (state half) | Local storage. Shared by durable and ephemeral paths |

**Files modified in Phase 5:**

| File | Nature of change |
|------|-----------------|
| `core/src/retrieval.rs` | Replaced file: deleted `Retrieve`/`LocalRetriever`/`EphemeralNodeRetriever`, added traits + concrete types |
| `core/src/event_dag/comparison.rs` | Removed `compare_unstored_event`, narrowed `R: Retrieve` → `E: GetEvents` |
| `core/src/event_dag/accumulator.rs` | Narrowed `R: Retrieve` → `E: GetEvents`, renamed `retriever` → `event_getter` |
| `core/src/event_dag/mod.rs` | Removed `compare_unstored_event` export |
| `core/src/entity.rs` | `apply_event`/`apply_state` take `E: GetEvents`; lifecycle methods take `(state_getter, event_getter)` |
| `core/src/node_applier.rs` | Staging pattern replaces eager `collection.add_event()`; two-arg getters |
| `core/src/node.rs` | `commit_remote_transaction` + `fetch_entities_from_local` use new types |
| `core/src/system.rs` | `create` + `load_system_catalog` use staging/new types |
| `core/src/context.rs` | `commit_local_trx_impl` uses staging; other methods use new types |
| `core/src/peer_subscription/client_relay.rs` | `remote_subscribe` uses new types |
| `core/src/event_dag/tests.rs` | `MockRetriever` implements `GetEvents`; tests use staging + `compare` |

---

## Deferred Items

- [ ] **`build_forward_chain` multi-meet bug** (comparison.rs) — P2, chains are informational only, nil impact
- [ ] **Missing `AppliedViaLayers` variant** in `StateApplyResult` — benign, no code path uses it
- [ ] **`test_sequential_text_operations`**: Keep `#[ignore]` with TODO referencing PR #236 (Yrs empty-string bug)

---

## Action Items from Review

### Must Fix (before merge)
- [ ] Remove dead code: `traits.rs`, `FrontierState`/`TaintReason`, `InsufficientCausalInfo`, unused methods
- [x] ~~Remove dev trace logging~~ — Removed (Phase 7)
- [ ] Fix default `event_stored` semantic trap
- [x] ~~Investigate durable_ephemeral test hangs~~ — Root-caused and fixed (Phase 6). 3/4 pass. Remaining failure is separate bug.
- [x] ~~Fix `test_durable_vs_ephemeral_concurrent_write`~~ — Removed `DuplicateCreation` early guard; `compare()` already returns `Disjoint` for different genesis and `StrictAscends` for re-delivery. Added `storage_is_definitive()` to `GetEvents` for cheap durable-node fast path.
- [ ] Update invariant #6 wording
- [ ] Remove unnecessary fork/snapshot for `EntityKind::Transacted` in `commit_local_trx_impl` — Investigation complete: entity is already a transaction fork, `snapshot()` creates a redundant double-fork. The transaction entity can be used directly as "after" for policy validation, with `upstream` as "before". The fork entity is discarded after commit anyway.

### Should Fix (before or shortly after merge)
- [ ] Use fork for creation events in `commit_remote_transaction` (policy check ordering)
- [ ] Add `validate_received_event` to `CachedEventGetter` BFS fetch path
- [x] ~~Convert `unimplemented!()` on `StateAndRelation` to error return~~ — Returns `InvalidUpdate` error instead of panicking
- [ ] Add entity-level idempotency test (re-deliver ancestor, verify head unchanged)
- [ ] Add unit tests for `LocalEventGetter`/`CachedEventGetter` staging lifecycle

### Nice to Have
- [ ] Deduplicate test helpers across inner test modules (~120 lines)
- [ ] Remove `SimpleRetriever` (identical to `MockRetriever`)
- [ ] Add size/count limits on peer event messages
- [ ] Add traversal limit to `collect_event_bridge`

---

## Reference: Prior Phases

`specs/concurrent-updates/event-accumulator-refactor-plan.md` — authoritative spec for Phases 1-4.

### Phase 1: Add new types (non-breaking, additive) — DONE (commit `d9669f9c`)

- `EventAccumulator`, `EventLayers`, `ComparisonResult`, `StateApplyResult`, `DuplicateCreation`
- `lru` crate dependency, unit tests for accumulator helpers

### Phase 2: Comparison Rewrite — DONE (commit `d9669f9c`)

- `Comparison` state machine replaces `CausalNavigator`
- Budget escalation with 4x retry, warm accumulator cache
- All comparison tests adapted to `MockRetriever` with concrete Event types

### Phase 3: Update Callers + Correctness Fixes — DONE (commit `5cbc255a`)

- Creation uniqueness guard (`DuplicateCreation`), `into_layers()` replaces `compute_layers()`
- LWW `older_than_meet` rule via `dag_contains()`, infallible layer `compare()`
- Backend replay for late-created backends, `apply_state` returns `StateApplyResult`

### Phase 4: Cleanup + Tests — DONE (commit `87447a9b`)

- Removed `navigator.rs`, old `EventLayer`, `compute_layers()`, `EventStaging`
- Added 7 tests (6 passing, 1 `#[ignore]`d TOCTOU stub)
- `InvalidEvent` guard for non-creation events on empty-head entities

### Phase 5: Retrieve Trait Split + Event Staging — DONE (commit `07e0b146`)

See top of document.

### Phase 6: BFS Comparison Fixes — DONE (commits `e142727c`..`15da302f`)

Three bugs found and fixed in the `compare()` BFS algorithm, triggered by ephemeral nodes committing locally.

**Root cause investigation:**

The `durable_ephemeral` integration tests were introduced on this branch (commit `53c082a6`) and passed through Phase 4. They broke at Phase 5 (`07e0b146`). Bisect confirmed: the Phase 5 trait split + staging pattern introduced a regression.

All 3 hanging tests share one trait: **the ephemeral node commits a transaction**. The passing test only reads. When an ephemeral node commits, it has the entity state (head clock) but not the historical events — those live on the durable node. Phase 5's switch from `compare_unstored_event` (parent-clock comparison, no event fetching) to `compare` (full BFS traversal, needs events) broke this because BFS tries to fetch events the ephemeral doesn't have.

**Debugging methodology:** Iterative MARK logging (per `debugging.mdc`). Narrowed from "hangs after MARK 5" → `commit_local_trx_impl` → `apply_event` → `compare()` → `step()` inner loop. Final log showed `step()` spinning at 915,000+ iterations with the same event ID on both frontiers, budget frozen at 999.

**Bug 1: Infinite busyloop on `EventNotFound`** (`comparison.rs:224`)

`step()` fetches each frontier event via `get_event`. When `EventNotFound` is returned, the code did `continue`, skipping `process_event` — the only place that removes IDs from frontiers. Unfetchable events stayed on the frontier forever.

**Fix:** `EventNotFound` → return `Err(e)` instead of `continue`. Committed as `b9220663`.

**Bug 2: No quick-check for trivial StrictDescends** (`comparison.rs`, new code)

For the common case where a new event's parents ARE the current head (linear extension), BFS is unnecessary. Added a pre-BFS check: fetch subject events, check if their parents ⊇ comparison clock. If so, return `StrictDescends` immediately without BFS.

This handles the ephemeral commit case: event B (in staging) has parent=[A] (the head). Quick check sees parent ⊇ head → StrictDescends. No need to fetch A.

**Fix:** Added quick-check block before the BFS loop. Committed as `a6d1f1b0`.

**Bug 3: Both-frontiers meet point not recognized**

After the quick check handles linear extension, concurrent commits still fail. When trx2 is committed (parent=[A], but head is now [B] from trx1), the quick check doesn't fire. BFS runs and eventually has event A on both subject and comparison frontiers. A is the meet point — BFS should recognize it as common without needing to fetch it.

**Fix:** In `step()`, before fetching, check if event is on both frontiers. If `get_event` returns `EventNotFound` AND the event is on both frontiers, call `process_event(id, &[])` — mark as common, don't traverse further. For events on only ONE frontier that are unfetchable, return error. Committed as `15da302f`.

**Design decision:** The both-frontiers check only triggers on `EventNotFound`, not unconditionally. If an event IS fetchable, we use its real parents so the BFS can continue past common ancestors to find the true minimal meet point.

**Final test results (all 3 bugs fixed):**
- `test_durable_writes_ephemeral_observes` — pass (always passed)
- `test_late_arriving_branch` — **pass** (was hanging)
- `test_ephemeral_writes_durable_receives` — **pass** (was hanging)
- `test_durable_vs_ephemeral_concurrent_write` — **FAIL** ("duplicate creation event" — separate bug, not BFS-related)
- `test_missing_event_busyloop` (new unit test) — pass (errors correctly on genuinely missing event)
- `test_both_frontiers_unfetchable_meet_point` (new unit test) — pass (common ancestor without fetch)
- All 52 event_dag unit tests — pass

**Remaining:** `test_durable_vs_ephemeral_concurrent_write` fails with "duplicate creation event for entity that already has a non-empty head". This is the `DuplicateCreation` guard in `apply_event` firing when the durable node receives the ephemeral's event via `CommitTransaction`. The creation event gets re-applied to an entity that already has it. This is a separate idempotency/relay issue, not a BFS bug.

**Key commits:**
| Commit | Content |
|--------|---------|
| `e142727c` | test: missing-event busyloop proof |
| `b9220663` | fix: EventNotFound → error in BFS |
| `a6d1f1b0` | fix: quick-check for direct parent StrictDescends |
| `15da302f` | fix: both-frontier events processed as common ancestors |

### Post-Phase-5 Cleanup — DONE (in `07e0b146`)

- Fixed 5 stale comments (references to `compare_unstored_event`, `event_exists`, `Retrieve`)
- Removed unused imports (`GetEvents`, `Event`, `EventId`, `Clock`, 6x `EventId` in test modules)
- Removed dead `add_event` standalone function from tests

### Phase 7: DuplicateCreation Fix + Cleanup — DONE (commit `93b0c68a`+)

**Root cause:** The `DuplicateCreation` early guard in `apply_event` (added Phase 3) relied on `event_stored()` to distinguish re-delivery from attack. On ephemeral nodes that receive entities via `StateSnapshot`, the creation event is never committed to event storage — only the entity state (including head clock) is persisted. When the creation event later arrives via subscription echo or `EventBridge`, `event_stored()` returns false, misclassifying a legitimate re-delivery as a disjoint genesis attack.

**Fix:** Removed the `DuplicateCreation` early guard entirely. The comparison algorithm already handles both cases:
- **Re-delivery:** BFS finds the creation event as an ancestor of the head → `StrictAscends` → no-op
- **Different genesis (attack):** BFS finds two different roots with empty parents → `Disjoint` → `LineageError::Disjoint` error

**Optimization:** Added `storage_is_definitive()` method to `GetEvents` trait (default `false`). `LocalEventGetter` accepts a `durable` flag at construction. On durable nodes, `event_stored()` is authoritative: true → cheap re-delivery no-op, false → cheap `Disjoint` rejection. On ephemeral nodes, falls through to BFS.

**Other cleanup:**
- Removed all dev trace logging (`[TRACE-AE]`, `[TRACE-AS]`, `[TRACE-SAE]`, STEP, CMP, AE, COMMIT)
- Converted `unimplemented!()` on `StateAndRelation` to `InvalidUpdate` error return
- `DuplicateCreation` variant can be removed from `MutationError` (now dead code)

---

## Critical Invariants

1. **No `E: Clone`.** Budget escalation is internal to `Comparison` — the event getter stays owned by the accumulator.
2. **BFS traversal: both-frontiers = common ancestor.** If an event appears on both subject and comparison frontiers, it's a meet point — process with empty parents, no fetch needed.
3. **Unfetchable events on a single frontier are errors.** `EventNotFound` in BFS must return `Err`, not silently `continue`. The old "dead end" behavior caused infinite busyloops.
4. **Idempotency and creation guards go before the retry loop** in `apply_event`, not inside it.
5. **Never call `into_layers()` on `BudgetExceeded`.** The DAG is incomplete.
6. **`stage_event` before head update (in memory); `commit_event` before `set_state` (to disk).** The event must be discoverable by BFS (via staging or storage) at the time the head is updated. The event must be committed to permanent storage before the entity state is persisted.
7. **`get_event` is the union view** (staging + storage). **`event_stored` is permanent-only.**
8. **Blanket `&R` impls are required.** All call sites pass getters by reference.
9. **Meet filter guarantees correctness.** The `common_child_count == 0` filter ensures that every head tip the new event descends from appears in the meet. Deep ancestors result in harmless no-op removals.
10. **Disjoint genesis is detected by `compare()`, not by early guards.** Two different creation events (both with `parent=[]`) for the same entity produce `Disjoint` from BFS. Re-delivery of the same creation event produces `StrictAscends`. The `DuplicateCreation` early guard was removed because `event_stored()` is unreliable on ephemeral nodes (StateSnapshot establishes entity state without storing individual events).
11. **`storage_is_definitive()` enables cheap disjoint rejection on durable nodes.** When true, `event_stored() == false` definitively means the event doesn't exist → `Disjoint` error without BFS. Default is `false` (safe for ephemeral nodes).

---

## Key Commits

| Commit | Content |
|--------|---------|
| `d9669f9c` | Phase 1-2: EventAccumulator types + comparison rewrite |
| `5cbc255a` | Phase 3: caller updates, correctness guards |
| `87447a9b` | Phase 4: cleanup, remove old types, add tests |
| `07e0b146` | Phase 5: Retrieve trait split, staging pattern, idempotency fix + cleanup |
| `71226a2f` | WIP: checkpoint before bisecting |
| `e142727c` | Phase 6: test proving missing-event busyloop |
| `b9220663` | Phase 6: EventNotFound → error in BFS |
| `a6d1f1b0` | Phase 6: quick-check for direct parent StrictDescends |
| `15da302f` | Phase 6: both-frontier events as common ancestors |
