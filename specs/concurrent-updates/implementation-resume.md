# EventAccumulator Refactor — Implementation Resume

**Worktree:** `/Users/daniel/ak/ankurah-201` (branch: `concurrent-updates-event-dag`)
**Date:** 2026-02-10
**Status:** Phases 1-5 complete + cleanup done. Ready for commit. Spec comparison agent running (results pending).

---

## Current State

- `cargo check` — passes (no errors, no new warnings)
- `cargo test` — all pass, 1 `#[ignore]`d (`test_toctou_retry_exhaustion`)
- `grep -r "compare_unstored_event" core/src/` — zero hits
- `grep -r "event_exists" core/src/` — zero hits
- All stale comments fixed, unused imports removed, dead code removed
- Integration tests (durable_ephemeral) are slow but passing

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
- [ ] **Spec comparison agent results** — dispatched, results pending. Check output at `/private/tmp/claude-501/-Users-daniel-ak/tasks/aacba98.output`

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

### Phase 5: Retrieve Trait Split + Event Staging — DONE (uncommitted)

See top of document.

### Post-Phase-5 Cleanup — DONE (uncommitted)

- Fixed 5 stale comments (references to `compare_unstored_event`, `event_exists`, `Retrieve`)
- Removed unused imports (`GetEvents`, `Event`, `EventId`, `Clock`, 6x `EventId` in test modules)
- Removed dead `add_event` standalone function from tests

---

## Critical Invariants

1. **No `E: Clone`.** Budget escalation is internal to `Comparison` — the event getter stays owned by the accumulator.
2. **Don't alter BFS traversal logic.** The comparison algorithm is unchanged across all phases.
3. **Out-of-DAG parents are dead ends, not errors.** Use `if let Some` / `else { continue }`.
4. **Idempotency and creation guards go before the retry loop** in `apply_event`, not inside it.
5. **Never call `into_layers()` on `BudgetExceeded`.** The DAG is incomplete.
6. **`commit_event` BEFORE head update.** The head must never reference an event not in permanent storage.
7. **`get_event` is the union view** (staging + storage). **`event_stored` is permanent-only.**
8. **Blanket `&R` impls are required.** All call sites pass getters by reference.

---

## Key Commits

| Commit | Content |
|--------|---------|
| `d9669f9c` | Phase 1-2: EventAccumulator types + comparison rewrite |
| `5cbc255a` | Phase 3: caller updates, correctness guards |
| `87447a9b` | Phase 4: cleanup, remove old types, add tests |
| (uncommitted) | Phase 5: Retrieve trait split, staging pattern, idempotency fix + cleanup |
