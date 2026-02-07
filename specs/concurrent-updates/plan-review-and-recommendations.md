# EventAccumulator Plan: Review & Recommended Changes

**Reviewer:** Claude Opus 4.6 (incorporating Codex spec-additions)
**Date:** 2026-02-05
**Input documents:**
- `event-accumulator-refactor-plan.md` (the plan under review)
- `event-accumulator-research.md` (supporting research)
- `lww-causal-register-fix.md` (LWW fix spec)
- `claude-metareview.md` (compartmentalized review findings)
- `spec-additions-codex.md` (Codex's recommended spec changes)
- Current code state (10 commits after the plan)

---

## 1. Assessment of Current Plan

The EventAccumulator plan is a well-motivated architectural refactor that eliminates three layers of indirection (`CausalNavigator`, `AccumulatingNavigator`, `staged_events`) in favor of a cleaner ownership model. It enables future streaming (#200) and simplifies the caller-side code significantly.

However, **the plan is primarily a plumbing refactor, not a bug-fix plan**. Cross-referencing against the metareview's priority-ordered findings:

| Metareview Finding | Severity | Plan addresses? |
|---|---|---|
| P0-1: InsufficientCausalInfo crash | Critical | Architecture enables fix, but mechanism unspecified |
| P0-2: Idempotency / spurious multi-head | Critical | Not addressed |
| P1-3: O(n²) `children_of` | High | Not addressed (sketch has directionality bug) |
| P1-4: Budget too small / no escalation | High | Not addressed |
| P1-5: Entity creation race | Medium | Not addressed |
| P2-7: `build_forward_chain` bug | Medium | Indirectly mooted |
| P2-8: Backend misses earlier layers | Medium-High | Not addressed |

The plan's value is real — cleaner architecture, streaming enablement, ownership simplification — but it needs explicit additions to close the correctness gaps that the review identified.

---

## 2. Recommended Changes to the Plan

### 2.1 CRITICAL: Resolve InsufficientCausalInfo via semantic rule

**Problem:** `is_descendant()` in `layers.rs:221` operates on the static `Arc<BTreeMap>` of events accumulated during BFS. When a stored property's `event_id` predates the meet, it isn't in the map, and `compare()` returns `Err(RetrievalError)` — which becomes `MutationError::InsufficientCausalInfo`. The event is rejected. This is the P0-1 bug (7/8 reviewer agreement).

**Why it's provably safe to resolve without fetching:** The BFS walks backward from both heads to the meet. Every event *above* the meet is traversed and accumulated. If a stored `event_id` is absent from the accumulated set, it is provably at or below the meet — meaning every candidate in the current layer (which is above the meet) causally descends from it. The stored value must lose.

**Recommended behavioral rule (add to spec and implement in `apply_layer`):**

> If a stored value's `event_id` is **not present** in the accumulated event set for a layer, it must be treated as **strictly older than the meet** and **must lose** to any candidate in the layer for the same property.
>
> If a *candidate's* `event_id` is missing from the accumulated event set, this remains an error (`InsufficientCausalInfo`) because the system cannot validate the candidate's lineage.

This is simpler and more correct than a retriever-fallback approach — it avoids making `compare()` async, avoids additional storage fetches, and leverages a structural invariant that is already guaranteed by the BFS. The asymmetry between stored values and candidates is key: stored values below the meet are *known* to be older; candidates outside the map are *unknown* and must be rejected.

**Implementation in `apply_layer`:**
```rust
// When seeding winners from stored values:
for (prop, entry) in values.iter() {
    let event_id = entry.event_id()?;
    if !layer.events_contains(&event_id) {
        // Stored event predates the meet — any layer candidate wins.
        // Do NOT insert into winners; first layer candidate takes it.
        continue;
    }
    winners.insert(prop.clone(), Candidate { value: entry.value(), event_id, from_to_apply: false });
}
```

**Add to plan Phase 2** (alongside EventAccumulator changes): update `EventLayer` and `LWWBackend::apply_layer` to implement this rule. Add `events_contains(&EventId) -> bool` to `EventLayer`.

---

### 2.2 CRITICAL: Add idempotency pre-check to `apply_event`

**Problem:** Redundant delivery of a historical event creates a spurious multi-head where an ancestor and descendant coexist as tips. `compare_unstored_event` only catches redundancy when the event is currently in the head, not when it's in history.

**Two options (implementation guarantee preferred):**
- **Caller contract:** Before calling `apply_event`, the caller must check if the event is already stored.
- **Implementation guarantee (preferred):** `apply_event` consults storage to reject events already present in history.

**Recommended addition to plan:**

Phase 1: Add `event_exists(&EventId) -> Result<bool>` to the `Retrieve` trait alongside `store_event`.

Phase 3: In `apply_event`, before comparison:
```rust
if retriever.event_exists(&event.id()).await? {
    return Ok(false); // Already stored, no-op
}
```

**Spec addition:** The spec currently claims idempotent application; this is only true if the history check is enforced.

---

### 2.3 HIGH: Pre-build children index in `EventLayers`

**Problem:** `children_of()` does an O(N) linear scan for every parent lookup, giving O(N²) total cost. 7/8 review agents flagged this; A4 identified it as a DoS vector via crafted "comb" DAG topologies.

**Also:** The plan's `EventLayers::next()` code sketch adds `event.parent().members()` to `next_frontier`, which walks *backward* (ancestors). Layer computation must walk *forward* (children). This is a bug in the plan sketch.

**Recommended addition to plan:**

In `EventLayers::new()`, pre-build a parent→children index from the accumulated DAG in O(N):

```rust
pub struct EventLayers<R: Retrieve> {
    accumulator: EventAccumulator<R>,
    children_index: BTreeMap<EventId, Vec<EventId>>,
    // ...
}

impl<R: Retrieve> EventLayers<R> {
    fn new(accumulator: EventAccumulator<R>, meet: Vec<EventId>, current_head: Vec<EventId>) -> Self {
        let mut children_index: BTreeMap<EventId, Vec<EventId>> = BTreeMap::new();
        for (id, parents) in &accumulator.dag {
            for parent in parents {
                children_index.entry(parent.clone()).or_default().push(id.clone());
            }
        }

        // Initialize frontier with children of meet (forward direction)
        let frontier: BTreeSet<EventId> = meet.iter()
            .flat_map(|m| children_index.get(m).cloned().unwrap_or_default())
            .collect();

        Self { accumulator, children_index, meet, frontier, /* ... */ }
    }
}
```

Fix the `next()` sketch to advance forward using the children index.

**Spec addition (performance note):** Implementations should build a parent→children index to avoid O(n²) scans when computing layers.

---

### 2.4 HIGH: Budget — increase default, clarify semantics, add escalation

**Problem:** `COMPARISON_BUDGET = 100` is insufficient for real-world DAGs. 6/8 review agents flagged this. The plan says "budget tracking stays in Comparison" but doesn't change the value or add recovery. The spec implies resumption from frontiers, but frontiers alone are not sufficient (they lack visited sets, meet candidates, counters).

**Recommended additions to plan:**

Phase 2.5 (new phase between Comparison internals and caller updates):
- [ ] Increase default budget from 100 to 1000
- [ ] Add budget escalation in callers: on `BudgetExceeded`, retry with 2× budget up to a configurable maximum
- [ ] Make budget a parameter with sensible default, not a const

**Spec clarification:** `BudgetExceeded` means comparison terminated early. The returned frontiers are **not sufficient** for resumption without additional internal state. Until resumption is implemented, callers must treat this as a hard error or retry with a larger budget. If resumption is intended in the future, define a resumable comparison token that captures all required state.

---

### 2.5 HIGH: Seeded/unstored events must be first-class during comparison

**Problem:** When applying a batch of events, comparisons must consider in-flight events to avoid missing parents. If a required parent is present only in the seed set (not yet stored), comparison must still succeed and include that event in the accumulated set.

The plan's `EventAccumulator.seed()` method handles the implementation, but the *spec* doesn't describe this requirement. Without it, batch application can produce spurious `InsufficientCausalInfo` or incorrect layer computation.

**Spec addition:**
> Comparison must be able to access **seeded/unstored** events supplied by the caller (e.g., an event batch) and treat them as first-class in traversal. If a required parent is present only in the seed set, comparison must still succeed and include that event in the accumulated set.

No plan change needed — the plan already handles this. But the spec must document the requirement.

---

### 2.6 MEDIUM: Handle newly-created backends across layer boundaries

**Problem:** In `entity.rs:308-317`, if a backend type is first encountered at Layer N, it's created empty and only receives Layer N's events. Operations from Layers 1..N-1 are lost. (B2 unique finding from metareview.)

**Recommended addition to plan, Phase 3:**

When iterating layers, track which backends have been created. After creating a new backend at Layer N, replay Layers 1..N-1 for that backend:

```rust
let mut backend_first_seen: BTreeMap<String, usize> = BTreeMap::new();
let mut applied_layers: Vec<EventLayer> = Vec::new();

while let Some(layer) = layers.next().await? {
    let layer_idx = applied_layers.len();

    for evt in &layer.to_apply {
        for (backend_name, _) in evt.operations.iter() {
            if !state.backends.contains_key(backend_name) {
                let backend = backend_from_string(backend_name, None)?;
                // Replay earlier layers for this backend
                for earlier in &applied_layers {
                    backend.apply_layer(earlier)?;
                }
                backend.apply_layer(&layer)?;
                state.backends.insert(backend_name.clone(), backend);
                backend_first_seen.insert(backend_name.clone(), layer_idx);
            }
        }
    }

    for (name, backend) in state.backends.iter() {
        if backend_first_seen.get(name) != Some(&layer_idx) {
            backend.apply_layer(&layer)?;
        }
    }

    applied_layers.push(layer);
}
```

Alternatively, pre-scan all layers for backend names before iteration starts and create all needed backends upfront.

---

### 2.7 MEDIUM: Guard against entity creation race

**Problem:** Two concurrent creation events (empty parent clock) for the same entity_id can leave the entity with two genesis events — permanently corrupted. (B4 unique finding from metareview.)

**Recommended addition to plan, Phase 3 (apply_event changes):**

After the creation fast-path at `entity.rs:224-238`, add validation:

```rust
if event.is_entity_create() && !state.head.is_empty() {
    return Err(MutationError::DuplicateCreation);
}
```

Or: detect the empty-meet `DivergedSince` that results from two creation events and reject it.

---

### 2.8 MEDIUM: Clarify StrictDescends application semantics

**Problem:** The spec implies replay of the forward chain for `StrictDescends`, but the code only applies the incoming event (assumes causal delivery). This mismatch is misleading.

**Spec clarification:**
> `StrictDescends` handling assumes **causal delivery** — all parent events have already been applied. Implementations may optionally replay the forward chain, but are not required to if causal delivery is guaranteed by the protocol.

No plan change needed, but the spec should be explicit.

---

### 2.9 LOW-MEDIUM: Resolve `apply_state` return type

**Problem:** `apply_state` returns `Ok(false)` for multiple semantically distinct cases (already applied, older, diverged-requires-events). The plan's Open Questions §1 already raises this.

**Resolve the open question — adopt the result enum:**
```rust
pub enum StateApplyResult {
    Applied,               // StrictDescends — state applied directly
    AppliedViaLayers,      // DivergedSince — merged via layer iteration
    AlreadyApplied,        // Equal — no-op
    Older,                 // StrictAscends — incoming is older, no-op
    DivergedRequiresEvents,// DivergedSince but no events available for merge
}
```

Add to plan Phase 3 (Update callers).

---

### 2.10 LOW: Update spec API signatures

**Problem:** The spec describes older signatures for `apply_layer` and `ValueEntry`.

**Spec updates needed:**
- Update `apply_layer` signature to take `EventLayer<Id, E>` with DAG context for causal comparisons
- Update `ValueEntry` to reflect the `Uncommitted | Pending | Committed` three-state model (already implemented in commit 90634303)
- Update `compare()` signature once it returns `ComparisonResult<R>` instead of `AbstractCausalRelation`

---

## 3. Additions to Migration Path

### Phase 1 additions (new types, non-breaking)
- [ ] Add `event_exists(&EventId) -> Result<bool>` to `Retrieve` trait (for §2.2)
- [ ] Add `events_contains(&EventId) -> bool` to `EventLayer` (for §2.1)
- [ ] Pre-build children index in `EventLayers::new()` (for §2.3)

### Phase 2.5 (new phase): Budget improvements
- [ ] Increase default budget from 100 to 1000
- [ ] Add budget escalation: on `BudgetExceeded`, retry with 2× budget up to configurable maximum
- [ ] Document that `BudgetExceeded` frontiers are not resumable

### Phase 3 additions (update callers)
- [ ] Implement semantic rule in `LWWBackend::apply_layer`: stored event_id absent from events → loses to any candidate (for §2.1)
- [ ] Add `event_exists` pre-check in `apply_event` (for §2.2)
- [ ] Track backend creation during layer iteration; replay earlier layers for late-created backends (for §2.6)
- [ ] Add creation race guard in `apply_event` (for §2.7)
- [ ] Replace `apply_state` `bool` return with `StateApplyResult` enum (for §2.9)

### Phase 4 additions (cleanup + tests)
- [ ] `test_stored_event_below_meet_loses`: Stored property event_id below meet; verify any layer candidate wins without error
- [ ] `test_idempotent_redelivery`: Re-deliver historical event; verify no spurious multi-head
- [ ] `test_creation_race_rejected`: Two creation events for same entity; verify second is rejected
- [ ] `test_backend_created_mid_layers`: Backend first seen at Layer 2; verify Layer 1 operations are replayed
- [ ] `test_budget_escalation`: DAG exceeding initial budget; verify escalation succeeds
- [ ] `test_seeded_event_visible_in_comparison`: Unstored parent in seed set; verify comparison succeeds
- [ ] `test_toctou_retry_exhaustion`: Rapidly moving head; verify clean error after MAX_RETRIES

---

## 4. Items the Plan Already Handles Well

These metareview findings are addressed or mooted by the plan as-is:

- **P2-7 (build_forward_chain bug):** Callers switch to `into_layers()`, making chains irrelevant
- **P3-11 (into_events vs get_events clone):** `AccumulatingNavigator` eliminated entirely
- **P3-12 (Arc ownership transfer):** `EventAccumulator` owns events, transfers through `into_layers()`
- **CausalNavigator over-abstraction:** Eliminated per plan
- **staged_events awkwardness:** Replaced by `EventAccumulator.seeded`
- **Seeded event visibility (§2.5):** Plan handles implementation via `EventAccumulator.seed()`; spec just needs to document the requirement

---

## 5. Suggested Implementation Ordering

Given the severity of the findings, consider reordering work to deliver correctness fixes before the full refactor:

**Option A: Incremental fixes first, then refactor**
1. Implement semantic rule in `apply_layer` — stored event_id absent from events → skip (no refactor needed, ~10 lines changed in `lww.rs`)
2. Add idempotency pre-check in `apply_event`
3. Increase budget to 1000
4. Add creation race guard
5. Then proceed with full EventAccumulator refactor (Phases 1-4)

**Option B: Refactor with fixes integrated**
1. Implement EventAccumulator refactor Phases 1-2
2. Integrate all fixes in Phase 3
3. Phase 4 cleanup + expanded tests

**Recommendation: Option A.** The semantic rule fix (§2.1) is ~10 lines in `lww.rs` and resolves the Critical P0-1 bug immediately. The idempotency pre-check and creation guard are similarly small. These should not wait behind a multi-phase architectural refactor. The refactor delivers cleaner architecture but doesn't need to be on the critical path for correctness.

---

## 6. Spec Sections to Update

| Recommendation | Spec section |
|---|---|
| §2.1 Stored event_id below meet → loses | "LWW Resolution Within Layer" |
| §2.2 Idempotency contract | "Idempotency / Apply Event" |
| §2.4 BudgetExceeded semantics | "BudgetExceeded" / action table |
| §2.5 Seeded events in comparison | "Event Seeding / Staging" (new section) |
| §2.8 StrictDescends causal delivery | "StrictDescends" / action table |
| §2.9 StateApplyResult enum | "apply_state Semantics" |
| §2.10 API signatures | "API Reference" |
| §2.3 Children index | "Performance Notes" (new section) |

---

## 7. Summary

The EventAccumulator plan is architecturally sound and the right long-term direction. It needs **10 additions** to close the correctness and spec gaps identified by the compartmentalized review and Codex analysis:

**Correctness fixes (should not wait for refactor):**
1. Semantic rule: stored event_id absent from accumulated set → loses to any layer candidate
2. Idempotency pre-check before comparison
3. Entity creation race guard
4. Backend replay for late-created backends

**Plan structural changes:**
5. Pre-build children index in `EventLayers` (+ fix directionality bug in sketch)
6. Budget increase + escalation (new Phase 2.5)
7. `StateApplyResult` enum (resolve open question)

**Spec-only clarifications:**
8. Seeded events must be first-class during comparison
9. StrictDescends assumes causal delivery
10. API signature updates (apply_layer, ValueEntry, compare return type)

The first fix (semantic rule) is ~10 lines and resolves the Critical blocker. It should ship immediately regardless of the refactor timeline.
