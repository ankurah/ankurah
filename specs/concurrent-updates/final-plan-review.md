# Final Plan Review: event-accumulator-refactor-plan.md

**Reviewer:** Claude Opus 4.6 (final gate review)
**Date:** 2026-02-05
**Branch:** `concurrent-updates-event-dag`
**Input:** Plan + metareview + spec-additions + plan-review + codex-followup + codex-review-response + 4 matrix reviews (A1, A4, B1, B4) + 6 source files

---

## 1. Metareview Coverage Matrix

| Finding | Severity | How Plan Addresses It | Verdict |
|---------|----------|----------------------|---------|
| **P0-1: InsufficientCausalInfo crash** | Critical | Fully addressed. Plan introduces `dag_contains()` on `EventLayer` and the `older_than_meet` flag in `Candidate` struct within LWW `apply_layer`. Behavioral rule clearly stated in "LWW Resolution When Stored event_id Is Older Than Meet" section. Implementation code sketch in section 6 demonstrates the asymmetric treatment: stored values absent from DAG lose; candidates absent from DAG produce an error. `EventLayer.compare()` precondition documented ("callers must only call `compare()` when both ids are in the DAG"). | COVERED |
| **P0-2: Idempotency / spurious multi-head** | Critical | Fully addressed. Plan adds `event_exists(&EventId)` to `Retrieve` trait (Phase 1). Idempotency guard in `apply_event` rejects already-stored events before comparison (Phase 3, section 4 code sketch). Behavioral rule "Idempotency Contract" documented. Test case in Phase 4: "re-delivery of historical event is rejected." | COVERED |
| **P1-3: O(n^2) children_of** | High | Fully addressed. Plan replaces `children_of` with a pre-built `children_index: BTreeMap<EventId, Vec<EventId>>` constructed in `EventLayers::new()` in O(N). The `next()` method uses this index for O(1) child lookups per parent. Code sketch is correct -- iterates `accumulator.dag` entries and inverts parent pointers into child lists. | COVERED |
| **P1-4: Budget too small / no escalation** | High | Fully addressed. Plan increases default budget from 100 to 1000 (`DEFAULT_BUDGET: usize = 1000` in section 4 code sketch). Budget escalation added: on `BudgetExceeded`, retry with `DEFAULT_BUDGET * 4` up to one escalation. `R: Clone` requirement enables fresh retriever for retry. BudgetExceeded semantics clarified as "fresh comparison, NOT resumption" in Resolved Design Questions #11. | COVERED |
| **P1-5: Entity creation race** | Medium | Fully addressed. Plan adds creation uniqueness guard: `if event.is_entity_create() && !self.head().is_empty() { return Err(MutationError::DuplicateCreation) }` in section 4 code sketch. `DuplicateCreation` variant added to `MutationError` (Phase 1). Behavioral rule "Entity Creation Uniqueness" documented. Test case in Phase 4: "second creation event for same entity is rejected." | COVERED |
| **P2-7: build_forward_chain bug** | Medium | Indirectly mooted. Plan eliminates `compute_layers` (standalone) and `build_forward_chain` -- callers switch to `ComparisonResult.into_layers()`. The `subject_chain` and `other_chain` fields in `DivergedSince` are still produced by `Comparison` but are informational only; the actual layer computation uses `EventAccumulator`'s DAG, not the chains. Phase 4 removes standalone `compute_layers`. | MOOTED |
| **P2-8: Backend misses earlier layers** | Medium-High | Fully addressed. Plan includes backend-replay logic in section 4 code sketch: when a backend is first seen at Layer N, earlier layers are replayed for it (`for earlier in &applied_layers { backend.apply_layer(earlier)? }`). The `applied_layers: Vec<EventLayer>` is maintained during iteration. Phase 3 checklist item: "Implement backend-replay for late-created backends across layer boundaries." Test case in Phase 4: "backend first seen at Layer N gets Layers 1..N-1 replayed." | COVERED |

**Summary:** All 7 metareview findings are addressed by the plan. None are missing.

---

## 2. Code Compatibility Issues

### 2.1 EventAccumulator.seed() -- `event.parent().members().to_vec()`

The plan's `seed()` method calls `event.parent().members().to_vec()`. In the actual codebase, `Event::parent()` returns `&Clock` (from `TEvent` impl at `traits.rs:51`), and `Clock::members()` returns `&[EventId]` (from `TClock` impl at `traits.rs:42` via `as_slice()`). Calling `.to_vec()` on `&[EventId]` is valid since `EventId` implements `Clone`. **Compatible.**

### 2.2 EventAccumulator.accumulate() -- `event.id()`

The plan calls `event.id()` which returns `EventId` (a `[u8; 32]` wrapper). `EventId` derives `Clone`, `Copy`, `Eq`, `Ord`. Using it as `BTreeMap` key is valid. However, `Event::id()` is a computed property (SHA-256 hash of `entity_id + operations + parent`) -- it is not stored on the struct. Calling `id()` multiple times recomputes the hash. The plan calls `event.id()` twice in `accumulate()` (line 155 and 158). **Minor inefficiency but functionally correct.** Consider caching the id in a local variable.

### 2.3 `Retrieve` trait expansion

The plan adds `store_event(&Event)` and `event_exists(&EventId)` to `Retrieve`. Currently `Retrieve` is a minimal trait with only `get_state()`. The plan correctly notes this in Phase 1. The existing `Retrieve` implementors (`LocalRetriever`, `EphemeralNodeRetriever`) would need these methods added. `LocalRetriever` already has `collection.add_event()` internally, so `store_event` can delegate to it. `event_exists` would need a new storage method. **Feasible but requires storage layer changes not detailed in the plan.**

### 2.4 `R: Clone` requirement

The plan requires `R: Clone` for retrievers. `LocalRetriever` already derives `Clone` (it wraps `Arc<LocalRetrieverInner>`). `EphemeralNodeRetriever` does NOT implement `Clone` -- it holds `&'a Node` and `&'a C` references plus a `Mutex<Option<HashMap<...>>>`. **This is a compatibility gap.** Either `EphemeralNodeRetriever` needs to be restructured to be `Clone`-able (e.g., wrap the staged events in `Arc<Mutex<...>>`), or the plan's `R: Clone` bound needs to be relaxed with a different approach for budget escalation (e.g., pass budget as a parameter and retry internally).

### 2.5 `EventLayer` type change

The plan changes `EventLayer` from holding `events: Arc<BTreeMap<Id, E>>` (full events) to `dag: Arc<BTreeMap<Id, Vec<Id>>>` (parent pointers only). This is a breaking change to `EventLayer`'s internals but preserves the public API (`already_applied`, `to_apply`, `compare()`, `dag_contains()`). The `compare()` method changes from using `is_descendant` on full events to `is_descendant_dag` on parent pointers. **Compatible and architecturally cleaner.** The `is_descendant_dag` function in the plan correctly walks parent pointers without needing full event data.

### 2.6 `apply_event` signature change

Current: `pub async fn apply_event<G>(&self, getter: &G, event: &Event) -> Result<bool, MutationError> where G: CausalNavigator<EID = EventId, Event = Event> + Send + Sync`

Plan: Takes `retriever: R` where `R: Retrieve + Clone` instead of `CausalNavigator`. This is a breaking API change. All callers of `apply_event` (in `entity.rs`, `node_applier.rs`, and test code) must be updated. The plan lists `node_applier.rs` in Phase 3 but does not enumerate all call sites. **The plan's Files to Modify section should include all `apply_event` callers.**

### 2.7 `apply_state` signature change

Current: returns `Result<bool, MutationError>`. Plan: returns `Result<StateApplyResult, MutationError>`. The `WeakEntitySet::with_state()` method at `entity.rs:690` uses `apply_state`'s boolean return: `let changed = entity.apply_state(retriever, &state).await?;` and returns `(Some(changed), entity)`. This caller needs updating for the enum return type. **Not mentioned in the plan's Files to Modify.** The `with_state` method and its callers need to handle `StateApplyResult` variants.

### 2.8 `LruCache` dependency

The plan uses `LruCache` from the `lru` crate. This is not currently a dependency. **Needs to be added to `Cargo.toml`.** Not mentioned in the plan.

---

## 3. Missing Steps or Gaps

### 3.1 Storage layer method for `event_exists`

The plan adds `event_exists(&EventId)` to `Retrieve` but does not describe the storage layer implementation. The current `StorageCollectionWrapper` has `get_events()` and `add_event()` but no existence check. A new storage method (or a `get_events` call that checks for a single event) is needed. **Phase 1 should include a storage layer task.**

### 3.2 `EphemeralNodeRetriever` Clone compatibility

As noted in 2.4, `EphemeralNodeRetriever` cannot be cloned due to lifetime references. The plan does not address this. **Either the retriever needs restructuring or the budget escalation approach needs adaptation for non-Clone retrievers.** One option: make budget escalation an internal loop within `Comparison` rather than requiring the caller to clone and retry.

### 3.3 `get_event` method on Retrieve

The plan's `EventAccumulator` calls `self.retriever.get_event(id)` and `self.retriever.store_event(&event)`. The current `Retrieve` trait has neither of these methods -- it only has `get_state()`. The plan mentions adding `store_event` and `event_exists` in Phase 1, but `get_event` is also needed and is not explicitly listed. Currently, event retrieval is handled by `CausalNavigator::expand_frontier()`. The plan needs to add `get_event(&EventId) -> Result<Event, RetrievalError>` to the `Retrieve` trait. **This is implied but not explicitly stated in the migration checklist.**

### 3.4 `compare()` function now returns `ComparisonResult<R>` -- callers of `apply_state`

The plan's `apply_state` calls `compare(retriever, &state.head, &self.head(), DEFAULT_BUDGET)` which returns `ComparisonResult<R>`. But `apply_state` only needs the relation, not the accumulator (since state snapshots cannot be merged via layers). The plan's `apply_state` code sketch accesses `result.relation` directly, which is correct. However, the `compare()` function now creates and populates an `EventAccumulator` even for `apply_state` calls where it will never be used. **This is acceptable (the accumulator is lightweight) but could be optimized by having a lighter-weight comparison mode for state-only comparisons.**

### 3.5 Test for TOCTOU retry exhaustion

The plan lists this test in Phase 4 but gives no guidance on how to test it. TOCTOU testing requires simulating concurrent head mutations during comparison, which is inherently racy and hard to test deterministically. **The plan should note that this test requires either a testing hook (e.g., injected delay between comparison and mutation) or a stress test approach.**

### 3.6 Migration of existing `apply_event` callers from `CausalNavigator` to `Retrieve`

The plan's Phase 3 says "Update any other `compare()` callers" but does not enumerate them. Searching the codebase, the callers include:
- `entity.rs::apply_event` (uses `AccumulatingNavigator<&G>`)
- `entity.rs::apply_state` (uses `G` directly)
- `WeakEntitySet::get_or_retrieve` (constraint: `R: Retrieve + CausalNavigator`)
- `WeakEntitySet::with_state` (calls `apply_state`)
- `node_applier.rs` (multiple call sites)

**The plan should enumerate all sites that currently use `CausalNavigator` and specify how each transitions to `Retrieve`.**

---

## 4. Contradictions

### 4.1 `is_descendant_dag` error on missing ID vs. plan's invariant

The plan states (EventLayers precondition): "accumulator.dag contains all events on paths from both heads to the meet." It also states that `is_descendant_dag` returns `Err` on missing IDs and that this is "a caller bug, not an expected path."

However, the plan's `EventLayers::next()` frontier expansion uses `!self.accumulator.dag.contains_key(p)` to treat out-of-DAG parents as implicitly processed. This means the DAG may contain events whose parents are NOT in the DAG (they are below the meet). If `is_descendant_dag` is called on two events within the DAG, the traversal may reach a parent that IS in the DAG but whose own parents are NOT in the DAG. At that point, `dag.get(&id)` returns the event's parents, but those parents are not in the DAG, causing an `Err`.

**Wait -- the plan's `is_descendant_dag` operates on `dag: &BTreeMap<Id, Vec<Id>>` (parent-pointer map), not on full events.** The `dag` map contains all accumulated event IDs as keys, with their parent lists as values. When `is_descendant_dag` traverses from `descendant` upward, it calls `dag.get(&id)`. If `id` is in the DAG (key exists), it gets the parent list and continues. If a parent is NOT in the DAG (key does not exist), `dag.get(&id)` returns `None`, triggering the `ok_or_else` error.

This means: if event X is in the DAG and X's parent P is below the meet (not in the DAG), then `is_descendant_dag(dag, X, some_ancestor_of_X)` would error when it reaches X and tries to look up X's parents. Wait, no -- X is in the DAG, so `dag.get(&X)` returns `Some(parents)`. The parents include P. When the frontier pushes P and then tries `dag.get(&P)`, P is NOT in the DAG, so it returns `Err`.

**This IS a potential issue.** The `is_descendant_dag` function will error when the backward traversal reaches events whose parents are outside the DAG (below the meet). However, the LWW `apply_layer` only calls `layer.compare()` when BOTH IDs are confirmed to be in the DAG (via `dag_contains()`). For two events that are both above the meet, the `is_descendant_dag` traversal may still reach below-meet parents when walking backward.

**Example:** Events A (genesis), B (parent=A), C (parent=A), meet=A. DAG contains {A, B, C} with A's parents=[]. `is_descendant_dag(dag, B, C)`: frontier=[B], process B, parents=[A], push A. Process A, parents=[], no more frontier. B does not descend from C. Return false. `is_descendant_dag(dag, C, B)`: same pattern. Return false. Result: Concurrent. **This works because A (genesis) has empty parents and IS in the DAG.**

But consider: meet={M}, M's parents=[P1, P2] where P1 and P2 are below the meet and NOT in the DAG. Events X (parent=[M]) and Y (parent=[M]) are both in the DAG. `is_descendant_dag(dag, X, Y)`: frontier=[X], process X, parents=[M], push M. Process M, parents=[P1, P2]. `dag.get(&P1)` -> None -> **Err!**

**This is a real contradiction.** The plan's `is_descendant_dag` will error when traversing through the meet point to its below-meet parents. The fix is either:
(a) Stop traversal when a parent is not in the DAG (treat it as a dead end, not an error), or
(b) Ensure the meet point's parents are never traversed (but `is_descendant_dag` has no knowledge of what the meet is).

**Option (a) is the correct fix.** The traversal should treat missing DAG entries as "end of known DAG" rather than an error. The plan's `compute_ancestry_from_dag` function already does this correctly (uses `if let Some(parents) = dag.get(&id)` instead of `.ok_or_else`). The `is_descendant_dag` function should use the same pattern.

This is a **correctness bug in the plan's code sketch** for `is_descendant_dag`. Changing the `ok_or_else` to a `if let Some(...) / else { continue }` pattern would fix it, since if the ancestor is not found by the time all reachable nodes are visited, the answer is "not a descendant."

### 4.2 StrictAscends-to-DivergedSince transformation and depth precedence

The metareview (Section 3.2) raises that the StrictAscends transformation can produce surprising results where an old-branch event wins via lexicographic tiebreak. The plan does not explicitly address this concern. The plan's LWW resolution treats all concurrent events the same way (lexicographic tiebreak), which is correct for true concurrency but may be semantically surprising for the StrictAscends case.

This is not a contradiction within the plan itself -- the plan consistently uses lexicographic tiebreak for `Concurrent` events. It IS a known semantic concern documented in the metareview as Section 3.2 (rated "spec accuracy issue, not code correctness issue"). The plan's Spec Alignment Notes section mentions updating "LWW Resolution Within Layer" but does not specifically address the depth-precedence documentation gap. **Minor gap -- the spec should clarify that depth precedence is emergent, not enforced.**

### 4.3 No contradictions found between plan sections

The plan's Behavioral Rules, code sketches, migration phases, and resolved design questions are internally consistent. The `older_than_meet` rule is correctly applied in only the LWW `apply_layer` section. The idempotency guard is correctly placed before comparison. The creation guard is correctly placed before the TOCTOU loop. The budget escalation correctly uses a fresh comparison.

---

## 5. Test Coverage Assessment

The plan's Phase 4 tests cover:

| Test | Critical Scenario | Assessment |
|------|------------------|------------|
| Stored event_id below meet loses to layer candidate | P0-1 fix validation | Covers the core bug |
| Re-delivery of historical event is rejected | P0-2 fix validation | Covers idempotency |
| Second creation event for same entity is rejected | P1-5 fix validation | Covers creation race |
| Backend first seen at Layer N gets Layers 1..N-1 replayed | P2-8 fix validation | Covers backend replay |
| Budget escalation succeeds where initial budget fails | P1-4 fix validation | Covers budget escalation |
| TOCTOU retry exhaustion produces clean error | Existing concern | Hard to test deterministically |
| Merge event with parent from non-meet branch is correctly layered | Mixed-parent boundary fix | Covers the codex-review-response fix |

**Missing test cases:**

1. **StrictAscends transformation with deep history:** An event arriving from an old branch point (parent deep in history) should be correctly processed. This was the primary scenario for the `InsufficientCausalInfo` bug and should have a dedicated test beyond the "stored event_id below meet" test.

2. **Multi-head entity extending one tip:** Entity head=[B,C], event D parent=[C]. This triggers StrictAscends->DivergedSince. Should verify correct head update to [B,D] and correct LWW resolution.

3. **Seeded event visibility during comparison:** The plan lists this in the plan-review-and-recommendations (section 2.5) but does NOT include it in the Phase 4 test checklist. The spec-additions document recommends it. **Should be added.**

4. **Empty-meet DivergedSince rejection (creation guard secondary path):** Two creation events where the first already set the head, and the second arrives producing DivergedSince with empty meet. Verify the creation guard catches this.

5. **`is_descendant_dag` behavior when traversal reaches below-meet parents:** Per contradiction 4.1, verify that the traversal does not error when it encounters DAG boundaries.

---

## 6. Additional Findings from Matrix Reviews Not in Metareview

### 6.1 A4 Finding #4: Events with multi-parent clocks spanning the meet boundary silently orphaned

This was independently identified by A4 (rated High/Medium) and is now addressed by the codex-review-response's generalized frontier initialization and the plan's "Mixed-Parent Events Spanning the Meet Boundary" behavioral rule. **Covered.**

### 6.2 A4 Finding #8: Pre-migration LWW properties without event_id block all backends

Properties from before per-property event tracking was introduced lack `event_id`. The LWW `apply_layer` at line 183-186 returns a hard error for these. The plan's LWW code sketch preserves this behavior (the `entry.event_id()` unwrap/error). **This is not addressed by the plan.** If pre-migration data exists, the plan's refactored `apply_layer` will still fail. This is a pre-existing issue but worth noting.

### 6.3 A4 Finding #5/B4: Cyclic event graph causes issues

The plan does not address cycle detection. Cycles in the DAG are prevented by construction (EventId is a hash of content+parents), but malicious input could theoretically create them. The plan's `is_descendant_dag` has a `visited` set for cycle protection in traversal, which is sufficient. **Acceptable -- not a plan gap.**

### 6.4 B1 Finding: Origin propagation correctness

B1 analyzed origin propagation in depth and confirmed it is correct. The plan does not change the core comparison algorithm (only wraps it with `EventAccumulator`), so this remains sound. **No issue.**

---

## 7. Overall Assessment

### Verdict: IMPLEMENTABLE with two required fixes

The plan is comprehensive, well-structured, and addresses all identified critical and high-severity issues from the review process. The architecture is clean, the migration phases are logical, and the behavioral rules are clearly stated.

**Two issues must be fixed before implementation begins:**

1. **`is_descendant_dag` must handle missing DAG entries gracefully** (Contradiction 4.1). The function should return `Ok(false)` when it encounters a parent not in the DAG, not `Err`. This matches the pattern already used by `compute_ancestry_from_dag` in the same plan. The fix is one line: change `dag.get(&id).ok_or_else(|| ...)` to `let Some(parents) = dag.get(&id) else { continue; }`.

2. **`EphemeralNodeRetriever` does not implement `Clone`** (Gap 3.2). The plan requires `R: Clone` for budget escalation. Either restructure `EphemeralNodeRetriever` to be cloneable (wrap internals in `Arc`) or implement budget escalation within the `Comparison` struct so callers do not need to clone the retriever.

**Three minor additions recommended:**

3. Add `get_event(&EventId) -> Result<Event, RetrievalError>` to the Retrieve trait expansion list in Phase 1 (Gap 3.3).
4. Add `lru` crate to dependencies (Gap 2.8).
5. Add the "seeded event visible in comparison" test to Phase 4 checklist (Missing test #3).

With these fixes applied, the plan is ready for implementation.
