# Review Matrix A3: Spec-to-Code Conformance Audit

**Reviewer scope:** Line-by-line cross-reference of `specs/concurrent-updates/spec.md` against the implementation.
**Date:** 2026-02-05
**Branch:** `concurrent-updates-event-dag` (based off `main`)
**Seed context:** `specs/concurrent-updates/review-digest-pr-comments.md` (prior reviewer feedback digest)

---

## 1. AbstractCausalRelation Variant-by-Variant Audit

### 1.1 `Equal`

| Aspect | Spec | Code | Match? |
|--------|------|------|--------|
| Meaning | "Identical lattice points" | `compare` returns `Equal` when `subject.members() == comparison.members()` | YES |
| Action | "No-op" | `apply_event` returns `Ok(false)` for `Equal` | YES |
| Unstored event | Not explicitly described | `compare_unstored_event` returns `Equal` if `comparison.members().contains(&event.id())` | YES (spec omits this but the code handles it correctly) |

### 1.2 `StrictDescends { chain }`

| Aspect | Spec | Code | Match? |
|--------|------|------|--------|
| Meaning | "Subject strictly after other" | `unseen_comparison_heads == 0` triggers StrictDescends | YES |
| Chain content | "Forward chain from other head to subject head (for replay)" | Chain built from `subject_visited` reversed, excluding comparison heads | YES |
| Chain order | "Events are in causal order: oldest first, newest last" | `subject_visited.iter().rev()` produces oldest-first | YES for linear chains; see Finding #3 for complex DAGs |
| Action | "Apply forward chain" | `apply_event` IGNORES the chain; applies only the single incoming event's operations via `apply_operations_from_event` | PARTIAL -- see Finding #1 |
| Head update | Not explicitly stated | `state.head = event.id().into()` (replaces entire head with single event) | YES (correct for strictly descending single event) |

### 1.3 `StrictAscends`

| Aspect | Spec | Code | Match? |
|--------|------|------|--------|
| Meaning | "Subject strictly before other" | `unseen_subject_heads == 0` triggers StrictAscends | YES |
| Action (compare) | "No-op (incoming event is older)" | `apply_event` returns `Ok(false)` | YES |
| Unstored event transform | "Transform to DivergedSince" with `meet = parent`, `other = head tips not in parent` | Code computes exact difference of comparison minus parent | YES |
| `subject_chain` in transform | Not explicitly stated | `vec![event.id()]` (just the incoming event) | YES |
| `other_chain` in transform | "Empty other_chain triggers conservative resolution" | `vec![]` | YES |

### 1.4 `DivergedSince { meet, subject, other, subject_chain, other_chain }`

| Aspect | Spec | Code | Match? |
|--------|------|------|--------|
| Meaning | "True concurrency requiring merge" | Frontiers exhaust with common ancestors found | YES |
| Meet | "Greatest common ancestor frontier" | `meet_candidates` filtered by `common_child_count == 0` (minimal ancestors) | YES |
| Subject/Other | "Immediate children of meet toward subject/other" | `collect_immediate_children` gathers children from tracked `subject_children`/`other_children` | YES |
| Chains | "Full forward chain from meet to tip" | `build_forward_chain` reverses visited, skips past meet | YES with caveats -- see Finding #3 |
| Action | "Per-property LWW merge" | `apply_event` computes layers, calls `backend.apply_layer()` | YES |
| Head update | Not explicitly stated | Removes meet IDs from head, inserts event ID | PARTIAL -- see Finding #2 |

### 1.5 `Disjoint { gca, subject_root, other_root }`

| Aspect | Spec | Code | Match? |
|--------|------|------|--------|
| Meaning | "Different genesis events" | Subject and other roots differ | YES |
| Action | "Reject per policy" | Returns `Err(LineageError::Disjoint)` | YES |
| GCA field | "Optional non-minimal common ancestors" | Always `None` in current implementation | Matches type but no code populates it |

### 1.6 `BudgetExceeded { subject, other }`

| Aspect | Spec | Code | Match? |
|--------|------|------|--------|
| Meaning | "Traversal budget exhausted" | `remaining_budget == 0` with non-empty frontiers | YES |
| Action | "Resume with frontiers" | Returns `Err(LineageError::BudgetExceeded { ... })` with frontiers | MISMATCH -- see Finding #4 |
| Frontier contents | "Contains frontiers to resume later" | Includes `subject_frontier.ids` and `comparison_frontier.ids` | YES (data is there, but unused) |

---

## 2. Algorithm Step-by-Step Audit

### 2.1 Backward BFS Process

| Step | Spec | Code Location | Match? |
|------|------|---------------|--------|
| 1. Initialize two frontiers | "Two frontiers = {subject_head} and {other_head}" | `Comparison::new` creates `subject_frontier` and `comparison_frontier` from clock members | YES |
| 2. Expand | "Fetch events at current frontier positions" | `step()` calls `navigator.expand_frontier(&all_frontier_ids, budget)` | YES |
| 3a. Remove from frontier | "Remove from frontier, add parents" | `process_event` calls `frontier.remove(&id)` then `frontier.extend(parents)` | YES |
| 3b. Track which frontier | "Track which frontier(s) have seen it" | `NodeState.seen_from_subject` and `seen_from_comparison` | YES |
| 3c. Accumulate visited | "Accumulate in visited list for chain building" | `subject_visited.push(id)` / `other_visited.push(id)` | YES |
| 4. Detect | "Check termination conditions" | `check_result()` checks in order: tainted, StrictDescends, StrictAscends, frontiers empty | YES |
| 5. Repeat | "Until relationship determined, frontiers empty, or budget exhausted" | Main `loop` in `compare()`, `step()` returns `None` to continue | YES |

### 2.2 Termination Conditions

| Condition | Spec | Code | Match? |
|-----------|------|------|--------|
| StrictDescends | `unseen_comparison_heads == 0` | `self.unseen_comparison_heads == 0` check in `check_result` | YES |
| StrictAscends | `unseen_subject_heads == 0` | `self.unseen_subject_heads == 0` check in `check_result` | YES |
| DivergedSince (common ancestors) | "Frontiers empty + common ancestors found" | `subject_frontier.is_empty() && comparison_frontier.is_empty()` with `any_common && outstanding_heads.is_empty()` | YES |
| Disjoint | "Frontiers empty + different genesis roots" | Checked when `!any_common || !outstanding_heads.is_empty()` with different roots | YES |
| DivergedSince (no common) | "Frontiers empty + no common, can't prove disjoint" | Falls through when roots don't differ | YES |
| BudgetExceeded | "Budget exhausted" | `remaining_budget == 0` | YES |

---

## 3. Resolution Rule Audit

### 3.1 Depth Precedence (Causal Dominance)

**Spec:** "Per-property winner determined by `layer.compare(a, b)`: Descends/Ascends decides the winner"

**Code** (`/Users/daniel/ak/ankurah-201/core/src/property/backend/lww.rs:209-213`):
```rust
CausalRelation::Descends => { *current = candidate; }
CausalRelation::Ascends => {}
```

**Match:** YES. Causally later events beat causally earlier events.

### 3.2 Lexicographic Tiebreak

**Spec:** "Concurrent falls back to lexicographic EventId"

**Code** (`/Users/daniel/ak/ankurah-201/core/src/property/backend/lww.rs:214-218`):
```rust
CausalRelation::Concurrent => {
    if candidate.event_id > current.event_id { *current = candidate; }
}
```

**Match:** YES. Higher EventId wins among concurrent events.

### 3.3 Empty Chain Handling

**Spec:** "When `other_chain` is empty (StrictAscends transformation case): Conservative choice: keep current value if it has a tracked `event_id`"

**Code:** The stored last-write value is seeded into the winners map (`/Users/daniel/ak/ankurah-201/core/src/property/backend/lww.rs:180-189`). When `other_chain` is empty, there are no already_applied events in the layer (since `compute_layers` starts from the meet, and the other branch has no events to traverse). The stored value (seeded from the winners map) competes against the incoming event. Since the stored value's `event_id` exists, `layer.compare()` is called.

**Match:** PARTIAL -- the behavior depends on whether the stored event_id is present in the `events` map within the EventLayer. If it isn't (which is likely for the StrictAscends transformation case since the other branch's events weren't accumulated), the `is_descendant` call will fail with a `RetrievalError`, which gets converted to `MutationError::InsufficientCausalInfo`. This is a **bail-out**, not a conservative "current wins". See Finding #5.

### 3.4 Only Apply Winners from `to_apply`

**Spec:** "Only mutate state for winners from to_apply set"

**Code** (`/Users/daniel/ak/ankurah-201/core/src/property/backend/lww.rs:236-243`):
```rust
for (prop, candidate) in winners {
    if candidate.from_to_apply {
        values.insert(prop.clone(), ValueEntry::Committed { ... });
    }
}
```

**Match:** YES. Only `from_to_apply == true` candidates cause state mutation.

---

## 4. Undocumented Behavior (Code does what spec doesn't describe)

### 4.1 Empty Clock Early Exit

**Code** (`/Users/daniel/ak/ankurah-201/core/src/event_dag/comparison.rs:52-62`): When either clock is empty, returns `DivergedSince` with all-empty fields.

**Spec:** No mention of empty clock behavior. The comment says "Empty clocks have no common ancestors" which is reasonable but the spec's table doesn't cover this case.

### 4.2 Three-State ValueEntry Enum

**Spec** describes `ValueEntry` as a flat struct with `committed: bool`. The code uses a three-variant enum: `Uncommitted`, `Pending`, `Committed`. The `Pending` state (values collected for an event but not yet committed) is an implementation detail the spec omits entirely.

### 4.3 Backend Creation During Layer Application

**Code** (`/Users/daniel/ak/ankurah-201/core/src/entity.rs:308-317`): During DivergedSince handling, if a `to_apply` event has operations for a backend that doesn't exist yet, the code creates the backend and applies the layer to it. The spec does not describe this lazy backend creation.

### 4.4 `CausalRelation` Enum and `layer.compare()`

The spec mentions `layer.compare(a, b)` returning causal relations but does not formally define the `CausalRelation` enum (`Descends`, `Ascends`, `Concurrent`) as a standalone type. The code defines it in `/Users/daniel/ak/ankurah-201/core/src/event_dag/layers.rs:26-31`.

### 4.5 `is_descendant` Returns False for Self-Comparison

**Code** (`/Users/daniel/ak/ankurah-201/core/src/event_dag/layers.rs:207-209`):
```rust
if descendant == ancestor { return Ok(false); }
```

Yet `layer.compare(a, a)` returns `Descends` because the `a == b` check comes first:
```rust
if a == b { return Ok(CausalRelation::Descends); }
```

This is not documented but is semantically acceptable in context (the LWW resolution treats self-comparison as "candidate replaces current" which is a no-op for the same event).

### 4.6 `MutationError::InsufficientCausalInfo`

The spec does not mention this error type by name. The code introduces it in `/Users/daniel/ak/ankurah-201/core/src/error.rs:171` and uses it in `apply_layer` when `layer.compare()` fails. The spec says "Missing DAG info is an error (bail out)" which is directionally correct but the specific error type is undocumented.

### 4.7 `compute_ancestry` Function

The spec's API Reference section does not mention `compute_ancestry`, but the code exports and uses it in `entity.rs:285`. This function computes the set of all ancestors of the current head to partition events into `already_applied` vs `to_apply`.

---

## 5. Missing Behavior (Spec promises what code doesn't implement)

### 5.1 BudgetExceeded Resumption

**Spec:** "Resume with frontiers" (Action column for BudgetExceeded)
**Code:** Returns error. No caller implements resumption. Frontiers are included in the error but serve only as debugging info.

### 5.2 Forward Chain Replay for StrictDescends

**Spec:** "Apply forward chain" (Action column for StrictDescends)
**Code:** The chain is computed and returned in the relation variant, but `apply_event` ignores it entirely. Only the single incoming event's operations are applied. This relies on the assumption that events are delivered in causal order.

### 5.3 Spec's PropertyBackend Trait Signature Mismatch

**Spec:**
```rust
fn apply_layer(
    &self,
    already_applied: &[&Event],
    to_apply: &[&Event],
) -> Result<(), MutationError>
```

**Code:**
```rust
fn apply_layer(&self, layer: &EventLayer<EventId, Event>) -> Result<(), MutationError>;
```

The actual signature takes a single `&EventLayer` struct rather than two separate slices. The `EventLayer` additionally carries an `events: Arc<BTreeMap<Id, E>>` field for DAG context (used by `layer.compare()`). The spec's API Reference is outdated relative to the implementation.

---

## 6. Ambiguities the Code Resolves Without Documentation

### 6.1 Head Update Semantics for DivergedSince

The spec does not explicitly describe how the head is updated for the `DivergedSince` case. The code (`/Users/daniel/ak/ankurah-201/core/src/entity.rs:320-326`) removes meet IDs from the head and inserts the new event ID. This is correct for the StrictAscends-transformed DivergedSince (where meet = parent clock, which overlaps the head), but for BFS-discovered DivergedSince (where meet may be deep in history), the remove is a silent no-op and the head simply grows.

### 6.2 Which Events Are Accumulated

The spec says "Accumulate full events during traversal (via AccumulatingNavigator)" but doesn't specify that only events fetched during the BFS are accumulated. Events on the other branch that were never traversed (because the comparison resolved before reaching them) are NOT in the accumulated map. This matters for the StrictAscends transformation case where `other_chain` is empty.

### 6.3 Layer Skipping When No `to_apply`

The code (`/Users/daniel/ak/ankurah-201/core/src/event_dag/layers.rs:130`) only creates a layer if `!to_apply.is_empty()`. The spec says layers are "sets of concurrent events at the same causal depth from the meet point" but does not mention that layers with only already_applied events are skipped. This is an optimization that could affect backends if they needed to see all layers for context, but currently no backend requires this.

### 6.4 Taint Precedence

The code checks `subject_frontier.is_tainted()` BEFORE checking `unseen_comparison_heads == 0` or `unseen_subject_heads == 0`. This means assertion-based tainting takes priority over BFS-discovered relationships. The spec does not describe assertion taint handling at all (assertions are an undocumented-in-spec feature currently unused by any navigator).

---

## 7. Test Coverage Against Spec Claims

| Spec Claim | Test Exists? | Location | Notes |
|------------|-------------|----------|-------|
| Multi-head extension | YES | `tests.rs:test_multihead_event_extends_one_tip` | |
| Deep diamond concurrency | YES | `tests.rs:test_deep_diamond_asymmetric_branches` | |
| Short branch from deep point | YES | `tests.rs:test_short_branch_from_deep_point` | |
| Per-property LWW resolution | YES | `lww_resolution.rs:test_per_property_concurrent_writes` and `tests.rs:lww_layer_tests::test_apply_layer_multiple_properties` | |
| Same-depth lexicographic tiebreak | YES | `lww_resolution.rs:test_lexicographic_tiebreak` and `tests.rs:lww_layer_tests::test_apply_layer_higher_event_id_wins` | |
| Idempotency (redundant delivery) | YES | `tests.rs:test_same_event_redundant_delivery`, `test_compare_event_redundant_delivery` | See Finding #6 |
| Chain ordering | YES | `tests.rs:test_forward_chain_ordering`, `test_diverged_chains_ordering` | Linear only; no multi-merge test |
| Disjoint detection | YES | `tests.rs:test_incomparable` | |
| Layer computation | YES | `layers.rs:tests` (4 tests) | |
| LWW apply_layer | YES | `tests.rs:lww_layer_tests` (7+ tests) and `tests.rs:determinism_tests` (4 tests) and `tests.rs:edge_case_tests` (7 tests) | |
| Yrs apply_layer | YES | `tests.rs:yrs_layer_tests` (4 tests) | |
| Budget resumption | NO | None | Spec mentions in Future Work but claims it in action table |
| Deterministic convergence | YES | `tests.rs:determinism_tests`, `lww_resolution.rs:test_lww_order_independence` | |
| TOCTOU retry pattern | NO | None | Spec describes pattern; no test exercises it. Comment in `entity.rs:566` explicitly punts this |
| apply_state DivergedSince | PARTIAL | `stateandvent_divergence.rs` | Tests the integration path but doesn't unit-test the Ok(false) return |
| Empty meet DivergedSince | YES | `tests.rs:test_empty_clocks` | |

---

## 8. Confidence-Rated Findings

| # | Finding | Severity | Confidence | Details |
|---|---------|----------|------------|---------|
| 1 | **StrictDescends chain is computed but never used.** The spec's action table says "Apply forward chain" for `StrictDescends`, but `apply_event` in `/Users/daniel/ak/ankurah-201/core/src/entity.rs:256-271` ignores the chain entirely and only applies the single incoming event. This is acknowledged as a design decision (assumes causal delivery) but the spec's action table is misleading. The spec should either say "Apply single event (chain available for replay if needed)" or document the causal-delivery assumption in the action column. | Medium | High | The chain computation in `build_forward_chain` is dead work for the `StrictDescends` case in `apply_event`. The spec describes the chain as enabling replay, but no code uses it for that purpose. |
| 2 | **Head pruning in DivergedSince removes meet IDs, not superseded tips.** The code at `/Users/daniel/ak/ankurah-201/core/src/entity.rs:323-326` removes `meet` IDs from the head. The comment says "remove superseded tips" and "incoming event extends tips in its parent clock (meet)". For the StrictAscends-transformed case, meet = event's parent clock, so this correctly removes parent tips from the head. But for BFS-discovered DivergedSince where meet is deep in history (e.g., genesis), the removes are no-ops. The spec does not document this head update logic at all. | Medium | High | The head correctly grows in the BFS-discovered case (new event is concurrent with all existing tips). But if the meet happens to include IDs that ARE in the head (possible in complex multi-head scenarios), those tips would be incorrectly removed even though they may still be concurrent with the new event. The code assumes meet IDs in the head are superseded, which is only guaranteed for the StrictAscends transformation. |
| 3 | **`build_forward_chain` is not topologically sorted across merges.** The spec says "Forward chains are event sequences in causal order (oldest to newest) from the meet point to each head." The code reverses BFS visit order, which produces correct causal order for linear chains but may not produce a valid topological order for DAGs with merges. The chain construction at `/Users/daniel/ak/ankurah-201/core/src/event_dag/comparison.rs:447-460` finds the first meet event and drops everything before it, which can miss events that entered the BFS from a different path. No test covers chain ordering for merge-containing DAGs. | Medium | Medium | The forward chain is primarily used for two purposes: (1) replay in StrictDescends (unused -- Finding #1) and (2) the layer computation in DivergedSince. For layers, the chain is not directly used (layers are computed from the events map via frontier expansion). The incorrectly-ordered chain in subject_chain/other_chain is informational only and doesn't affect correctness of layer computation. |
| 4 | **BudgetExceeded action described as "Resume with frontiers" but code returns error.** The spec's action table at line 22 says the action for `BudgetExceeded` is "Resume with frontiers." Both `apply_event` and `apply_state` in `/Users/daniel/ak/ankurah-201/core/src/entity.rs` convert `BudgetExceeded` to `Err(LineageError::BudgetExceeded)`. No caller implements resumption. The spec's Future Work section acknowledges "Budget resumption: Continue interrupted traversals" as future work, contradicting the action table. | Low | High | The frontiers are preserved in the error type, so resumption is architecturally possible but not implemented. The spec should change the action to "Return error with frontiers (resumption not yet implemented)". |
| 5 | **Empty other_chain StrictAscends transformation may cause InsufficientCausalInfo error.** The spec says "Conservative choice: keep current value if it has a tracked event_id." But the actual behavior is: the stored value's event_id is seeded into winners, then when a new candidate from the incoming event competes, `layer.compare()` is called. The `EventLayer.events` map only contains events accumulated during BFS traversal. If the stored value's event_id isn't in that map (which is likely when other_chain is empty -- the other branch wasn't traversed), `is_descendant` in `/Users/daniel/ak/ankurah-201/core/src/event_dag/layers.rs:221` will fail with `RetrievalError::Other("missing event for ancestry lookup")`, which becomes `MutationError::InsufficientCausalInfo`. This is a bail-out, not conservative resolution. | High | Medium | This may be mitigated in practice if the stored event_id happens to be a head tip that WAS traversed during BFS (e.g., in the StrictAscends case, the comparison clock members are traversed). Whether the stored event_id is in the accumulated events depends on the specific DAG topology. The known per-property LWW bug from the review digest (Section 2.1, "CRITICAL: Multi-Head LWW Resolution Bug") is closely related: a non-head event that is the last writer of a property will have an event_id NOT in the accumulated events map. |
| 6 | **Idempotency invariant depends on caller pre-check.** The spec claims "Idempotent application: Redundant delivery is a no-op." The code handles redundant delivery at the head correctly (returns `Equal`). But `test_event_in_history_not_at_head` (line 1116 in tests.rs) explicitly documents that an event already in history but not at the head returns `DivergedSince`, which would re-apply it. The test says "the caller should check if the event is already stored before calling compare_unstored_event." The spec does not mention this caller responsibility. | Medium | High | The invariant is practically maintained if the protocol ensures events are only delivered once, but the spec's unconditional claim of idempotency is inaccurate for the `compare_unstored_event` function itself. |
| 7 | **Spec API Reference shows outdated `apply_layer` signature.** The spec shows `apply_layer(&self, already_applied: &[&Event], to_apply: &[&Event])` but the actual signature is `apply_layer(&self, layer: &EventLayer<EventId, Event>)`. The `EventLayer` struct also carries an `events: Arc<BTreeMap<Id, E>>` field for DAG context that enables `layer.compare()`, which is critical for the LWW resolution algorithm. | Low | High | The spec's signature predates the addition of `layer.compare()` for causal dominance checking. The spec body correctly describes using `layer.compare()` but the API Reference section is stale. |
| 8 | **Spec `ValueEntry` struct does not match code's three-state enum.** The spec shows a flat struct with `committed: bool`. The code uses `enum ValueEntry { Uncommitted, Pending, Committed }`. The `Pending` state (operations collected but event not yet committed) is functionally significant for transaction isolation but the spec omits it entirely. | Low | High | The `Pending` variant is used during `to_operations()` (line 147 of lww.rs) to mark values that have been collected into an event but not yet committed. This prevents double-collection. The spec's simplified model doesn't capture this lifecycle. |
| 9 | **`compute_layers` signature in spec uses concrete types; code is generic.** The spec shows `compute_layers<'a>(events: &'a BTreeMap<EventId, Event>, ...)`. The code is `compute_layers<Id, E>(events: &BTreeMap<Id, E>, ...)`. Additionally, the spec's return type is `Vec<EventLayer<'a>>` with a lifetime, while the code returns `Vec<EventLayer<Id, E>>` (no lifetime -- events are cloned). | Low | High | Pure documentation drift. The generic implementation is more flexible and enables unit testing with mock types. |
| 10 | **Assertions/taints are fully implemented in code but unmentioned in spec.** The code has a complete assertion system (`AssertionRelation`, `TaintReason`, taint-based early termination in `check_result`) across `/Users/daniel/ak/ankurah-201/core/src/event_dag/comparison.rs:385-442` and `frontier.rs`. The spec makes no mention of assertions, tainting, or the `CausalNavigator`/`NavigationStep` types. The spec's "Future Work" mentions "Attested lineage shortcuts" but the infrastructure is already built. | Low | Medium | Assertions are currently unused (both `LocalRetriever` and `EphemeralNodeRetriever` return empty assertions per the review digest). The code is dead infrastructure waiting for future attestation support. Including it in the spec would prevent confusion about the taint precedence behavior (Section 6.4). |
| 11 | **No test for TOCTOU retry exhaustion.** The spec describes the TOCTOU retry pattern (Section "Key Design Decisions, #3") and the code implements it with `MAX_RETRIES = 5`. No test verifies the `MutationError::TOCTOUAttemptsExhausted` error path. The code comment at `/Users/daniel/ak/ankurah-201/core/src/entity.rs:566` says "TODO - Implement TOCTOU Race condition tests. Require real backend state mutations to be meaningful. punting that for now." | Low | High | The retry pattern is sound (confirmed by all review rounds), but the exhaustion path is untested. |
| 12 | **Empty clock comparison returns DivergedSince, not documented in spec.** The code at `/Users/daniel/ak/ankurah-201/core/src/event_dag/comparison.rs:52-62` returns `DivergedSince { meet: [], ... }` when either clock is empty. The spec's termination conditions table does not list empty clocks as a special case. This behavior is tested (`test_empty_clocks`) but undocumented. | Low | High | This is the genesis event case. When an entity has no head (empty clock), any incoming event is treated as "diverged since nothing" which triggers layer-based application. The entity creation path (`apply_event` lines 224-238) short-circuits before comparison for genesis events, so this empty-clock path in `compare` is primarily hit by `apply_state`. |
| 13 | **`apply_event` DivergedSince applies layers to ALL backends, not just those with operations.** The code at `/Users/daniel/ak/ankurah-201/core/src/entity.rs:304-306` calls `backend.apply_layer(layer)` for every backend in the entity state, regardless of whether the layer's events have operations for that backend. The spec does not describe this broadcast-to-all-backends behavior. For LWW backends, this causes the stored value to be seeded and potentially re-evaluated against itself (harmless but wasteful). For Yrs backends, this is a no-op (no matching operations). | Low | Medium | This is functionally correct but wasteful. A more targeted approach would check `layer.to_apply` events for relevant backend operations before calling `apply_layer`. |

---

## Summary

The spec and code are broadly aligned on the core algorithm and data structures. The most significant conformance issues are:

1. **The spec's action table overpromises in two places**: "Apply forward chain" for StrictDescends (chain is ignored) and "Resume with frontiers" for BudgetExceeded (returns error). These are aspirational descriptions rather than accurate documentation of current behavior.

2. **The empty other_chain case (Finding #5) intersects with the known per-property LWW bug** from the review digest. The spec claims conservative resolution, but the code may bail out with `InsufficientCausalInfo` when the stored value's event_id isn't in the accumulated events map. This is the same fundamental issue as the unresolved CRITICAL bug from the PR review (Section 2.1 of the digest).

3. **The spec's API Reference section is stale** (Finding #7, #8, #9). The `apply_layer` signature, `ValueEntry` struct, and `compute_layers` signature all differ from the implementation. These are documentation defects, not logic bugs.

4. **Head update semantics for DivergedSince (Finding #2) are undocumented** and have a subtle edge case for multi-head entities where meet IDs happen to be in the current head.

5. **Test coverage is comprehensive for documented behavior** but missing for: TOCTOU retry exhaustion (Finding #11), chain ordering across merges (Finding #3), and the InsufficientCausalInfo error path in the StrictAscends transformation (Finding #5).
