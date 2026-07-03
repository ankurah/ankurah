# Informed Spec Compliance Review

Date: 2026-02-09
Reviewer: Informed Spec Compliance Agent (Round 2)
Branch: `concurrent-updates-event-dag`
Worktree: `/Users/daniel/ak/ankurah-201`

---

## Prior Deviation Reconfirmation

### Deviation 1: `apply_layer` takes `&EventLayer` (not separate slices)

**CONFIRMED JUSTIFIED -- NEEDS SPEC UPDATE (lines 217-222)**

The spec's API Reference shows:
```rust
fn apply_layer(
    &self,
    already_applied: &[&Event],
    to_apply: &[&Event],
) -> Result<(), MutationError>
```

The actual code (`core/src/property/backend/mod.rs:84`):
```rust
fn apply_layer(&self, layer: &EventLayer) -> Result<(), MutationError>;
```

The `EventLayer` struct carries `already_applied`, `to_apply`, AND `dag: Arc<BTreeMap<EventId, Vec<EventId>>>`. The DAG context is essential for `layer.compare()` and `layer.dag_contains()`, which are used by the LWW resolution algorithm. The spec's signature is not just simplified -- it's missing a critical parameter. The deviation is fully justified but the spec must be updated.

### Deviation 2: Idempotency via BFS `StrictAscends` instead of `event_stored()` pre-check

**CONFIRMED JUSTIFIED**

The code at `entity.rs:235-241` comments explain the rationale: callers store events to storage BEFORE calling `apply_event`, so `event_stored()` would produce false positives. Instead, BFS naturally discovers re-delivered events and returns `StrictAscends` (ancestor of head). The `event_stored()` check IS used for the creation event fast-path on durable nodes (`entity.rs:252-258`), which is a separate concern. No spec update needed -- the spec (line 190) claims idempotent application, and the implementation achieves it through a different mechanism.

**CAVEAT**: The idempotency claim relies on the event being discoverable by BFS. The `test_event_in_history_not_at_head` test at `tests.rs:1111-1144` confirms BFS correctly returns `StrictAscends` for a historical event, validating this approach.

### Deviation 3: `DuplicateCreation` removed -- `compare()` handles both cases

**CONFIRMED JUSTIFIED -- PARTIAL SPEC UPDATE NEEDED (lines 132-143)**

The spec describes a `compare_unstored_event` function and a "Multi-Head StrictAscends Transformation" (lines 132-143). This function no longer exists. The current approach stages the event, then calls `compare()` with the event's own ID as a single-element subject clock. BFS discovers the same relationships without needing a separate function or post-hoc transformation.

The spec's Section "Key Design Decisions #1" (lines 132-143) describes the OLD mechanism:
> "The event is staged in the event getter (GetEvents) before calling compare() with the event's own ID as the subject."

This IS accurate for the current implementation's approach. But the intermediate step describing the `StrictAscends` transformation is vestigial. The code never produces `StrictAscends` and then transforms it -- BFS directly produces `DivergedSince` for the multi-head extension case. The spec text at lines 138-142 describes internal mechanics of a function that no longer exists.

### Deviation 4: Budget escalation (not in spec) -- strictly beneficial

**CONFIRMED JUSTIFIED -- NEEDS SPEC UPDATE (line 22, line 60)**

Budget escalation is implemented in `comparison.rs:107-129`. The initial budget is retried at 4x if `BudgetExceeded` is returned. The spec's action table (line 22) says "Resume with frontiers" and Future Work (line 253) says "Budget resumption: Continue interrupted traversals." Neither mentions the existing escalation mechanism.

The spec should:
1. Add budget escalation to the algorithm description (Section "Algorithm: Backward BFS")
2. Change the BudgetExceeded action from "Resume with frontiers" to "Error (escalation attempted internally; frontiers preserved for future resumption)"

### Deviation 5: `ValueEntry` enum vs struct

**CONFIRMED JUSTIFIED -- NEEDS SPEC UPDATE (lines 174-179)**

The spec shows:
```rust
struct ValueEntry {
    value: Option<Value>,
    event_id: EventId,
    committed: bool,
}
```

The code (`lww.rs:21-25`):
```rust
enum ValueEntry {
    Uncommitted { value: Option<Value> },
    Pending { value: Option<Value> },
    Committed { value: Option<Value>, event_id: EventId },
}
```

The three-variant enum is strictly richer. `Pending` is used during `to_operations()` (lww.rs:142-147) to prevent double-collection of operations. The spec's simplified model should be updated or explicitly noted as a simplification.

### Deviation 6: `compare()` signature differences (async, budget param, generic return)

**CONFIRMED JUSTIFIED -- NEEDS SPEC UPDATE (lines 196-204)**

The spec shows:
```rust
pub fn compare<E>(
    subject: &Clock,
    comparison: &Clock,
    events: E,
) -> Result<ComparisonResult, RetrievalError>
```

The actual code (`comparison.rs:49-54`):
```rust
pub(crate) async fn compare<E: GetEvents>(
    event_getter: E,
    subject: &Clock,
    comparison: &Clock,
    budget: usize,
) -> Result<ComparisonResult<E>, RetrievalError>
```

Key differences:
- `async` (required for `get_event` calls during BFS)
- `budget: usize` parameter (essential for budget escalation)
- `event_getter` is first parameter (Rust convention for the "context" param)
- `ComparisonResult<E>` carries the accumulator generically (enables `into_layers`)
- `pub(crate)` visibility (not `pub`)

The spec's signature is significantly outdated. Additionally, the spec mentions a `compare_unstored_event` function (implied in lines 138-142) that does not exist in the current code.

### Deviation 7: Forward chains informational only (not consumed by replay or merge)

**CONFIRMED JUSTIFIED -- NEEDS SPEC UPDATE (lines 18, 26-29)**

The spec states:
- Line 18: StrictDescends action is "Apply forward chain"
- Lines 26-29: "Forward chains enable: Replay (apply events in correct order for StrictDescends), Merge (compute per-property depths for DivergedSince resolution)"

In the code:
- `apply_event` for `StrictDescends` (`entity.rs:300-314`) applies only the single incoming event's operations. The chain is computed but never read.
- For `DivergedSince`, layer computation uses the accumulated DAG (via `into_layers`), not the forward chains from the relation variant.
- The forward chains in `subject_chain`/`other_chain` are purely informational/debugging data.

The spec should change:
- Line 18: "Apply forward chain" -> "Apply single event (chain available for diagnostics)"
- Lines 26-29: Remove or qualify the claims about chain usage

### Deviation 8: Spec's "conservative resolution" language (lines 147-150) is inaccurate

**CONFIRMED -- NEEDS SPEC UPDATE (lines 145-150)**

The spec says (lines 147-150):
> "When `other_chain` is empty (StrictAscends transformation case):
> - We can't compute depths for the other branch
> - Conservative choice: keep current value if it has a tracked `event_id`
> - Rationale: Current value was set by a more recent event on the head's branch"

The actual behavior is more nuanced due to the `older_than_meet` rule:
1. The stored value's `event_id` is checked against `layer.dag_contains()` (`lww.rs:194`)
2. If NOT in the DAG (older than meet), the incoming event wins UNCONDITIONALLY (`lww.rs:213-215`)
3. If IN the DAG, standard causal comparison + lexicographic tiebreak applies (`lww.rs:218-229`)

For the multi-head extension case where `other_chain` is empty:
- The comparison clock members (head tips not in the event's parent) are NOT in the accumulated DAG (BFS starts from the event's single-element clock and walks toward the head -- it does NOT accumulate the other branch's events because the meet is the event's parent, and events above the meet on the other branch are the head tips themselves which were never fetched by BFS in this path)
- Wait -- actually, in the current unified `compare()`, both frontiers are explored. The comparison frontier starts from [B, C] and subject frontier from [D]. BFS fetches events from both. So B and C ARE accumulated.
- The meet is identified at C (where D's parent is). B is on the comparison side but not a descendant of the meet C -- B is concurrent with C.
- `compute_layers` builds layers from events above the meet. B is above the meet but its parent is A, not C. The layer frontier initializes with events whose in-DAG parents are all in the processed set (meet = {C}). B's parent is A, which may or may not be in the DAG.

This is complex enough that the simple "keep current value" claim is misleading. The actual behavior depends on whether the stored value's event_id is in the accumulated DAG. The spec should be rewritten to describe the `older_than_meet` rule directly.

---

## Requirement-by-Requirement Conformance

### Spec Section: Core Concepts -- Causal Relationships (lines 12-22)

| Spec Line | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|
| 17 | `Equal`: Identical lattice points, No-op | COMPLIANT | `comparison.rs:70-73`, `entity.rs:296-298` | Early exit when `subject.as_slice() == comparison.as_slice()` |
| 18 | `StrictDescends`: Subject strictly after other, Apply forward chain | DEVIATED | `comparison.rs:413-418`, `entity.rs:300-314` | Chain computed but only single event applied. Justified: assumes causal delivery. |
| 19 | `StrictAscends`: Subject strictly before other, No-op | COMPLIANT | `comparison.rs:423-424`, `entity.rs:316-319` | Returns `Ok(false)` -- correct no-op |
| 20 | `DivergedSince`: True concurrency, Per-property LWW merge | COMPLIANT | `comparison.rs:428-471`, `entity.rs:321-381` | Full layer computation + per-backend apply_layer |
| 21 | `Disjoint`: Different genesis, Reject per policy | COMPLIANT | `comparison.rs:436-444`, `entity.rs:383-384` | Returns `Err(LineageError::Disjoint)` |
| 22 | `BudgetExceeded`: Budget exhausted, Resume with frontiers | DEVIATED | `comparison.rs:472-477`, `entity.rs:386-393` | Returns hard error, no resumption. Budget escalation is internal. |

### Spec Section: Forward Chains (lines 26-30)

| Spec Line | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|
| 27 | Replay: Apply events in correct order for StrictDescends | DEVIATED | `entity.rs:300-314` | Chain not used for replay; single event applied |
| 28 | Merge: Compute per-property depths for DivergedSince resolution | DEVIATED | `accumulator.rs:73-76`, `layers.rs` | Layers computed from accumulated DAG, not from chains |
| 30 | Chains accumulated during backward BFS traversal and reversed | COMPLIANT | `comparison.rs:286-291`, `comparison.rs:368-381` | Visited lists reversed in `build_forward_chain` |

### Spec Section: Algorithm -- Backward BFS (lines 32-60)

| Spec Line | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|
| 44 | Initialize: Two frontiers from subject/other heads | COMPLIANT | `comparison.rs:214-236` | `Frontier::new(subject_ids)` and `Frontier::new(comparison_ids)` |
| 46 | Expand: Fetch events at frontier positions | COMPLIANT | `comparison.rs:238-275` | Fetches via `accumulator.get_event(id)` |
| 47 | Remove from frontier, add parents | COMPLIANT | `comparison.rs:282-283`, `347-363` | `frontier.remove(&id)` then `frontier.extend(parents)` |
| 48 | Track which frontier(s) have seen it | COMPLIANT | `comparison.rs:294-304` | `NodeState.seen_from_subject`, `seen_from_comparison` |
| 49 | Accumulate in visited list | COMPLIANT | `comparison.rs:285-291` | `subject_visited.push(id)` / `other_visited.push(id)` |
| 53 | StrictDescends when `unseen_comparison_heads == 0` | COMPLIANT | `comparison.rs:413-418` | Exact field check |
| 54 | StrictAscends when `unseen_subject_heads == 0` | COMPLIANT | `comparison.rs:423-424` | Exact field check |
| 55 | DivergedSince when frontiers empty + common ancestors | COMPLIANT | `comparison.rs:428-471` | Meet computed as minimal common ancestors |
| 56 | Disjoint when frontiers empty + different genesis roots | COMPLIANT | `comparison.rs:436-444` | Checks `subject_root != other_root` |
| 57 | DivergedSince with empty meet when can't prove disjoint | COMPLIANT | `comparison.rs:447-453` | Falls through when roots don't differ |
| 58 | BudgetExceeded when budget exhausted | COMPLIANT | `comparison.rs:472-477` | `remaining_budget == 0` check |

### Spec Section: Conflict Resolution -- Layer Model (lines 62-76)

| Spec Line | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|
| 68 | Events applied in layers at same causal depth from meet | COMPLIANT | `accumulator.rs:146-218` | Frontier expansion from meet, topological ordering |
| 71 | `EventLayer` has `already_applied`, `to_apply`, `events` | DEVIATED | `accumulator.rs:229-234` | Generic `<EventId, Vec<EventId>>` DAG structure, not `BTreeMap<EventId, Event>`. The field is `dag: Arc<BTreeMap<EventId, Vec<EventId>>>` (parent pointers only), not full events. Justified: parent pointers sufficient for causal comparison. |
| 78 | Use causal comparison when possible; lexicographic only for concurrent | COMPLIANT | `lww.rs:218-229` | `layer.compare()` -> Descends/Ascends; fallback to `event_id >` for Concurrent |

### Spec Section: Resolution Process (lines 81-85)

| Spec Line | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|
| 82 | Navigate backward from both heads to find meet | COMPLIANT | `entity.rs:294` | Calls `compare()` which performs BFS |
| 83 | Accumulate full events via AccumulatingNavigator | DEVIATED | `accumulator.rs:46-51` | Named `EventAccumulator`, not `AccumulatingNavigator`. Accumulates DAG structure + LRU cache, not full events in memory. |
| 84 | Compute layers via frontier expansion from meet | COMPLIANT | `accumulator.rs:128-162` | `EventLayers::new` seeds frontier from meet children |
| 85 | Apply layers in causal order via `backend.apply_layer()` | COMPLIANT | `entity.rs:350-369` | Loop over layers, call `backend.apply_layer(&layer)` |

### Spec Section: Backend Interface (lines 87-95)

| Spec Line | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|
| 93 | `apply_layer` method on PropertyBackend trait | DEVIATED | `backend/mod.rs:84` | Takes `&EventLayer` not separate slices (see Deviation 1) |

### Spec Section: LWW Resolution Within Layer (lines 97-107)

| Spec Line | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|
| 100 | Examine ALL events (already_applied + to_apply) plus stored value | COMPLIANT | `lww.rs:181-242` | Seeds from stored values, iterates both lists |
| 101-102 | Per-property winner by `layer.compare(a, b)`: Descends/Ascends decides | COMPLIANT | `lww.rs:218-223` | Descends replaces, Ascends keeps |
| 103 | Concurrent falls back to lexicographic EventId | COMPLIANT | `lww.rs:224-228` | `candidate.event_id > current.event_id` |
| 104 | Missing DAG info: treated as dead end in traversal | COMPLIANT | `accumulator.rs:298-299` | `is_descendant_dag` treats missing entries as dead ends (returns false), not errors. The `compare()` method is infallible. |
| 105 | Only mutate state for winners from to_apply set | COMPLIANT | `lww.rs:248-253` | `if candidate.from_to_apply { values.insert(...) }` |
| 106 | Track event_id per property | COMPLIANT | `lww.rs:250` | `ValueEntry::Committed { value, event_id }` |

### Spec Section: Backend-Specific Behavior (lines 125-128)

| Spec Line | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|
| 127 | LWW: Determine winner using causal comparison; only apply winners from to_apply | COMPLIANT | `lww.rs:169-265` | Full implementation as described |
| 128 | Yrs: Apply all operations from to_apply; CRDT handles idempotency; already_applied ignored | COMPLIANT | `yrs.rs:186-209` | Only iterates `layer.to_apply`, ignores `already_applied` |

### Spec Section: Key Design Decisions (lines 130-182)

| Spec Line | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|
| 132-143 | Multi-Head StrictAscends Transformation | DEVIATED | `entity.rs:293-294` | The described `compare([C], [B, C])` then transform mechanism no longer exists. The code uses `compare([D], [B, C])` directly. BFS produces `DivergedSince` naturally. Outcome is identical but mechanism differs. |
| 145-150 | Empty Other-Chain Conservative Resolution | DEVIATED | `lww.rs:190-197` | `older_than_meet` rule means incoming event can WIN, not "keep current." See Deviation 8. |
| 153-168 | TOCTOU Retry Pattern | COMPLIANT | `entity.rs:289-398` | Both StrictDescends (via `try_mutate`) and DivergedSince (manual lock + head check) implement the pattern. Two different mechanisms for the same goal. |
| 172-182 | Per-Property Event Tracking | DEVIATED | `lww.rs:20-25` | Three-variant enum instead of struct. See Deviation 5. |

### Spec Section: Invariants (lines 184-190)

| Spec Line | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|
| 187 | No regression: never skip required intermediate events | COMPLIANT | `entity.rs:300-314`, `entity.rs:350-369` | StrictDescends applies event; DivergedSince applies all layers |
| 188 | Single head for linear history | COMPLIANT | `entity.rs:308` | StrictDescends: `state.head = new_head.clone()` (replaces head) |
| 189 | Deterministic convergence | COMPLIANT | `lww.rs` (BTreeMap ordering), `accumulator.rs` (BTreeSet frontiers) | Tests in `determinism_tests` confirm |
| 190 | Idempotent application | COMPLIANT | `comparison.rs:70-73`, `comparison.rs:413-424` | Equal for head event, StrictAscends for historical event. Requires event to be staged/in-storage for BFS discovery. |

### Spec Section: API Reference (lines 192-230)

| Spec Line | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|
| 196-204 | `compare` function signature | DEVIATED | `comparison.rs:49-54` | Different param order, async, budget param, generic return. See Deviation 6. |
| 210-215 | `apply_operations_with_event` signature | COMPLIANT | `backend/mod.rs:58-62` | Signature matches; default impl ignores event_id. |
| 217-222 | `apply_layer` signature | DEVIATED | `backend/mod.rs:84` | Takes `&EventLayer`, not separate slices. See Deviation 1. |
| 227-229 | Layer computation via `into_layers()` | DEVIATED | `accumulator.rs:73-76`, `accumulator.rs:94-99` | `into_layers` takes `current_head` parameter not shown in spec. Returns `Option<EventLayers>`, not `EventLayers` directly. |

### Spec Section: Test Coverage (lines 232-246)

| Spec Line | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|
| 237 | Multi-head extension tested | COMPLIANT | `tests.rs:648-690` | `test_multihead_event_extends_one_tip` |
| 238 | Deep diamond concurrency tested | COMPLIANT | `tests.rs:784-850` | `test_deep_diamond_asymmetric_branches` |
| 239 | Short branch from deep point tested | COMPLIANT | `tests.rs:852-920` | `test_short_branch_from_deep_point` |
| 240 | Per-property LWW resolution tested | COMPLIANT | `tests.rs:1220-1241` | `test_apply_layer_multiple_properties` |
| 241 | Same-depth lexicographic tiebreak tested | COMPLIANT | `tests.rs:1154-1173` | `test_apply_layer_higher_event_id_wins` |
| 242 | Idempotency tested | COMPLIANT | `tests.rs:1076-1144`, `tests.rs:1699-1747` | Multiple idempotency tests |
| 243 | Chain ordering tested | COMPLIANT | `tests.rs:984-1070` | `test_forward_chain_ordering`, `test_diverged_chains_ordering` |
| 244 | Disjoint detection tested | COMPLIANT | `tests.rs:219-293` | `test_incomparable` |
| 245 | Layer computation tested | COMPLIANT | `tests.rs:1901-1999+` | `test_merge_event_with_parent_from_non_meet_branch` + `accumulator.rs:310+` |
| 246 | LWW apply_layer tested | COMPLIANT | `tests.rs:1150-1274` | 7 tests in `lww_layer_tests` |
| 247 | Yrs apply_layer tested | COMPLIANT | `tests.rs:1280-1398` | 4 tests in `yrs_layer_tests` |

---

## Undocumented Behavior (code does what spec does not describe)

### U1: Quick-check linear extension fast path
**Code**: `comparison.rs:77-105`. Before launching BFS, checks if every comparison head appears as a parent of some subject event. If so, returns `StrictDescends` immediately without fetching comparison events.
**Impact**: Significant optimization for the common case; essential for ephemeral nodes. Spec should document this.

### U2: Empty clock early exit
**Code**: `comparison.rs:56-68`. Returns `DivergedSince` with all-empty fields when either clock is empty.
**Impact**: The `compare(empty, empty)` case returns `DivergedSince` not `Equal`. This is intentional (genesis creation path handles empty clocks separately in `entity.rs:263-278`).

### U3: Entity creation fast path
**Code**: `entity.rs:262-278`. Creation events on entities with empty heads bypass comparison entirely.
**Impact**: Not in spec but essential for correctness -- empty head would produce `DivergedSince(meet=[])` which is the wrong semantics for a genesis event.

### U4: Non-creation event on empty head rejection
**Code**: `entity.rs:282-285`. Non-creation events on entities with empty heads are rejected with `InvalidEvent`.
**Impact**: Prevents applying updates to non-existent entities.

### U5: `older_than_meet` rule in LWW resolution
**Code**: `lww.rs:190-197`, `lww.rs:213-215`. Stored values whose `event_id` is not in the accumulated DAG auto-lose to any layer candidate.
**Impact**: This is a critical resolution rule not described in the spec's LWW section. It is mentioned only indirectly in the implementation-resume document.

### U6: `compute_ancestry_from_dag` function
**Code**: `accumulator.rs:268-284`. Computes full ancestry set by backward walk through DAG parent pointers. Used to partition events into `already_applied` vs `to_apply`.
**Impact**: Not in spec API Reference. Essential for correct layer partitioning.

### U7: Unfetchable both-frontiers optimization
**Code**: `comparison.rs:255-267`. Events on both frontiers that can't be fetched are processed as common ancestors with empty parents.
**Impact**: Essential for ephemeral nodes. Not in spec.

### U8: `EventLayer.compare()` returns `Descends` for identity (a == a)
**Code**: `accumulator.rs:251-253`. No `Equal` variant in `CausalRelation`.
**Impact**: Semantically acceptable (reflexive descent) but non-obvious. Not in spec.

### U9: `apply_state` method
**Code**: `entity.rs:408-475`. Returns `DivergedRequiresEvents` for `DivergedSince`, deferring to event-based resolution.
**Impact**: Key part of the sync protocol, entirely absent from spec.

### U10: Layer emits layers with only `already_applied` (no `to_apply`)
**Code**: `accumulator.rs:213-217`. The layer iterator returns `None` only when BOTH `to_apply` AND `already_applied` are empty. Layers with only `already_applied` events ARE emitted.
**Impact**: Prior A3 review (section 6.3) incorrectly claimed layers with only `already_applied` are skipped. They are NOT skipped. This means backends see these "context-only" layers. For LWW, this is harmless (winners map seeds from stored values and no `from_to_apply` candidates exist, so no mutation). For Yrs, `to_apply` is empty so no operations are applied.

---

## Missing Behavior (spec promises what code does not implement)

### M1: Budget resumption
**Spec line 22**: "Resume with frontiers"
**Code**: Returns `Err(LineageError::BudgetExceeded)`. No caller implements resumption. Budget escalation (1x -> 4x) is internal, not resumption.
**Spec line 253**: Acknowledges this as "Future Work" -- contradicts the action table.

### M2: Forward chain replay for StrictDescends
**Spec line 18**: "Apply forward chain"
**Code**: Applies only the single incoming event. Chain is computed but unused.

---

## Spec Update Recommendations

| Priority | Spec Line(s) | Current Text | Recommended Change |
|---|---|---|---|
| HIGH | 18 | `StrictDescends` action: "Apply forward chain" | Change to: "Apply single event (chain available for diagnostics)" |
| HIGH | 22 | `BudgetExceeded` action: "Resume with frontiers" | Change to: "Error with frontiers (internal escalation attempted; resumption not yet implemented)" |
| HIGH | 145-150 | "Conservative choice: keep current value if it has a tracked event_id" | Rewrite to describe the `older_than_meet` rule: "If stored value's event_id is not in the accumulated DAG (older than meet), any layer candidate wins. Otherwise, standard causal comparison + lexicographic tiebreak applies." |
| HIGH | 196-204 | `compare` function signature | Update to match actual: `pub(crate) async fn compare<E: GetEvents>(event_getter: E, subject: &Clock, comparison: &Clock, budget: usize) -> Result<ComparisonResult<E>, RetrievalError>` |
| HIGH | 217-222 | `apply_layer` with two slice params | Update to: `fn apply_layer(&self, layer: &EventLayer) -> Result<(), MutationError>` and describe EventLayer struct with DAG context |
| MEDIUM | 26-29 | Forward chains enable Replay and Merge | Change to: "Forward chains are computed for diagnostic/debugging purposes. Replay uses direct event application; merge uses the accumulated DAG via `into_layers()`." |
| MEDIUM | 71-75 | `EventLayer` struct definition | Update to show generic `EventLayer` with `dag: Arc<BTreeMap<EventId, Vec<EventId>>>` (parent pointers, not full events) |
| MEDIUM | 97-107 | LWW Resolution Within Layer | Add step 0: "older_than_meet rule: if stored value's event_id is absent from the accumulated DAG, it auto-loses to any layer candidate" |
| MEDIUM | 132-143 | Multi-Head StrictAscends Transformation (two-step: compare parent, then transform) | Rewrite to describe current mechanism: "The event is staged, then compare() is called with the event's own ID as subject. BFS naturally discovers the divergence without needing a separate transform step." |
| MEDIUM | 174-179 | `ValueEntry` as flat struct | Update to show three-variant enum with `Uncommitted`, `Pending`, `Committed` |
| LOW | 227-229 | `into_layers()` on `ComparisonResult` | Update to show `into_layers(current_head: Vec<EventId>) -> Option<EventLayers>` |
| LOW | After line 60 | (missing) | Add description of quick-check linear extension fast path |
| LOW | After line 60 | (missing) | Add description of empty clock handling |
| LOW | After line 60 | (missing) | Add description of budget escalation mechanism |
| LOW | After line 128 | (missing) | Document `apply_state` method and its `DivergedRequiresEvents` behavior |

---

## Test Coverage Gaps

| Spec Claim / Code Path | Test Status | Notes |
|---|---|---|
| TOCTOU retry exhaustion (spec lines 153-168) | `#[ignore]` | `test_toctou_retry_exhaustion` exists but is ignored -- needs mock infrastructure |
| BudgetExceeded during `apply_event` | NOT TESTED | Budget is hardcoded to `DEFAULT_BUDGET=1000` in `apply_event`; no test forces exhaustion through the full path |
| `Disjoint` in `apply_event` (non-creation) | PARTIALLY TESTED | `test_second_creation_event_rejected` tests creation path; no test for disjoint non-creation events through `apply_event` |
| `InsufficientCausalInfo` error in LWW | NOT TESTED | The `older_than_meet` rule makes this error unreachable for the stored-value case, but the error is still possible if `entry.event_id()` returns `None` for a `Committed` entry (impossible by construction) |
| Multi-meet chain trimming | NOT TESTED | `build_forward_chain` stops at first meet member; no test has multiple meet members. Known issue #245. |
| `apply_state` with `DivergedSince` | NOT DIRECTLY TESTED | Integration tests exercise the path indirectly through `with_state` fallback |
| Empty `other_chain` conservative resolution (stored value wins) | INDIRECTLY TESTED | The multi-head extension tests exercise this path, but no isolated test verifies the specific resolution outcome |

---

## New Findings (not in prior reviews)

### N1: `compare()` does NOT use `compare_unstored_event` -- spec implies it exists

The spec (line 138) says: "The event is staged in the event getter (GetEvents) before calling compare() with the event's own ID as the subject."

This is accurate for the current code. But both prior reviews (A3 and B3) reference `compare_unstored_event` extensively (A3 section 1.2-1.4, B3 sections 1.2-1.4). That function does not exist in the codebase (confirmed via grep). The prior reviews may have been based on an earlier state of the branch or a misreading. The current code has only one compare function: `compare()` in `comparison.rs:49`.

**Severity**: Low (documentation/review accuracy, not code issue)

### N2: Layer iterator emits layers with only `already_applied` -- contradicts A3 finding

Prior A3 review (section 6.3) states: "The code (`layers.rs:130`) only creates a layer if `!to_apply.is_empty()`." This is INCORRECT for the current code. The current `accumulator.rs:213-217` returns `None` only when BOTH lists are empty. Layers with `to_apply.is_empty()` but non-empty `already_applied` ARE emitted. These "context-only" layers are harmless (LWW sees no `from_to_apply` winners; Yrs has nothing to apply) but this means ALL backends receive ALL layers, including purely contextual ones.

**Severity**: Low (functionally harmless, but contradicts prior review claim)

### N3: `is_descendant_dag` includes start node in visited but returns true at ancestor check

The `is_descendant_dag` function (`accumulator.rs:288-308`) starts from `descendant` and returns `true` when it reaches `ancestor`. If `descendant == ancestor`, the function would visit `descendant`, check `id == ancestor` (true), and return `true`. Wait -- let me re-check. The function adds `descendant` to `visited`, then checks `if &id == ancestor { return true }`. So `is_descendant_dag(a, a)` returns `true` (a is a descendant of itself).

But `EventLayer::compare(a, a)` (`accumulator.rs:251-253`) returns `Descends` via the `if a == b { return CausalRelation::Descends }` short-circuit BEFORE calling `is_descendant_dag`. The `is_descendant_dag` self-comparison returning `true` is consistent but never reached for the identity case.

**Severity**: Informational only

### N4: Head update for DivergedSince removes `meet` members, not `event.parent` members

The spec (line 141) says: "Meet = event's parent clock (the divergence point)"

The code (`entity.rs:375-378`):
```rust
for parent_id in &meet {
    state.head.remove(parent_id);
}
state.head.insert(event.id());
```

The `meet` comes from the BFS comparison result, NOT from the event's parent clock. For the multi-head extension case, the meet IS a subset of the event's parent clock (e.g., meet=[C] when event D has parent [C]). For BFS-discovered divergence where meet is deep in history (e.g., genesis A), the meet members are NOT in the current head, so `remove` is a no-op and the head simply grows by adding the new event.

This is correct behavior: the head should grow when there's true concurrency (the new event IS concurrent with existing head tips). But the spec doesn't document this head update logic at all. Prior reviews noted this as well (A3 finding #2, B3 finding #10).

**Severity**: Low (correct behavior, but undocumented)

---

## Summary

| Category | Count |
|---|---|
| COMPLIANT | 39 |
| DEVIATED (justified, spec needs update) | 12 |
| NON-COMPLIANT | 0 |
| Undocumented behaviors (code-only) | 10 |
| Missing behaviors (spec-only) | 2 |
| Spec update recommendations | 14 |
| Test coverage gaps | 7 |
| New findings (beyond prior reviews) | 4 |

**Overall assessment**: The implementation is sound and all deviations from the spec are justified by implementation necessities. There are zero non-compliant requirements. The spec needs updating in 14 places to match the implementation, with the highest priority items being: (1) the `older_than_meet` rule in LWW resolution, (2) the `compare()` and `apply_layer()` function signatures, (3) the forward chain usage claims, and (4) the BudgetExceeded action description.

Both prior reviewers' conclusion of "0 non-compliant requirements" is **CONFIRMED**. The 8 previously identified deviations are all **CONFIRMED JUSTIFIED**, though most need corresponding spec updates. The `compare_unstored_event` function referenced extensively in both prior reviews does not exist in the current code, suggesting the prior reviews may have been based on an intermediate state of the branch or the spec's description rather than the actual code.
