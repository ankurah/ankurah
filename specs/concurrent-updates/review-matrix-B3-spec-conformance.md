# Spec Conformance Review: Matrix B3

**Reviewer:** Specification auditor (spec-to-code alignment)
**Branch:** `concurrent-updates-event-dag` based off `main`
**Scope:** Line-by-line cross-reference of `specs/concurrent-updates/spec.md` against implementation
**Seed context:** `review-holistic-code.md` (used for structural orientation only)

---

## 1. AbstractCausalRelation Variants: Spec vs Code

### 1.1 `Equal`

**Spec (line 17):** "Identical lattice points" / "No-op"

**Code (comparison.rs:64-66):**
```rust
if subject.members() == comparison.members() {
    return Ok(AbstractCausalRelation::Equal);
}
```

**Code (comparison.rs:95-97, compare_unstored_event):**
```rust
if comparison.members().contains(&event.id()) {
    return Ok(AbstractCausalRelation::Equal);
}
```

**Assessment:** CONFORMANT. The `compare` function checks member equality. The `compare_unstored_event` function checks if the event's ID is already in the comparison head, which correctly detects redundant delivery.

**Undocumented nuance:** `compare_unstored_event` treats "event ID in comparison head" as Equal, even if the comparison head has *additional* members. The spec says "Identical lattice points" which suggests full equality. However, this is correct behavior -- if the event is already in the head, it is redundant regardless of other head members. The spec should note this semantic difference between `compare` (strict equality) and `compare_unstored_event` (membership check).

### 1.2 `StrictDescends`

**Spec (line 18):** "Subject strictly after other" / "Apply forward chain"

**Code (comparison.rs:543-548):**
```rust
if self.unseen_comparison_heads == 0 {
    let chain: Vec<_> = self.subject_visited.iter().rev()
        .filter(|id| !self.original_comparison.contains(id)).cloned().collect();
    return Some(AbstractCausalRelation::StrictDescends { chain });
}
```

**Assessment:** CONFORMANT. When subject's traversal has seen all comparison heads, this means subject descends from comparison. The chain is built by reversing the visited list (backward -> forward order) and filtering out comparison heads. This matches the spec's "Forward chain from other head to subject head."

**Code (compare_unstored_event:104-113):** Parent-Equal and Parent-StrictDescends both map to `StrictDescends` with the event appended to the chain.

**Assessment:** CONFORMANT with spec section "compare_unstored_event" / line 105-113.

### 1.3 `StrictAscends`

**Spec (line 19):** "Subject strictly before other" / "No-op (incoming is older)"

**Code (comparison.rs:553-555):**
```rust
if self.unseen_subject_heads == 0 {
    return Some(AbstractCausalRelation::StrictAscends);
}
```

**Assessment:** CONFORMANT.

### 1.4 `StrictAscends` Transformation in `compare_unstored_event`

**Spec (lines 132-143), "Multi-Head StrictAscends Transformation":**
> When event D with parent [C] arrives at entity head [B, C]:
> - compare([C], [B, C]) returns StrictAscends
> - Transform to DivergedSince: Meet = event's parent clock, Other = head tips not in parent
> - Empty other_chain triggers conservative resolution

**Code (comparison.rs:115-148):**
```rust
AbstractCausalRelation::StrictAscends => {
    let parent_members: BTreeSet<_> = event.parent().members().iter().cloned().collect();
    let comparison_members: BTreeSet<_> = comparison.members().iter().cloned().collect();
    let meet: Vec<_> = parent_members.iter().cloned().collect();
    let other: Vec<_> = comparison_members.difference(&parent_members).cloned().collect();
    AbstractCausalRelation::DivergedSince {
        meet,
        subject: vec![],
        other,
        subject_chain: vec![event.id()],
        other_chain: vec![],
    }
}
```

**Assessment:** CONFORMANT. The meet is set to the parent clock members. The other is the set difference (comparison minus parent). The subject_chain is `[event.id()]` and other_chain is empty. This matches the spec precisely.

**Verified with test:** `test_multihead_event_extends_one_tip` (tests.rs:803-833) confirms meet=[3] and other=[2] for the exact spec example.

### 1.5 `DivergedSince`

**Spec (line 20):** "True concurrency" / "Per-property LWW merge"

**Code (comparison.rs:558-601, check_result):** When frontiers are empty and common ancestors are found, builds forward chains via `build_forward_chain` and collects immediate children.

**Assessment:** CONFORMANT. The meet is computed as minimal common ancestors (those with `common_child_count == 0`). Forward chains and immediate children are populated.

### 1.6 `Disjoint`

**Spec (line 21):** "Different genesis events" / "Reject per policy"

**Code (comparison.rs:564-574):** When no common ancestors found and different roots discovered.

**Code (entity.rs:331-332):**
```rust
AbstractCausalRelation::Disjoint { .. } => {
    return Err(LineageError::Disjoint.into());
}
```

**Assessment:** CONFORMANT. Different roots result in `Disjoint`; `apply_event` converts this to an error.

### 1.7 `BudgetExceeded`

**Spec (line 22):** "Traversal budget exhausted" / "Resume with frontiers"

**Code (comparison.rs:602-607):** Returns remaining frontier IDs.

**Code (entity.rs:334-341):** Converts to `LineageError::BudgetExceeded`.

**Assessment:** PARTIALLY CONFORMANT. The spec says "Resume with frontiers" but the code converts `BudgetExceeded` to a hard error in `apply_event`. There is no resumption mechanism. The spec's "Future Work" section (line 262) acknowledges "Budget resumption: Continue interrupted traversals" as future work, but the action column of the table says "Resume with frontiers" as if it is implemented.

---

## 2. Algorithm: Backward BFS

### 2.1 Initialization (Spec line 44)

**Spec:** "Two frontiers = {subject_head} and {other_head}"

**Code (comparison.rs:247-269, Comparison::new):**
```rust
subject_frontier: Frontier::new(subject_ids.clone()),
comparison_frontier: Frontier::new(comparison_ids.clone()),
```

**Assessment:** CONFORMANT.

### 2.2 Expand / Process (Spec lines 45-49)

**Spec:**
1. Fetch events at frontier positions
2. Remove from frontier, add parents
3. Track which frontier(s) have seen it
4. Accumulate in visited list

**Code (comparison.rs:272-297, step):** Collects all frontier IDs, calls `navigator.expand_frontier()`, processes events and assertions.

**Code (comparison.rs:299-383, process_event):**
- Lines 300-301: Removes from frontier
- Lines 304-309: Tracks visited list
- Lines 312-314: Marks seen from subject/comparison
- Lines 364-382: Extends frontiers with parents

**Assessment:** CONFORMANT. The process exactly matches the spec's described steps.

### 2.3 Termination Conditions (Spec lines 53-60)

| Spec Condition | Code Location | Match |
|---|---|---|
| `unseen_comparison_heads == 0` -> StrictDescends | comparison.rs:543-548 | YES |
| `unseen_subject_heads == 0` -> StrictAscends | comparison.rs:553-555 | YES |
| Frontiers empty + common ancestors -> DivergedSince | comparison.rs:558-601 | YES |
| Frontiers empty + different roots -> Disjoint | comparison.rs:564-574 | YES |
| Frontiers empty + no common, can't prove disjoint -> DivergedSince with empty meet | comparison.rs:576-583 | YES |
| Budget exhausted -> BudgetExceeded | comparison.rs:602-607 | YES |

**Assessment:** CONFORMANT. All six termination conditions are implemented.

### 2.4 Empty Clock Handling

**Spec (line 53, implicit):** Not explicitly specified.

**Code (comparison.rs:52-62):**
```rust
if subject.members().is_empty() || comparison.members().is_empty() {
    return Ok(AbstractCausalRelation::DivergedSince {
        meet: vec![], subject: vec![], other: vec![],
        subject_chain: vec![], other_chain: vec![],
    });
}
```

**Assessment:** AMBIGUOUS. The spec does not specify how empty clocks should be handled. The code returns `DivergedSince` with all-empty fields for ANY case where either clock is empty. This means:
- `compare(empty, empty)` returns `DivergedSince` (arguably should be `Equal`)
- `compare(non_empty, empty)` returns `DivergedSince` (arguably should be an error or special case)
- `compare(empty, non_empty)` returns `DivergedSince` (same)

The test at tests.rs:285-305 verifies this behavior, so it is intentional. But the spec is silent on this. The `entity.rs` create path handles the empty-clock case separately (line 224-238), so in practice, empty clocks only arise at entity creation.

---

## 3. Conflict Resolution: Layered Event Application

### 3.1 The Layer Model (Spec lines 68-76)

**Spec:**
```rust
struct EventLayer {
    already_applied: Vec<Event>,
    to_apply: Vec<Event>,
    events: Arc<BTreeMap<EventId, Event>>,
}
```

**Code (layers.rs:17-24):**
```rust
pub struct EventLayer<Id, E> {
    pub already_applied: Vec<E>,
    pub to_apply: Vec<E>,
    events: Arc<BTreeMap<Id, E>>,
}
```

**Assessment:** CONFORMANT. The code uses generic type parameters `<Id, E>` rather than concrete types, which is a superset of the spec's simplified notation. Field names and semantics match.

### 3.2 Resolution Process (Spec lines 81-85)

| Spec Step | Code Implementation | Match |
|---|---|---|
| 1. Navigate backward from both heads to find meet | entity.rs:251 `compare_unstored_event` | YES |
| 2. Accumulate full events via AccumulatingNavigator | entity.rs:250, `AccumulatingNavigator::new(getter)` | YES |
| 3. Compute layers via frontier expansion from meet | entity.rs:288, `compute_layers(...)` | YES |
| 4. Apply layers in causal order via `backend.apply_layer()` | entity.rs:301-317 | YES |

**Assessment:** CONFORMANT.

### 3.3 Layer Computation Algorithm (Spec implicit, layers.rs:82-99 doc comment)

**Spec (lines 68-69):** "Events are applied in layers -- sets of concurrent events at the same causal depth from the meet point."

**Code (layers.rs:100-154):** Uses frontier expansion from meet children. The algorithm:
1. Starts with children of meet events
2. Partitions into already_applied / to_apply by checking `current_head_ancestry`
3. Only creates a layer if there are `to_apply` events
4. Advances frontier to children whose parents are ALL processed

**Assessment:** CONFORMANT. The layering algorithm correctly groups events by causal depth from the meet point.

**Finding:** When a layer has `to_apply` events but no `already_applied` events and the only `to_apply` events don't touch a particular backend, that backend's `apply_layer` is still called (entity.rs:304-305 iterates all backends for every layer). The spec does not mention this, but it is harmless because `apply_layer` checks for relevant operations.

### 3.4 LWW Resolution Within Layer (Spec lines 99-106)

**Spec:**
1. Examine ALL events (already_applied + to_apply) plus stored last-write value
2. Per-property winner by `layer.compare(a, b)`:
   - Descends/Ascends decides winner
   - Concurrent falls back to lexicographic EventId
   - Missing DAG info is an error (bail out)
3. Only mutate state for winners from to_apply set
4. Track event_id per property

**Code (lww.rs:169-254):**

Step 1 -- Seeding from stored values (lww.rs:180-189):
```rust
for (prop, entry) in values.iter() {
    let Some(event_id) = entry.event_id() else { return Err(...) };
    winners.insert(prop.clone(), Candidate { value: entry.value(), event_id, from_to_apply: false });
}
```
Then iterates `already_applied` and `to_apply` events (lww.rs:193-231).

Step 2 -- Causal comparison (lww.rs:203-219):
```rust
let relation = layer.compare(&candidate.event_id, &current.event_id)...;
match relation {
    CausalRelation::Descends => { *current = candidate; }
    CausalRelation::Ascends => {}
    CausalRelation::Concurrent => {
        if candidate.event_id > current.event_id {
            *current = candidate;
        }
    }
}
```

Step 3 -- Only to_apply winners written (lww.rs:237-243):
```rust
if candidate.from_to_apply {
    values.insert(prop.clone(), ValueEntry::Committed { value: candidate.value, event_id: candidate.event_id });
}
```

Step 4 -- Event tracking via `ValueEntry::Committed { event_id }`.

**Assessment:** CONFORMANT. All four sub-steps match the spec.

### 3.5 Backend-Specific Behavior (Spec lines 126-128)

**Spec:**
- LWW: "Determine winner using causal comparison; only apply winners from to_apply"
- Yrs: "Apply all operations from to_apply; CRDT handles idempotency; already_applied ignored"

**Code:**
- LWW (lww.rs:169-254): Matches as analyzed above.
- Yrs (yrs.rs:186-209): Only iterates `layer.to_apply`, ignores `already_applied`.

**Assessment:** CONFORMANT.

---

## 4. Key Design Decisions: Spec vs Code

### 4.1 Empty Other-Chain Conservative Resolution (Spec lines 146-150)

**Spec:** "When other_chain is empty: keep current value if it has a tracked event_id."

**Code:** In the StrictAscends transformation (comparison.rs:142-148), `other_chain` is set to `vec![]`. In `apply_event` (entity.rs:277-329), when `DivergedSince` is received, layers are computed from the meet. If `other_chain` is empty, the `compute_layers` function will only find events on the subject side. The already-stored values (with event_ids) will seed the `winners` map in LWW, and since there are no `other` events in the layers, only `to_apply` events from the subject chain compete. The stored value has `from_to_apply: false` and wins if its event_id is lexicographically higher.

**Assessment:** CONFORMANT but the mechanism is **indirect**. The spec describes the conservative resolution as a direct property of "empty other_chain." In the code, it is an emergent behavior of the layering + LWW candidate competition. The stored value competes because it is seeded into `winners`, and since it is not `from_to_apply`, it will not be written even if it "wins" -- which effectively means the current value is kept. This is correct but the causal chain is non-obvious.

**IMPORTANT SUBTLETY:** When the stored value has a higher event_id than the incoming event, the stored value "wins" and nothing changes (correct). When the incoming event has a higher event_id, the incoming event wins and IS written because it is `from_to_apply: true`. This means the "conservative" behavior only applies when the current value's event_id is higher. The spec says "keep current value if it has a tracked event_id" which implies the current ALWAYS wins in this case. **The code does NOT match that claim** -- the incoming event can still win if it has a higher event_id.

### 4.2 TOCTOU Retry Pattern (Spec lines 153-168)

**Spec:**
```rust
for attempt in 0..MAX_RETRIES {
    let head = entity.head();
    let relation = compare(event, head).await;
    let mut state = entity.state.write();
    if state.head != head { continue; }
    // Apply under lock
    break;
}
```

**Code:**
- **StrictDescends (entity.rs:260-270):** Uses `try_mutate` which does lock + head check + body.
- **DivergedSince (entity.rs:291-327):** Manually acquires lock + checks head.

**Assessment:** PARTIALLY CONFORMANT. The spec shows a single, unified TOCTOU pattern. The code uses two different code paths: `try_mutate` for StrictDescends and manual lock acquisition for DivergedSince. Both achieve the same goal but through different mechanisms. The spec does not document this divergence.

### 4.3 Per-Property Event Tracking (Spec lines 172-182)

**Spec:**
```rust
struct ValueEntry {
    value: Option<Value>,
    event_id: EventId,
    committed: bool,
}
```

**Code (lww.rs:21-25):**
```rust
enum ValueEntry {
    Uncommitted { value: Option<Value> },
    Pending { value: Option<Value> },
    Committed { value: Option<Value>, event_id: EventId },
}
```

**Assessment:** DIVERGENT but equivalent. The spec shows a flat struct with `event_id` and `committed` fields. The code uses an enum with three variants. The semantics are the same: `Committed` has an event_id, `Uncommitted` and `Pending` do not. The spec's description of "Uncommitted local changes may exist in memory without an event_id, but they are never serialized" (line 182) is correct for the code.

---

## 5. API Reference: Spec vs Code

### 5.1 `compare` Function

**Spec (lines 197-203):**
```rust
pub async fn compare<N, C>(
    navigator: &N, subject: &C, comparison: &C, budget: usize,
) -> Result<AbstractCausalRelation<N::EID>, RetrievalError>
```

**Code (comparison.rs:41-46):**
```rust
pub async fn compare<N, C>(
    navigator: &N, subject: &C, comparison: &C, budget: usize,
) -> Result<AbstractCausalRelation<N::EID>, RetrievalError>
```

**Assessment:** CONFORMANT. Signatures match exactly.

### 5.2 `compare_unstored_event` Function

**Spec (lines 205-211):**
```rust
pub async fn compare_unstored_event<N, E>(
    navigator: &N, event: &E, comparison: &E::Parent, budget: usize,
) -> Result<AbstractCausalRelation<N::EID>, RetrievalError>
```

**Code (comparison.rs:81-87):**
```rust
pub async fn compare_unstored_event<N, E>(
    navigator: &N, event: &E, comparison: &E::Parent, budget: usize,
) -> Result<AbstractCausalRelation<N::EID>, RetrievalError>
```

**Assessment:** CONFORMANT.

### 5.3 `PropertyBackend::apply_layer` Method

**Spec (lines 224-228):**
```rust
fn apply_layer(
    &self,
    already_applied: &[&Event],
    to_apply: &[&Event],
) -> Result<(), MutationError>
```

**Code (backend/mod.rs:84):**
```rust
fn apply_layer(&self, layer: &EventLayer<EventId, Event>) -> Result<(), MutationError>;
```

**Assessment:** DIVERGENT. The spec shows `apply_layer` taking two slice parameters (`already_applied` and `to_apply`), but the actual code takes a single `&EventLayer<EventId, Event>` parameter. The `EventLayer` struct bundles `already_applied`, `to_apply`, and the DAG context (`events: Arc<BTreeMap<...>>`). The `EventLayer` approach is strictly superior because it also provides `layer.compare()` for intra-layer causal comparison. **The spec's API reference is outdated/simplified.**

### 5.4 `compute_layers` Function

**Spec (lines 235-239):**
```rust
fn compute_layers<'a>(
    events: &'a BTreeMap<EventId, Event>,
    meet: &[EventId],
    current_head_ancestry: &BTreeSet<EventId>,
) -> Vec<EventLayer<'a>>
```

**Code (layers.rs:100-104):**
```rust
pub fn compute_layers<Id, E>(
    events: &BTreeMap<Id, E>,
    meet: &[Id],
    current_head_ancestry: &BTreeSet<Id>,
) -> Vec<EventLayer<Id, E>>
```

**Assessment:** MOSTLY CONFORMANT. The code uses generic type parameters instead of concrete types. The spec shows a lifetime `'a` tied to the input reference, but the code clones the events into an `Arc` internally (layers.rs:107), so no lifetime parameter is needed. **The spec's lifetime annotation does not match the implementation.**

### 5.5 `apply_operations_with_event` Method

**Spec (lines 217-221):**
```rust
fn apply_operations_with_event(
    &self, operations: &[Operation], event_id: EventId,
) -> Result<(), MutationError>
```

**Code (backend/mod.rs:58-62):**
```rust
fn apply_operations_with_event(&self, operations: &[Operation], event_id: EventId) -> Result<(), MutationError> {
    let _ = event_id;
    self.apply_operations(operations)
}
```

**Assessment:** CONFORMANT. The signature matches; the default implementation (for CRDTs) ignores event_id. LWW overrides to track it (lww.rs:164-167).

---

## 6. Invariants: Spec Claims vs Code

### 6.1 No Regression (Spec line 187)

**Spec:** "Never skip required intermediate events"

**Code:** `StrictDescends` applies operations directly. `DivergedSince` applies via layers in causal order. Neither skips events.

**Assessment:** CONFORMANT (for apply_event). For `apply_state`, there is no event replay -- it replaces backend state wholesale. The spec does not distinguish apply_event vs apply_state here.

### 6.2 Single Head for Linear History (Spec line 188)

**Spec:** "Multi-heads only for true concurrency"

**Code:**
- `StrictDescends`: `state.head = new_head.clone()` (entity.rs:264) -- single event head.
- `DivergedSince`: Removes meet members, inserts event.id() (entity.rs:323-326).

**Assessment:** CONFORMANT for StrictDescends (head is replaced entirely). For DivergedSince, the head grows because only meet members are removed and the new event is added. This correctly produces multi-heads only for true concurrency.

### 6.3 Deterministic Convergence (Spec line 189)

**Spec:** "Same events -> same final state"

**Code:** LWW uses `BTreeMap` for ordered iteration (deterministic) and lexicographic EventId for tiebreaking. `compute_layers` uses `BTreeSet` for frontiers (deterministic iteration order).

**Assessment:** CONFORMANT. BTreeMap/BTreeSet usage ensures deterministic ordering. Lexicographic EventId comparison is a total order. Tests in determinism_tests confirm this.

### 6.4 Idempotent Application (Spec line 190)

**Spec:** "Redundant delivery is a no-op"

**Code (comparison.rs:95-97):**
```rust
if comparison.members().contains(&event.id()) {
    return Ok(AbstractCausalRelation::Equal);
}
```

And (entity.rs:252-254):
```rust
AbstractCausalRelation::Equal => {
    return Ok(false);
}
```

**Assessment:** PARTIALLY CONFORMANT. Redundant delivery of an event whose ID is **at the head** is idempotent (returns Equal, no-op). However, redundant delivery of an event whose ID is **in history but not at the head** will return `DivergedSince` (as tested in test_event_in_history_not_at_head, tests.rs:1116-1144), which would re-apply layers. For LWW, this may not cause data corruption (the same event_id competing again should produce the same winner), but it is not a clean no-op. The spec's claim of "redundant delivery is a no-op" is not fully accurate for events in history.

---

## 7. Undocumented Behaviors (Code Does, Spec Does Not Describe)

### 7.1 Entity Creation Path

The `apply_event` method (entity.rs:224-238) has a special fast path for entity creation events (`event.is_entity_create()`). When the entity's head is empty, it directly applies operations and sets head, bypassing the comparison algorithm entirely. The spec does not mention this.

### 7.2 Assertion-Based Shortcuts

The comparison algorithm supports assertion-based shortcuts (comparison.rs:385-443) via `process_assertion`, with types `Descends`, `NotDescends`, `PartiallyDescends`, and `Incomparable`. The spec mentions assertions briefly in "Future Work" (line 260) but the assertion infrastructure is fully implemented in the code, including frontier tainting, assertion chaining, and taint-based result determination (comparison.rs:490-538).

### 7.3 `apply_state` Method

The entity has a separate `apply_state` method (entity.rs:352-428) that handles state snapshots. For `DivergedSince`, it returns `Ok(false)` without applying (entity.rs:395-408), with a comment explaining that events are needed for proper merge. The spec does not document `apply_state` at all.

### 7.4 Backend Creation During Layer Application

In entity.rs:308-317, during layer application, if a `to_apply` event references a backend that does not yet exist on the entity, the code creates the backend and applies the layer to it. The spec does not describe this on-demand backend creation.

### 7.5 `compute_ancestry` Function

The `compute_ancestry` function (layers.rs:172-199) performs a backward BFS from the head to compute the full ancestry set. This is used in `apply_event` (entity.rs:285) to partition events into already_applied vs to_apply. The spec mentions `current_head_ancestry` as a parameter to `compute_layers` but does not describe how it is computed.

### 7.6 `EventLayer::compare` Returns `Descends` for Identity

`layer.compare(a, a)` returns `CausalRelation::Descends` (layers.rs:58-59), not a separate `Equal` variant. This is not mentioned in the spec and creates a semantic ambiguity -- `Descends` for `a == b` means "a is itself," which is technically correct (reflexive) but non-obvious. The `CausalRelation` enum does not have an `Equal` variant.

---

## 8. Missing Behaviors (Spec Promises, Code Does Not Implement)

### 8.1 Budget Resumption

**Spec (line 22):** Action column says "Resume with frontiers."
**Spec (line 262):** Future Work section says "Budget resumption: Continue interrupted traversals."

**Code:** `BudgetExceeded` is converted to a hard error in both `apply_event` (entity.rs:334-341) and `apply_state` (entity.rs:414-422). No resumption mechanism exists.

**Impact:** The action column is misleading. The code returns frontiers in the `BudgetExceeded` variant, but no caller uses them for resumption.

### 8.2 Depth Precedence Resolution

**Spec (line 28, line 68):** "Compute per-property **depths** for DivergedSince resolution." Also: "events at the same **causal depth** from the meet point."

The spec's LWW resolution test `test_deeper_branch_wins` (lww_resolution.rs:15-76) has a comment "D's value wins because depth 2 > depth 1."

**Code:** The LWW `apply_layer` (lww.rs:169-254) does NOT compare depths. It compares event IDs using `layer.compare()` which checks causal ancestry (Descends/Ascends/Concurrent), and for Concurrent events uses lexicographic EventId as tiebreaker. The concept of "depth" is expressed through the layering: events at deeper layers are applied AFTER shallower layers, so a deeper event's value naturally overwrites a shallower event's value because the deeper layer is applied later and the deeper event becomes the stored value that seeds the next layer's competition.

**Assessment:** The spec's language of "depth precedence" is accurate in terms of outcome (deeper values tend to win) but misleading about mechanism. The code does not count or compare depths numerically. Instead, the layered application order ensures deeper events are evaluated last, and because they causally descend from shallower events on the same branch, `layer.compare` returns `Descends`. The spec should clarify that "depth precedence" is an emergent property of causal ordering within layers, not a direct comparison.

---

## 9. Test Coverage vs Spec Claims

### 9.1 Claims Tested

| Spec Claim | Unit Test | Integration Test |
|---|---|---|
| Forward chains in causal order | test_forward_chain_ordering, test_diverged_chains_ordering | N/A |
| Multi-head StrictAscends transform | test_multihead_event_extends_one_tip, test_multihead_three_way_concurrency | test_concurrent_transactions_same_entity |
| Lexicographic tiebreak | test_apply_layer_higher_event_id_wins, test_two_event_determinism | test_lexicographic_tiebreak |
| Per-property resolution | test_apply_layer_multiple_properties | test_per_property_concurrent_writes |
| Layer computation | test_simple_two_branch, test_multi_layer, test_diamond_reconvergence, test_three_way_concurrency | N/A |
| Yrs CRDT idempotency | test_yrs_apply_layer_ignores_already_applied, test_yrs_apply_layer_order_independent | N/A |
| Disjoint detection | test_incomparable | N/A |
| Redundant delivery | test_same_event_redundant_delivery, test_compare_event_redundant_delivery | N/A |
| Deterministic convergence | test_two_event_determinism, test_three_event_determinism | test_two_event_determinism_same_property |
| Budget exceeded | test_budget_exceeded | N/A |
| Deep diamond concurrency | test_deep_diamond_asymmetric_branches | test_deep_diamond_determinism |

### 9.2 Claims NOT Tested

| Spec Claim | Gap |
|---|---|
| "Idempotent application" for events in history (not at head) | No test for re-delivery of an already-applied event that is not the head. test_event_in_history_not_at_head shows it returns DivergedSince, not no-op. |
| BudgetExceeded during `apply_event` | Budget is hardcoded to 100; no test triggers it through `apply_event`. |
| TOCTOU retry exhaustion | No test for `TOCTOUAttemptsExhausted` error. |
| Disjoint in `apply_event` | No test applies a disjoint event through the full `apply_event` path. |
| `InsufficientCausalInfo` error | No test triggers the missing DAG info error in LWW resolution. |
| `apply_state` with DivergedSince | The test `test_apply_state_diverged_since` does not actually test `apply_state` with a DivergedSince -- it tests `apply_event` paths. |
| Empty other_chain conservative resolution | No isolated test verifies that the stored value wins when other_chain is empty AND the stored value has a higher event_id. |
| Multi-meet node chain trimming | No test has a meet set with multiple nodes. |

---

## 10. Ambiguities Resolved by Code Without Documentation

### 10.1 What Happens When `state.head.remove(parent_id)` Returns False?

In entity.rs:323-324, the code removes meet members from the head. If a meet member is NOT in the head (e.g., the meet is deep in history), `remove` returns false and nothing happens. This is correct but undocumented. No assertion or log captures this case.

### 10.2 Which Events Participate in `compute_ancestry`?

In entity.rs:285, `compute_ancestry` is called with events from the `AccumulatingNavigator`. But these events are only those traversed during the BFS comparison -- they may not include ALL events between the meet and the head. If the BFS terminates early (e.g., StrictDescends detected before full traversal), some events may be missing. However, this code path is only reached for `DivergedSince`, where the BFS explores both branches fully.

### 10.3 Ordering Within a Layer

The spec says events within a layer are "mutually concurrent" (line 68). However, `layer.compare()` can return `Descends`/`Ascends` for events within a layer if they are on the same branch. This happens because `compute_layers` partitions by causal depth from the meet, but events at the same depth on the same branch ARE causally related. The `layer.compare()` correctly handles this case, but the spec's claim of "mutually concurrent" is an oversimplification.

### 10.4 `is_descendant` Strict vs Reflexive

In layers.rs:207-209, `is_descendant(descendant, ancestor)` returns `false` when `descendant == ancestor`. This means it tests STRICT descendancy. The `layer.compare` method handles the `a == b` case separately (returns `Descends`). This is a semantic choice: identity is treated as `Descends` rather than having a separate `Equal` variant. Not documented in the spec.

---

## Confidence-Rated Findings

| # | Finding | Severity | Confidence | Details |
|---|---|---|---|---|
| 1 | **Spec API for `apply_layer` does not match code.** Spec shows two slice parameters; code uses `&EventLayer` which bundles data + DAG context for `layer.compare()`. | Medium | High | Spec lines 224-228 vs backend/mod.rs:84. The spec's simplified signature omits the DAG context, which is essential for intra-layer causal comparison. The spec should be updated to show the actual `EventLayer` parameter. |
| 2 | **Spec claims "Resume with frontiers" for BudgetExceeded, but code converts it to a hard error.** No resumption mechanism exists. | Medium | High | Spec line 22 action column says "Resume with frontiers." Code at entity.rs:334-341 returns `Err(LineageError::BudgetExceeded{...})`. The action column should say "Error (frontiers available for future resumption)" or similar. |
| 3 | **Spec's "empty other_chain means current value wins" is not fully accurate.** Code: incoming event wins if its EventId > stored value's EventId. | High | High | Spec line 149: "Conservative choice: keep current value if it has a tracked event_id." Code at lww.rs:209-219: standard candidate competition applies -- incoming event wins when `candidate.event_id > current.event_id`. The "conservative" behavior only holds when the stored value has a higher EventId. The spec overstates the conservatism. |
| 4 | **Spec claims "Idempotent application: Redundant delivery is a no-op" but code does not fully guarantee this.** Re-delivery of a historical (non-head) event returns DivergedSince and re-applies layers. | Medium | High | Spec line 190. Tests.rs:1116-1144 (`test_event_in_history_not_at_head`) confirms DivergedSince for historical event. For LWW the re-application may be benign but is not a no-op. The spec should note that idempotency requires the caller to check if the event is already stored. |
| 5 | **Spec `compute_layers` signature shows lifetime parameter `'a` that does not exist in code.** Code clones events into `Arc`, eliminating the lifetime. | Low | High | Spec line 235-239 vs layers.rs:100-104. Minor documentation issue. |
| 6 | **Spec `ValueEntry` shown as a struct; code uses an enum with three variants.** | Low | High | Spec lines 174-179 vs lww.rs:21-25. Semantically equivalent but structurally different. The spec should show the actual enum or note the simplification. |
| 7 | **Undocumented entity creation fast path in `apply_event`.** | Low | High | entity.rs:224-238 bypasses comparison for `is_entity_create()` events when head is empty. The spec should document this first-event special case. |
| 8 | **Undocumented `apply_state` method and its `DivergedSince` behavior.** | Medium | High | entity.rs:352-428. `apply_state` returns `Ok(false)` for diverged states without applying, deferring to event-based resolution. The spec does not mention `apply_state` at all, but it is a key part of the system's sync protocol. |
| 9 | **Spec claims events within a layer are "mutually concurrent" but code's `layer.compare()` can return Descends/Ascends for intra-layer events.** | Medium | Medium | Spec line 68 vs layers.rs:57-70. Events at the same causal depth from the meet can still be on the same branch (causally related). The `layer.compare()` correctly handles this, but the spec's description is imprecise. This mainly affects LWW resolution where a causally-later event on the same branch always wins over an earlier one. |
| 10 | **DivergedSince head update removes `meet` members from head, but meet can contain IDs not in the current head.** No assertion or logging for this case. | Low | Medium | entity.rs:323-326. When meet is deep in history (not at any head tip), the `remove` is a no-op. This is safe but could mask subtle DAG topology bugs. The holistic review also noted this. |
| 11 | **Assertion-based shortcuts are fully implemented but only mentioned as "Future Work" in the spec.** | Medium | High | Spec line 260 vs comparison.rs:385-443, frontier.rs:19-34, navigator.rs:60-71. The assertion infrastructure (taint, PartiallyDescends, Incomparable, chaining) is production-ready with 10+ tests. The spec should promote this from "Future Work" to a documented feature. |
| 12 | **`layer.compare(a, a)` returns `Descends` rather than a dedicated `Equal`.** | Low | High | layers.rs:58-59. `CausalRelation` enum has no `Equal` variant. This is technically correct (reflexive descent) and does not cause bugs, but could confuse API consumers. The spec does not mention this choice. |
| 13 | **No test for multi-meet chain trimming.** The `build_forward_chain` logic (comparison.rs:447-460) only finds the first meet member in the reversed visited list. For multi-member meet sets, events between meet members may remain in the chain. | Medium | Medium | comparison.rs:453 uses `chain.iter().position(|id| meet.contains(id))` which stops at the first match. This is correct ONLY if meet members do not appear in the visited list (they should be boundary nodes), but no test verifies this assumption for multi-meet scenarios. |
| 14 | **DivergedSince path uses manual lock + head check instead of `try_mutate`.** | Low | High | entity.rs:291-298 vs entity.rs:260-266. Both achieve the same goal but through different mechanisms. If `try_mutate` is enhanced (e.g., with metrics), the DivergedSince path will not benefit. |
| 15 | **Missing test coverage for `InsufficientCausalInfo`, `TOCTOUAttemptsExhausted`, `BudgetExceeded` during `apply_event`, and `Disjoint` in `apply_event`.** | Medium | High | These are error paths documented in the spec or error enum but have no tests exercising them through the full `apply_event` pathway. |
| 16 | **`compare(empty, empty)` returns `DivergedSince` with empty fields rather than `Equal`.** | Low | Medium | comparison.rs:53-62. Two empty clocks are arguably "equal" (both represent no history). Returning `DivergedSince` with all-empty fields is functional but semantically questionable. The spec does not specify this behavior. Entity creation handles this case separately, so it is unlikely to surface as a bug. |
| 17 | **Spec example (lines 109-123) shows layer assignment but does not specify what happens when layers have only `already_applied` and no `to_apply`.** | Low | High | Code at layers.rs:130: "Only create layer if there's something to apply." Layers with only `already_applied` events are silently skipped. This is correct behavior but not documented in the spec. |
