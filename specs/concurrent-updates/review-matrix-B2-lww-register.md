# Deep Review: LWW Property Backend and Layer Comparison Logic

**Reviewer:** CRDT/Replication Specialist (Matrix B2)
**Scope:** LWW register implementation with causal awareness -- `lww.rs`, `layers.rs`, `relation.rs`, `entity.rs`, `comparison.rs`
**Branch:** `concurrent-updates-event-dag`

---

## 1. ValueEntry State Machine: Committed/Pending/Uncommitted

### State Transitions

The `ValueEntry` enum in `/Users/daniel/ak/ankurah-201/core/src/property/backend/lww.rs` lines 21-25 defines three states:

```rust
enum ValueEntry {
    Uncommitted { value: Option<Value> },
    Pending { value: Option<Value> },
    Committed { value: Option<Value>, event_id: EventId },
}
```

**Transition graph:**

1. `set()` (line 71) -> `Uncommitted`
2. `to_operations()` (line 147) -> `Uncommitted` becomes `Pending`
3. `apply_operations_with_event()` (line 164) -> creates `Committed` (overwriting whatever was there)
4. `apply_layer()` (line 239) -> creates `Committed` directly (for winners from `to_apply`)
5. `from_state_buffer()` (line 131) -> creates `Committed` from deserialization

**Assessment: The transitions are correct for the intended lifecycle.** A local edit creates `Uncommitted`, `to_operations()` atomically transitions it to `Pending` (indicating "ops have been collected, waiting for event ID"), and `apply_operations_with_event()` stamps it as `Committed` with the event ID from the committed event.

### Can States Get Stuck?

**Pending can get stuck if the commit fails.** After `to_operations()` is called, values transition from `Uncommitted` to `Pending`. If the transaction subsequently fails to commit (e.g., the server rejects it, the node crashes), those `Pending` values persist in the backend with no `event_id` and no path back to `Uncommitted`. They cannot be serialized via `to_state_buffer()` (which requires `event_id`, lines 117-122) and they cannot participate in LWW resolution via `apply_layer()` (which seeds from stored values requiring `event_id`, lines 183-186).

However, in practice this is unlikely to be problematic: the `LWWBackend` held by a `Transacted` entity snapshot is discarded if the transaction fails. The snapshot is a fork (line 86-96 of `lww.rs`), and its lifetime is tied to the transaction. But if the fork is somehow retained or if `to_operations()` is called on a non-transaction backend, this becomes a real issue.

**More concerning:** `to_operations()` (line 137-157) takes a `write()` lock and mutates entries from `Uncommitted` to `Pending` in-place as a side effect of reading. If called twice without an intervening commit, the second call returns `Ok(None)` (no `Uncommitted` entries remain) -- which is correct behavior. But there is no mechanism to roll back `Pending` to `Uncommitted` if needed. A `rollback()` method or explicit state transition on failure would be more robust.

---

## 2. Per-Property Resolution in DivergedSince

### Architecture

When `DivergedSince` is detected, `entity.rs` (lines 277-327) computes layers and applies them in causal order. Each layer is passed to `backend.apply_layer()`. For LWW, the resolution logic is in `lww.rs` lines 169-254.

### How the LWW Resolver Picks Winners Per Property

The resolution algorithm in `apply_layer()`:

1. **Seed** (lines 180-189): Read the current stored values, each with an `event_id`. These become initial candidates in `winners: BTreeMap<PropertyName, Candidate>`.

2. **Compete** (lines 193-231): For each event in `already_applied` + `to_apply`, deserialize its LWW operations, and for each property:
   - If there is an existing candidate for that property in `winners`, call `layer.compare(candidate_event_id, current_winner_event_id)`.
   - If `Descends`: new candidate replaces the winner.
   - If `Ascends`: keep current winner.
   - If `Concurrent`: lexicographic EventId tiebreak (`candidate.event_id > current.event_id`).
   - If no existing candidate: insert directly.

3. **Mutate** (lines 234-243): Only apply values where the winner came from `to_apply`.

**Assessment: This is fundamentally correct for per-property resolution within a single layer.** Each property independently competes, and the winner is tracked per-property. Different properties can have different winners from different events, which is the correct LWW-per-field semantic.

### Cross-Layer Accumulation

Critically, `apply_layer()` is called once per layer in causal order (entity.rs lines 301-317). The `winners` BTreeMap is scoped to a single `apply_layer()` call, not accumulated across layers. This is correct because each layer's stored values already reflect the winners from previous layers (the backend state is mutated in place). The "seed" step re-reads the current backend state at the start of each layer, which includes mutations from previous layers.

**Potential issue with the seed step:** The seed reads from `self.values` (the backend's stored state). For the first layer, this contains the pre-merge state. For subsequent layers, it contains the state after applying previous layers' winners. This is correct, but there is a subtle interaction: if a `to_apply` event in Layer 1 writes property X, and Layer 2's `already_applied` also writes property X, the seed in Layer 2 will show property X with Layer 1's winner's event_id. Layer 2's `already_applied` event will then compete against this, and the `compare()` will determine causality correctly (Layer 2's event descends from Layer 1's event, so Layer 2's `already_applied` event should win and become the new stored value... but wait, `already_applied` winners are NOT written to state). This is a problem I will address in Finding #1.

---

## 3. Depth-Based Precedence Trace-Through

The system does NOT use an explicit depth counter. Instead, "further from meet wins" is determined by the causal structure within layers. Let me trace through the four cases using the actual code paths.

### Case 1: Property written at depth 3 on branch X, depth 20 on branch Y

The property was last written on branch X at layer 3, and on branch Y at layer 20. Events are organized into layers by `compute_layers()`. The layer where the property was written on X (layer 3) will be processed before the layer where it was written on Y (layer 20).

- **Layer 3**: X's event (in one of `already_applied`/`to_apply`) sets the property. Y's branch has no event at this layer for this property. The winner at layer 3 is X's write.
- **Layers 4-19**: Neither branch writes this property. Layers may not even be created if no events touch this property.
- **Layer 20**: Y's event writes this property. The seed now contains X's value from layer 3. `layer.compare(Y_event, X_event)` looks up both events in the DAG context. **Y's event does NOT descend from X's event** -- they are on different branches. They are `Concurrent`. So the tiebreak is lexicographic by EventId. This means Y does NOT necessarily win!

**CRITICAL FINDING:** The spec and integration tests describe "deeper branch wins" behavior, but the actual code does not implement depth-based precedence. The LWW resolution within each layer uses causal comparison (descends/ascends/concurrent), not depth. Events at different layers on different branches are concurrent (no causal relationship), so they fall through to lexicographic EventId tiebreak. The "deeper branch wins" illusion only holds when the deeper event causally descends from the shallower one (i.e., they share a linear chain), which is true for sequential writes on a single branch but NOT for writes on different concurrent branches.

Wait -- let me re-examine. Layers are computed globally, not per-branch. At layer 20, the seed value for property X has `event_id` set to the layer-3 writer from branch X. The layer-20 event from Y is being compared against this seed. But `layer.compare()` operates on the EventLayer's `events` map, which contains ALL events. The layer-3 event from branch X and the layer-20 event from branch Y are indeed concurrent in the DAG (neither descends from the other). So the comparison returns `Concurrent`, and the winner is determined by lexicographic EventId.

**This means "deeper branch wins" is NOT a property of the system.** The actual semantic is: within a single layer, causal descendants beat ancestors, and concurrent events are resolved by lexicographic EventId. Across layers, the winner from the previous layer's stored state competes against the current layer's events, and since cross-branch events are concurrent, the tiebreak is always lexicographic.

**However**, let me re-read the test `test_deeper_branch_wins` (lww_resolution.rs). In that test:
- A is genesis
- B sets title at depth 1 (branch 1)
- C sets title at depth 1 (branch 2)
- D merges B and C, sets title at depth 2

D's parent is [B, C], meaning D causally descends from both B and C. So `compare(D, B)` returns `Descends`, and D wins. This is correct but it is because D is a merge event, not because of depth per se. The test name is misleading -- it actually tests that a merge event beats its ancestors.

**What about the case where D is NOT a merge event?** If D has parent [C] only (extending branch 2 but not branch 1), then:
- Layer 1: B and C compete. Winner is the one with higher EventId (concurrent).
- Layer 2: D competes against Layer 1's winner.
  - If Layer 1's winner was C (D's ancestor): `compare(D, C)` = `Descends`, D wins. Correct.
  - If Layer 1's winner was B (not D's ancestor): `compare(D, B)` = `Concurrent`, lexicographic tiebreak. D may or may not win.

**This means depth-based precedence is NOT guaranteed when the deeper write is on a different branch from the layer-1 winner.**

### Case 2: Property written on only one branch (empty other chain)

In `compare_unstored_event()`, the `StrictAscends` transformation produces `DivergedSince` with `other_chain: vec![]` (comparison.rs lines 142-148). When `compute_layers()` is called with `meet` and events accumulated during traversal, the `other_chain` being empty means no events from the other branch are in the accumulated events map (except possibly the head itself). The layers will contain the incoming event in `to_apply` and the head's event(s) in `already_applied`.

In `apply_layer()`, the stored value (from the current state) will seed as a candidate. The incoming event's value will compete. `layer.compare()` between the incoming event and the stored event returns `Concurrent` (different branches, no causal relationship). Winner is lexicographic.

**Assessment: The writer does NOT necessarily win.** The stored value may beat the incoming event if its EventId is lexicographically higher. The holistic review's claim that "empty other_chain means current wins" is not accurate -- it depends on EventId comparison. The stored value participates as a seed candidate, and the incoming event competes normally.

### Case 3: Property written at equal depth on both branches

Both events are in the same layer. `layer.compare()` returns `Concurrent`. Lexicographic EventId tiebreak. **Correct.**

### Case 4: Property written multiple times on one branch

On a single branch, writes at different depths end up in different layers. Only the latest matters because each subsequent layer's write causally descends from the previous one (`compare(later, earlier)` = `Descends`), so the later write replaces the earlier. At the final layer where the property was last written, that value competes against the other branch. **Correct -- only the latest matters for each branch.**

---

## 4. Cross-Replica Convergence

### The Critical Question

If replica R1 sees events [A, B, C] and R2 sees [C, A, B], do they converge?

Let me trace through the actual resolution code for a concrete scenario:

**Setup:**
- Genesis event G
- Event A: parent [G], sets title="A"
- Event B: parent [G], sets title="B"
- Event C: parent [G], sets title="C"
- All three are concurrent (same parent)

**Replica R1 applies A, then B, then C:**

1. Apply A: `compare_unstored_event(A, [G])` -> `StrictDescends`. Apply directly. Head becomes [A]. Backend: title="A" with event_id=A_id.

2. Apply B: `compare_unstored_event(B, [A])` -> B's parent is [G], compare([G], [A]). [G] is strictly before [A] (StrictAscends). Transform to `DivergedSince { meet: [G], other: [A], subject_chain: [B], other_chain: [] }`.
   - `compute_ancestry(events, [A])` includes {G, A}.
   - `compute_layers(events, [G], {G, A})` -> Layer 1: already_applied=[A], to_apply=[B].
   - `apply_layer(Layer 1)`: Seed with stored title="A" (event_id=A_id). Candidate B. `compare(B_id, A_id)` -- both A and B have parent [G], so they are concurrent. Winner: max(A_id, B_id) by lexicographic comparison.
   - Head becomes [A, B] (meet [G] is removed from head [A], event B added: head = [B]... wait, let me re-read).

   Actually, entity.rs lines 320-326:
   ```rust
   for parent_id in &meet {
       state.head.remove(parent_id);
   }
   state.head.insert(event.id());
   ```
   Meet is [G]. Head is [A]. Remove G from head [A] -- G is not in head, so no-op. Insert B. Head becomes [A, B]. **Correct.**

3. Apply C: `compare_unstored_event(C, [A, B])` -> C's parent is [G], compare([G], [A, B]). [G] is StrictAscends (before both A and B). Transform to `DivergedSince { meet: [G], other: [A, B], subject_chain: [C], other_chain: [] }`.
   - Accumulated events from BFS: {G, A, B} (from traversal).
   - After inserting C: events = {G, A, B, C}.
   - `compute_ancestry(events, [A, B])` = {G, A, B}.
   - `compute_layers(events, [G], {G, A, B})` -> Layer 1: children of G are {A, B, C}. A and B are in ancestry (already_applied), C is not (to_apply). Layer 1: already_applied=[A, B], to_apply=[C].
   - `apply_layer(Layer 1)`: Seed with stored title (winner of A vs B from step 2, event_id=max(A_id, B_id)). Candidate C competes. `compare(C_id, stored_event_id)` -> Concurrent. Winner: max(C_id, stored_event_id) = max(A_id, B_id, C_id).
   - **Final state: title = value from event with max(A_id, B_id, C_id).**

**Replica R2 applies C, then A, then B:**

1. Apply C: `compare_unstored_event(C, [G])` -> `StrictDescends`. Apply directly. Head becomes [C]. Backend: title="C" with event_id=C_id.

2. Apply A: `compare_unstored_event(A, [C])` -> A's parent is [G], compare([G], [C]). StrictAscends. Transform to `DivergedSince { meet: [G], other: [C], subject_chain: [A], other_chain: [] }`.
   - Accumulated events: {G, C}. After inserting A: {G, A, C}.
   - `compute_ancestry(events, [C])` = {G, C}.
   - `compute_layers(events, [G], {G, C})` -> Layer 1: children of G are {A, C}. C is in ancestry (already_applied), A is not (to_apply). Layer 1: already_applied=[C], to_apply=[A].
   - `apply_layer(Layer 1)`: Seed with stored title="C" (event_id=C_id). Candidate A. `compare(A_id, C_id)` -> Concurrent. Winner: max(A_id, C_id).
   - Head becomes [C, A] (remove G from [C], no-op; insert A).

3. Apply B: `compare_unstored_event(B, [C, A])` -> B's parent is [G], compare([G], [C, A]). StrictAscends. Transform to `DivergedSince { meet: [G], other: [C, A], subject_chain: [B], other_chain: [] }`.
   - Accumulated events: {G, A, C} (from BFS to find [G]). After inserting B: {G, A, B, C}.
   - `compute_ancestry(events, [C, A])` = {G, A, C}.
   - `compute_layers(events, [G], {G, A, C})` -> Layer 1: children of G are {A, B, C}. A and C are in ancestry (already_applied), B is not (to_apply). Layer 1: already_applied=[A, C], to_apply=[B].
   - `apply_layer(Layer 1)`: Seed with stored title (winner of step 2, event_id=max(A_id, C_id)). Candidate B. `compare(B_id, max(A_id, C_id))` -> Concurrent. Winner: max(B_id, max(A_id, C_id)) = max(A_id, B_id, C_id).
   - **Final state: title = value from event with max(A_id, B_id, C_id).**

**Both replicas converge to the same final value.** The convergence guarantee holds because:
1. All events end up in the same layer (they are all children of the same meet point).
2. Within a layer, the competition is pairwise with lexicographic tiebreak for concurrent events.
3. The max() function over EventIds is commutative and associative.

**IMPORTANT SUBTLETY:** The convergence relies on the stored seed's `event_id` being updated correctly after each layer application. If the seed still showed the original stored value's event_id instead of the layer-competition winner's event_id, the pairwise competition in subsequent steps could yield different results. Let me verify: after `apply_layer()` line 239, the winner's entry is `ValueEntry::Committed { value, event_id }` written to `self.values`. The next `apply_layer()` call re-reads `self.values` in the seed step. So the event_id IS updated. **Convergence is maintained.**

### BUT: There is a subtle convergence problem with the seed step

Consider a scenario where the stored value's event_id refers to an event that is NOT in the layer's `events` map (because the layer's `events` were accumulated during a BFS that started from different heads on different replicas). In this case, `layer.compare(candidate_event_id, stored_event_id)` will fail with `InsufficientCausalInfo`. This is correctly handled as an error (line 203-207).

But this raises the question: **is the stored value's event_id always present in the layer's events map?** The `events` map is populated by the `AccumulatingNavigator` during BFS traversal, which explores from the event's parent and the entity's head backward to the meet point. Events that set the stored value were applied in the past, so their event_ids should be in the head's ancestry. The BFS traversal accumulates events along the way, so these events should be present. However, if the stored value was set by an event that is NOT in the ancestry path traversed during BFS (e.g., if the stored event is deeper than the meet point), it may not be in the accumulated events.

Let me think about this more carefully. The stored value's `event_id` refers to the event that last set that property. If that event is an ancestor of the current head, and the meet point is the GCA of the head and the incoming event's parent, then the stored event is either:
- At or above the meet point: NOT in the accumulated events (BFS stops at meet).
- Below the meet point but above the head: IN the accumulated events.

If the stored event is above the meet point, its event_id is NOT in the `events` map. The `layer.compare()` call will fail with `InsufficientCausalInfo`. This is a real problem.

**Wait** -- let me re-examine. The seed step only runs for `Committed` values (those with an `event_id`). If the property was last set by an event above the meet point, its `event_id` is in the ancestry but NOT in the accumulated events. The comparison will fail. But the question is: does this actually happen?

Consider: Head is [H] via chain A->B->C->D->E->F->G->H. Property X was set at event B. Incoming event Z has parent [D] (branching at D). Meet is [D]. The accumulated events from BFS are {D, E, F, G, H, Z} (roughly -- depends on BFS traversal). Event B is NOT in this set. The seed has event_id = B_id. If Z also sets property X, `layer.compare(Z_id, B_id)` will look up B in the events map and fail.

**This is a real bug.** The `events` map only contains events between the meet and the heads, but the stored value's event_id could refer to an event above the meet point. See Finding #2.

---

## 5. Layer Comparison API in layers.rs

### EventLayer::compare() (lines 57-70)

```rust
pub fn compare(&self, a: &Id, b: &Id) -> Result<CausalRelation, RetrievalError> {
    if a == b {
        return Ok(CausalRelation::Descends);
    }
    if is_descendant(&self.events, a, b)? {
        return Ok(CausalRelation::Descends);
    }
    if is_descendant(&self.events, b, a)? {
        return Ok(CausalRelation::Ascends);
    }
    Ok(CausalRelation::Concurrent)
}
```

**Semantic issue:** `a == b` returns `Descends`. This is mathematically questionable -- equality is typically `a <= b AND b <= a`, not strict descent. However, in the context of LWW resolution, this is harmless: if two candidates reference the same event, the "winner" is the same event either way. The value and event_id are identical, so the result is correct.

### is_descendant() (lines 201-230)

This performs a DFS from `descendant` backward toward `ancestor` using parent links. It only uses events in the `events` map. As discussed in section 4, this map may not contain events above the meet point, leading to `RetrievalError` when a parent is missing.

**Performance concern:** `is_descendant()` is called twice per candidate-vs-winner comparison (once for each direction). For N candidates in a layer, this is O(N) comparisons, each potentially traversing the full DAG subset. The DFS visits all events reachable from the candidate, which in the worst case is the entire `events` map. Total cost per layer: O(N^2 * |events|).

### compute_layers() (lines 100-154)

The algorithm is a frontier expansion from the meet point:
1. Start with children of meet
2. Partition into already_applied / to_apply based on ancestry
3. Advance frontier to children whose parents are all processed

**Correctness:** The algorithm correctly computes layers in topological order. The `processed` set ensures each event appears in exactly one layer. The "all parents processed" check (line 145) ensures events with multiple parents (merge events) are placed in the correct layer.

**children_of() performance (lines 159-166):** Linear scan over all events for each call. Called once per frontier event per layer iteration. Total cost: O(L * F * |events|) where L = number of layers, F = average frontier size. For large DAGs with many layers, this is O(|events|^2). This is a known concern from the holistic review.

### compute_ancestry() (lines 172-199)

Backward BFS from head to collect all ancestors. Correct and efficient (O(|events|) with the visited set).

---

## 6. LWW / Yrs Backend Consistency

### LWW apply_layer() (lww.rs lines 169-254)

- Seeds from stored state
- Competes all events using causal comparison
- Only mutates for `to_apply` winners

### Yrs apply_layer() (yrs.rs lines 186-209)

- Ignores `already_applied` completely
- Applies all `to_apply` operations
- Relies on Yrs CRDT internal idempotency

**Assessment: The two backends are handled consistently with respect to the contract.** The key design insight is correct: LWW needs the full comparison context (already_applied + to_apply + stored state) to determine winners, while Yrs only needs to_apply because CRDTs handle idempotency internally.

**Potential issue with Yrs:** If the same Yrs update is applied twice (once via `apply_operations_with_event` during initial linear application, and again via `apply_layer` during DivergedSince resolution), the Yrs CRDT should handle this via its internal state vector. However, looking at `apply_layer` in yrs.rs: it unconditionally applies all `to_apply` operations via `apply_update()`. If these operations were already applied during an earlier `apply_operations_with_event()` call, Yrs's internal deduplication should prevent double-application. This is correct because Yrs updates carry client IDs and logical clocks.

But there is a subtle issue in the entity.rs DivergedSince path: `apply_layer()` is called for ALL backends for EVERY layer (line 304). Each backend receives layers that may contain events with operations for OTHER backends. The backend checks `event.operations.get(&Self::property_backend_name())` (lww.rs line 194, yrs.rs line 193) and skips events with no relevant operations. This is correct but wasteful. More importantly, it means both LWW and Yrs receive the same layers, and each processes only their own operations. This is fine.

**However:** In entity.rs lines 308-317, there is a second loop that creates backends for operations in `to_apply` events that don't exist yet. For each newly created backend, `apply_layer(layer)` is called again. This means the layer is applied to the newly created backend, but it was already applied to all existing backends in lines 303-306. The newly created backend starts empty, so it only picks up operations from the current layer. But what about operations from PREVIOUS layers that this backend should have processed? If backend "lww" is first encountered in Layer 3, it only sees Layer 3's events, missing Layers 1 and 2. **This is a bug** -- see Finding #3.

---

## 7. Head Update Logic in DivergedSince

Entity.rs lines 320-326:
```rust
for parent_id in &meet {
    state.head.remove(parent_id);
}
state.head.insert(event.id());
```

The `meet` here comes from the `DivergedSince` variant's `meet` field. This is the set of greatest common ancestors (GCAs) of the incoming event's parent and the entity's head.

**Problem scenario:** If the meet is deep in history (e.g., the genesis event), removing meet event IDs from the head is a no-op (they are not in the head). The new event is inserted, growing the head by one. This is correct for the first concurrent event, but after many concurrent events from the same deep meet point, the head grows unboundedly.

**More subtle problem:** The meet for the `StrictAscends` transformation case (comparison.rs line 131) is set to the event's parent clock, NOT the GCA. For example, if event D has parent [C] and the entity head is [B, C], the meet is [C]. Removing [C] from head [B, C] and inserting D gives head [B, D]. This is correct.

But what if event D has parent [A] (deep ancestor) and the entity head is [B, C]? The meet is [A]. Removing [A] from head [B, C] is a no-op. Inserting D gives head [B, C, D]. The head now has three tips. When another event E arrives with parent [A], the head becomes [B, C, D, E]. **Head growth is unbounded for events branching from deep ancestors.**

---

## 8. The apply_layer Stored-Value Seed Problem (Detailed)

In `apply_layer()` lines 180-189, the stored values are read and seeded as initial candidates:

```rust
let values = self.values.read().unwrap();
for (prop, entry) in values.iter() {
    let Some(event_id) = entry.event_id() else {
        return Err(...)
    };
    winners.insert(prop.clone(), Candidate { value: entry.value(), event_id, from_to_apply: false });
}
```

The seeded event_id is then compared against incoming candidates using `layer.compare()`. If the seeded event_id refers to an event that is above the meet point (not in the accumulated events), the comparison will fail.

**This is the root of the convergence risk.** The fix would be one of:
1. Include all events from genesis to heads in the accumulated events (expensive).
2. When the stored event_id is not in the events map, treat it as a known-ancestor (i.e., any incoming event at this layer automatically descends from it).
3. Track the "layer depth" of the stored value's event and use that for comparison instead of the event_id.

Option 2 seems most practical: if `events.get(stored_event_id)` returns None, the stored event is above the meet point, meaning every event in the current layer descends from it. The candidate should always win.

---

## 9. The "Already Applied Winner Not Written" Problem

In `apply_layer()` lines 234-243:
```rust
for (prop, candidate) in winners {
    if candidate.from_to_apply {
        values.insert(prop.clone(), ValueEntry::Committed { value: candidate.value, event_id: candidate.event_id });
        changed_fields.push(prop);
    }
}
```

Only winners from `to_apply` are written to the backend state. If the winner is from `already_applied`, the backend state is NOT updated. This means the stored event_id for that property still reflects whatever was in the backend before this layer was applied.

**Problem scenario with multi-layer application:**

Consider:
- Stored state has property X with event_id=E1 (from some old event).
- Layer 1: already_applied=[EventA with X="A"], to_apply=[EventB with X="B"]. EventA has higher event_id. Winner is EventA (from already_applied). State is NOT updated.
- Layer 2: to_apply=[EventC with X="C"]. Seed has event_id=E1 (old event, not updated in Layer 1). `layer.compare(C_id, E1)` may fail if E1 is not in the events map, or may give an incorrect result.

**Wait** -- actually, let me reconsider. If the stored event_id=E1 correctly represents the event that set the value, and EventA (from already_applied) also sets property X but does NOT become the stored value, then there is a discrepancy between what the backend thinks is the current value and what actually won in the layer competition.

Actually, I need to reconsider more carefully. The "seed" step reads the stored backend values. If the entity had already applied EventA's operations via `apply_operations_with_event()` earlier, the stored value for X already reflects EventA's write, with event_id=EventA_id. So the seed correctly represents EventA.

The issue only arises if `already_applied` events write values that were NOT previously applied via `apply_operations_with_event()`. But `already_applied` events ARE by definition already in the entity's state -- that is the point of `compute_ancestry()` partitioning. So their operations HAVE been applied. The stored value should already reflect the latest write among the `already_applied` events.

**Revised assessment:** The "seed from stored state" approach is correct because `already_applied` events have already been applied to the backend, and their values/event_ids are in the stored state. The seed captures the most recent write among all previously applied events. The competition against `to_apply` events then determines if any new event should overwrite. If the winner is from `already_applied`, no state change is needed (it is already the stored value). **This logic is correct.**

However, there is still a correctness concern: the stored value's event_id might NOT be from any event in this layer. It could be from an earlier layer's winner or from the pre-merge state. If it is from the pre-merge state and that event is above the meet point, the comparison fails. This is the same issue as Finding #2.

---

## 10. Newly Created Backend Missing Earlier Layers

In entity.rs lines 308-317:
```rust
for evt in &layer.to_apply {
    for (backend_name, _) in evt.operations.iter() {
        if !state.backends.contains_key(backend_name) {
            let backend = backend_from_string(backend_name, None)?;
            backend.apply_layer(layer)?;
            state.backends.insert(backend_name.clone(), backend);
        }
    }
}
```

If a backend is first encountered in Layer N, it is created fresh and only receives Layer N's events. But earlier layers (1 through N-1) may have contained events with operations for this backend. Those operations are lost.

For LWW, this means properties set in earlier layers are not present in the new backend's state. For Yrs, this means earlier text operations are missing. This is a correctness bug for any entity that first encounters a backend type via a concurrent branch.

In practice, this may be rare: typically all backends are created when the entity is first created (genesis event), and the genesis event is before the meet point. But it is possible for a concurrent branch to introduce a new backend type.

---

## Confidence-Rated Findings

| # | Finding | Severity | Confidence | Details |
|---|---------|----------|------------|---------|
| 1 | **Stored event_id may reference event outside layer's events map, causing InsufficientCausalInfo error** | Critical | High | In `apply_layer()` (lww.rs lines 180-189), the stored value's `event_id` is used as a comparison candidate. If this event was set by an event above the meet point (before the divergence), it is not in the `EventLayer.events` map. `layer.compare()` will call `is_descendant()` which will fail with `RetrievalError` for the missing event, propagated as `MutationError::InsufficientCausalInfo`. This makes the entire `apply_event()` fail for a structurally valid scenario: property X was set at genesis, two branches diverge, and one branch sets X again. File: `/Users/daniel/ak/ankurah-201/core/src/property/backend/lww.rs` lines 203-207. Fix: when the stored event_id is not found in the events map, treat the stored value as a known ancestor (any layer event automatically beats it). |
| 2 | **Newly created backend during DivergedSince misses earlier layers** | High | High | In `entity.rs` lines 308-317, if a backend type is first encountered in Layer N of a DivergedSince resolution, it is created empty and only receives Layer N's events. Operations from Layers 1 through N-1 for that backend are permanently lost. File: `/Users/daniel/ak/ankurah-201/core/src/entity.rs` lines 308-317. Fix: either pre-scan all layers for required backends before applying, or replay earlier layers for newly created backends. |
| 3 | **"Deeper branch wins" is NOT a property of the system** | Medium | High | The spec (spec.md section "LWW Resolution Within Layer") and the integration test `test_deeper_branch_wins` imply that events on a deeper branch automatically win. In reality, events on different branches at different layers are `Concurrent` (no causal relationship), and the winner is determined by lexicographic EventId tiebreak. The test passes only because event D is a merge event (parent=[B,C]) that causally descends from both branches, NOT because of its depth. If D only extended branch 2 (parent=[C]), the result would depend on EventId comparison, not depth. This is a documentation/spec inaccuracy, not a code bug. File: `/Users/daniel/ak/ankurah-201/specs/concurrent-updates/spec.md` |
| 4 | **EventLayer::compare returns Descends for a==b** | Low | High | In `layers.rs` line 58-59, when `a == b`, `compare()` returns `CausalRelation::Descends`. Mathematically, reflexive descent is "a descends from itself," which is a non-standard convention (most systems use a separate `Equal` variant for this). In practice this is harmless because the only caller is `apply_layer()` which never compares a candidate against itself (each event gets a distinct EventId from SHA-256). But it could confuse future API consumers. File: `/Users/daniel/ak/ankurah-201/core/src/event_dag/layers.rs` line 58-59. |
| 5 | **Pending values block to_state_buffer with no recovery path** | Medium | Medium | After `to_operations()` transitions values from `Uncommitted` to `Pending`, those values lack an `event_id`. If the transaction fails before `apply_operations_with_event()` is called, `Pending` values are stuck: `to_state_buffer()` fails (line 117-122), and there is no method to revert `Pending` back to `Uncommitted` or remove them. In practice, transaction backends are forks that get discarded on failure, but there is no guard preventing `to_state_buffer()` from being called on a backend with `Pending` values. File: `/Users/daniel/ak/ankurah-201/core/src/property/backend/lww.rs` lines 137-157. |
| 6 | **children_of() is O(N) per call, creating O(N^2) layer computation** | Medium | High | `children_of()` (layers.rs lines 159-166) performs a linear scan over all events for every call. It is called once per frontier event per layer iteration. For a DAG with N events across L layers, the cost is O(L * F * N) where F is the average frontier size. In the worst case (many small layers), this approaches O(N^2). A pre-computed parent-to-children index (built once in O(N)) would reduce each lookup to O(1). File: `/Users/daniel/ak/ankurah-201/core/src/event_dag/layers.rs` lines 159-166. |
| 7 | **DivergedSince head update removes meet IDs that may not be in head** | Low | High | In entity.rs lines 323-324, `state.head.remove(parent_id)` removes meet IDs from the head. When the meet is deep in history (not a current head tip), the remove is a no-op, and the new event is simply appended to the head. This causes unbounded head growth when many events branch from a deep ancestor. While technically correct (the head accurately represents all un-superseded tips), it degrades performance over time. File: `/Users/daniel/ak/ankurah-201/core/src/entity.rs` lines 323-326. |
| 8 | **Cross-replica convergence is correct for concurrent events in the same layer** | N/A (Positive) | High | Traced through the full resolution path for three concurrent events applied in different orders on two replicas. Both converge to the same final state because: (a) all concurrent events from the same meet point end up in the same layer, (b) pairwise competition with lexicographic tiebreak is commutative, and (c) the stored seed's event_id is updated correctly between layers. This holds as long as Finding #1 does not trigger. |
| 9 | **No test for apply_layer with stored value above meet point** | Medium | High | The existing unit tests (tests.rs lww_layer_tests) create backends from scratch, so the stored seed is always empty or from the same layer's events. There is no test where the stored value's event_id references an event that is outside the EventLayer's events map. This is the scenario that triggers Finding #1. File: `/Users/daniel/ak/ankurah-201/core/src/event_dag/tests.rs` |
| 10 | **apply_layer is called on ALL existing backends for EVERY layer, even if a layer has no relevant operations** | Low | High | Entity.rs line 304: `for (_backend_name, backend) in state.backends.iter()`. Each backend receives every layer and internally checks for relevant operations. For entities with many backends and many layers, this is wasteful but not incorrect. The LWW and Yrs backends both correctly skip events with no matching operations. File: `/Users/daniel/ak/ankurah-201/core/src/entity.rs` lines 303-306. |
