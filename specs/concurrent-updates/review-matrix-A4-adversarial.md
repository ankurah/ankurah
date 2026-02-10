# Adversarial Security & Correctness Review: concurrent-updates-event-dag

**Reviewer Role:** Adversarial tester (break-the-implementation focus)
**Branch:** concurrent-updates-event-dag
**Date:** 2026-02-05
**Seed Context:** review-digest-pr-comments.md

---

## 1. Extreme Depth Asymmetry (1 vs 1000 depth branches)

### Scenario
Entity head is at event H_1000 (a chain of 1000 events from genesis A). An incoming event E has parent [A] (the genesis). This creates a 1-depth branch vs a 1000-depth branch.

### Trace through the code

In `entity.rs:apply_event`, `compare_unstored_event` is called. Since E's parent is [A] and entity head is [H_1000]:
- `compare([A], [H_1000], 100)` is invoked.
- The comparison BFS starts with subject frontier = {A}, comparison frontier = {H_1000}.
- The comparison frontier must walk back 1000 steps to reach A.
- **Budget is 100.** At budget 1 per step (as consumed in `expand_frontier`), the comparison exhausts the budget after ~100 steps.
- Result: `BudgetExceeded`.
- `apply_event` returns `Err(LineageError::BudgetExceeded {...})`.

**Finding: The incoming event is valid but is rejected because budget (100) is insufficient.** For a DAG where one branch has >100 events from the meet point, the system cannot determine the causal relationship, and the event is hard-rejected (not deferred -- rejected as an error). The system has no resumption mechanism despite BudgetExceeded carrying frontier data.

### Memory impact
If budget were raised high enough (say 10000), the `AccumulatingNavigator` would accumulate all 1000 events into its `BTreeMap<EventId, Event>`, plus the `states` HashMap in Comparison would hold NodeState for all 1000 events. Each `NodeState` includes two `Vec<Id>` for children and one for origins. For 1000 events this is manageable (tens of KB), but for 100,000+ it would be significant.

The `compute_layers` call then clones the entire accumulated event map via `Arc::new(events.clone())` at layers.rs:107. This doubles memory usage momentarily.

### children_of O(n) scan
In `layers.rs:165`, `children_of` iterates ALL events in the map for each parent lookup. In the 1 vs 1000 scenario, computing layers from meet A would call `children_of(A)`, which scans all ~1001 events. Then for B (child of A on the long branch), it scans 1001 events again. Total: O(n^2) where n is the number of events. For 1000 events, this is ~1M comparisons -- slow but feasible. For 10,000 events it becomes problematic (100M comparisons).

---

## 2. Cascading Diverge-Then-Reconverge (Nested Diamonds)

### Scenario: Double Diamond
```
        A (genesis)
       / \
      B   C
     / \ / \
    D   E   F
     \ / \ /
      G   H
       \ /
        I
```
Entity head = [G], incoming event has parent [H]. Meet should be E (common ancestor of G and H).

### Trace through comparison

`compare_unstored_event(event_I_parent=[H], head=[G], budget)`:
- Calls `compare([H], [G], budget)`.
- Subject frontier = {H}, Comparison frontier = {G}.
- Step 1: Fetch H (parents: E, F) and G (parents: D, E). Subject frontier = {E, F}. Comparison frontier = {D, E}.
- E appears in BOTH frontiers. It becomes a meet candidate.
- `unseen_comparison_heads` for G: H was fetched from subject, D from comparison -- neither is G. G was removed from comparison frontier when fetched.
- Actually, let me re-trace: Subject frontier starts as {H}, comparison as {G}. Both are fetched. H's parents {E,F} go to subject frontier. G's parents {D,E} go to comparison frontier.
- H is `from_subject=true`, adds E,F to subject frontier.
- G is `from_comparison=true`, adds D,E to comparison frontier.
- Now subject frontier = {E, F}, comparison frontier = {D, E}.
- Step 2: Fetch E, F, D. E is in BOTH frontiers (from_subject=true and from_comparison=true).
  - E is common. `meet_candidates` gets E. E's parents (B, C) are added to both frontiers for tracking.
  - `unseen_comparison_heads`: Subject needs to see G. When was G seen by subject? G was only in comparison frontier. So `unseen_comparison_heads` starts at 1 (for G). Subject visits H... H is not in original_comparison {G}. Subject visits E, F... E is not G, F is not G. So `unseen_comparison_heads` remains 1.
  - `unseen_subject_heads`: Comparison needs to see H. Comparison visits G... G is not in original_subject {H}. Comparison visits D, E... neither is H. So `unseen_subject_heads` remains 1.
- Frontiers: after processing E from both sides, F from subject, D from comparison. Subject frontier contains B, C (from E's parents) plus F's parents. Comparison frontier contains B, C (from E's parents) plus D's parents.
- Eventually frontiers exhaust and we reach the meet_candidates filter. E has `common_child_count > 0`? E's children in the traversal: H from subject (added via `add_child`), G from comparison (added via `add_child`). But common_child_count is only incremented for parents of common nodes. E became common, so E's parents (B, C) get common_child_count incremented. E itself only has common_child_count if E is a parent of another common node. Since nothing below E is common (H and G are not common), E has common_child_count = 0.
- The meet filter at line 561 keeps E because common_child_count == 0. This is correct.

**Result: Meet = [E], which is correct.** The nested diamond is handled properly because the BFS naturally finds the deepest common ancestor.

### Deeper nesting
For a 3-level nested diamond, the algorithm still works because it tracks per-node state. The key invariant is that `meet_candidates` collects ALL nodes seen from both sides, and the filter `common_child_count == 0` selects only the minimal (deepest) ancestors. This appears correct for arbitrary diamond nesting.

**No bug found for nested diamonds.**

---

## 3. TOCTOU Retry Loop Termination

### Code path (entity.rs:248-346)
```rust
const MAX_RETRIES: usize = 5;
for attempt in 0..MAX_RETRIES {
    // ... expensive async comparison ...
    match compare_unstored_event(...).await? {
        StrictDescends { .. } => {
            if self.try_mutate(&mut head, |state| { ... })? {
                return Ok(true);
            }
            continue;  // head moved, retry
        }
        DivergedSince { .. } => {
            let mut state = self.state.write().unwrap();
            if state.head != head {
                head = state.head.clone();
                continue;  // head moved, retry
            }
            // ... apply layers ...
            return Ok(true);
        }
        // other cases return immediately
    }
}
Err(MutationError::TOCTOUAttemptsExhausted)
```

### Termination analysis

The loop runs at most `MAX_RETRIES` (5) times. Each iteration performs an async comparison (potentially expensive -- up to 100 budget steps), then attempts the mutation. If the head moves between comparison and mutation, it retries.

**Can concurrent writers cause all 5 retries to fail?**

Yes. If 6 concurrent writers are all trying to apply events to the same entity, and they interleave such that each writer's comparison completes just before another writer's mutation commits (moving the head), then all 5 retries can fail. The worst case is:

1. Writer A reads head = H0, begins comparison.
2. Writer B commits, head becomes H1.
3. Writer A finishes comparison, tries mutate, sees H1 != H0, retries.
4. Writer C commits, head becomes H2.
5. Writer A finishes comparison (against H1), tries mutate, sees H2 != H1, retries.
6. ... continues 5 times total.

After 5 failures, `TOCTOUAttemptsExhausted` is returned. **This is a correctness issue under high contention** -- the event is valid but permanently lost (the error propagates up and the caller does not retry). There is no backoff between retries.

### Adversarial exploitation
An attacker with control of 6+ concurrent connections could intentionally time event submissions to force a specific event to be perpetually rejected. Each round, one attacker submits a cheap event (no operations, just advances the head) right as the victim's comparison finishes.

**Severity: Medium.** In practice, contention on a single entity from 6+ simultaneous writers is unusual, and the comparison step is fast for simple cases. But the attack is theoretically possible.

---

## 4. Race Conditions: Simultaneous apply_state and apply_event

### Scenario
Thread 1 calls `apply_event(E1)` for an event that triggers `DivergedSince`.
Thread 2 calls `apply_event(E2)` for a different concurrent event.

Both threads:
1. Read the head (same value H0).
2. Both do comparison (both get DivergedSince with meet M).
3. Both compute layers.
4. Thread 1 acquires write lock, checks head == H0, applies layers, updates head to H1.
5. Thread 2 acquires write lock, checks head == H0 -- fails (head is now H1).
6. Thread 2 retries with head H1.

**This is correctly handled by the TOCTOU pattern.** Thread 2 retries with the new head. No deadlock possible because:
- There is only ONE lock (`state: RwLock<EntityInnerState>`).
- Locks are never held across await points (verified in the code -- the write lock in DivergedSince is acquired at line 292 and held only for the synchronous layer application).

### Deadlock analysis
Could two entities deadlock? Entity E1 and Entity E2 each have their own `RwLock`. If Thread A holds E1's lock and waits for E2's lock while Thread B holds E2's lock and waits for E1's lock -- but this scenario is not possible in the current code because:
- `apply_event` only acquires the lock on the entity it's applying to.
- The `AccumulatingNavigator` only reads from storage (no entity locks).
- No cross-entity lock acquisition exists in any code path.

**No deadlock possible.**

### BUT: The AccumulatingNavigator creates a new navigator per retry
At line 250: `let acc_navigator = AccumulatingNavigator::new(getter);` is inside the retry loop. Each retry creates a fresh navigator, re-doing the entire BFS traversal. This is wasteful but correct.

---

## 5. Malicious/Malformed Events

### 5a. Event claims parents that don't exist

If an event E has parent [X] where X is not in storage:
- `compare_unstored_event(E, head, budget)` calls `compare([X], head, budget)`.
- The navigator's `expand_frontier` is called with frontier_ids = [X, ...head...].
- `LocalRetriever::expand_frontier` (retrieval.rs:76) calls `collection.get_events([X])`.
- If X is not in storage, `get_events` returns an empty result for X.
- X stays in the frontier but is never expanded. The frontier never removes X.
- The loop continues until budget exhausts.
- Result: `BudgetExceeded`.

**Finding: Missing parent events cause BudgetExceeded, not a descriptive error.** The system wastes the full budget trying to expand a phantom event ID before failing.

### 5b. Event graph has cycles

If events are stored such that A's parent is B and B's parent is A:
- The BFS traversal would alternate between A and B forever.
- However, `process_event` removes the event from the frontier (line 300-301) and adds its parents. If A is processed, it's removed, and B is added. When B is processed, it's removed, and A is added. A is already in `states`, but it's added to the frontier again.
- **The frontier.extend does not deduplicate against already-processed events.** The frontier is a `BTreeSet`, so duplicate IDs are not inserted twice, but A would be re-inserted into the frontier even though it was already processed.
- Wait, actually: the frontier `remove` at line 300 returns false if the ID wasn't in the frontier. And `process_event` only processes it if `from_subject || from_comparison` is true, which requires the ID to have been in a frontier. If A is re-added to the subject frontier after being removed, it would be processed again on the next step.
- **This creates an infinite loop (bounded only by budget).** Each step processes A, adds B; next step processes B, adds A; repeat until budget exhausts.
- Result: `BudgetExceeded` after budget steps. No crash, but incorrect behavior -- this is a resource waste.

**Finding: Cycles in the event graph are not detected and cause budget exhaustion.** There is no visited-set check preventing re-expansion of already-processed events.

### 5c. Event ID collisions

Event IDs are SHA-256 hashes of (entity_id, operations, parent_clock). A collision would require a SHA-256 collision, which is computationally infeasible. The attacker would need to find two different (entity_id, operations, parent) tuples that hash to the same 32-byte value. **Not a practical concern.**

However, if the same event is delivered twice (redundant delivery), the `compare_unstored_event` check at line 95 catches exact ID matches in the head. If the event's ID is in the head, it returns `Equal` (no-op). If the event was already applied but is no longer at the head (superseded), it falls through to the comparison logic and is handled as `DivergedSince` -- which re-applies layer computation. The `already_applied` partitioning in `compute_layers` should handle this correctly since the event would be in `current_head_ancestry`.

---

## 6. Resource Exhaustion: Worst-Case Complexity

### 6a. O(n^2) comparison time via wide DAGs

Construct a DAG where genesis A has 50 children B1..B50, each of which has one child C1..C50. Head = [C1..C50], incoming event parent = [A].

- `compare([A], [C1..C50], 100)` starts with subject frontier = {A}, comparison frontier = {C1..C50} (50 elements).
- Step 1: Navigator fetches all 51 IDs (1 from subject + 50 from comparison). Processing 50 events, each adding its parent (B_i) to the comparison frontier. Subject processes A, adds nothing (genesis).
- Now comparison frontier = {B1..B50}. Subject frontier empty (A was genesis).
- Step 2: Navigator fetches 50 B_i events. Each has parent A. Subject frontier is empty so nothing from subject. Comparison adds A 50 times (deduped to once in BTreeSet).
- A is now in the comparison frontier. Subject already processed A.
- Step 3: A is fetched from comparison. A has no parents. Subject already has A in `states`. A becomes common.

Total events fetched: 1 + 50 + 50 + 1 = 102 (budget 100 might be exceeded). **With 50-wide fan-out, budget 100 is barely enough.**

### 6b. Pathological chain construction memory

`build_forward_chain` at comparison.rs:447 creates a new Vec by reversing `visited`. For a 1000-event linear chain, this is a 1000-element Vec allocation. Reasonable.

`compute_layers` at layers.rs:107 does `Arc::new(events.clone())` where events is the full accumulated BTreeMap. For 1000 events with typical Event size (~1KB operations), this is ~1MB clone. For 10,000 events: ~10MB. **Bounded by budget, so maximum is ~100 events per the current budget=100.**

### 6c. Worst-case for is_descendant in EventLayer

`is_descendant` (layers.rs:201) does BFS from descendant upward to find ancestor. In the worst case (descendant is at maximum depth, ancestor is genesis), this visits every event in the accumulated map. Since `apply_layer` calls `layer.compare()` for each property conflict between a candidate and the current winner, the worst case is:
- N events in the layer, each writing the same property.
- For each candidate after the first, `compare` is called. Each `compare` calls `is_descendant` twice.
- Each `is_descendant` is O(E) where E is the number of events in the map.
- Total: O(N * E) per property.

With budget 100, both N and E are bounded by 100. So worst case is ~10,000 operations. Acceptable.

---

## 7. LWW + Yrs Mixed Backend Interaction Bugs

### Scenario
Entity has both `lww` and `yrs` backends. Event E1 writes to lww property "title", Event E2 writes to yrs property "body". Both are concurrent.

### Trace through entity.rs DivergedSince handler (line 277-329)

```rust
// Apply layers in causal order
for layer in &layers {
    for (_backend_name, backend) in state.backends.iter() {
        backend.apply_layer(layer)?;
    }
    // Create backends for operations in to_apply events that don't exist yet
    for evt in &layer.to_apply {
        for (backend_name, _) in evt.operations.iter() {
            if !state.backends.contains_key(backend_name) {
                let backend = backend_from_string(backend_name, None)?;
                backend.apply_layer(layer)?;
                state.backends.insert(backend_name.clone(), backend);
            }
        }
    }
}
```

**Critical finding: Every backend receives every layer, regardless of whether the layer contains operations for that backend.**

For the lww backend:
- `apply_layer` (lww.rs:169) seeds winners from stored values (all committed properties).
- It iterates events looking for `event.operations.get("lww")`.
- For events that only have yrs operations, the lww scan finds nothing. The winners map remains seeded with stored values.
- **No stored value has `from_to_apply: false`, so they won't be written.** But wait -- the stored values are seeded as candidates. If no to_apply event touches a property, the winner has `from_to_apply: false`, so it's skipped in the write phase (line 238). Correct behavior.

For the yrs backend:
- `apply_layer` (yrs.rs:186) only processes `event.operations.get("yrs")`. Events without yrs operations are ignored. Correct behavior.

**BUT there is a subtle interaction bug:** If the lww backend encounters a stored value without an `event_id` (e.g., loaded from a pre-migration state buffer), `apply_layer` returns a hard error at line 184-186:
```rust
let Some(event_id) = entry.event_id() else {
    return Err(MutationError::UpdateFailed(...));
};
```
This kills the entire entity update, including the yrs operations that would have succeeded. **A single pre-migration lww property blocks all backend updates.**

### The layer.compare() issue with stored values
When the lww backend seeds winners with stored values, those stored values have event_ids that may NOT be present in the layer's events BTreeMap. When a new candidate arrives and `layer.compare(&candidate.event_id, &current.event_id)` is called, if `current.event_id` (from the stored value) is not in the events map, `is_descendant` at layers.rs:221 returns `Err(RetrievalError::Other("missing event..."))`. This propagates as `MutationError::InsufficientCausalInfo`.

**This is the known per-property LWW bug from the review digest, but it has a NEW angle the reviewers missed:**

Even ignoring the "non-head last-write" issue, the `layer.compare()` call will fail with `InsufficientCausalInfo` whenever a stored value's event_id is not in the accumulated events. The accumulated events only contain events traversed during the BFS comparison. If the stored value's event was from long ago (many events back), it won't be in the accumulated set.

**Example:**
1. Event E1 (parent: genesis) writes property "title" = "Hello".
2. Event E2 (parent: E1) writes property "body" = "World" (does NOT write title).
3. Entity head = [E2]. The lww backend has title with event_id = E1.
4. Event E3 (parent: genesis) arrives, writes property "title" = "Goodbye". DivergedSince with meet = genesis.
5. AccumulatingNavigator traverses: E3's parent (genesis), E2, E1 (comparison traversal goes E2 -> E1 -> genesis).
6. Accumulated events: {genesis, E1, E2}. But wait -- E3 is unstored, so it's added manually at entity.rs:282.
7. `compute_layers` starts from meet (genesis). Children of genesis: E1, E2, E3 (if E3 is added). First layer: E1 (already_applied), E2 (already_applied), E3 (to_apply).
8. LWW apply_layer: stored values include title=Hello with event_id=E1. New candidate: title=Goodbye from E3. `layer.compare(E3_id, E1_id)` -- but E3 is the unstored event. Is E3 in the events map? Yes, it was inserted at line 282. And E1 is in the accumulated events. So `is_descendant(E3, E1)` checks E3's parents... E3's parent is genesis (empty clock). Genesis has no parents. E3 is NOT a descendant of E1. `is_descendant(E1, E3)` checks similarly. Neither descends from the other. Returns `Concurrent`. Winner decided by EventId comparison (lexicographic on SHA-256 hash). **This works in this simple case.**

But consider a deeper scenario:
1. Events: genesis -> E1 (title=A) -> E2 (body=B) -> E3 (body=C) -> E4 (body=D). Head = [E4].
2. Incoming: E5 (parent: E2, writes title=Z). DivergedSince with meet = E2.
3. Accumulated events during BFS: E5's parent E2, head E4, then E3 (parent of E4), E2 again from comparison side. Accumulated: {E2, E3, E4, E5}.
4. LWW backend stored values: title=A with event_id=E1.
5. Layer starts from meet E2. Children of E2 in accumulated events: E3 and E5. Layer 0: E3 (already_applied), E5 (to_apply).
6. E5 writes title=Z. Stored winner for title is event_id=E1.
7. `layer.compare(E5_id, E1_id)` -- **E1 is NOT in the events map** (it was not traversed by BFS because traversal started from E2/E4 and only went backward, hitting E2 as the meet and stopping).

**Result: `Err(RetrievalError::Other("missing event for ancestry lookup: E1"))` -> `MutationError::InsufficientCausalInfo`.**

**This is a REAL BUG.** Whenever the stored last-write event for a property is older than the meet point, `layer.compare()` will fail because the event is not in the accumulated events map. The digest identified the "non-head last-write" issue but framed it as a winner-selection problem. The actual manifestation is a hard error from `is_descendant` because the event simply isn't available in the layer's context.

---

## 8. The "Empty Chain" Edge Case in LWW

### Scenario: StrictAscends transformation

Entity head = [B, C] (concurrent tips), incoming event E with parent [C].
`compare_unstored_event` returns:
```rust
AbstractCausalRelation::DivergedSince {
    meet: [C],       // parent members
    subject: [],     // Immediate children not tracked
    other: [B],      // head tips not in parent
    subject_chain: [E],   // just the incoming event
    other_chain: [],      // empty
}
```

In `apply_event` (DivergedSince handler):
- `acc_navigator.get_events()` -- what events did it accumulate? The comparison was `compare([C], [B,C], budget)`.
  - Subject frontier: {C}. Comparison frontier: {B, C}.
  - C is in both frontiers. It's immediately common.
  - `unseen_comparison_heads`: subject needs to see B and C. C was in subject frontier and is in original_comparison, so `unseen_comparison_heads` decrements to 1. But B was only in comparison frontier. Subject never sees B.
  - `unseen_subject_heads`: comparison needs to see C. C was in comparison frontier and is in original_subject, so `unseen_subject_heads` decrements to 0.
  - Since `unseen_subject_heads == 0`, returns `StrictAscends`.
- This becomes the `StrictAscends` case in `compare_unstored_event`, which is transformed to `DivergedSince { meet: [C], other: [B], other_chain: [], subject_chain: [E] }`.

- AccumulatingNavigator accumulated events: C was fetched (and B). So accumulated events = {B, C}. (But actually, C was fetched from both frontiers, so only one copy.)
- `events.insert(event.id(), event.clone())` adds E.
- Accumulated events: {B, C, E}.
- `compute_ancestry(events, head=[B,C])` = {B, C}. (B's parents and C's parents might go to genesis, but genesis is not in the accumulated events, so ancestry stops at B and C.)
  - Wait -- `compute_ancestry` at layers.rs:172 walks backward from head through events in the map. B's parents: let's say B has parent [A] (genesis). If A is not in the accumulated events map, ancestry stops at B. So ancestry = {B, C}.
- `compute_layers(events={B,C,E}, meet=[C], current_ancestry={B,C})`.
  - Start with children of C in accumulated events: any event whose parent contains C.
    - B: Does B's parent contain C? No (B's parent is [A]). So B is NOT a child of C.
    - E: Does E's parent contain C? E's parent is [C]. Yes! E is a child of C.
  - Frontier = {E}. E is not in current_ancestry, so to_apply = [E]. Layer 0 has to_apply = [E], already_applied = [].
  - Next frontier: children of E = none (nothing has E as parent). Done.
- Layers: one layer with to_apply = [E], already_applied = [].

**LWW apply_layer for this layer:**
- Seeds winners from stored values (all committed properties with their event_ids).
- E's operations are extracted. For each property E writes, a candidate is created.
- If stored value has event_id B_id (because B wrote that property), and B_id is NOT in the layer's events map... wait, is B in the events map? Yes, B is in the accumulated events that were passed to `compute_layers`, and `compute_layers` at line 107 does `Arc::new(events.clone())` -- so the layer's events map is the full accumulated map {B, C, E}.
- `layer.compare(E_id, B_id)`: `is_descendant(events, E_id, B_id)` starts at E. E's parents = [C]. C's parents -- wait, C has parent [A] (genesis). A is not in the events map. So we get `Err("missing event for ancestry lookup: A")`.

**This ALSO triggers InsufficientCausalInfo.** Whenever the events map doesn't contain the FULL ancestry back to a common ancestor, `is_descendant` can fail.

**Actually wait** -- let me re-read `is_descendant` at layers.rs:214-228:
```rust
while let Some(id) = frontier.pop() {
    if !visited.insert(id.clone()) { continue; }
    if &id == ancestor { return Ok(true); }
    let event = events.get(&id).ok_or_else(|| ...)?;
    ...
}
```

If E's parent is C, and we're checking `is_descendant(E, B)`:
- Start with frontier = [E].
- Pop E. Not B. Get E's event. Parents = [C]. Push C.
- Pop C. Not B. Get C's event. Parents = [A]. A is not in events.
- **`events.get(&A)` returns None. Returns Err.**

**Finding confirmed: `is_descendant` returns a hard error when any ancestor in the traversal path is missing from the events map, even if the answer could be determined as "not a descendant" by other means.**

This is more general than the per-property bug identified in the digest. The `is_descendant` function cannot handle partial DAG context -- it requires the COMPLETE ancestry of both events to be present in the events map, or it errors. But the accumulated events from BFS only contain events traversed during comparison, which may be a small subset of the full DAG.

---

## 9. Head Pruning Bug in DivergedSince (entity.rs:320-326)

```rust
// Update head: remove superseded tips, add new event
for parent_id in &meet {
    state.head.remove(parent_id);
}
state.head.insert(event.id());
```

The code removes meet IDs from the head and adds the incoming event. But the meet is the COMMON ANCESTOR, not the head tip. Consider:

- Head = [B, C]. Meet = [A] (genesis). Incoming event E has parent [A].
- After this code: head removes A (which is NOT in head [B,C] -- `remove` returns false, no-op). Then inserts E.
- New head = [B, C, E]. This is correct -- E is concurrent with B and C.

Now consider the StrictAscends transformation case:
- Head = [B, C]. Event E parent = [C]. StrictAscends transformed to DivergedSince { meet: [C], other: [B] }.
- After this code: head removes C (which IS in head [B,C]). Inserts E.
- New head = [B, E]. This is correct -- E supersedes C, and B remains.

What about: Head = [D, E] where D and E both descend from B. Meet = [B]. B is NOT in the head.
- Head removes B (not in head, no-op). Inserts incoming event F.
- New head = [D, E, F]. But F's parent is [B], and B is an ancestor of both D and E.
- **This means F is concurrent with D and E (correct), but the head now has 3 tips.** F should be added without removing anything. This is correct behavior.

**One subtle issue:** The meet might contain IDs that ARE in the head in some scenarios. Consider Head = [B, C] and meet = [B] (event extends B specifically, where B is a head tip). Meet [B] is correctly removed, event is inserted. New head = [C, E]. Correct.

**No bug found in head pruning logic.** The logic is: remove meet-point IDs from head (which removes any tips that the incoming event is "replacing"), then add the new event. This works correctly for all cases I can construct.

BUT WAIT -- there is a scenario the code doesn't handle: What about events that are LATER in the `other_chain` that supersede head tips? If the other_chain has events that are descendants of head tips, those head tips should also be pruned. But the code only prunes meet-point IDs, not `other` IDs. Let me think...

Actually, the `other` field contains head tips that are NOT in the meet. These represent branches that the incoming event is concurrent with. They should NOT be pruned because the incoming event doesn't supersede them. The incoming event only supersedes the branch from meet to the incoming event's parent. So the pruning is correct.

---

## 10. NEW Finding: apply_event Double-Applies to New Backends

In entity.rs:300-317:
```rust
for layer in &layers {
    // Apply to all existing backends
    for (_backend_name, backend) in state.backends.iter() {
        backend.apply_layer(layer)?;
    }
    // Create backends for new operations
    for evt in &layer.to_apply {
        for (backend_name, _) in evt.operations.iter() {
            if !state.backends.contains_key(backend_name) {
                let backend = backend_from_string(backend_name, None)?;
                backend.apply_layer(layer)?;  // <--- applies the layer
                state.backends.insert(backend_name.clone(), backend);
            }
        }
    }
}
```

If a to_apply event introduces a new backend name (e.g., first "lww" operation on this entity), the code:
1. Creates a fresh empty backend.
2. Calls `apply_layer(layer)` on it.
3. Inserts it into `state.backends`.

But what if the NEXT layer also has events for this backend? The next iteration of the outer `for layer in &layers` loop will call `backend.apply_layer(layer)` on the now-existing backend. This is correct.

However, if TWO different events in the SAME layer both introduce the same new backend name, the second event's inner loop iteration finds `state.backends.contains_key(backend_name)` is now true (because the first event's iteration inserted it), so it skips. The layer was already applied once. This is also correct.

**BUT: what if two different layers' to_apply events introduce the same new backend?** Layer 0's to_apply has event E1 with backend "foo". Layer 1's to_apply has event E2 with backend "foo".
- Layer 0: "foo" not in backends. Create, apply layer 0, insert.
- Layer 1: "foo" IS in backends (from layer 0). The outer loop applies layer 1 to all backends including "foo". Correct.

**No bug found.** The logic handles new backends correctly.

---

## 11. NEW Finding: LocalRetriever Does Not Return Staged Events in expand_frontier

Looking at `LocalRetriever::expand_frontier` (retrieval.rs:70-80):
```rust
async fn expand_frontier(&self, frontier_ids: &[Self::EID], _budget: usize)
    -> Result<NavigationStep<Self::EID, Self::Event>, RetrievalError> {
    let events = self.0.collection.get_events(frontier_ids.to_vec()).await?;
    let event_payloads = events.into_iter().map(|e| e.payload).collect();
    Ok(NavigationStep { events: event_payloads, assertions: Vec::new(), consumed_budget: 1 })
}
```

**It only queries the storage collection, not the staged events.** In contrast, `EphemeralNodeRetriever::expand_frontier` (retrieval.rs:171-243) DOES check staged events first. The digest noted this but said it was "planned for EventAccumulator refactor."

The practical impact: During `apply_event`, the `AccumulatingNavigator` wraps a `getter` which is either `LocalRetriever` or `EphemeralNodeRetriever`. For the `EphemeralNodeRetriever`, staged events are consulted during BFS. For `LocalRetriever`, they are not.

In `commit_remote_transaction` (node.rs:542-594), events are applied one at a time using `LocalRetriever`. Events are stored via `collection.add_event` BEFORE `apply_event` is called (line 585 in the applied case, but actually looking more carefully: for the initial create case, `apply_event` is called at line 561, and `collection.add_event` is at line 585 -- AFTER apply_event). Wait, let me re-read...

Actually, in `commit_remote_transaction`:
```rust
for event in events.iter_mut() {
    let retriever = LocalRetriever::new(collection.clone());
    let entity = ...;

    if event.payload.is_entity_create() && entity.head().is_empty() {
        entity.apply_event(&retriever, &event.payload).await?;
        (entity.clone(), entity.clone(), true)
    } else {
        let forked = entity.snapshot(trx_alive);
        forked.apply_event(&retriever, &event.payload).await?;
        (entity.clone(), forked, false)
    };

    // ... policy checks ...

    let applied = if already_applied { true } else { entity.apply_event(&retriever, &event.payload).await? };

    if applied {
        collection.add_event(event).await?;  // stored AFTER apply
    }
}
```

Events are processed sequentially. Event N is stored AFTER it's applied. When Event N+1 is being compared, its parent might reference Event N. Since Event N was already stored (line 585), the `LocalRetriever` CAN find it in storage. **So the sequential processing order saves us here.**

But in `node_applier.rs::apply_updates` (line 46-48), events within a single `SubscriptionUpdateItem` are applied sequentially, and the retriever is created once per update item. Events from event_fragments are saved BEFORE apply_event is called (line 76 `save_events` is called first, then `apply_event`). So staged events don't matter here either.

**The staged events issue is primarily relevant for `apply_deltas` where `EventBridge` events are staged before comparison.** In `node_applier.rs:292-301`, events are staged into the retriever, then `apply_event` is called for each event. The `EphemeralNodeRetriever` checks staged events in `expand_frontier`, so this works.

**For LocalRetriever used in `commit_remote_transaction`: no staged events needed because events are stored sequentially.**

The digest's concern is valid but limited in practice to edge cases where out-of-order event delivery occurs within a single batch.

---

## 12. NEW Finding: unseen_comparison_heads Underflow Potential

In comparison.rs:370:
```rust
if self.original_comparison.contains(&id) {
    self.unseen_comparison_heads -= 1;
}
```

This decrements unconditionally when a comparison head ID is processed from the subject side. But what if the SAME ID is processed from the subject side TWICE? This shouldn't happen because `frontier.remove(&id)` at line 300 removes it and returns false on the second attempt, so `from_subject` would be false. But what if the ID was added back to the subject frontier by a cycle? In a cyclic graph, an event could be re-added to the frontier and re-processed.

If event ID X is in `original_comparison` and is processed from subject twice, `unseen_comparison_heads` would be decremented twice, causing underflow (wrapping to `usize::MAX` since it's unsigned). This would prevent `unseen_comparison_heads == 0` from ever being true, effectively blocking the `StrictDescends` detection.

**However,** this requires a cycle in the event graph, which shouldn't exist in valid data. But combined with finding 5b (cycles from malicious data), this could cause the comparison to return `BudgetExceeded` instead of `StrictDescends` -- a safety-preserving degradation but still incorrect.

Actually wait -- let me reconsider. If an event is re-added to the frontier and re-fetched, `frontier.remove(&id)` returns true (it was re-inserted). So `from_subject` is true again. The decrement happens again. But `unseen_comparison_heads` was already decremented. **This IS an underflow bug** for cyclic inputs. With usize, subtraction would panic in debug mode or wrap in release mode.

---

## 13. NEW Finding: build_forward_chain Incorrectly Handles Multi-Path DAGs

`build_forward_chain` at comparison.rs:447-460:
```rust
fn build_forward_chain(&self, visited: &[N::EID], meet: &BTreeSet<N::EID>) -> Vec<N::EID> {
    let mut chain: Vec<_> = visited.iter().rev().cloned().collect();
    if !meet.is_empty() {
        if let Some(pos) = chain.iter().position(|id| meet.contains(id)) {
            chain = chain.into_iter().skip(pos + 1).collect();
        }
    }
    chain
}
```

This reverses the visited list and truncates at the FIRST meet event found. For a linear chain, this produces correct causal order. But for a diamond DAG:

```
    A (meet)
   / \
  B   C
   \ /
    D
```

If subject visited D, then B, then C, then A (in BFS order from D backward), `visited = [D, B, C, A]`. Reversed: `[A, C, B, D]`. Find first meet (A) at position 0. Skip to position 1: `[C, B, D]`.

But the correct causal order from A to D is either `[B, D]` or `[C, D]` (both valid topological orders), or `[B, C, D]` if we want all events. The chain `[C, B, D]` includes both B and C but in reverse causal order (C before B, but B and C are concurrent -- so order between them doesn't matter).

**For layer computation, the chain is used to identify which events to process, and compute_layers does its own topological ordering via frontier expansion.** The chain from `build_forward_chain` is stored in the `AbstractCausalRelation` struct but is only used as informational data. The actual layer computation in `entity.rs` uses `compute_layers` which ignores the chain entirely, using the accumulated events map and meet point directly.

Wait, let me verify: does anything USE `subject_chain` or `other_chain` from DivergedSince? In entity.rs:277: `AbstractCausalRelation::DivergedSince { meet, .. }` -- it only destructures `meet`, using `..` for the rest. The chains are not used.

**So `build_forward_chain` is unused in the DivergedSince path.** It's only used for `StrictDescends { chain }`, which is used in `apply_event` only for the `StrictDescends` case (line 256) -- and that case doesn't even use the chain! It just applies the event directly.

In `apply_state` (line 374), StrictDescends also doesn't use the chain -- it just overwrites the state.

**The chain data is currently dead in the DivergedSince case and unused in practice for StrictDescends.** The ordering concern raised in the digest is valid (the chain IS incorrectly ordered for multi-path DAGs) but has no practical impact because no consumer uses it.

---

## 14. NEW Finding: compute_layers Can Orphan Events When Meet Has Multiple Heads

If meet = [M1, M2] and there's an event E whose parent is [M1, X] where X is NOT a meet event and not in the accumulated events:

```rust
// layers.rs:142-149
.filter(|id| {
    if let Some(event) = events.get(id) {
        event.parent().members().iter().all(|p| processed.contains(p))
    } else {
        false
    }
})
```

E's parents are [M1, X]. M1 is in processed (meet set). X is not in processed (and may not be in the events map at all). So `all(|p| processed.contains(p))` returns false. **E is never included in any layer.**

This means events whose parents span across the meet boundary (one parent in meet, one parent outside the accumulated events) are silently dropped from layer computation. Their operations are never applied.

**Scenario:** Entity head = [H]. Event E has parents [M, P] where M is the meet and P is an ancestor of H but not in the accumulated events. E's operations would be silently lost.

This is only possible if P is older than the meet point AND not in the accumulated events. Since the meet is the common ancestor of both branches, P being older than meet means P is a deep ancestor. The accumulated events from BFS may not include P if traversal stopped at the meet.

**Severity: Medium-High.** Events with multi-parent clocks that span across the meet boundary are silently dropped.

---

## Confidence-Rated Findings

| # | Finding | Severity | Confidence | Details |
|---|---------|----------|------------|---------|
| 1 | **LWW `layer.compare()` fails with InsufficientCausalInfo when stored value's event_id is older than meet point** | Critical | High | When an LWW property was last written by an event older than the meet point (e.g., a property set 5 events ago, never touched since), the event is not in the accumulated events map. `is_descendant()` at layers.rs:221 returns a hard error because it can't find the event. This causes `apply_event` to fail with `MutationError::InsufficientCausalInfo`. This is not a rare edge case -- it happens whenever a property was set long before the meet point. File: `core/src/property/backend/lww.rs:203`, `core/src/event_dag/layers.rs:221`. |
| 2 | **Budget too small for asymmetric depth; valid events hard-rejected** | High | High | With budget=100 and a DAG where one branch has >100 events from the meet, comparison returns BudgetExceeded which becomes a hard error. Valid events are rejected, not deferred. No resumption mechanism exists. File: `core/src/entity.rs:246,334-341`. |
| 3 | **`children_of` O(n) scan causes O(n^2 * p) layer computation** | High | High | `children_of` at layers.rs:159-166 iterates ALL events for EACH parent lookup. With n events and p parents per layer, total cost is O(n^2 * p). For n=1000 this is ~1M operations per parent. Pre-building a parent-to-children index would reduce this to O(1) per lookup. File: `core/src/event_dag/layers.rs:159-166`. |
| 4 | **Events with multi-parent clocks spanning the meet boundary are silently orphaned in layer computation** | High | Medium | In `compute_layers`, an event whose parents include both a meet-point event and an event outside the accumulated events map will never satisfy `all(|p| processed.contains(p))` and will be silently excluded from all layers. Its operations are lost. File: `core/src/event_dag/layers.rs:142-149`. |
| 5 | **Cyclic event graph causes integer underflow in unseen_comparison_heads** | Medium | High | If malicious events create a cycle, the same comparison head ID can be processed from the subject frontier multiple times, decrementing `unseen_comparison_heads` past zero. In debug builds this panics; in release it wraps to usize::MAX, permanently blocking StrictDescends detection. File: `core/src/event_dag/comparison.rs:370`. |
| 6 | **TOCTOU retry loop has no backoff; 5 retries insufficient under high contention** | Medium | High | An attacker controlling 6+ concurrent connections can reliably exhaust all 5 retries for a targeted event, causing permanent rejection via `TOCTOUAttemptsExhausted`. No exponential backoff or adaptive retry count. File: `core/src/entity.rs:243,248,345-346`. |
| 7 | **Missing parent events waste full budget before failing** | Medium | High | If an event claims a parent that doesn't exist in storage, the comparison BFS keeps the phantom ID in its frontier, re-requesting it every step until budget exhausts. Returns BudgetExceeded rather than a descriptive error. File: `core/src/event_dag/comparison.rs:272-297`, `core/src/retrieval.rs:70-80`. |
| 8 | **Pre-migration LWW properties without event_id cause hard errors on any layer application** | Medium | Medium | `apply_layer` requires all stored LWW values to have an event_id. Properties from before per-property event tracking was added lack event_ids, causing a hard MutationError that blocks ALL backend updates (including Yrs operations on the same entity). No migration path exists. File: `core/src/property/backend/lww.rs:183-186`. |
| 9 | **Cyclic event graph not detected; causes budget exhaustion** | Medium | High | The comparison BFS has no visited-set guard preventing re-expansion of already-processed events. Cycles cause infinite frontier oscillation until budget exhausts. File: `core/src/event_dag/comparison.rs:299-383`. |
| 10 | **LocalRetriever.expand_frontier ignores staged events** | Medium | High | Unlike EphemeralNodeRetriever, LocalRetriever does not check staged events during BFS traversal. If events are staged but not yet stored, the comparison may miss them. Impact is limited to specific code paths (commit_remote_transaction stores sequentially, mitigating this). File: `core/src/retrieval.rs:70-80`. |
| 11 | **Per-property LWW winner can be wrong when last-write event is not a head tip (known, UNRESOLVED)** | High | High | As documented in the review digest: when the last writer of a property has been superseded in the head clock (a later event on the same branch didn't touch that property), the stored value's event_id may not be in `current_head`. The existing seeding mechanism at lww.rs:180-189 seeds ALL stored values unconditionally, which is better than the original head-membership filter, but the comparison in `layer.compare()` can still fail per finding #1. Two fix branches exist (codex-lwwc-fix, claude-lww-fix) but neither is merged. File: `core/src/property/backend/lww.rs:177-189`. |
| 12 | **`build_forward_chain` produces incorrect ordering for multi-path DAGs** | Low | High | The chain reversal and truncation approach produces non-topological ordering when the visited list contains events from multiple concurrent paths. However, the chain data is currently unused by any consumer in both the DivergedSince and StrictDescends code paths, making this a latent defect only. File: `core/src/event_dag/comparison.rs:447-460`. |
| 13 | **`compute_layers` clones the full event map per call** | Low | High | `Arc::new(events.clone())` at layers.rs:107 deep-clones the entire BTreeMap. Bounded by budget (max ~100 events), so memory impact is limited, but unnecessary. Could use `Arc::new(events)` with ownership transfer or a reference. File: `core/src/event_dag/layers.rs:107`. |
| 14 | **AccumulatingNavigator recreated per retry, wasting prior BFS work** | Low | Medium | Each retry iteration in apply_event creates a fresh AccumulatingNavigator, discarding all previously accumulated events. Could retain events across retries. File: `core/src/entity.rs:250`. |

---

## Summary of Reviewer Concerns: Were They ACTUALLY Addressed?

| Concern from Digest | Status | My Assessment |
|---------------------|--------|---------------|
| Multi-head LWW resolution bug (critical) | Partial fix applied, acknowledged as unresolved | **Still broken** -- Finding #1 shows it's actually worse than described (hard error, not just wrong winner) |
| LocalRetriever doesn't consult staged events | Acknowledged, planned for EventAccumulator | **Confirmed still present** (Finding #10), but impact is limited by sequential processing |
| build_forward_chain not topological | Acknowledged, unresolved | **Confirmed** (Finding #12), but currently harmless (chain data unused) |
| TOCTOU pattern is sound | "Verified as SOUND across all review rounds" | **Partially disagree** -- pattern is correct but 5 retries with no backoff is exploitable (Finding #6) |
| Thread safety properly handled | "No locks across await points... consistent lock ordering" | **Confirmed correct** -- I found no deadlock or lock ordering issues |
| children_of O(n) | "Acceptable for typical workloads" | **Understated** -- O(n^2) is triggered at moderate scale (Finding #3) |
| Budget restored to 100 | Fixed | **Insufficient** for deep DAGs (Finding #2) |

## What the Reviewers MISSED

1. **Finding #1 (Critical):** `layer.compare()` hard-fails when stored values' event_ids are outside the accumulated events. This is the real manifestation of the LWW bug -- not just wrong winners, but a hard error crash. No reviewer identified this specific failure mode.

2. **Finding #4 (High):** Multi-parent events spanning the meet boundary are silently orphaned. No reviewer analyzed what happens when `compute_layers` encounters events with parents outside the accumulated map.

3. **Finding #5 (Medium):** Integer underflow on cyclic inputs. No reviewer considered what happens to the counter-based StrictDescends/StrictAscends detection under malformed input.

4. **Finding #8 (Medium):** Pre-migration data compatibility -- lww values without event_ids cause hard errors during layer application, blocking all backends. The migration gap was briefly mentioned but not identified as a hard failure.
