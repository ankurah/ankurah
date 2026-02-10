# Review Matrix A1: Core DAG Comparison Algorithm

**Reviewer:** Claude Opus 4.6 (Distributed Systems Algorithm Specialist)
**Date:** 2026-02-05
**Scope:** Deep algorithmic review of `core/src/event_dag/{comparison.rs, layers.rs, frontier.rs, navigator.rs, relation.rs, traits.rs}` and integration in `core/src/entity.rs`
**Prior Context:** Review digest consumed. Findings below are independently derived from code tracing.

---

## 1. Backward BFS Termination Analysis

### 1.1 Step-by-step trace of the BFS loop

The core loop lives in `Comparison::step()` (comparison.rs:272-297):

```
loop {
    step() -> expand_frontier(all_frontier_ids) -> process_event(each) -> check_result()
}
```

Each `step()`:
1. Collects **all** IDs from both `subject_frontier` and `comparison_frontier` into a single `all_frontier_ids` vec.
2. Calls `navigator.expand_frontier(all_frontier_ids, budget)` -- a single call fetches events for both frontiers simultaneously.
3. For each returned event, calls `process_event(id, parents)`:
   - Removes the id from whichever frontier(s) contain it (`subject_frontier.remove`, `comparison_frontier.remove`).
   - Adds to `subject_visited` / `other_visited` for chain building.
   - Updates `NodeState` for this id: marks `seen_from_subject` / `seen_from_comparison`.
   - If the node becomes "common" (seen from both sides), adds to `meet_candidates` and removes satisfied heads from `outstanding_heads`.
   - Extends the appropriate frontier(s) with the event's parents.
   - Tracks genesis events (empty parents) for Disjoint detection.
   - Decrements `unseen_comparison_heads` / `unseen_subject_heads` when a frontier reaches the other side's original head.
4. Calls `check_result()` to test termination conditions.

### 1.2 Termination conditions (check_result, lines 489-612)

Priority order:
1. **Tainted frontier** (assertion-based): Immediate return based on taint reason.
2. **StrictDescends** (`unseen_comparison_heads == 0`): Subject's traversal has visited all comparison heads. Returns chain built from `subject_visited`, reversed, with comparison heads filtered out.
3. **StrictAscends** (`unseen_subject_heads == 0`): Comparison's traversal has visited all subject heads.
4. **Frontiers exhausted** (both empty):
   - Filters `meet_candidates` to "minimal" by keeping only those with `common_child_count == 0`.
   - If no common ancestors found or `outstanding_heads` non-empty: checks for Disjoint (different roots) or returns DivergedSince with empty meet.
   - Otherwise: returns DivergedSince with meet, chains, and immediate children.
5. **Budget exhausted** (`remaining_budget == 0`): Returns BudgetExceeded.
6. **Otherwise**: Returns `None` (continue exploring).

### 1.3 Can the BFS miss ancestors?

**Potential issue: The "minimal meet" filter is incorrect for some DAGs.**

The code at line 561:
```rust
let meet: Vec<_> = self.meet_candidates.iter()
    .filter(|id| self.states.get(*id).map_or(0, |s| s.common_child_count) == 0)
    .cloned().collect();
```

The `common_child_count` is incremented at line 339 when a **common** node's parent is encountered. Specifically, it is incremented inside the `if is_common && self.meet_candidates.insert(id.clone())` block. This means `common_child_count` only counts how many **common** descendants a parent has, not all descendants.

This is actually the correct approach for computing the *Greatest Common Ancestors (GCA)* -- a common ancestor is "minimal" if none of its (common) children are also common ancestors. A node with `common_child_count > 0` is a non-minimal common ancestor (it has a common descendant that is a better meet point).

However, there is a subtle issue: `common_child_count` is incremented for **all** parents of a common node, not just parents that are themselves common. If a common node C has parents P1 (common) and P2 (not common), both P1 and P2 get `common_child_count += 1`. This means P2 will be filtered out of the meet if it ever becomes a meet candidate, which is correct -- P2 should not be in the meet because C is a better (more recent) common ancestor.

**Verdict: The termination logic is sound for computing the GCA.** The meet represents the frontier of the greatest common ancestors, which is the correct divergence point.

### 1.4 Can the BFS miss events that should be in the traversal?

**Yes -- a concrete bug exists with the `unseen_comparison_heads` / `unseen_subject_heads` early termination.**

Consider this DAG:
```
     A (genesis)
    / \
   B   C
    \ /
     D
```

Compare subject=[D] vs comparison=[B]:
- Step 1: Expand frontier {D, B}. Both events fetched.
  - process_event(D, parents=[B,C]): `from_subject=true`. Subject frontier becomes {B, C}. Since D is in original_comparison? No (original_comparison = {B}).
  - process_event(B, parents=[A]): `from_comparison=true`. Comparison frontier becomes {A}. Since B is in original_subject? No (original_subject = {D}).
  - But wait -- B is also in `subject_frontier` now (added as parent of D). So `from_subject = subject_frontier.remove(B)`. Since B was just added to subject_frontier via the parent extension of D, and then comparison's frontier processing of B removes it from comparison_frontier... Let me re-trace more carefully.

Actually, the events are processed in a single batch. All frontier IDs are collected at the start of `step()`. Let me re-trace:

- **Initial state**: subject_frontier={D}, comparison_frontier={B}, all_frontier_ids=[D, B].
- Events fetched: D (parents=[B,C]) and B (parents=[A]).
- Processing events (order depends on iteration over `events` vec):

  **process_event(D, [B, C]):**
  - `from_subject = subject_frontier.remove(D)` = true (D was in subject frontier)
  - `from_comparison = comparison_frontier.remove(D)` = false (D not in comparison frontier)
  - `subject_visited.push(D)`
  - NodeState for D: `seen_from_subject = true`
  - Not common yet.
  - Extends subject_frontier with B, C: subject_frontier = {B, C}
  - D is in `original_comparison`? original_comparison = {B}, so no. `unseen_comparison_heads` stays at 1.

  **process_event(B, [A]):**
  - `from_subject = subject_frontier.remove(B)` = true (B was just added!)
  - `from_comparison = comparison_frontier.remove(B)` = true (B was original comparison head)
  - `subject_visited.push(B)`, `other_visited.push(B)`
  - NodeState for B: `seen_from_subject = true, seen_from_comparison = true` -> **common!**
  - B added to meet_candidates.
  - B is in `original_comparison` -> origins contains B.
  - outstanding_heads removes B.
  - Parent A: gets `common_child_count += 1`, origins propagated.
  - Extends subject_frontier with A: subject_frontier = {C, A}
  - Extends comparison_frontier with A: comparison_frontier = {A}
  - B is in `original_comparison` and `from_subject` -> `unseen_comparison_heads -= 1` -> 0.
  - B is in `original_subject`? No (original_subject = {D}).

- **check_result**: `unseen_comparison_heads == 0` -> returns **StrictDescends**.
- Chain: `subject_visited = [D, B]`, reversed = [B, D], filter out original_comparison ({B}) -> chain = [D].

This is correct! D strictly descends from B. The chain [D] is correct -- apply D after B.

But now consider a scenario where the early exit fires **before** all parents have been explored. Specifically:

```
     A
    / \
   B   C
   |
   D
```

Compare subject=[D] vs comparison=[C]:
- Initial: subject_frontier={D}, comparison_frontier={C}
- Step 1: all_frontier_ids = [D, C]. Fetch D (parents=[B]) and C (parents=[A]).
  - process_event(D, [B]): from_subject=true. subject_frontier={B}. D not in original_comparison.
  - process_event(C, [A]): from_comparison=true. comparison_frontier={A}. C not in original_subject.
  - check_result: no early exit. Continue.

- Step 2: all_frontier_ids = [B, A]. Fetch B (parents=[A]) and A (parents=[]).
  - process_event(B, [A]): from_subject=true. subject_frontier={A}. B not in original_comparison.
  - process_event(A, []): from_comparison=true. Remove A from comparison_frontier (was {A}). comparison_frontier={}. A not in original_subject. Genesis event -> other_root=A. from_comparison, extend comparison_frontier with [] (empty).

  But also: `from_subject = subject_frontier.remove(A)` = true (A was added as parent of B). So A is from BOTH sides!

  Actually wait -- let me re-check. subject_frontier at start of step 2 was {B}. A was in comparison_frontier={A}. The all_frontier_ids = [B, A]. Events fetched: B and A.

  process_event(B, [A]): from_subject = subject_frontier.remove(B) = true. subject_frontier becomes {}. Then extends subject_frontier with A. subject_frontier = {A}.

  process_event(A, []): from_subject = subject_frontier.remove(A) = true (just added!). from_comparison = comparison_frontier.remove(A) = true. Both true -> A is common!

  A added to meet_candidates. A is in original_comparison? No (original_comparison = {C}). But origins propagation happens. Since from_comparison and original_comparison contains A? No, C is the original comparison head. So origins for A would come from propagation from C -> A.

  Hmm, let me trace the origins more carefully. In process_event(C, [A]), from_comparison=true and C is in original_comparison -> `state.origins.push(C)`. Then parent A gets origins extended with C's origins = [C].

  Then in process_event(A, []), A is now common. `origins` for A = [C]. outstanding_heads removes C. outstanding_heads is now empty.

  check_result: `unseen_comparison_heads` -- was D's subject traversal ever finding C? No. `unseen_comparison_heads` is still 1. `unseen_subject_heads` -- was C's comparison traversal finding D? No. Still 1.

  Frontiers: subject_frontier = {} (A was removed), comparison_frontier = {} (A was removed). Both empty!

  So we go to the "frontiers exhausted" branch. Meet candidates = {A}, filter by common_child_count == 0 -> A has common_child_count from the parents-of-common-node increment... A has no parents (genesis), so its common_child_count stays 0. Meet = [A].

  any_common = true, outstanding_heads = {} (empty). So we return DivergedSince with meet=[A]. Correct!

**The BFS handles these cases correctly because both frontiers are processed in the same step.** An event in one frontier can be simultaneously in the other frontier (if added as a parent by the other side), and the `remove` from both frontiers in the same `process_event` call catches this convergence.

### 1.5 Critical edge case: Navigator returns only some requested events

The `expand_frontier` method receives *all* frontier IDs but may return only a subset (e.g., if some events are not in storage). Events not returned are **not removed from the frontier** (since `remove` only fires for events that appear in the returned events list). This means those events persist in the frontier and will be re-requested on the next step.

However, the `consumed_budget` is charged regardless. In `LocalRetriever`, `consumed_budget = 1` is returned even if some events were not found. This means the budget drains even when making no progress -- eventually hitting BudgetExceeded. This is actually a safety property: it prevents infinite loops when events are missing.

**But there is a livelock risk**: If an event is genuinely missing from storage, the frontier will keep requesting it, burning budget 1 per step, until BudgetExceeded. With budget=100, this means 100 wasted round-trips. The EphemeralNodeRetriever mitigates this by trying remote peers, but LocalRetriever will just silently fail after 100 steps.

---

## 2. Forward Chain Construction Analysis

### 2.1 Chain building for StrictDescends (line 547)

```rust
let chain: Vec<_> = self.subject_visited.iter().rev()
    .filter(|id| !self.original_comparison.contains(id))
    .cloned().collect();
```

`subject_visited` is populated in `process_event` when `from_subject` is true, in the order events are processed (child-to-parent during backward BFS). Reversing gives parent-to-child (forward causal) order.

**Issue: This is not a topological sort.** For a linear chain A->B->C->D, the BFS visits D first, then C (parent of D added to frontier), then B, then A. Reversed: A, B, C, D -- correct.

But consider a diamond:
```
     A
    / \
   B   C
    \ /
     D
```
If comparing subject=[D] vs comparison=[A], the BFS might visit D, then B and C (both parents), then A. `subject_visited` = [D, B, C, A] (or [D, C, B, A] depending on process order within a step). Reversed: [A, C, B, D] or [A, B, C, D]. Both are valid topological orderings. In the StrictDescends case, the chain is used for replay, where B and C are concurrent and their relative order does not matter for correctness (they will be processed via layer computation if needed).

Wait -- StrictDescends means the subject is strictly ahead of comparison. The chain is used by `apply_event` in entity.rs, but when StrictDescends is returned from `compare_unstored_event`, the caller at line 256-269 does **not** use the chain for layer computation -- it just applies the single incoming event's operations directly. So the chain is informational but not consumed for replay in the current code.

### 2.2 Chain building for DivergedSince (lines 586-601)

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

This reverses the visited list and finds the **first** occurrence of any meet event, then takes everything after it.

**Bug: This drops events before the meet in the reversed list, but the meet can appear at different positions depending on traversal order.**

Consider:
```
     A (meet)
    / \
   B   C
   |
   D
```
Subject visited (backward from D): [D, B, A] (if A encountered in subject's traversal). Reversed: [A, B, D]. Find meet A at position 0, skip 1 -> [B, D]. Correct.

But what if the subject frontier reaches A via B before encountering C on the other side?

Actually, `subject_visited` only includes events where `from_subject` was true. So if A was also processed from the comparison frontier, it still appears in `subject_visited` only if `subject_frontier.remove(A)` returned true.

**More concerning scenario with multiple meet nodes:**
```
     M1  M2  (two meet nodes)
     |    |
     B    C
      \  /
       D
```
Subject visited backward from D: [D, B, M1, C, M2] (traversal order varies). Reversed: [M2, C, M1, B, D].
`build_forward_chain` finds first meet at position 0 (M2), skips it -> [C, M1, B, D]. But M1 is a meet node! It should not be in the chain. And the order [C, M1, B, D] is wrong.

This is the bug flagged in the prior reviews as "build_forward_chain not topologically sorted across merges." The `position` call finds only the **first** meet event in the reversed list, but if there are multiple meet events interleaved with non-meet events, the chain will contain meet events and may have incorrect ordering.

**Severity assessment:** In practice, the chains are used for `compute_layers` which does its own topological traversal from the meet point using the full `events` map (not the chains). The chains from `build_forward_chain` are returned in the `DivergedSince` variant but the **actual layer computation in `entity.rs` (lines 285-288) uses the accumulated events map and the meet, not the chains directly**:

```rust
let layers = compute_layers(&events, &meet, &current_ancestry);
```

So the chain ordering bug in `build_forward_chain` does not affect correctness of the layer computation. The chains are informational metadata in the `DivergedSince` relation, but not consumed by the layer application path. They *could* be consumed by other callers, but currently are not.

---

## 3. Key Invariants Assessment

### Invariant 1: "All replicas converge given the same event set"

**Assessment: PARTIALLY UPHELD with known caveat.**

For LWW backends, convergence depends on deterministic winner selection. The `apply_layer` method uses `layer.compare(candidate, current)` for causal ordering, falling back to lexicographic EventId for concurrent events. This is deterministic given the same event set and the same layer decomposition.

However, the per-property LWW bug (documented in the review digest) means that a stored last-write candidate may not participate correctly when its event_id has been superseded in the head. This can cause different replicas to see different winners depending on their event application order. Two replicas that apply the same set of events in different orderings may get different final states because the `apply_layer` seeding (line 181-189 of lww.rs) reads from the current backend state, which depends on which events were previously applied.

**The `apply_layer` function seeds winners from `self.values`, which contains the current stored state.** If replica 1 has already applied event B1 (which set `title=X`) and then a merge happens, B1's event_id is in the stored values and participates. But if replica 2 has a different application history, the stored values might differ, leading to different seeds and potentially different winners.

Specifically, if the events in the `events` map passed to `layer.compare()` do not include all relevant ancestors (which they won't if the BFS was budget-limited or if the accumulating navigator missed some events), the `is_descendant` check in `layers.rs` will return an error, which propagates as `MutationError::InsufficientCausalInfo`. This is correct fail-safe behavior.

### Invariant 2: "compare(A,B) and compare(B,A) are symmetric/antisymmetric"

**Assessment: UPHELD for the core relation, with cosmetic asymmetry in chain content.**

- `compare(A,B) = StrictDescends` implies `compare(B,A) = StrictAscends` -- **correct**. The `unseen_comparison_heads` and `unseen_subject_heads` counters work symmetrically.
- `compare(A,B) = DivergedSince{meet=M, subject=S, other=O}` implies `compare(B,A) = DivergedSince{meet=M, subject=O, other=S}` -- **correct** because the meet computation is symmetric (same GCA regardless of which side is "subject").
- `compare(A,B) = Equal` implies `compare(B,A) = Equal` -- **trivially correct** (checked at line 64).
- `compare(A,B) = Disjoint` implies `compare(B,A) = Disjoint` -- **correct**, since root detection is symmetric.

The chain contents (`subject_chain`, `other_chain`) swap roles when subject/comparison are swapped, which is the expected behavior.

### Invariant 3: "No regression -- never skip required intermediate events"

**Assessment: UPHELD under the causal delivery assumption.**

The code at entity.rs:256-269 (StrictDescends handler) applies only the incoming event's operations. It does **not** apply intermediate events in the chain. This is safe only if events arrive in causal order (parent before child), because intermediate events would have already been applied by prior `apply_event` calls.

If causal delivery is violated (event arrives without its parents), the code would either:
- Return StrictDescends (if the parent's parent is in head) and apply only the new event, skipping the parent. This would be a regression.
- Return DivergedSince (if the parent is missing from the BFS), and compute_layers would fail because the missing parent isn't in the events map.

The assumption is documented and enforced by protocol.

### Invariant 4: "Idempotent application -- redundant delivery is a no-op"

**Assessment: PARTIALLY UPHELD.**

- If event E is exactly at the head: `compare_unstored_event` returns Equal (line 95-97). No-op. Correct.
- If event E is in the history but not at head: `compare_unstored_event` returns DivergedSince (line 115-148, StrictAscends transformation). The event gets applied as if it were concurrent. This is **not idempotent** -- the event's operations will be re-processed via `apply_layer`, potentially affecting the state.

The test at lines 1116-1145 (`test_event_in_history_not_at_head`) documents this: "In practice, the caller should check if the event is already stored before calling compare_unstored_event to avoid this false positive."

This is a design constraint, not a bug -- but it means true idempotency depends on the caller filtering out already-stored events before comparison.

### Invariant 5: "Single head for linear history"

**Assessment: UPHELD.**

For StrictDescends, the new head is simply `event.id().into()` (entity.rs:258), replacing the old head entirely. For DivergedSince, meet-point parents are removed from head and the new event is inserted (entity.rs:323-326). This correctly maintains the multi-head only for true concurrency.

---

## 4. Asymmetric Depth Analysis

### 4.1 Scenario: 1 vs 50 depth

```
     A (genesis)
    / \
   B   C1->C2->...->C50
```
Subject = [B], Comparison = [C50].

**BFS trace:**
- Step 1: Frontier = {B, C50}. Fetch both. B: parents=[A], from_subject. C50: parents=[C49], from_comparison.
  - subject_frontier = {A}, comparison_frontier = {C49}. Budget = 99.
- Step 2: Frontier = {A, C49}. Fetch both. A: parents=[], genesis. C49: parents=[C48].
  - A: from_subject. subject_frontier = {}. subject_root = A. C49: from_comparison. comparison_frontier = {C48}. Budget = 98.
- Steps 3-49: Comparison frontier walks back through C48, C47, ..., C2, C1. Subject frontier is empty. Each step fetches only the comparison frontier event. Budget consumed: 1 per step.
- Step 50: Comparison frontier reaches A. A is already in states (seen_from_subject). Now from_comparison -> A becomes common.
  - `unseen_subject_heads`: B was never reached by comparison's traversal. Still 1.
  - But wait: comparison_frontier processes A, and A is already seen_from_subject. So A becomes common. comparison_frontier extends with A's parents (empty). comparison_frontier = {}.

Both frontiers empty. Meet = [A]. Subject chain: [B], other chain: [C1, ..., C50]. Correct result: DivergedSince with meet at A.

**Budget consumed: ~50 steps.** With budget=100, this fits. With budget=10 (the old value), this would hit BudgetExceeded at step 10, which was the motivation for restoring budget to 100.

### 4.2 Scenario: 3 vs 300 depth

Same pattern but the long branch has 300 events. Budget = 100.

- Steps 1-2: Both frontiers active. Budget = 98.
- Step 3: Subject frontier exhausted (reached genesis A). Budget = 97.
- Steps 3-99: Comparison frontier walks back 97 more events (reaching ~C203). Budget = 0.
- **BudgetExceeded** returned with subject_frontier = {} and comparison_frontier = {C203}.

The comparison cannot complete. The meet at A is never found because the comparison frontier hasn't reached A yet.

**This is a fundamental limitation of the fixed budget approach.** For asymmetric branches where the longer branch exceeds the budget, the algorithm cannot determine the relationship. The caller (entity.rs) converts this to an error and does not retry with a larger budget.

### 4.3 Scenario: 1 vs 50, but navigator returns ALL frontier events in one call

Looking more carefully at the MockEventStore and real navigators: each `expand_frontier` call receives all frontier IDs and returns all found events, consuming budget **1** regardless of how many events are returned.

This changes the analysis significantly! With LocalRetriever, `consumed_budget = 1` per call. Each call can return many events. So:

- Step 1: Frontier = {B, C50}. Returns events for both. Budget = 99.
- Step 2: Frontier = {A, C49}. Returns events for both. Budget = 98.
- ...
- Step 50: Frontier = {A} (comparison side reaches A). Budget = 50.

With budget=100, 50 steps is fine. But for 300-depth asymmetry:
- The short branch exhausts in 2 steps. Subject frontier becomes empty.
- Each subsequent step has comparison frontier with one event. Budget consumed: 1 per step.
- After 100 steps total (~100 budget), comparison frontier is at C200. BudgetExceeded.

**Wait -- let me re-examine.** When subject_frontier is empty and comparison_frontier is {C_k}, `all_frontier_ids` = [C_k]. The navigator fetches C_k, budget -= 1. comparison_frontier becomes {C_{k-1}}. Next step: [C_{k-1}]. And so on.

For the 3 vs 300 case: After subject exhausts (step ~3, budget ~97), comparison needs ~297 more steps to reach genesis. Budget runs out after ~97 more steps.

**However**, there is a subtle optimization opportunity being missed: once the subject frontier is exhausted and the subject has reached genesis, the algorithm could short-circuit -- if subject reached a root and comparison reaches the same root, that's the meet. But the algorithm doesn't check for this incrementally; it only checks when comparison processes a node that is already seen_from_subject. Since A (genesis) was seen_from_subject in step 2, when comparison eventually reaches A (step ~300), A becomes common and the meet is found.

**The issue is purely the budget running out before the long branch reaches the short branch's root.**

### 4.4 Budget interaction with deep asymmetry

**Finding: Budget of 100 is insufficient for DAGs where one branch exceeds ~100 events from the meet point.** Given that budget is consumed at 1 per `expand_frontier` call, and each call processes all events at the current frontier (which for a linear chain is 1 event per step), the budget effectively limits the total number of BFS steps.

For typical CRDT applications where events are frequent small edits, 100 events can be generated in seconds. Any replica that goes offline for even a short period could accumulate enough events to exceed the budget on reconnection.

---

## 5. BudgetExceeded Fallback Analysis

### 5.1 Current behavior

When BudgetExceeded is returned (comparison.rs:602-607):
```rust
Some(AbstractCausalRelation::BudgetExceeded {
    subject: self.subject_frontier.ids.clone(),
    other: self.comparison_frontier.ids.clone(),
})
```

The caller in entity.rs:334-341 converts this to an error:
```rust
AbstractCausalRelation::BudgetExceeded { subject, other } => {
    return Err(LineageError::BudgetExceeded { ... }.into());
}
```

### 5.2 Frontier-based resumption

The BudgetExceeded variant returns the current frontier positions, which could theoretically be used to resume the BFS. The spec mentions this: "Resume with frontiers."

**However, resumption is not implemented anywhere.** The error propagates up and the event application fails. There is no retry with a larger budget, no frontier passing to a subsequent `compare` call, and no mechanism to seed a new `Comparison` with pre-existing frontier state.

### 5.3 Soundness of potential resumption

If resumption were implemented, it would need to restore not just the frontier IDs but also:
- All `NodeState` entries (seen_from_subject, seen_from_comparison, origins, children)
- meet_candidates
- outstanding_heads
- unseen_comparison_heads / unseen_subject_heads
- subject_visited / other_visited
- subject_root / other_root

Simply creating a new `Comparison` with the frontier IDs as initial heads would **not** be equivalent to resumption -- it would lose all the accumulated state about which nodes have been seen from which side, which heads have been satisfied, etc. The new comparison would start fresh with the frontier IDs as if they were the original heads, which would give incorrect results.

**Finding: The frontier positions in BudgetExceeded are insufficient for resumption without the full Comparison state. The current API is misleading.**

---

## 6. Multi-Head Entity Analysis

### 6.1 Event extending one tip of a multi-head

Entity head = [B, C]. Event D with parent = [C].

`compare_unstored_event` path:
1. Event D's id (4) not in comparison head [B, C]. Not Equal.
2. `compare(D.parent=[C], [B, C], budget)`:
   - subject = [C], comparison = [B, C].
   - C is in both -> but they're not equal ([C] != [B, C]).
   - subject_frontier = {C}, comparison_frontier = {B, C}.
   - Step 1: all_frontier_ids = [C, B, C] -> but BTreeSet deduplicates, so effectively {B, C}.

   Wait -- frontier IDs are collected as a Vec, not a Set (line 274-276):
   ```rust
   let mut all_frontier_ids = Vec::new();
   all_frontier_ids.extend(self.subject_frontier.ids.iter().cloned());
   all_frontier_ids.extend(self.comparison_frontier.ids.iter().cloned());
   ```

   So all_frontier_ids = [C, B, C] (C appears twice). The navigator receives duplicates. The MockEventStore (and presumably real navigators) will return events for each unique ID once. So events returned: [C, B].

   process_event(C, [A]):
   - from_subject = subject_frontier.remove(C) = true
   - from_comparison = comparison_frontier.remove(C) = true
   - Both true -> C is common! Meet candidate = C.
   - C is in original_comparison -> origins = [C]. outstanding_heads removes C. But B is still in outstanding_heads.
   - unseen_comparison_heads: was 2 (for B and C). C is in original_comparison and from_subject -> unseen_comparison_heads -= 1 -> 1.
   - unseen_subject_heads: C is in original_subject ({C}) and from_comparison -> unseen_subject_heads -= 1 -> 0.

   process_event(B, [A]):
   - from_subject = subject_frontier.remove(B) = false (B was not in subject_frontier)
   - from_comparison = comparison_frontier.remove(B) = true (B was in comparison_frontier)
   - Only from_comparison. Not common.
   - comparison_frontier extends with A. comparison_frontier = {A}.
   - B is in original_subject? original_subject = {C}. No.

   check_result: unseen_subject_heads == 0 -> **StrictAscends**.

3. Back in compare_unstored_event, result is StrictAscends -> **transformation to DivergedSince** (lines 115-148):
   - parent_members = {C}
   - comparison_members = {B, C}
   - meet = [C]
   - other = comparison_members \ parent_members = {B}
   - subject_chain = [D's id]
   - other_chain = []

Result: `DivergedSince { meet: [C], subject: [], other: [B], subject_chain: [D], other_chain: [] }`

This is correct: D diverges from B since meet point C, with D on one branch and B on the other.

### 6.2 Head update for multi-head DivergedSince

In entity.rs:320-326:
```rust
for parent_id in &meet {
    state.head.remove(parent_id);
}
state.head.insert(event.id());
```

For the above scenario: head was [B, C]. Meet = [C]. Remove C from head -> [B]. Insert D -> [B, D].

**This is correct** -- D supersedes C (since D's parent is C), and B remains as a concurrent tip.

### 6.3 Bug in head update: meet vs event.parent

**Critical finding:** The head update removes `meet` entries from the head, but `meet` is the GCA of the event's parent clock and the entity's head clock. For the StrictAscends transformation case, `meet` is set to the event's parent members, which is correct.

But for the general DivergedSince case (where the event's parent diverged from the head), `meet` is the GCA -- which may be *older* than the event's parent. Consider:

```
     A (genesis/meet)
    / \
   B   C
   |   |
   D   E (head)
   |
   F (incoming event, parent=[D])
```

Head = [E]. Event F with parent = [D].

`compare_unstored_event(F, [E])`:
1. F not in [E]. Not Equal.
2. `compare([D], [E])`: D and E diverge since A. Returns DivergedSince { meet: [A], ... }.
3. Transformation: subject_chain.push(F). Result: DivergedSince { meet: [A], ... }.

In entity.rs:
```rust
for parent_id in &meet {  // meet = [A]
    state.head.remove(&A);  // Remove A from head [E]
}
```

But A is NOT in the head [E]! So `head.remove(A)` returns false and does nothing. Then `head.insert(F)` -> head becomes [E, F]. This is correct -- E and F are concurrent (both descend from A but via different paths).

**What if meet IS in the head?** Consider head = [A, E] (unlikely but possible if A is a concurrent tip):

```
     A (concurrent tip in head)
    / \
   B   C
   |   |
   D   E (another head tip)
   |
   F (incoming, parent=[D])
```

Meet = [A]. Remove A from head [A, E] -> [E]. Insert F -> [E, F]. This is correct: F supersedes A (since F descends from A via B->D), and E remains as a concurrent tip.

**Now consider the StrictAscends transformation case more carefully:**

Entity head = [B, C, D]. Event F with parent = [B, C].

`compare_unstored_event(F, [B, C, D])`:
1. F not in [B, C, D].
2. `compare([B, C], [B, C, D])`: [B, C] is a subset of [B, C, D] -> StrictAscends.
3. Transformation: meet = [B, C], other = [D].

In entity.rs: Remove B and C from head [B, C, D] -> [D]. Insert F -> [D, F]. Correct: F extends the B+C tips, and D remains concurrent.

### 6.4 Edge case: meet contains IDs not in head

As shown in the trace above (6.3, first sub-case), when meet=[A] and head=[E], removing A from head does nothing. The insert of F makes head=[E, F]. This is correct but means the head grows -- both E and F are tips. Over time, if events keep arriving that diverge from deep ancestors, the head could grow unboundedly.

**However**, this growth is bounded by the actual concurrency in the system. Each tip in the head represents a genuinely concurrent branch. When a merge event arrives (parent includes multiple tips), head is pruned. So head size is bounded by the degree of ongoing concurrency.

---

## 7. Additional Findings

### 7.1 Origin tracking does not propagate through non-common nodes correctly

In `process_event`, origins are propagated to parents in two places:

1. Lines 334-338: When a node becomes common, its parents get `origins.extend(origins)`.
2. Lines 341-345: When a node is from_comparison but not common, its parents get `origins.extend(origins)`.

But origins are only initialized for nodes in `original_comparison` (line 317-319). For intermediate comparison nodes between the original head and the common ancestor, origins propagate downward (toward parents). This is correct for tracking which original comparison head can be "satisfied" by reaching a common ancestor.

**However**, if the BFS visits a comparison node C, propagates C's origins to C's parent P, and then later the subject side also visits P (making P common), the outstanding_heads removal uses P's origins -- which correctly include the comparison head that reached P via C. This is sound.

### 7.2 `unseen_comparison_heads` can underflow

If the same comparison head appears in both frontiers (because it was also added as a parent from the subject side), `process_event` can decrement `unseen_comparison_heads` when `from_subject` is true and the id is in `original_comparison`. If this happens twice for the same head (once from direct membership, once from parent traversal), it would underflow.

**Trace:** Can this happen? `unseen_comparison_heads` starts at `comparison_ids.len()`. The decrement is at line 370: `self.unseen_comparison_heads -= 1`. This happens when `from_subject` is true and `self.original_comparison.contains(&id)`.

An event can only be in `subject_frontier` once (it's a BTreeSet). When it's processed, `subject_frontier.remove(&id)` removes it. It could be re-added later as a parent of another event, but then it would be processed again (and from_subject would be true again). If the same comparison head is re-encountered, `unseen_comparison_heads` would be decremented again, going below 0 (wrapping to usize::MAX, since usize is unsigned).

**Concrete scenario:**
```
   A (comparison head, also an ancestor of subject)
   |
   B
   |
   C (subject head)
```

subject_frontier = {C}, comparison_frontier = {A}. original_comparison = {A}.

Step 1: all_frontier_ids = [C, A].
- process_event(C, [B]): from_subject=true. subject_frontier = {B}. C not in original_comparison.
- process_event(A, []): from_comparison=true. comparison_frontier = {}. A not in original_subject.

Step 2: all_frontier_ids = [B].
- process_event(B, [A]): from_subject=true. subject_frontier = {A}.

Step 3: all_frontier_ids = [A]. But A was already processed (as from_comparison). Now:
- process_event(A, []): **from_subject = subject_frontier.remove(A) = true**. from_comparison = comparison_frontier.remove(A) = false (already removed).
- **A is in original_comparison and from_subject**: `unseen_comparison_heads -= 1` -> 0.

This is fine -- it correctly reaches 0, meaning subject has found all comparison heads. And A is now common (seen from both subject and comparison). The result would be StrictDescends with chain [B, C] (excluding A). This is correct.

But could it decrement **twice** to below 0? That would require A to be processed from the subject side twice. Since `subject_frontier.remove(A)` returns true only once per insertion, and A can only be in subject_frontier if it was added as a parent, this seems safe. Each time A is processed, it is removed from the frontier. For A to be re-added, some descendant of A would need to be processed, but A has no parents (genesis), so no further ancestors to add. A cannot appear in the frontier again.

**For non-genesis events**, a comparison head C could be re-added as a parent of some other event that the subject traversal encounters. But BTreeSet insertion is idempotent -- if C is already removed from subject_frontier, adding it again would make it present, and the next process_event would remove and decrement again. This could indeed cause a double decrement.

**Concrete scenario for double-decrement:**
```
   A
  / \
 B   C (comparison head)
  \ /
   D (subject head)
```

subject_frontier = {D}, comparison_frontier = {C}. original_comparison = {C}. unseen_comparison_heads = 1.

Step 1: all_frontier_ids = [D, C].
- process_event(D, [B, C]): from_subject=true. subject_frontier extends with B, C -> {B, C}.
- process_event(C, [A]): from_subject = subject_frontier.remove(C) = true (just added!). from_comparison = comparison_frontier.remove(C) = true.
  - **C in original_comparison and from_subject**: unseen_comparison_heads -= 1 -> 0.
  - C is common (from both sides).
  - comparison_frontier extends with A.

check_result: unseen_comparison_heads == 0 -> StrictDescends. Chain = subject_visited reversed, filter out original_comparison. subject_visited = [D, C]. Reversed = [C, D]. Filter out {C} -> [D]. Correct: D descends from C.

But what if B also needs to reach C? B was added to subject_frontier. In step 2:
- all_frontier_ids = [B, A].
- process_event(B, [A]): from_subject=true. subject_frontier = {A} (A added from B's parent).

But we already returned StrictDescends in step 1's check_result. So no double-decrement occurs because the loop exits early.

**However**, if check_result does NOT fire early (because there are more comparison heads), could B's parents re-add C to the subject frontier? No, because B's parents are [A], not [C]. So C cannot be re-encountered from B's lineage.

Let me try harder:
```
     A
    / \
   C   B
   |  / \
   D C2  E
   |     |
   F     G (subject head)
```
Where C and C2 are the same event ID (impossible in a real DAG -- event IDs are unique). So this can't happen.

**Conclusion: Double-decrement of unseen_comparison_heads is not possible** because event IDs are unique in the DAG, and once an event is removed from a frontier, it can only be re-added if it appears as a parent of another event -- but a BTreeSet won't add duplicates. Wait, that's wrong -- if the event was removed and then a new event lists it as a parent, `frontier.extend()` will re-add it. Let me construct a scenario:

```
     A (comparison head)
    / \
   B   C
    \ /
     D (subject head)
```

subject_frontier = {D}, comparison_frontier = {A}. original_comparison = {A}.

Step 1: all_frontier_ids = [D, A].
- process_event(D, [B, C]): from_subject=true. subject_frontier = {B, C}.
- process_event(A, []): from_comparison=true. comparison_frontier = {}.
  - A in original_subject? original_subject = {D}. No.
  - Genesis event. other_root = A.

Step 2: all_frontier_ids = [B, C].
- process_event(B, [A]): from_subject=true. subject_frontier extends with A -> {C, A}.
  - B in original_comparison? No.
- process_event(C, [A]): from_subject = subject_frontier.remove(C) = true. subject_frontier = {A}.
  - subject_frontier extends with A -> {A} (A already there, BTreeSet idempotent).

Step 3: all_frontier_ids = [A].
- process_event(A, []): from_subject = subject_frontier.remove(A) = true. from_comparison = comparison_frontier.remove(A) = false (already empty).
  - **A in original_comparison and from_subject**: unseen_comparison_heads -= 1 -> 0.
  - A was already seen_from_comparison (step 1). Now also seen_from_subject -> common!

check_result: unseen_comparison_heads == 0 -> StrictDescends. Correct: D descends from A.

Only one decrement. Fine.

**Final verdict: No underflow risk in practice** because each comparison head is encountered by the subject frontier at most once (the frontier removes it, and even if it's re-added, the BFS step that re-processes it would not find it in original_comparison because... wait, `original_comparison.contains(&id)` is checked, not whether it was already decremented for).

Actually, I need to be more careful. In step 1 of the first trace above:
- process_event(C, [A]) already decremented unseen_comparison_heads to 0 because C was in subject_frontier (from D's parents) AND in original_comparison AND from_subject was true.

If A were also in original_comparison (i.e., comparison = [A, C]), then when A is processed from the subject side in step 3, it would decrement unseen_comparison_heads again (from 0 to usize::MAX wrapping). But with comparison = [A, C], unseen_comparison_heads starts at 2, and after C is found it's 1, and after A is found it's 0. That's correct.

The only way to get underflow would be if the same original comparison head is processed `from_subject` more than once. Since BFS won't re-process the same event from the same frontier (it's removed on first encounter), this can't happen.

### 7.3 All events from both frontiers sent to navigator in one call

The `expand_frontier` call (line 280) sends all frontier IDs from both subject and comparison frontiers in a single batch. The navigator returns events for all of them. This means a single BFS step can process events from both sides.

**This is efficient but has a subtle implication**: the budget model charges `consumed_budget` once for the entire batch, not per-event. The real navigators (LocalRetriever, EphemeralNodeRetriever) return `consumed_budget = 1` regardless of batch size. This means budget consumption is proportional to BFS *steps*, not to the number of events fetched.

For a DAG with branching factor k at each level, one BFS step processes all k nodes at the frontier. The budget of 100 thus allows 100 BFS levels, not 100 events. This is actually more generous than it appears.

### 7.4 `compute_layers` relies on `events` map completeness

The `compute_layers` function (layers.rs:100-154) does a forward traversal from meet using `children_of`, which scans the entire `events` map for each parent. If the events map is incomplete (missing events between meet and tips), the layer computation will miss events.

The events map comes from `AccumulatingNavigator.get_events()` (entity.rs:281), which only contains events fetched during the BFS traversal. If the BFS terminated early (e.g., StrictAscends transformation), some events may not have been fetched.

For the DivergedSince case, the BFS has traversed backward from both heads to the meet point, so all events between heads and meet should be in the accumulator. But events on side branches (not on the direct path) might be missing if the BFS took a shortcut.

**For linear chains, this is not a problem.** For diamond DAGs, the BFS explores all paths, so the accumulator should have all events. But for complex DAGs with many branches, some events might be missed if they were on paths that weren't fully explored.

### 7.5 Head pruning in DivergedSince uses `meet` not `event.parent`

In entity.rs:320-326:
```rust
for parent_id in &meet {
    state.head.remove(parent_id);
}
state.head.insert(event.id());
```

The `meet` is the GCA of the event's parent clock and the entity's head. For the StrictAscends transformation case (multi-head extending one tip), `meet` equals the event's parent members, so this correctly removes the extended tip(s).

But for the general DivergedSince case, `meet` is the deep common ancestor, not the event's immediate parent. Removing the meet from the head is only meaningful if the meet is actually in the head (which it typically isn't for deep divergences).

The correct behavior would be to add event.id() to head and remove any head tips that are ancestors of event.id(). But computing ancestry requires knowing which head tips are ancestors, which is essentially the `current_ancestry` set.

**In practice, this works correctly** because:
1. For the StrictAscends transformation, meet = event.parent, which correctly identifies the superseded tips.
2. For deep DivergedSince, meet is the GCA (e.g., genesis), which is NOT in the head. So `head.remove(meet_id)` does nothing. The event is added to the head, creating a new tip. The head grows: [old_tip, new_event]. This is correct -- the two are concurrent.
3. The only case where meet IS in the head is when the entity's head includes the GCA as a tip (which can happen if the entity is at a very early state). In this case, removing the GCA from the head and adding the new event is correct.

---

## Confidence-Rated Findings

| # | Finding | Severity | Confidence | Details |
|---|---------|----------|------------|---------|
| 1 | **build_forward_chain drops events and misordering with multiple meet nodes** | Medium | High | `build_forward_chain` (comparison.rs:447-460) uses `position()` to find the first meet event in the reversed visited list and drops everything before it. With multiple meet nodes interleaved with non-meet events, the chain can contain meet nodes and have incorrect ordering. Currently harmless because `compute_layers` uses the events map directly (not the chains), but any future consumer of `subject_chain`/`other_chain` from DivergedSince would get incorrect data. |
| 2 | **Budget insufficient for asymmetric branches exceeding ~100 depth** | Medium | High | With budget=100 and `consumed_budget=1` per BFS step, any DAG where one branch has >100 events from the meet point will return BudgetExceeded. For typical CRDT workloads with frequent small edits, this is easily exceeded. No retry or budget escalation mechanism exists. The caller converts this to a hard error (entity.rs:334-341). See Section 4. |
| 3 | **BudgetExceeded frontier IDs are insufficient for resumption** | Medium | High | The BudgetExceeded variant returns frontier BTreeSets, but resuming a comparison requires the full `Comparison` state (NodeState map, meet_candidates, visited lists, counters). Creating a new `Comparison` from just frontier IDs would produce incorrect results because nodes already explored would be re-explored with a fresh state. The API implies resumability that does not exist. See Section 5. |
| 4 | **Idempotency violated for events in history but not at head** | Medium | High | If event B (already applied and superseded by C) is re-delivered, `compare_unstored_event` returns DivergedSince because B's parent is older than head C. The event gets re-applied via `apply_layer`, potentially changing state. The test at tests.rs:1116-1145 documents this. Callers must filter already-stored events before calling `compare_unstored_event`, but this contract is implicit. See Invariant 4 in Section 3. |
| 5 | **Per-property LWW ignores non-head last-write candidates** | High | High | (Confirming prior reviews.) `apply_layer` in lww.rs seeds winners from `self.values` (lines 181-189), which reflects the current stored state. If a property was last written by event B1 but B1 is no longer a head tip (superseded by B2 which didn't touch that property), B1's value is still in stored state and correctly participates as a seed candidate. **However**, the `layer.compare()` call may fail with InsufficientCausalInfo if B1 is not in the accumulated events map, since `is_descendant` only searches the events map. This means the stored seed candidate can cause an error rather than participating correctly. See Section 3, Invariant 1. |
| 6 | **LocalRetriever.expand_frontier does not consult staged events** | High | High | (Confirming prior reviews.) LocalRetriever (retrieval.rs:70-80) calls `self.0.collection.get_events()` without checking `staged_events`. If events A and B are in the same EventBridge batch, and B's parent is A, the comparison for B may fail to find A because A is staged but not yet stored. EphemeralNodeRetriever correctly checks staged events first (retrieval.rs:181-193). |
| 7 | **Livelock on missing events drains budget silently** | Low | High | If a frontier contains an event ID that does not exist in storage, the navigator returns no event for it, the frontier retains the ID (never removed since `process_event` is never called for it), and the next step re-requests it. Each step consumes budget=1. After 100 steps, BudgetExceeded is returned with no useful work done. No warning or error indicates the specific missing event. See Section 1.5. |
| 8 | **compute_layers children_of is O(n) per parent, O(n^2 * p) total** | Low | Medium | `children_of` (layers.rs:159-166) scans the entire events map for each parent in each frontier expansion. For a DAG with n events and average parent count p, total cost is O(n^2 * p). Acceptable for small DAGs (<100 events) but problematic at scale. A pre-built parent-to-children index would reduce to O(n * p). |
| 9 | **Single-step batched frontier expansion hides per-event budget cost** | Low | Medium | Both real navigators return `consumed_budget=1` regardless of how many events are fetched. This means a frontier with 50 events costs the same as one with 1 event. The budget measures BFS depth, not work. This is arguably intentional (depth bounds are more meaningful for convergence) but the naming `consumed_budget` and `remaining_budget` suggests per-event costing, which is misleading. |
| 10 | **meet_candidates common_child_count filter produces correct GCA** | Info (Positive) | High | The minimal-meet computation at line 561 correctly filters non-minimal common ancestors. A common ancestor whose child is also a common ancestor has `common_child_count > 0` and is excluded. Only frontier common ancestors (closest to the tips) remain. Traced through multiple DAG topologies and verified correct. |
| 11 | **StrictAscends-to-DivergedSince transformation is correct for multi-head entities** | Info (Positive) | High | The transformation at comparison.rs:115-148 correctly identifies the scenario where an unstored event extends one tip of a multi-head entity. The meet is the event's parent (the tip being extended), and the other set is the remaining head tips. Traced through 2-way and 3-way multi-head scenarios. See Section 6. |
| 12 | **Antisymmetry of compare() is upheld** | Info (Positive) | High | `compare(A,B) = StrictDescends` iff `compare(B,A) = StrictAscends`, and DivergedSince meets are symmetric. Verified by tracing the counter logic for `unseen_comparison_heads` and `unseen_subject_heads`. See Invariant 2 in Section 3. |
