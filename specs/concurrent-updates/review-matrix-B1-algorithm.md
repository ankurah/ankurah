# Deep Algorithm Review: DAG Comparison (Matrix B1)

**Reviewer:** Algorithm specialist (distributed systems / causal DAGs)
**Scope:** Core BFS comparison, chain construction, layer computation, invariants
**Files reviewed:**
- `/Users/daniel/ak/ankurah-201/core/src/event_dag/comparison.rs`
- `/Users/daniel/ak/ankurah-201/core/src/event_dag/navigator.rs`
- `/Users/daniel/ak/ankurah-201/core/src/event_dag/frontier.rs`
- `/Users/daniel/ak/ankurah-201/core/src/event_dag/layers.rs`
- `/Users/daniel/ak/ankurah-201/core/src/event_dag/relation.rs`
- `/Users/daniel/ak/ankurah-201/core/src/event_dag/traits.rs`
- `/Users/daniel/ak/ankurah-201/core/src/event_dag/tests.rs`
- `/Users/daniel/ak/ankurah-201/core/src/entity.rs`
- `/Users/daniel/ak/ankurah-201/core/src/retrieval.rs`
- `/Users/daniel/ak/ankurah-201/core/src/property/backend/lww.rs`
- `/Users/daniel/ak/ankurah-201/core/src/property/backend/mod.rs`

**Seed context:** `review-holistic-code.md` (read and used as starting reference)

---

## 1. Backward BFS Step-by-Step Trace

### 1.1 Basic Linear Case: compare([C], [A]) where A->B->C

**Step 0 (init):**
- subject_frontier = {C}, comparison_frontier = {A}
- unseen_comparison_heads = 1 (need subject to see A)
- unseen_subject_heads = 1 (need comparison to see C)

**Step 1:** `expand_frontier` called with `[C, A]`. Both events fetched.
- `process_event(C, [B])`: from_subject=true, from_comparison=false. subject_visited=[C]. subject_frontier becomes {B}. C is not in original_comparison, so unseen_comparison_heads stays 1.
- `process_event(A, [])`: from_subject=false, from_comparison=true. other_visited=[A]. comparison_frontier becomes {}. A is not in original_subject, so unseen_subject_heads stays 1. A has empty parents, so other_root=A.
- `check_result()`: unseen_comparison=1, unseen_subject=1. subject_frontier={B}, comparison_frontier={}. Not both empty, not budget-exceeded. Return None (continue).

**Step 2:** `expand_frontier` called with `[B]`. B fetched.
- `process_event(B, [A])`: from_subject=true, from_comparison=false. subject_visited=[C,B]. subject_frontier becomes {A}. B is not in original_comparison.
- `check_result()`: subject_frontier={A}, comparison_frontier={}. Continue.

**Step 3:** `expand_frontier` called with `[A]`. A fetched (again, but frontier.remove returns true because A was added by subject).
- `process_event(A, [])`: from_subject=true (was in subject_frontier), from_comparison=false (already removed). subject_visited=[C,B,A]. A IS in original_comparison, so unseen_comparison_heads becomes 0.
- Node state for A: already seen_from_comparison=true (from step 1), now seen_from_subject=true. is_common()=true. But is_common detection happens in the NodeState check -- wait, let me re-read this.

Actually, looking more carefully: in step 1, when A was processed, `from_comparison=true` but A was not yet in subject_frontier. In step 3, `from_subject=true` because A is in subject_frontier (it was added as B's parent). So now A has `seen_from_subject=true` AND `seen_from_comparison=true`. `is_common()=true`. A gets added to `meet_candidates`. A's origins -- were they set? In step 1, A is an original_comparison member AND from_comparison=true, so `state.origins.push(A)`. In step 3, `is_common=true`, so `outstanding_heads.remove(A)` is called.

But: `check_result()` runs first checking `unseen_comparison_heads == 0`, which is true. So it returns `StrictDescends` with chain = subject_visited reversed and filtered. subject_visited=[C,B,A], reversed=[A,B,C], filter out original_comparison {A}, chain=[B,C].

**Verdict:** Correct. The chain [B,C] is the forward path from A to C's position.

### 1.2 Concurrent Case: compare([D], [E]) where A->B->D and A->C->E

**Step 0:** subject_frontier={D}, comparison_frontier={E}. unseen_comparison=1, unseen_subject=1.

**Step 1:** expand [D, E].
- process_event(D, [B]): from_subject=true. subject_visited=[D]. subject_frontier={B}.
- process_event(E, [C]): from_comparison=true. other_visited=[E]. comparison_frontier={C}.
- check_result: continue.

**Step 2:** expand [B, C].
- process_event(B, [A]): from_subject=true. subject_visited=[D,B]. subject_frontier={A}.
- process_event(C, [A]): from_comparison=true. other_visited=[E,C]. comparison_frontier={A}.
- check_result: continue.

**Step 3:** expand [A, A] -- but A appears only once in the combined frontier since it's in both. Actually, `all_frontier_ids` extends from both BTreeSets, so A appears twice. The navigator returns A's event once (or twice depending on implementation -- the MockEventStore iterates frontier_ids, so it processes A twice but HashMap lookup returns the same event). Then `process_event(A, [])` is called once (since the navigator deduplicates events -- actually no, it iterates `frontier_ids` and produces an event per match). Let me re-check.

Looking at MockEventStore::expand_frontier: it iterates `frontier_ids` and for each, if found in `self.events`, pushes the payload. So if A appears twice in frontier_ids, it gets pushed twice. Then in `step()`, the events vec has two copies of A. `process_event` is called twice for A.

First call: `process_event(A, [])`: from_subject = subject_frontier.remove(A) = true. from_comparison = comparison_frontier.remove(A) = true. subject_visited=[D,B,A], other_visited=[E,C,A]. A is in original_comparison, so unseen_comparison_heads becomes 0. A is in original_subject, so unseen_subject_heads becomes 0. NodeState: seen_from_subject=true, seen_from_comparison=true, is_common=true. origins: A is original_comparison member AND from_comparison=true, so origins=[A]. meet_candidates gets A. outstanding_heads.remove(A). Empty parents, so subject_root=A, other_root=A.

Second call: `process_event(A, [])`: from_subject = subject_frontier.remove(A) = FALSE (already removed). from_comparison = comparison_frontier.remove(A) = FALSE. Neither is true, so nothing happens (no visited push, no marks, no frontier extension).

check_result: unseen_comparison_heads=0, returns StrictDescends! This is **wrong** for a concurrent case.

Wait -- unseen_subject_heads is also 0. Let me re-read: the check order is:
1. Check tainting (no)
2. Check unseen_comparison_heads == 0 -> StrictDescends
3. Check unseen_subject_heads == 0 -> StrictAscends

Since unseen_comparison_heads hits 0 first (checked first), it returns StrictDescends. But the correct answer is DivergedSince!

**Hold on.** Let me re-examine. When A is processed in step 3, `from_subject=true` because A was in subject_frontier. `self.original_comparison.contains(&A)` -- is A in original_comparison? A is ID 1. original_comparison is the members of the comparison clock, which is {E}={5}. A=1 is NOT in original_comparison. So unseen_comparison_heads does NOT decrement.

I was confused -- I need to use concrete IDs. Let me redo:

**DAG:** 1->2->4 and 1->3->5. compare([4], [5]).

original_subject = {4}, original_comparison = {5}.

**Step 1:** expand [4, 5].
- process_event(4, [2]): from_subject=true. subject_frontier={2}. 4 is NOT in original_comparison ({5}). unseen_comparison stays 1.
- process_event(5, [3]): from_comparison=true. 5 IS in original_subject ({4})? No, 5 != 4. unseen_subject stays 1. comparison_frontier={3}.

**Step 2:** expand [2, 3].
- process_event(2, [1]): from_subject=true. subject_frontier={1}. 2 not in original_comparison.
- process_event(3, [1]): from_comparison=true. comparison_frontier={1}.

**Step 3:** expand [1, 1]. Navigator returns event 1 twice.
- First process_event(1, []): from_subject=subject_frontier.remove(1)=true. from_comparison=comparison_frontier.remove(1)=true. Both frontiers now empty. subject_visited=[4,2,1], other_visited=[5,3,1]. 1 is NOT in original_comparison ({5}), so unseen_comparison stays 1. 1 is NOT in original_subject ({4}), so unseen_subject stays 1. NodeState: seen_from_subject=true, seen_from_comparison=true, is_common=true. meet_candidates.insert(1). any_common=true. origins: 1 not in original_comparison, so no origin push from the "Track origins" block. But wait, origins were propagated earlier. Let me check origin propagation.

In step 1, process_event(5, [3]): from_comparison=true. 5 IS in original_comparison ({5}), so `state.origins.push(5)`. origins for node 5 = [5].
Then: is_common = false (only seen from comparison). So we enter the `else if from_comparison` branch: propagate origins to parents. Node 3 gets origins [5].

In step 2, process_event(3, [1]): from_comparison=true. 3 not in original_comparison. origins for node 3 = [5] (from propagation). is_common = false (only seen from comparison). Propagate origins to parents: node 1 gets origins [5].

In step 3, process_event(1, []): from_comparison=true. 1 not in original_comparison. NodeState for 1 already has origins=[5]. is_common=true (just became common). `meet_candidates.insert(1)`. In the "Remove satisfied heads" loop: `for origin in &origins` -> origin=5. `outstanding_heads.remove(5)` -> removes it. outstanding_heads is now empty.

Second process_event(1, []): from_subject=false, from_comparison=false. No-op.

check_result: unseen_comparison_heads=1 (not 0). unseen_subject_heads=1. Both frontiers empty. Compute meet: meet_candidates={1}. Check common_child_count for 1: it was incremented how? Looking at the common node handling: when is_common is true, `parent_state.common_child_count += 1` is called for each parent. But 1 has no parents (empty). So common_child_count for 1 stays 0. `meet = [1]` (filter keeps nodes with common_child_count==0). any_common=true. outstanding_heads is empty. So `!self.any_common || !self.outstanding_heads.is_empty()` = `false || false` = false. Falls through to DivergedSince computation.

Build chains: subject_chain = build_forward_chain([4,2,1], {1}) -> reversed=[1,2,4], find pos of 1 -> pos=0, skip 1 -> [2,4]. other_chain = build_forward_chain([5,3,1], {1}) -> reversed=[1,3,5], skip 1 -> [3,5].

Result: DivergedSince { meet: [1], subject_chain: [2,4], other_chain: [3,5] }.

**Verdict:** Correct.

### 1.3 Termination Correctness

The BFS terminates when:
- Both frontiers empty (all paths fully explored)
- Budget exhausted
- StrictDescends/Ascends detected early via unseen counters

The unseen counters track whether one traversal has reached the **head events** of the other clock. When subject's traversal encounters a comparison head event, that head is "seen." If all comparison heads are seen by subject, subject descends from comparison.

**Key subtlety:** The check at line 369 (`self.original_comparison.contains(&id)`) fires when the subject frontier processes an event that is one of the original comparison head IDs. This is correct: subject reaching a comparison head event means subject has that head in its causal past.

### 1.4 Can the BFS Miss Ancestors?

The BFS extends frontiers by adding all parents of every processed event. Because the DAG is finite and acyclic, every ancestor reachable from the heads will eventually be visited (assuming sufficient budget). The only way to miss ancestors is:

1. **Budget exhaustion:** Handled explicitly via `BudgetExceeded`.
2. **Missing events:** If `expand_frontier` cannot find an event, it simply is not returned. The frontier entry for that ID remains, but on the next iteration it will be re-requested. However, if the navigator consistently fails to return an event (e.g., not in storage), the ID stays in the frontier forever and eventually budget runs out. This is correct behavior -- the algorithm cannot determine a relationship without the events.
3. **Duplicate event processing:** When an event ID appears in both frontiers, it is processed once (the second `frontier.remove` returns false). Both flags are correctly set in a single call.

**Verdict:** The BFS does not miss ancestors under normal operation.

---

## 2. Forward Chain Construction Analysis

### 2.1 The `build_forward_chain` Method

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

The `visited` list is built in child-to-parent order during backward BFS (newest first). Reversing gives parent-to-child (causal) order. Then the meet event is found, and everything after it (toward the tip) is kept.

### 2.2 Multi-Meet-Node Issue

**Scenario:** Meet = {M1, M2} where both are common ancestors. The visited list (reversed) might be: [..., M1, X, M2, Y, Z, tip].

`chain.iter().position(|id| meet.contains(id))` finds M1 at some position p. Skip p+1, keeping [X, M2, Y, Z, tip]. M2 is a meet node but is included in the chain!

**Is this a real problem?** For this to happen, M1 and M2 must both be in the visited list, which means they were both traversed by the same frontier. If M1 and M2 are both meet points (minimal common ancestors), the chain from meet to tip should NOT include either of them. The chain should start from the immediate children of the meet.

However, examining when meet nodes appear in the visited list: A node becomes a meet_candidate when `is_common` becomes true. At that point, the node has already been pushed to `visited`. So meet nodes ARE in the visited list.

But: with multi-element meet sets, there is an additional concern. Consider the forward chain [M2, A, M1, B, C, tip]. The algorithm finds M2 first (position 0), keeps [A, M1, B, C, tip]. Now M1 is still in the chain, which is incorrect -- events between M2 and M1 should also be excluded.

**Assessment:** This is a genuine defect, but its severity depends on how common multi-node meet sets are. In practice, multi-node meet sets occur when the two clocks being compared have multiple independent common ancestors. The most common case is a single meet node. For entities with single-threaded editing (the common case), meet is always a single event.

**When could this bite?** When comparing multi-head clocks. For example:
```
     A
    / \
   B   C
   |   |
   D   E    <- head1 = [D, E]
   |   |
   F   G    <- head2 = [F, G]
```
compare([F,G], [D,E]) should give meet=[D,E]. The subject_visited list (backward from F,G) would be [F, G, D, E, B, C, A]. Reversed: [A, C, B, E, D, G, F]. Meet={D,E}. Position of first meet member: position of D or E (whichever comes first in the reversed list). If E is at position 3, skip to position 4: [D, G, F]. D is a meet node included in the chain!

However: the actual impact is that layers would incorrectly include D as a "to_apply" event. Since D is in `current_head_ancestry` (it is an ancestor of the current head which includes D), it would be classified as `already_applied` in the layer, making this effectively harmless for the layer computation. The chain would have an extra event but it would be placed in `already_applied` rather than `to_apply`.

Still, this is logically wrong and could cause issues in edge cases.

### 2.3 StrictDescends Chain Construction

```rust
if self.unseen_comparison_heads == 0 {
    let chain: Vec<_> = self.subject_visited.iter().rev()
        .filter(|id| !self.original_comparison.contains(id))
        .cloned().collect();
    return Some(AbstractCausalRelation::StrictDescends { chain });
}
```

This reverses the subject's visited list and filters out the comparison head events. For a linear chain A->B->C->D->E, comparing [E] vs [A]: subject_visited=[E,D,C,B,A], reversed=[A,B,C,D,E], filter out {A}, chain=[B,C,D,E]. Correct.

But what about partial traversals? If subject reaches comparison head A at step 3 but has only visited [E,D,A] (skipped B,C because the navigator did not return them or because of DAG structure), the chain would be [D,E]. This is a potential issue -- **the chain might be incomplete if the BFS does not visit every event between the comparison head and the subject head**.

For a linear chain, the BFS always visits every event because each event's parent is added to the frontier. For a DAG with branching, the BFS visits all reachable events. The chain construction assumes all events between meet/comparison-head and subject-head are in the visited list. This holds because the BFS expands all parents, so every event between head and comparison-head is visited before the comparison-head is reached.

Wait -- that is not quite right. The BFS traverses backward from subject head. It reaches comparison head A when A appears in the subject frontier. But A was added because its child B was in the frontier, and B was added because C was in the frontier, etc. So the full linear path E->D->C->B->A is traversed. All events are in visited.

But for DAG structures with branching: subject_visited contains ALL events visited by the subject traversal, including events from branches that do not lead to the comparison head. For example:
```
    A (comparison head)
   / \
  B   C
   \ /
    D
    |
    E (subject head)
```
subject_visited (backward from E) = [E, D, B, C, A] (order depends on frontier expansion). Reversed: [A, C, B, D, E]. Filter out {A}: [C, B, D, E]. This is a valid forward chain in causal order, though not minimal (both B and C are included, which is correct for a DAG).

**Verdict:** Chain construction is correct for the StrictDescends case and for single-meet-node DivergedSince. There is a defect for multi-node meet sets, but its practical impact is limited.

---

## 3. Key Invariants Analysis

### Invariant 1: "All replicas converge given the same event set"

**Mechanism:** LWW uses lexicographic EventId as a deterministic tiebreaker for concurrent events. BTreeMap/BTreeSet provide deterministic ordering. Layer computation from the same meet point with the same event set produces the same layers.

**Assessment:** Upheld, with one caveat. The `compute_ancestry` function is used to partition events into `already_applied` vs `to_apply`. Two replicas with the same event set but different current heads would compute different ancestry sets, resulting in different partitioning. This is by design -- each replica's partitioning reflects its current state. The LWW resolution within `apply_layer` correctly accounts for this by seeding winners from stored values (which differ per replica) and then competing all candidates. The final state converges because the same winner is determined regardless of which side the events are on.

**Potential issue:** The seeding from stored values in `apply_layer` (lww.rs lines 180-190) requires all stored properties to have `event_id`. If a property was set before per-property event tracking was introduced, it lacks an `event_id` and `apply_layer` returns an error. This prevents convergence for pre-migration entities. **Severity: Medium** (migration concern, not algorithm defect).

### Invariant 2: "compare(A,B) and compare(B,A) are antisymmetric"

Expected: if compare(A,B) = StrictDescends, then compare(B,A) = StrictAscends, and vice versa. If compare(A,B) = DivergedSince{meet=M}, then compare(B,A) = DivergedSince{meet=M}.

**Analysis of StrictDescends/StrictAscends symmetry:**
- `unseen_comparison_heads == 0` triggers StrictDescends. Swapping subject and comparison means the old comparison becomes the new subject. If the original subject saw all comparison heads, the new comparison (original subject) is causally descended from the new subject (original comparison). So the new comparison should see all new subject heads, triggering StrictAscends. This is correct.

**Analysis of DivergedSince symmetry:**
- The meet should be the same set regardless of direction. The BFS explores both traversals symmetrically, and the meet is the set of common ancestors with `common_child_count == 0`. Swapping subject/comparison swaps the traversal labels but produces the same common ancestors. The meet is symmetric.
- The subject_chain and other_chain swap, which is correct.

**Assessment:** Upheld. The algorithm is symmetric in meet computation and antisymmetric in direction labels.

### Invariant 3: "No regression -- never skip required intermediate events"

**Mechanism:** StrictDescends provides a forward chain of all events between comparison head and subject head. The caller is expected to apply these events in order. The chain is built from `subject_visited`, which contains every event traversed by the subject's BFS backward from its head.

**Assessment:** Upheld for linear chains. For DAG topologies in the StrictDescends path, the chain contains all events in the subject's backward traversal minus the comparison heads. These events are in approximately causal order (reversed BFS order), which corresponds to parent-to-child for linear chains but may not be perfectly ordered for DAGs with concurrent branches. However, the callers of StrictDescends do NOT use the chain for ordered replay -- they use `apply_operations_from_event` directly (entity.rs line 260-263). The chain is informational.

For DivergedSince, the layer computation handles ordering via the `compute_layers` frontier expansion, which correctly enforces parent-before-child ordering.

**Assessment:** Upheld.

### Invariant 4: "Idempotent application -- redundant delivery is a no-op"

**Mechanism:** `compare_unstored_event` first checks if the event's ID is in the comparison clock (line 95). If so, returns Equal, which causes `apply_event` to return `Ok(false)`.

**Edge case:** An event whose ID is NOT in the head clock but IS in history (e.g., event B when head is C, with B->C). `compare_unstored_event` does not detect this -- it compares B's parent clock against the head clock. Since B's parent is older than C, the StrictAscends transformation produces a DivergedSince result. The entity would incorrectly re-apply B as a concurrent event.

The holistic review notes this (line 1129-1144 in tests.rs) and acknowledges it returns DivergedSince. The comment says "the caller should check if the event is already stored before calling compare_unstored_event."

**Assessment:** The algorithm itself does not guarantee idempotency for events deep in history. The caller (entity.rs `apply_event`) relies on this convention. **This is a correctness gap if callers do not pre-check.** Looking at the callers: in `entity.rs`, `apply_event` does NOT pre-check if the event is already stored. It goes straight to `compare_unstored_event`. So redundant delivery of a non-head event will cause a spurious DivergedSince merge. **Severity: High.**

However: If the event IS already applied, the LWW backend's `apply_layer` will seed with stored values that include that event's effects. Re-applying the same event's operations would produce the same candidate with the same event_id. Since the candidate equals the stored value (same event_id, same value), it would win but already be in `already_applied` (or at least not change state). So the state would likely not change. But the head WOULD change: the meet point removal and event_id insertion in entity.rs (lines 323-326) would modify the head incorrectly, potentially removing a meet-point event from the head that should remain.

**Let me trace this concretely:**
- History: A->B->C. Head = [C].
- Event B re-arrives. compare_unstored_event(B, [C]):
  - compare([A], [C]) (B's parent vs head): StrictAscends (A < C).
  - Transform to DivergedSince { meet=[A], other=[C], subject_chain=[B], other_chain=[] }.
- In apply_event: meet=[A]. `state.head.remove(A)` -- A is not in head [C], so no-op. `state.head.insert(B)`. Head becomes [B, C].
- **This is incorrect.** The head now has B and C as concurrent tips, but B is an ancestor of C. The entity has a "spurious multi-head."

**This is a confirmed bug.** Redundant delivery of a non-head historical event creates an invalid multi-head state.

### Invariant 5: "Single head for linear history -- multi-heads only for true concurrency"

Depends on Invariant 4. If redundant delivery of historical events is possible, this invariant is violated. See above.

---

## 4. Asymmetric Depth Analysis

### Scenario: 1 vs 50

**DAG:**
```
A (genesis)
|
B (subject head -- depth 1 from A)
|
A -> C1 -> C2 -> ... -> C50 (comparison head -- depth 50 from A)
```
Wait, this is a linear chain. B descends from A, and C1 also descends from A. Let me make it properly asymmetric:

```
      A (genesis)
     / \
    B   C1 -> C2 -> ... -> C50
```
compare([B], [C50]):

subject_frontier starts at {B}, comparison_frontier starts at {C50}.

**Step 1:** expand [B, C50]. Both fetched.
- process_event(B, [A]): from_subject=true. subject_frontier={A}. subject_visited=[B].
- process_event(C50, [C49]): from_comparison=true. comparison_frontier={C49}. other_visited=[C50].

**Step 2:** expand [A, C49]. Both fetched.
- process_event(A, []): from_subject=true. subject_frontier={}. subject_visited=[B,A]. A has empty parents -> subject_root=A.
- process_event(C49, [C48]): from_comparison=true. comparison_frontier={C48}. other_visited=[C50,C49].

**Steps 3-50:** Each step expands only the comparison frontier (subject frontier is empty). expand [C48], then [C47], etc. Each step fetches one event and adds its parent.

**Step 51:** expand [A] (comparison frontier reached A).
- process_event(A, []): from_subject=false (A not in subject_frontier, which is empty). from_comparison=true. A already has NodeState with seen_from_subject=true (from step 2). Now seen_from_comparison=true. is_common=true. meet_candidates.insert(A).
- Origins for A: check if A is in original_comparison ({C50}). No. So origins are whatever was propagated. C50 was original_comparison member, so it got origins=[C50] in step 1. Origins propagated: C50->C49->...->C1->A all get origins=[C50]. So A.origins=[C50]. outstanding_heads.remove(C50) succeeds.
- comparison_frontier becomes {} (A has no parents).

**Step 51 check_result:** Both frontiers empty. meet=[A] (common_child_count for A is 0 -- wait, was it incremented? When A became common, its parents get common_child_count += 1. But A has no parents. So common_child_count stays at 0. Meet = [A]. any_common=true. outstanding_heads is empty. Build chains.

subject_chain = build_forward_chain([B,A], {A}) -> reversed=[A,B], find A at pos 0, skip to pos 1, chain=[B].
other_chain = build_forward_chain([C50,C49,...,C1,A], {A}) -> reversed=[A,C1,...,C50], find A at pos 0, skip to pos 1, chain=[C1,...,C50].

Result: DivergedSince { meet=[A], subject_chain=[B], other_chain=[C1,...,C50] }. **Correct.**

**Budget concern:** This took 51 steps. With the default budget of 100 in entity.rs (line 246), this succeeds. But with 1 vs 100 depth, it would need 101 steps.

The budget is consumed per `expand_frontier` call. Each call returns consumed_budget=1 (in production retriever, line 79 of retrieval.rs). So 101 calls consume budget 101 > 100. **BudgetExceeded would fire.**

### Scenario: 3 vs 300

With budget=100, this will hit BudgetExceeded after 100 steps. At that point, the comparison frontier will still be deep (around 200 events from the root), and the subject frontier will have been empty since step 3. The returned BudgetExceeded contains the current frontier positions for later resumption.

**But there is no resumption mechanism.** In entity.rs line 334, BudgetExceeded is turned into an error: `LineageError::BudgetExceeded`. The entity returns an error, and the event is not applied.

This is documented behavior (the spec mentions "Resume with frontiers" but the implementation does not implement resumption). For deeply asymmetric branches with total depth > 100, the system fails.

**Severity assessment:** The budget of 100 is quite low for a system that may have entities with hundreds of events. A linear chain of 101 events comparing against an empty (new) branch would exceed budget. However, the production `consumed_budget` might be different from the test mock's constant 1. Let me re-check.

Looking at LocalRetriever (retrieval.rs line 79): `consumed_budget: 1`. EphemeralNodeRetriever: `consumed_budget: cost` where cost is 1 for local, 5 for remote. So each `expand_frontier` call costs 1-5 budget. With budget=100, this means 20-100 expansion steps. For a linear chain of length N where subject is at tip and comparison is at root, the BFS needs N/2 steps (expanding from both ends). So max chain length is ~40-200 events before budget exhaustion.

This is worth flagging but is a capacity limitation, not an algorithm correctness issue.

### Asymmetric Depth: Does the BFS Explore Efficiently?

The BFS expands BOTH frontiers in each step (line 274-276: all_frontier_ids includes both). This means the short branch's frontier empties quickly (after a few steps), and subsequent steps only expand the long branch's frontier. This is correct but **not optimal** -- the short branch could reach the root before the long branch, and from that point, only the long branch needs expansion. The algorithm handles this correctly by continuing to expand the comparison frontier even after the subject frontier is empty.

**Critical observation:** When one frontier is empty, the combined frontier_ids only contain the other frontier. The navigator fetches those events. Budget is consumed. This is correct but could be made more efficient by detecting early when one side has reached a root while the other is still exploring.

---

## 5. BudgetExceeded Fallback Analysis

### What Happens When Budget Runs Out

```rust
} else if self.remaining_budget == 0 {
    Some(AbstractCausalRelation::BudgetExceeded {
        subject: self.subject_frontier.ids.clone(),
        other: self.comparison_frontier.ids.clone(),
    })
}
```

The frontiers are returned as-is. To resume, one would create a new `Comparison` with these frontiers and additional budget.

### Is Frontier-Based Resumption Sound?

**Concern 1:** The frontiers contain event IDs that have not been fetched yet (they are the "next to explore" positions). Starting a new comparison from these positions would correctly continue the BFS from where it left off, EXCEPT: the `states` HashMap, `meet_candidates`, `outstanding_heads`, `unseen_comparison_heads`, `unseen_subject_heads`, `subject_visited`, `other_visited` are all lost. A resumed comparison would start fresh with only the frontier positions, losing all accumulated state.

This means resumption would need to re-traverse everything from the new starting frontiers, which is equivalent to starting a new comparison with the frontier positions as the "heads." This is NOT sound because:

1. The `original_subject` and `original_comparison` would be set to the frontier IDs, not the original head IDs. The unseen counter logic depends on these being the true head IDs.
2. Events visited before budget exhaustion would be re-visited, but their role (subject vs comparison) might differ because the frontier positions are a mixture of both sides' exploration state.
3. The chain accumulation would only capture events from the frontier positions forward, missing events between the original heads and the frontier.

**Verdict:** Frontier-based resumption as described in the spec is NOT currently implemented, and the frontier data returned in BudgetExceeded is insufficient for correct resumption without preserving the full Comparison state. The BudgetExceeded is currently treated as a hard error.

### Production Impact

With budget=100 and consumed_budget=1 per step, entities with more than ~100 events between comparison point could fail. This includes:

- Long-lived entities with many sequential edits
- Entity head comparisons where meet is the genesis event

**Severity: Medium.** The budget is a capacity limit, not a correctness issue. Events that exceed budget simply fail to be applied. However, the hardcoded budget of 100 with no configuration option makes this inflexible.

---

## 6. Multi-Head Entity Analysis

### When an Event Extends Just One Tip

**Scenario:** Entity head = [B, C] (multi-head). Event D arrives with parent = [C].

`compare_unstored_event(D, [B,C])`:
1. Check: D's ID in comparison head? Not yet. Continue.
2. `compare(event.parent()=[C], comparison=[B,C])`:
   - subject={C}, comparison={B,C}.
   - Step 1: expand [C, B, C]. Navigator gets events for [C, B, C] (C appears twice).
     - process_event(C): from_subject=subject_frontier.remove(C)=true. from_comparison=comparison_frontier.remove(C)=true. C is in original_comparison ({B,C}). unseen_comparison_heads goes from 2 to 1 (since from_subject is true and C is in original_comparison). C is in original_subject ({C}). unseen_subject_heads goes from 1 to 0 (since from_comparison is true and C is in original_subject).
     - process_event(B): from_subject=false (B not in subject_frontier). from_comparison=comparison_frontier.remove(B)=true. B is in original_subject ({C})? No. unseen_subject_heads already 0.
   - check_result: unseen_subject_heads == 0 -> StrictAscends!

Wait, the check order is: unseen_comparison_heads first (line 543), then unseen_subject_heads (line 553). unseen_comparison_heads = 1 (not 0). unseen_subject_heads = 0. Returns **StrictAscends**.

3. Back in compare_unstored_event, result is StrictAscends. Transform:
   - parent_members = {C}
   - comparison_members = {B, C}
   - meet = [C]
   - other = comparison_members - parent_members = {B}
   - Result: DivergedSince { meet=[C], subject=[...], other=[B], subject_chain=[D], other_chain=[] }

4. In entity.rs apply_event, DivergedSince path:
   - compute_ancestry for head [B,C]: includes B, C, and all their ancestors.
   - compute_layers from meet [C]: children of C in the event set. The event set includes events accumulated during BFS plus event D.

   Wait -- the accumulated events from the AccumulatingNavigator only include events fetched during the comparison of [C] vs [B,C]. In this case, events C and B were fetched (possibly only C if B was the second process_event). Actually, both C and B would be fetched because they were in the frontier_ids.

   Then event D is added to the events map (entity.rs line 282).

   compute_layers(&events, &[C], &current_ancestry): events = {B, C, D} (and their ancestors if fetched). Children of C in event map: D (since D's parent contains C). If B's parent is [A] and A is not in the events, then B has no parent referencing C. So children_of(events, C) = [D].

   Layer 0: frontier = {D}. D is not in current_ancestry (current_ancestry includes B, C, and their ancestors). So D is in to_apply. Layer = {already_applied: [], to_apply: [D]}.

   But this is wrong! B is a concurrent branch from the meet point C that has already been applied. The layer should include B as already_applied so that LWW can compare D's values against B's values.

   **The issue:** children_of looks for events whose parent clock contains the meet event C. B's parent clock is [A] (not [C]), so B is not a child of C. B is not included in the layers at all.

Actually, wait. The meet is [C], and we are looking at the divergence from C. B is concurrent with C (both are children of A). B is NOT a child of C. The divergence point is C, and the only event after C on the subject (incoming) side is D. There is nothing after C on the other (head) side that descends from C. The other_chain was empty.

This means the layers correctly contain only [D] as to_apply with nothing as already_applied at the same depth. The LWW resolution seeds from stored values (which include B's effects since B is applied to the entity). So when D's values compete, they compete against the stored last-write values (which were set by B or by events before B). The LWW resolution compares D's event_id against the stored event_id for each property. Since other_chain is empty, no layer events compete with D -- but the stored value seeds provide the competition.

**This is where the "conservative resolution" applies.** The stored values from B have event_id set. D's candidate competes against the stored candidate via `layer.compare()`. But the layer's event context only contains the events accumulated during BFS + D. Does it contain B? If B was fetched during the BFS, yes. Let me check: the BFS for `compare([C], [B,C])` fetched both B and C. The AccumulatingNavigator captured both. So B IS in the events map passed to compute_layers. And D is added. The events map = {A (maybe), B, C, D}.

Now, `layer.compare(D_id, B_id)`: calls `is_descendant(events, D_id, B_id)`. D's parent is [C]. C's parent is... depends on what C's parent is. If the DAG is A->B and A->C, then C's parent is [A]. So D's ancestry is D->C->A. B is not in D's ancestry. Then `is_descendant(events, B_id, D_id)`: B's ancestry is B->A. D is not in B's ancestry. So D and B are concurrent. Tiebreak: lexicographic EventId comparison. Higher EventId wins.

This is correct behavior -- D and B are truly concurrent, and the tiebreaker picks a deterministic winner.

**BUT:** There is still a subtle issue. The `layer.compare()` call in LWW requires that both event IDs are in the events map. D is there (just added). B is there (accumulated during BFS). But what about their ancestors? B's parent is A. Is A in the events map? The BFS for compare([C], [B,C]) expanded [C, B] and their parents. C's parent is A, B's parent is A. Were the parents fetched? Let me trace:

Step 1 of the BFS: expand_frontier([C, B, C]). C and B are fetched. process_event(C, [A]): subject_frontier extends with {A}. process_event(B, [A]): comparison_frontier extends with {A}. But then unseen_subject_heads hit 0 and the BFS terminated in check_result before another expansion step.

So A was added to both frontiers but never fetched. A is NOT in the accumulated events map.

Now, `is_descendant(events, D_id, B_id)`: starts at D. D is in events. D's parent is [C]. C is in events. C's parent is [A]. A is NOT in events. `events.get(&A)` returns None. The function returns `Err(RetrievalError::Other("missing event for ancestry lookup"))`.

This propagates to `layer.compare()` which returns Err, which the LWW backend converts to `MutationError::InsufficientCausalInfo`. **The entire apply_layer fails.**

**This is a bug.** The BFS terminates early (correct for the comparison algorithm) but the accumulated events are insufficient for the subsequent `is_descendant` calls within `layer.compare()`.

The severity depends on how often this path is taken. The specific scenario (multi-head entity, event extending one tip, StrictAscends transformation) is a core use case described in the spec as a key design decision. If the `is_descendant` traversal needs to reach the genesis event, and the BFS terminated before reaching it, the layer comparison will fail.

However: `is_descendant` only needs to traverse until it either finds the ancestor or exhausts all paths. If D and B are concurrent (neither is an ancestor of the other), the traversal will eventually reach events not in the map and error out. The traversal would need the full event graph between D and B's common ancestor to determine concurrency. If the common ancestor (A) is missing from the events map, the traversal fails with `missing event for ancestry lookup`.

**Mitigating factor:** This specific failure path only occurs when: (a) an event extends one tip of a multi-head entity, (b) the StrictAscends transformation is triggered, (c) the `is_descendant` traversal in layer.compare needs to reach events that the BFS did not accumulate. The fix would be to ensure the BFS accumulates enough events, or to handle the `InsufficientCausalInfo` error gracefully (e.g., fall back to lexicographic comparison when ancestry cannot be determined).

---

## 7. Additional Observations

### 7.1 Origin Propagation and Outstanding Heads

The `outstanding_heads` mechanism tracks which comparison heads have been accounted for. Origins are propagated from comparison head events downward through the comparison traversal. When a common ancestor is found, the origins at that ancestor tell us which comparison heads are "satisfied."

**Potential issue:** If the comparison has multiple heads (e.g., [B, C]) and the common ancestor A has origins=[B] but not [C], then only B is removed from outstanding_heads. The algorithm would not recognize that C is also satisfied even though A is an ancestor of C.

But wait -- C's origins are propagated when C is processed. If C's traversal reaches A, A gets C's origin. The BFS must visit C before reaching A (since it walks backward from C through parents). So by the time A becomes common, it should have origins from all comparison heads that have A as an ancestor.

This holds as long as the comparison traversal visits all events between comparison heads and common ancestors. Since the comparison frontier expands all parents, this is guaranteed.

**Verdict:** Correct.

### 7.2 Frontier Deduplication

The `Frontier` uses `BTreeSet`, which deduplicates IDs. When both subject and comparison frontiers contain the same ID, it appears in both sets. The combined `all_frontier_ids` sent to the navigator contains duplicates (line 274-276). The navigator might return the event twice. `process_event` handles this correctly via `frontier.remove`: the first call removes the ID from both frontiers (if present in both), the second call gets false/false and is a no-op.

Actually no -- the event is processed once per occurrence in the navigator's returned events list. If the navigator returns two copies (because the ID was in frontier_ids twice), process_event is called twice. The first call removes from both frontiers. The second call gets false/false. This is correct.

### 7.3 `compute_layers` children_of Performance

The holistic review correctly identifies that `children_of` performs a linear scan over all events for each parent. For layers computation with N events and K meet nodes, this is O(N*K) to find the initial frontier, then O(N*F) per layer iteration where F is the frontier size. Worst case is O(N^2).

For the algorithm review, this is a performance concern, not a correctness concern. However, it could become a correctness issue if the layer computation is so slow that it causes timeout in the TOCTOU retry loop.

### 7.4 Head Update Logic in DivergedSince

```rust
for parent_id in &meet {
    state.head.remove(parent_id);
}
state.head.insert(event.id());
```

**Concern:** The `meet` comes from the comparison between the unstored event's parent and the entity head. For the StrictAscends transformation case, `meet` is the event's parent clock members. For the BFS DivergedSince case, `meet` is the GCA of the event's parent and the entity head.

**Case 1 (StrictAscends transformation):** meet = event's parent members. If event's parent is [C] and head is [B, C], meet = [C]. head.remove(C) -> head becomes [B]. head.insert(D) -> head becomes [B, D]. Correct.

**Case 2 (BFS DivergedSince):** meet = GCA. If event parent is [X] and head is [H], with GCA at A (deep ancestor), meet = [A]. head.remove(A) -> A not in head, no-op. head.insert(D) -> head becomes [H, D]. This creates a multi-head with both H and D, which is correct since they are concurrent.

**Case 3 (BFS DivergedSince, multi-meet):** If meet = [M1, M2] and head = [H1, H2] where M1 is in the head but M2 is not, then head.remove(M1) removes it, head.remove(M2) is a no-op, head.insert(D) adds D. Head becomes [H2, D]. This is correct only if M1 being in the head means the incoming event supersedes the M1 tip.

Actually, is it correct for meet nodes to be in the head? The meet is the GCA of the event's parent clock and the entity head clock. If M1 is in the entity head, it means M1 is a current tip. If M1 is also a common ancestor of the event's parent and the head, then the event descends from M1 (since it is in the event's parent's ancestry and M1 IS the head). So the event supersedes M1, and removing M1 from the head is correct.

**Verdict:** The head update logic is correct for all cases I've traced.

---

## 8. Summary of Issues Found

### Critical: Redundant delivery of historical events creates invalid multi-heads

When an event that is already in the entity's history (but not at the head) is delivered again, `compare_unstored_event` returns DivergedSince instead of recognizing it as redundant. The entity then modifies its head to include the historical event as a concurrent tip, violating the "single head for linear history" invariant.

### High: InsufficientCausalInfo when extending one tip of a multi-head entity

The StrictAscends-to-DivergedSince transformation in `compare_unstored_event` triggers early BFS termination, leaving the accumulated event set incomplete. Subsequent `layer.compare()` calls in LWW may fail because `is_descendant` encounters events not in the accumulated set.

### Medium: build_forward_chain is incorrect for multi-node meet sets

When the meet set has multiple nodes, the chain trimming logic finds only the first meet node in the forward chain and keeps everything after it, potentially including other meet nodes and events that should be excluded.

---

## Confidence-Rated Findings

| # | Finding | Severity | Confidence | Details |
|---|---------|----------|------------|---------|
| 1 | **Redundant delivery of historical (non-head) events creates invalid multi-head state.** `compare_unstored_event` does not detect that an event is already in the entity's causal history (only checks if it is at the head). The StrictAscends transformation produces DivergedSince, and the head update logic in `entity.rs` (lines 323-326) inserts the event ID into the head without verifying it does not already exist in history. This creates a spurious multi-head where an ancestor and descendant coexist as tips. | Critical | High | Traced in Section 3, Invariant 4. The test at line 1116 of tests.rs confirms this behavior and documents it as expected ("caller should check"). But entity.rs `apply_event` does NOT perform this pre-check. Fix: either check event storage before `compare_unstored_event`, or detect when the meet is an ancestor of the current head and reject. |
| 2 | **InsufficientCausalInfo failure when extending one tip of a multi-head entity.** When `compare_unstored_event` triggers the StrictAscends->DivergedSince transformation, the underlying BFS terminates early (correctly). But the `AccumulatingNavigator` captures only the events fetched during BFS, which may not include all ancestors needed by `is_descendant` in `EventLayer::compare`. This causes `apply_layer` to fail with `MutationError::InsufficientCausalInfo`. | High | High | Traced in Section 6. The specific failure occurs when `layer.compare()` tries to determine if two events from different branches are causally related and the BFS did not fetch their common ancestor (e.g., event A in the A->B, A->C DAG when BFS terminated after fetching only B and C). |
| 3 | **`build_forward_chain` trimming is incorrect for multi-node meet sets.** `chain.iter().position(|id| meet.contains(id))` finds only the first meet node. Other meet nodes and events between them remain in the chain. | Medium | High | Traced in Section 2.2. Impact is limited because: (a) meet nodes are typically single events for common use cases, and (b) extra events would be classified as `already_applied` in layer computation if they are in current_head_ancestry. Could cause incorrect chain lengths or include redundant events in edge cases with multi-head comparisons. |
| 4 | **BudgetExceeded is a hard error with no resumption path.** The returned frontier positions are insufficient for correct resumption (missing accumulated state, unseen counters, visited lists). The budget of 100 with consumed_budget=1 per step limits effective DAG depth to ~100 events. | Medium | High | Entity.rs converts BudgetExceeded to LineageError. No retry with higher budget. No state preservation for incremental resumption. Entities with >100 events between comparison points will fail to apply events from distant branches. The budget is hardcoded and non-configurable. |
| 5 | **Duplicate event processing from combined frontier.** When the same event ID is in both subject and comparison frontiers, `all_frontier_ids` (line 274-276) sends it to the navigator twice. The navigator may return the event twice. The second `process_event` call is a harmless no-op, but this wastes a navigator call and potentially a remote fetch. | Low | High | Performance issue, not correctness. Could be fixed by deduplicating `all_frontier_ids` before sending to navigator. |
| 6 | **Empty-clock comparison returns DivergedSince instead of Equal.** `compare(empty, empty)` returns `DivergedSince{meet:[], ...}` (line 53-62). Two empty clocks represent the same lattice point (bottom element) and should arguably be Equal. | Low | High | Callers handle this case, but the semantic incorrectness could confuse future consumers of the API. The test at line 293 explicitly asserts this behavior, so it is intentional. |
| 7 | **`compare_unstored_event` StrictAscends transformation: `other_chain` is always empty.** This means the LWW backend cannot compute per-property depths for the other branch and falls back to "keep current value" heuristic. This is conservative but may produce incorrect results if the incoming event's value should actually win. | Medium | Medium | The empty `other_chain` means no events from the other branch participate in the layer. LWW seeds from stored values (which may be set by events on the other branch), so the stored event_id provides implicit competition. However, if the stored event_id is from an event on a different branch than the meet-to-head path, the causal comparison in layer.compare may be incorrect or fail. |
| 8 | **`children_of` in layers.rs is O(N) per call, causing O(N^2) layer computation.** For large event sets (hundreds of events), this could cause significant latency. Combined with the TOCTOU retry loop, slow layer computation increases the chance of head movement and retry exhaustion. | Low | High | Performance concern, not correctness. A pre-computed parent-to-children index would make this O(1) per lookup. The holistic review also flagged this. |
| 9 | **Origin propagation only happens on the `from_comparison` path (line 341-346).** If an event is processed from subject only, origins are not propagated to its parents. This is correct by design (origins track comparison heads), but the asymmetry means the subject traversal cannot contribute to outstanding_heads accounting. If the subject traversal reaches a common ancestor before the comparison traversal brings origins to that ancestor, the meet might be found without origins, leaving outstanding_heads non-empty. | Low | Medium | The BFS continues until both frontiers are empty, so the comparison traversal will eventually bring origins to common ancestors. The outstanding_heads check at line 564 ensures that if origins are missing, the result falls through to DivergedSince-with-empty-meet rather than silently producing an incorrect result. However, the result quality degrades (empty meet instead of correct meet). |
| 10 | **In `entity.rs` DivergedSince handler, `acc_navigator.get_events()` is used instead of `acc_navigator.into_events()`.** `get_events()` (line 104 of navigator.rs) clones the entire events map. `into_events()` would consume the navigator and avoid the clone. Since the navigator is not used after this point, `into_events()` would be more efficient. | Low | High | Performance nit. The navigator variable goes out of scope shortly after, so the inner map would be dropped anyway. But for large event sets, the unnecessary clone could be significant. |
