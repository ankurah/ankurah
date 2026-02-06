# Adversarial Security & Correctness Review: PR #201 -- Concurrent Updates Event DAG

**Reviewer Role:** Adversarial tester / red team
**Objective:** Attempt to break the implementation -- find inputs, topologies, timing, and edge cases that produce incorrect behavior
**Scope:** Core event DAG comparison, entity application, LWW/Yrs backends, layer computation
**Methodology:** Independent code trace with no access to prior reviewer discussions

---

## 1. Critical Bug: `build_forward_chain` Produces Incorrect Chains for Multi-Meet DAGs

**File:** `/Users/daniel/ak/ankurah-201/core/src/event_dag/comparison.rs`, lines 447-460

**The Problem:**

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

This function reverses the `visited` list (BFS order, child-to-parent becomes parent-to-child) and then finds the *first* meet member. Everything before it (inclusive) is trimmed.

**Breaking Scenario -- Multi-Meet with Interleaving:**

Consider this DAG:
```
    M1    M2       (meet = {M1, M2})
    |     |
    A     B
     \   /
       C
```

If the BFS visited order (child-to-parent) is `[C, A, M1, B, M2]`, the reversed (forward) chain is `[M2, B, M1, A, C]`. The `position` call finds `M2` at index 0 (the first meet member). The result after skip is `[B, M1, A, C]`.

**This incorrectly includes M1 in the chain.** Meet nodes should NOT be in the chain -- they are the boundary. Furthermore, event `B` appears *before* `M1` in the chain, which is non-causal (B is a child of M2, not a parent of M1).

The visited list is not topologically sorted -- it is in BFS discovery order, which for a DAG with multiple paths can interleave events from different branches. Reversing BFS order does not produce a valid topological sort for DAGs.

**Impact:** The `subject_chain` and `other_chain` in `DivergedSince` may contain extra events (meet nodes), miss events, or have them in incorrect order. Any consumer relying on these chains for replay or layer computation would produce wrong results. Currently `compute_layers` does NOT use these chains (it recomputes from meet), mitigating the impact, but the chains are part of the public API (`AbstractCausalRelation::DivergedSince`) and any future consumer would break.

**Severity:** Medium (mitigated by `compute_layers` not using chains, but the API contract is violated)

---

## 2. Critical Bug: `DivergedSince` Head Update Removes Meet IDs, Not Parent IDs

**File:** `/Users/daniel/ak/ankurah-201/core/src/entity.rs`, lines 320-326

```rust
// Update head: remove superseded tips, add new event
for parent_id in &meet {
    state.head.remove(parent_id);
}
state.head.insert(event.id());
```

The variable is named `parent_id` but it iterates over `meet`, not over the event's actual parents. The `meet` is the greatest common ancestor of the event's parent clock and the entity's current head. These can be deep ancestors, NOT current head members.

**Breaking Scenario -- Late-arriving event from old branch point:**

```
Entity head: [H]     (linear chain: A -> B -> C -> D -> E -> F -> G -> H)
Event Y arrives:      parent = [X], where X's parent = [D]
                      (compare_unstored_event: meet = [D])
```

The head update tries `state.head.remove(D)`. But D is NOT in the head (which is `[H]`). This is a no-op, so the head becomes `[H, Y]`. This is actually correct behavior for this case -- D was never in the head.

But consider this case:
```
Entity head: [B, C]   (A is genesis, B and C are concurrent children of A)
Event E arrives:       parent = [A]  (E branches from genesis)
compare_unstored_event result: StrictAscends transformed to DivergedSince with meet = [A]
```

The head update tries `state.head.remove(A)`. A is NOT in the head `[B, C]`, so this is a no-op. Head becomes `[B, C, E]`.

**Is this correct?** Actually, E is concurrent with *both* B and C. E should be added to the head alongside them. So the result `[B, C, E]` is correct. But the meet is `[A]`, not `[B]` or `[C]`. The code *happens* to work because `remove` is a no-op for non-members.

**However, consider the StrictAscends -> DivergedSince transformation:**

When `compare_unstored_event` transforms `StrictAscends` into `DivergedSince`, the `meet` is set to `event.parent().members()` (the event's parent clock). In the example above, if the event's parent is `[A]`, meet = `[A]`, other = `[B, C]` minus `[A]` = `[B, C]`.

The `other_chain` is empty in this case (no traversal was done from meet to head). This means `compute_layers` gets an empty other_chain and won't have events to build layers from. But wait -- `compute_layers` uses the `events` map from `acc_navigator.get_events()`, not the chains. The accumulated events from BFS traversal during `compare_unstored_event` will include events B, C, A (traversed backward from `[B, C]` to `[A]`). But the incoming event E and its parent X (if any intermediate events) need to be in the events map too.

Looking at lines 281-282:
```rust
let mut events = acc_navigator.get_events();
events.insert(event.id(), event.clone());
```

This inserts the incoming event but NOT the events between the event's parent and genesis. For event E with parent [A], only E is inserted. A, B, C should be in the accumulated events from the BFS. So the events map should contain {A, B, C, E}.

Then `compute_layers(&events, &meet, &current_ancestry)` with meet = [A], current_ancestry = {A, B, C} would compute:
- frontier = children of A in events = {B, C, E}
- B and C are in current_ancestry -> already_applied
- E is not -> to_apply
- Layer 0: already_applied=[B,C], to_apply=[E]

This is correct. The layer computation works, and LWW will compare E against the stored values (which came from B and C).

**But there is still a subtle issue:** The head update should be removing the tips that the new event supersedes, not the meet. For a proper head update when event E (parent=[A]) arrives and head=[B,C]:
- E does NOT supersede B or C (it's concurrent with them)
- E should be added to the head: `[B, C, E]`
- No tips should be removed

The current code: removes nothing from meet (since A is not in head), adds E. Result: `[B, C, E]`. This is correct by accident. But if the meet DID contain head members, they would be incorrectly removed.

**Actually-breaking scenario:**
```
Entity head: [C, D]   where C has parent [A], D has parent [A]
Event E arrives:       parent = [C]
compare_unstored_event: parent [C] vs head [C, D]
  -> compare([C], [C, D]) -> StrictAscends (because Past([C]) subset of Past([C, D]))
  -> Transformed to DivergedSince { meet: [C], other: [D] }
```

Head update: `state.head.remove(C)` removes C from `[C, D]`, then inserts E. Result: `[D, E]`.

**This is correct!** E extends C (parent=[C]), so C is indeed superseded by E. D remains as a concurrent tip. Head `[D, E]` is the right answer.

But what if the meet is deeper?
```
Entity head: [F, G]   where F and G both descend from A
Event E arrives:       parent = [B] where B has parent [A]
compare_unstored_event: parent [B] vs head [F, G]
  -> compare([B], [F, G]) -> StrictAscends
  -> Transformed to DivergedSince { meet: [B], other: [F, G] - [B] = [F, G] }
```

Head update: `state.head.remove(B)` -- B is NOT in head [F, G], so no-op. Insert E. Result: `[F, G, E]`.

This is correct -- E is concurrent with both F and G.

**Conclusion:** The head update logic works correctly *only because* the StrictAscends transformation sets meet = event.parent.members, and the event's parent members are either in the head (and should be removed) or not in the head (and the remove is a no-op). For the general DivergedSince case (from actual BFS), the meet could be deep ancestors that are never in the head, so the removes are harmless no-ops. The code is correct but the intent is unclear and fragile.

**Severity:** Low (correct by accident, but fragile)

---

## 3. High Bug: `unseen_comparison_heads` Counter Underflow via Head Overlap

**File:** `/Users/daniel/ak/ankurah-201/core/src/event_dag/comparison.rs`, lines 369-372

```rust
if self.original_comparison.contains(&id) {
    self.unseen_comparison_heads -= 1;
}
```

This decrements when the subject frontier encounters a comparison head. But what if the same event ID is in BOTH `original_subject` and `original_comparison`? (This happens when the clocks share members.)

**Breaking Scenario:**

```
subject  = [A, B]
comparison = [B, C]
```

Event B is in both. When the subject frontier processes B (from_subject=true), it reaches line 369 and finds B in `original_comparison`, so `unseen_comparison_heads` goes from 2 to 1. When the comparison frontier ALSO processes B (from_comparison=true), at line 379 it finds B in `original_subject`, so `unseen_subject_heads` goes from 2 to 1.

But then in the *same* `process_event` call (if B is fetched once and both frontiers claim it), `from_subject` and `from_comparison` are BOTH true. This means lines 369-372 and 379-381 both execute in the same call.

Wait -- let me re-examine. The frontier `remove` at lines 300-301:
```rust
let from_subject = self.subject_frontier.remove(&id);
let from_comparison = self.comparison_frontier.remove(&id);
```

If B is in both frontiers, both remove calls return true. Then at line 369:
```rust
if from_subject && self.original_comparison.contains(&id) {
    self.unseen_comparison_heads -= 1;
}
```

And at line 379:
```rust
if from_comparison && self.original_subject.contains(&id) {
    self.unseen_subject_heads -= 1;
}
```

Both fire. `unseen_comparison_heads` goes from 2 to 1, `unseen_subject_heads` from 2 to 1.

Now, C is only in the comparison frontier. When subject's traversal later encounters C (via parents of B, which go to C's parents... wait, that does not make sense for this topology).

Actually let me construct a concrete DAG:
```
    Root
    / \
   A   C
   |
   B
```

subject = [A, B], comparison = [B, C].

In this case B is shared. The frontiers start as: subject_frontier = {A, B}, comparison_frontier = {B, C}.

In the first step, `all_frontier_ids = [A, B, B, C]` (duplicates possible). The navigator fetches events for all of them. Events A, B, C are returned.

Processing B: from_subject=true (removed from subject), from_comparison=true (removed from comparison). B is in original_comparison, so unseen_comparison_heads -= 1 (2->1). B is in original_subject, so unseen_subject_heads -= 1 (2->1).

Processing A: from_subject=true (removed from subject). A is NOT in original_comparison. Subject frontier extends to A's parents = [Root].

Processing C: from_comparison=true (removed from comparison). C is NOT in original_subject. Comparison frontier extends to C's parents = [Root].

Now subject_frontier = {Root} (from A), comparison_frontier = {Root} (from C).

Next step fetches Root. from_subject=true, from_comparison=true. Root is common. Meet = {Root}. unseen_comparison_heads is still 1 (C hasn't been "seen" by subject). unseen_subject_heads is still 1 (A hasn't been "seen" by comparison).

Frontiers are now empty. `check_result` runs. Neither unseen counter is 0. Frontiers are empty. Meet candidates include Root and B (if B became common).

Wait, B was processed by BOTH frontiers. So `is_common = true`. B is added to `meet_candidates`. Then Root also becomes common.

The minimal meet (common_child_count == 0) would be Root (since B has Root as a child in the common set? No, `common_child_count` tracks how many common children a node has).

Actually, when B is processed as common, its parents get `common_child_count += 1`. B's parent is A. So A's `common_child_count` becomes 1. But A is not itself common (only seen from subject).

Root: when processed, it becomes common (from_subject and from_comparison). Its `common_child_count` is 0 (no common children yet? Wait, A and C are both Root's children, but only Root itself is common, not A or C individually). Wait, the `common_child_count` is incremented for a parent when a *common* node is processed. B is common, so B's parents (A) get `common_child_count += 1`. But A is not common. Root: when processed as common, its parents would get incremented, but Root has no parents (genesis). So Root's `common_child_count` remains 0.

B is common, and B's `common_child_count` is 0 (no common children of B were found). So B is a meet candidate.
Root is common, and Root's `common_child_count` is... let me trace again.

When B is processed as common: "for parent in parents" -> parent is A. A's `common_child_count += 1`.
When Root is processed as common: "for parent in parents" -> Root has empty parents (genesis). Nothing incremented.

Now the minimal meet (common_child_count == 0) filters: B has common_child_count 0 (no common children), Root has common_child_count 0. Both are minimal? But B descends from Root, so Root should NOT be minimal.

Actually, the filter is: `self.states.get(*id).map_or(0, |s| s.common_child_count) == 0`. For Root: common_child_count is 0 (nobody incremented it because B's parent is A, not Root). For B: common_child_count is 0 as well.

So meet = [B, Root]. **This is wrong.** B is a descendant of Root, so Root should be filtered out. The minimal meet should be just [B].

The issue is that `common_child_count` only tracks *direct* common children. Root has no direct common children (A and C are not common), even though Root is an ancestor of B which IS common. The algorithm should track that Root has B as a common descendant, but it only counts direct children.

**Wait, actually, this is fundamentally the meet algorithm bug.** The `common_child_count` is supposed to be incremented when a common node is found, for each of its parents. B is common, and B's parents = [A]. So A's common_child_count goes to 1. But Root doesn't get incremented because none of Root's *direct children* are common (A is not common, C is not common).

Root IS common (seen from both frontiers), but Root has common_child_count 0. B IS common, and B has common_child_count 0. So both appear as "minimal common ancestors" with no common children.

**The correct meet for subject=[A,B] vs comparison=[B,C] should be [B] alone.** Subject B is already shared. The algorithm should detect that [A,B] vs [B,C] means subject has A extra and comparison has C extra, with B in common. The result should be DivergedSince with meet=[B] (or possibly StrictDescends if A descends from B... but in my DAG A does not descend from B; rather B descends from A).

Actually, wait. I mixed up my DAG. Let me reconsider. In the DAG I set up:
```
    Root
    / \
   A   C
   |
   B
```
subject = [A, B], comparison = [B, C]. Past(A) = {Root, A}, Past(B) = {Root, A, B}. So Past(subject) = {Root, A, B}. Past(C) = {Root, C}. Past(comparison) = {Root, A, B, C}.

Past(subject) is a subset of Past(comparison)! So the correct answer is `StrictAscends`.

But the algorithm gives DivergedSince because the `unseen_comparison_heads` counter mechanism is confused by the shared member B. After processing B from both sides, `unseen_comparison_heads` drops from 2 to 1, but to reach 0 (StrictDescends), subject's traversal needs to also "see" C. Subject's traversal goes backward through A to Root, not through C. So unseen_comparison_heads never reaches 0.

Meanwhile, comparison's traversal processes B and C. B is in original_subject, so unseen_subject_heads drops from 2 to 1. C's parent is Root. Comparison traversal goes to Root (from C). Root is in the subject frontier too. Processing Root from comparison side: Root is in original_subject? No, A and B are in original_subject, not Root.

So unseen_subject_heads stays at 1 (A was never "seen" by comparison's traversal in the sense of being in original_subject). But Root is processed by both, becoming common.

The fundamental issue: the early-exit conditions (`unseen_comparison_heads == 0` for StrictDescends, `unseen_subject_heads == 0` for StrictAscends) only trigger when one side's traversal discovers ALL of the other side's original head members. But with shared head members, the decrement from the shared-head processing does not count toward the "other side discovering us" path.

In this example, comparison's traversal needs to "see" A (subject head member) to confirm StrictAscends. Comparison goes backward from B and C. B's parents = [A]. So comparison's traversal processes A's parent as part of B's parents? No -- comparison frontier starts with {B, C}. Processing B removes it and extends frontier with B's parents = {A}. Processing C removes it and extends frontier with C's parents = {Root}. Now comparison_frontier = {A, Root}.

Next step: processing A (from comparison frontier, from_comparison=true). A IS in original_subject! So `unseen_subject_heads -= 1` goes from 1 to 0. Now `check_result` would return `StrictAscends`.

OK, so the algorithm DOES correctly handle this case, just not in the first step. It takes two steps. The concern about underflow does not materialize because the initial count matches the initial frontier size, and each ID is only removed once from each frontier.

**Revised assessment:** The head-overlap case works correctly but requires additional BFS steps. No bug here, just a performance concern.

**Severity:** Not a bug (false alarm after detailed trace)

---

## 4. High Bug: `compute_layers` Silently Drops Events When Parent Is Not In Event Map

**File:** `/Users/daniel/ak/ankurah-201/core/src/event_dag/layers.rs`, lines 138-150

```rust
frontier = frontier
    .iter()
    .flat_map(|id| children_of(&events, id))
    .filter(|id| !processed.contains(id))
    .filter(|id| {
        if let Some(event) = events.get(id) {
            event.parent().members().iter().all(|p| processed.contains(p))
        } else {
            false  // <--- Event not in map, silently dropped
        }
    })
    .collect();
```

**Breaking Scenario:**

If an event in the DAG has a parent that was NOT accumulated during BFS (e.g., because it is on a different branch that was never traversed), that parent will never be in `processed`. The child event will never satisfy "all parents processed" and will be permanently excluded from the frontier, along with all its descendants.

This can happen when the events map from `AccumulatingNavigator` is incomplete. The BFS in `compare_unstored_event` only traverses backward from the event's parent and the entity's head. If the DAG has events that were added by different code paths and are referenced as parents but were never in either frontier, they won't be accumulated.

**Concrete scenario:**
```
    A (genesis)
   / \
  B   C
  |   |
  D   E
   \ /
    F (parent = [D, E])
```

If the BFS comparison was between [F]'s parent [D,E] and head [B], the traversal from [D,E] goes to [B,C] (D's parent is B, E's parent is C). The traversal from head [B] goes to [A]. When [B] is seen by both frontiers, it becomes common. But what about C? C is only visited from the subject frontier (via E). The comparison frontier started at [B] and went to [A]. C was never in the comparison frontier.

Actually, let me re-examine. F is the incoming event with parent [D, E]. Entity head is [B]. `compare_unstored_event` compares [D, E] vs [B]. Subject frontier = {D, E}, comparison frontier = {B}.

Step 1: Fetch D, E, B. Process D (from subject): parents = [B], extend subject_frontier with [B]. Process E (from subject): parents = [C], extend subject_frontier with [C]. Process B (from comparison): parents = [A], extend comparison_frontier with [A].

Now subject_frontier = {B, C}, comparison_frontier = {A}.

Step 2: Fetch B, C, A. Process B (from subject): B is in original_comparison, so unseen_comparison_heads drops from 1 to 0. Early exit: StrictDescends!

So this actually returns StrictDescends, not DivergedSince. The events accumulated would be D, E, B, C, A.

But what if the entity head is [G] where G descends from A through a different path?

```
    A (genesis)
   /|\
  B  C  G
  |  |
  D  E
   \/
    F (parent=[D,E])
```

F's parent [D,E] vs head [G]. Subject frontier={D,E}, comparison frontier={G}.

Step 1: Fetch D, E, G. D parents=[B], E parents=[C], G parents=[A]. Subject frontier={B,C}. Comparison frontier={A}.

Step 2: Fetch B, C, A. B parents=[A], C parents=[A]. Subject frontier={A, A} = {A}. Comparison frontier empty (A was already fetched).

Wait, A is in comparison_frontier from step 1. When we process A in step 2: from_comparison=true (if it was still in comparison_frontier). Actually, A was added to comparison_frontier in step 1 when processing G. In step 2, the `all_frontier_ids` includes A from comparison_frontier. Processing A: from_comparison=true. A is in original_subject? No. A parents=[]. Genesis.

Subject frontier after step 2: processing B (from subject) extends with [A]. Processing C (from subject) extends with [A]. Subject frontier = {A}. But A is also being processed in this same step from comparison.

Wait, A is in BOTH frontiers in step 2. from_subject=true, from_comparison=true. A becomes common. Meet candidates include A.

unseen_comparison_heads: G was in original_comparison. Subject's traversal has NOT seen G. unseen_comparison_heads = 1 (still).

Frontiers now: subject emptied (A processed), comparison emptied (A processed). Check result: frontiers empty. Meet = [A] (common_child_count 0). Outstanding heads still has G? Let me check.

outstanding_heads is initialized to comparison_ids = {G}. It's only updated when a common node is found with origins. Origins track which comparison heads reach a node. G is in original_comparison. When G is processed (from_comparison), its origins are set to [G]. G's parents = [A]. A gets G's origins propagated. When A becomes common, `for origin in &origins { self.outstanding_heads.remove(origin); }`. A's origins include G (propagated from G -> A). So outstanding_heads removes G. Now outstanding_heads is empty.

So the result is DivergedSince with meet=[A], subject_chain, other_chain built from visited events.

Accumulated events: D, E, G, B, C, A. Events map has all of them. `compute_layers` starts from meet [A]. Children of A = {B, C, G}. B and G are in current_ancestry (head ancestry = {G, A}), C is not.

Wait, current_ancestry = `compute_ancestry(&events, head.as_slice())` where head = [G]. Ancestry of [G] = {G, A}. So B is NOT in current_ancestry. Only G and A are.

Children of A in the events map: B, C, G. Partition: G is in current_ancestry -> already_applied. B and C are not -> to_apply. Layer 0 = {already_applied: [G], to_apply: [B, C]}.

But this is wrong! B is not a "new" event -- it's already part of the entity's history (it was stored before). The issue is that `current_ancestry` is computed from the head, and if B is not an ancestor of G, it won't be in the ancestry set. And in this DAG, B is NOT an ancestor of G (they're siblings).

But wait -- B is not part of the entity's current state at all. The entity's state is built from G's ancestry {G, A}. Events B, C, D, E, F are all "new" from the entity's perspective. So applying B and C as to_apply is actually correct!

Actually no. The entity head is [G]. Its state only reflects A and G. Event B was never applied. So B IS new. C is new. D is new. E is new. F is new. All of them need to be applied.

Layer 0: already_applied=[G], to_apply=[B, C]. This layer correctly identifies that B and C are concurrent with G and need to be applied.

Next frontier: children of {B, C, G} = children of B = {D}, children of C = {E}, children of G = none. D is not processed. E is not processed. D's parents = [B]. Is B processed? Yes. So D is ready. E's parents = [C]. C is processed. E is ready. Frontier = {D, E}. Layer 1: already_applied=[], to_apply=[D, E].

Next frontier: children of {D, E} = F (parent [D, E]). Both D and E are processed. F is ready. Layer 2: already_applied=[], to_apply=[F].

This works correctly.

**Revised assessment:** The event map from AccumulatingNavigator contains all events traversed during BFS, which should be sufficient for `compute_layers` in normal cases. The silent drop only occurs if events are genuinely missing from the map, which would indicate a corrupt DAG or incomplete storage.

**Severity:** Low (defensive, but correct for normal operation)

---

## 5. Critical: LWW `apply_layer` Compares Stored Values Against Layer Events Without Full DAG Context

**File:** `/Users/daniel/ak/ankurah-201/core/src/property/backend/lww.rs`, lines 169-254

The LWW `apply_layer` seeds winners from stored values (with their event_ids), then competes them against events from the layer using `layer.compare()`.

**Breaking Scenario:**

The stored value has event_id `X`. The layer contains events `Y` and `Z`. The `layer.compare(Y, X)` call tries to find X in the layer's events map (`Arc<BTreeMap<Id, E>>`). But X might not be in the layer's events map -- it's an event from the entity's deep history, not part of the current layer.

Looking at the `is_descendant` function in `layers.rs`:
```rust
let event = events.get(&id).ok_or_else(||
    RetrievalError::Other(format!("missing event for ancestry lookup: {}", id)))?;
```

This returns `Err(RetrievalError)`, which is mapped to `MutationError::InsufficientCausalInfo` in `apply_layer`.

**So any time a stored property's event_id is not in the layer's events map, the entire layer application FAILS with a hard error.**

When does this happen? Whenever the entity has been updated previously (stored values have event_ids from past events), and the new layer's events map only contains events from the current comparison BFS. For a DivergedSince with a deep meet point, the stored value's event_id could be from an event that is NOT in the accumulated events map.

**Concrete example:**
```
A -> B -> C -> D (entity head, D sets property x, stored event_id for x = D's id)
A -> E -> F (concurrent branch)
```

Meet = A. Layer events = {B, C, D, E, F} (accumulated from BFS). Stored x has event_id = D. Layer competition: new event F sets x. `layer.compare(F.id, D.id)` looks up D in the events map. D IS in the events map (accumulated during BFS backward from head). So this works.

But what about a deeper history?
```
A -> B (sets x, event_id for x = B) -> C -> D -> E (head, never writes x)
A -> F (concurrent, writes x)
```

Meet = A. Layer events = {B, C, D, E, F}. Stored x has event_id = B. `layer.compare(F.id, B.id)` looks up F in events (yes), then B in events (yes). So this works too, as long as B is in the accumulated events.

But what if B is below the meet?
```
R -> A -> B (sets x) -> C (meet point)
C -> D -> E (head, doesn't write x)
C -> F -> G (concurrent, writes x)
```

Meet = C. The BFS traversed backward from [E's parent] and [G's parent] to find meet C. Events accumulated: E, D, G, F, C (and possibly more). But B is BELOW C -- it's C's ancestor. B was NOT in the BFS traversal because the BFS stops at the meet.

Stored x has event_id = B. Layer events map contains {C, D, E, F, G} but NOT B. When layer 0 = {already_applied: [D, F], to_apply: [...]}, and the LWW backend tries `layer.compare(some_event, B.id)`, B is not in the events map. **HARD FAILURE: InsufficientCausalInfo.**

**This is a real bug that will manifest whenever:**
1. A property was last written by an event below the meet point
2. A concurrent branch modifies the same property
3. The LWW resolution tries to compare the new event against the stored event_id

**Impact:** Entity application fails with a hard error for a legitimate concurrent update scenario. This breaks the entire merge for that entity.

**Severity:** Critical

---

## 6. High: `children_of` O(N^2) Performance Enables DoS Via Wide DAGs

**File:** `/Users/daniel/ak/ankurah-201/core/src/event_dag/layers.rs`, lines 159-166

```rust
fn children_of<Id, E>(events: &BTreeMap<Id, E>, parent: &Id) -> Vec<Id> {
    events.iter()
        .filter(|(_, e)| e.parent().members().contains(parent))
        .map(|(id, _)| id.clone())
        .collect()
}
```

This scans ALL events for every `children_of` call. It's called once per frontier member per layer iteration.

**Resource Exhaustion Attack:**

An attacker constructs a DAG with N events in a "comb" pattern:
```
Meet -> E1 -> E2 -> E3 -> ... -> EN (linear chain on one side)
Meet -> F1 (concurrent)
```

Layer computation starts with frontier = children of Meet = {E1, F1}. But `children_of` scans all N+1 events to find children of Meet. Then advancing the frontier: children of E1 scans all N+1 events. For each of the N layers, one `children_of` call scans all N events. Total: O(N^2) operations.

With N=1000 events, this is 1,000,000 operations. With N=10,000 (which could be accumulated from a very deep history), this is 100,000,000 operations.

**Impact:** An attacker can craft a DAG topology that causes O(N^2) CPU consumption during `compute_layers`. This could be used for DoS against a node.

**Severity:** High (quadratic DoS)

---

## 7. Medium: TOCTOU Retry Loop Has No Backoff and Fixed Budget Creates Livelock Window

**File:** `/Users/daniel/ak/ankurah-201/core/src/entity.rs`, lines 248-347

```rust
const MAX_RETRIES: usize = 5;
const COMPARISON_BUDGET: usize = 100;

for attempt in 0..MAX_RETRIES {
    let acc_navigator = AccumulatingNavigator::new(getter);
    match crate::event_dag::compare_unstored_event(&acc_navigator, event, &head, COMPARISON_BUDGET).await? {
        // ...
    }
}
```

Each retry re-creates the AccumulatingNavigator and re-does the full BFS comparison from scratch. There is no exponential backoff and no yield point between retries.

**Breaking Scenario: Infinite-seeming retry under contention**

With 20+ concurrent writers (as tested in `test_rapid_concurrent_transactions`), the sequence is:
1. Thread A reads head, starts BFS comparison (takes time T)
2. During that time, Thread B commits, changing the head
3. Thread A's TOCTOU check fails, retries
4. Thread A reads new head, starts BFS again (takes time T)
5. During that time, Thread C commits, changing the head again
6. Repeat

With N concurrent writers each taking time T for BFS, and no backoff, the probability of a retry succeeding decreases as N increases. After MAX_RETRIES=5 failures, the event is rejected with `TOCTOUAttemptsExhausted`.

**However**, each successful commit reduces contention (one fewer writer). So in practice, some writers will succeed and others will be retried. But with a fixed budget of 100, BFS for deep histories could be expensive, making T large and increasing collision probability.

**Additional concern:** The BFS comparison budget is 100. For a DAG with 50 sequential events + concurrent branches (as in `test_deep_lineage_concurrent_fork`), the BFS needs to traverse ~50 steps, consuming most of the budget. If there are multiple concurrent heads requiring traversal on each side, the budget could be exhausted, causing `BudgetExceeded` instead of `TOCTOUAttemptsExhausted`.

**Impact:** Under sustained high write contention on a single entity, legitimate events may be rejected. The test acknowledges this but the production impact could be significant for hot entities.

**Severity:** Medium

---

## 8. Medium: Yrs `apply_layer` Replays `already_applied` Operations Redundantly

**File:** `/Users/daniel/ak/ankurah-201/core/src/property/backend/yrs.rs`, lines 186-209

```rust
fn apply_layer(&self, layer: &EventLayer<EventId, Event>) -> Result<(), MutationError> {
    // ...
    for event in &layer.to_apply {
        if let Some(operations) = event.operations.get(&Self::property_backend_name().to_string()) {
            for operation in operations {
                self.apply_update(&operation.diff, &changed_fields)?;
            }
        }
    }
    // ...
}
```

The Yrs backend only applies `to_apply` events, NOT `already_applied`. This is correct because Yrs is a CRDT and already-applied operations are already in the Yrs document state.

**BUT** -- what if the Yrs document state does NOT actually contain the `already_applied` operations? This happens in the `apply_layer` path in `entity.rs` lines 301-317:

```rust
for (_backend_name, backend) in state.backends.iter() {
    backend.apply_layer(layer)?;
}
```

This iterates over EXISTING backends. If a Yrs backend already exists, it has some state. But was the `already_applied` event's operations already applied to this Yrs doc? The `already_applied` events are supposed to be "events already in the current state." But the current state was built from sequential `apply_operations_with_event` calls, NOT from `apply_layer`.

When `apply_event` takes the `DivergedSince` path, it computes layers and calls `apply_layer` on all backends. The `already_applied` events in each layer are events from the current head's ancestry. Their operations WERE applied to the backend via earlier `apply_operations_with_event` calls (in previous `apply_event` or `StrictDescends` paths).

For Yrs, this is fine -- the Yrs document already has those operations integrated, and Yrs ignores duplicate operations via its internal state vector. The `apply_layer` correctly only applies `to_apply`.

**But for LWW**, the `apply_layer` method reads stored values (seeded from the backend's current state with event_ids), then competes them against layer events. The `already_applied` events are re-parsed to extract their property values for competition. This is correct because the stored values already reflect the `already_applied` events, and the competition ensures the right winner is chosen.

**Actually, I found a subtle issue:** In the `apply_layer` call, the LWW backend's stored values are read. But these stored values reflect ALL events applied so far, not just the events in this layer. If an event from a deeper layer (closer to meet) set a property, that value is in the stored state. When a shallower layer (further from meet) is applied, the stored value still has the deeper event's value. The competition in the shallower layer compares the new event against the stored value (which may be from a different layer entirely).

This could produce incorrect results if the stored event_id is from a deeper layer and the `layer.compare()` call fails because that event is not in the CURRENT layer's events map. Wait -- each layer shares the SAME events `Arc<BTreeMap>` (from line 107 in `compute_layers`). So all events are available for comparison in every layer.

**Revised assessment:** This is not a bug for Yrs (idempotent by design) and LWW has access to the full events map across layers.

**Severity:** Not a bug

---

## 9. Medium: Entity Creation Race Allows Duplicate Creation Events

**File:** `/Users/daniel/ak/ankurah-201/core/src/entity.rs`, lines 224-239

```rust
if event.is_entity_create() {
    let mut state = self.state.write().unwrap();
    if state.head.is_empty() {
        // this is the creation event for a new entity, so we simply accept it
        for (backend_name, operations) in event.operations.iter() {
            state.apply_operations_from_event(backend_name.clone(), operations, event.id())?;
        }
        state.head = event.id().into();
        drop(state);
        self.broadcast.send(());
        return Ok(true);
    }
    // If head is no longer empty, fall through to normal lineage comparison
}
```

This correctly re-checks under the lock. But what about two DIFFERENT creation events (same entity_id, different operations)?

An entity is identified by `EntityId`. Two different events with different operations but both claiming to be "entity_create" (empty parent clock) can race. The first one wins and sets the head. The second one falls through to normal lineage comparison. It would be compared as `compare_unstored_event(event2, head=[event1_id])`.

Since event2 has empty parent clock, `compare` is called with `event2.parent()` = empty clock vs `head` = [event1_id]. Empty clock triggers the early exit at comparison.rs line 53: returns `DivergedSince { meet: [], ... }`.

The `apply_event` handler for `DivergedSince` with empty meet: computes layers from empty meet. `compute_layers` with meet=[] would have `frontier = children of meet = children of nothing = empty`. So 0 layers. Head update: for parent_id in &meet (empty) -> no removes. Insert event2. Head becomes [event1_id, event2_id].

**The entity now has TWO genesis events in its head.** This is semantically wrong -- an entity should have exactly one creation event. Any future comparison involving this head will encounter two roots and may return `Disjoint`.

**Impact:** An attacker (or a network race condition) that causes two creation events to arrive for the same entity produces a permanently corrupted entity head.

**Severity:** Medium (requires specific race condition, but the entity becomes permanently corrupted)

---

## 10. Medium: `is_descendant` DFS in `layers.rs` Has No Cycle Protection

**File:** `/Users/daniel/ak/ankurah-201/core/src/event_dag/layers.rs`, lines 201-230

```rust
fn is_descendant<Id, E>(events: &BTreeMap<Id, E>, descendant: &Id, ancestor: &Id) -> Result<bool, RetrievalError> {
    let mut visited = BTreeSet::new();
    let mut frontier: Vec<Id> = vec![descendant.clone()];
    while let Some(id) = frontier.pop() {
        if !visited.insert(id.clone()) {
            continue;
        }
        // ...
        for parent in event.parent().members() {
            if !visited.contains(parent) {
                frontier.push(parent.clone());
            }
        }
    }
    Ok(false)
}
```

The `visited` set provides cycle protection. If the DAG has a cycle (A -> B -> A), the visited check prevents infinite looping. This is correct.

However, the `is_descendant` function errors on missing events:
```rust
let event = events.get(&id).ok_or_else(||
    RetrievalError::Other(format!("missing event for ancestry lookup: {}", id)))?;
```

If the events map is incomplete (which it will be -- it only contains events accumulated during BFS, not the entire entity history), the DFS will fail with an error when it reaches an event not in the map.

This is the same root cause as Finding #5 (LWW InsufficientCausalInfo). The `is_descendant` function and `EventLayer::compare` both assume the events map is complete for the relevant portion of the DAG, but the accumulated events only cover the BFS traversal path.

**Severity:** High (same root cause as Finding #5, exacerbates it)

---

## 11. Low: `compute_ancestry` Can Be Exploited for Memory Exhaustion

**File:** `/Users/daniel/ak/ankurah-201/core/src/event_dag/layers.rs`, lines 172-199

```rust
pub fn compute_ancestry<Id, E>(events: &BTreeMap<Id, E>, head: &[Id]) -> BTreeSet<Id> {
    let mut ancestry = BTreeSet::new();
    let mut frontier: BTreeSet<Id> = head.iter().cloned().collect();
    while !frontier.is_empty() {
        // ...
        for id in frontier {
            if ancestry.insert(id.clone()) {
                if let Some(event) = events.get(&id) {
                    for parent in event.parent().members() {
                        next_frontier.insert(parent.clone());
                    }
                }
            }
        }
        frontier = next_frontier;
    }
    ancestry
}
```

This walks the entire ancestry from the head to the roots. For a DAG with N events where the head descends from all of them, the ancestry set contains N elements. Combined with the `events.clone()` at line 107 of `compute_layers`, memory usage is O(N) for the events map plus O(N) for the ancestry set.

For a DAG with 10,000 events (plausible for a long-lived entity), this is manageable. But there is no upper bound, and the `events` map is cloned into an `Arc` (line 107), so the peak memory during `compute_layers` is 2x the events map plus the ancestry set.

**Severity:** Low (expected for normal operation, but no bound)

---

## 12. Medium: LWW Backend Lock Ordering Between `values` and `field_broadcasts` Creates Potential Deadlock

**File:** `/Users/daniel/ak/ankurah-201/core/src/property/backend/lww.rs`

In `apply_layer` (lines 169-254):
1. Line 181: `let values = self.values.read().unwrap();` (read lock on values)
2. Line 236: `let mut values = self.values.write().unwrap();` (write lock on values -- after dropping read lock)
3. Line 246: `let field_broadcasts = self.field_broadcasts.lock().expect(...)` (lock on field_broadcasts)

In `apply_operations_internal` (lines 280-312):
1. Line 289: `let mut values = self.values.write().unwrap();` (write lock on values)
2. Line 304: `let field_broadcasts = self.field_broadcasts.lock().expect(...)` (lock on field_broadcasts)

In `listen_field` (lines 256-263):
1. Line 258: `let mut field_broadcasts = self.field_broadcasts.lock().expect(...)` (lock on field_broadcasts)

The lock ordering is consistent: values first, then field_broadcasts. Since `listen_field` only takes field_broadcasts, there's no ordering violation.

However, if `apply_layer` and `apply_operations_internal` are called concurrently on the same backend (from different threads), they both try to write-lock `values`. The `RwLock` will serialize them. No deadlock.

**Revised assessment:** Lock ordering is consistent. No deadlock.

**Severity:** Not a bug

---

## 13. Low: MockEventStore `consumed_budget` Always Returns 1 Regardless of Events Fetched

**File:** `/Users/daniel/ak/ankurah-201/core/src/event_dag/tests.rs`, line 142

```rust
let consumed_budget = 1; // Match the real implementation pattern
```

The test navigator always consumes 1 budget unit per expansion step, regardless of how many events are fetched. The real `LocalRetriever` also returns 1 (line 79 of retrieval.rs). This means the budget counts "rounds" of expansion, not "events fetched."

This means `COMPARISON_BUDGET = 100` allows 100 expansion rounds. Each round fetches ALL frontier events (which could be many). For a DAG with branching factor K and depth D, the frontier can grow to K^D events, all fetched in a single round for budget cost 1.

**Impact:** The budget does not actually bound the number of events traversed, only the number of BFS rounds. An attacker could create a DAG that forces many events to be fetched per round, effectively bypassing the budget.

**Severity:** Low (the budget still bounds depth, which limits total work for balanced DAGs)

---

## 14. Medium: `apply_event` DivergedSince Path Does Not Use `acc_navigator` From The Winning Retry

**File:** `/Users/daniel/ak/ankurah-201/core/src/entity.rs`, lines 248-330

```rust
for attempt in 0..MAX_RETRIES {
    let acc_navigator = AccumulatingNavigator::new(getter);
    match compare_unstored_event(&acc_navigator, event, &head, COMPARISON_BUDGET).await? {
        // ...
        DivergedSince { meet, .. } => {
            let mut events = acc_navigator.get_events();  // <-- uses this attempt's navigator
            // ...
            let mut state = self.state.write().unwrap();
            if state.head != head {
                head = state.head.clone();
                continue;  // <-- retry discards acc_navigator
            }
            // ... apply layers ...
        }
    }
}
```

On retry, the `acc_navigator` from the failed attempt is discarded and a new one is created. The new attempt's accumulated events reflect the BFS traversal from the NEW head value. This is correct -- the events need to match the current head.

However, `acc_navigator.get_events()` is called at line 281, and the events are used at line 288 for `compute_layers`. But between lines 281 and 292 (where the write lock is acquired), the head could change again. If it does, the code correctly retries (line 296-298). But the events accumulated in the navigator were based on the head at line 251 (BFS time), and the head is re-checked at line 294. If they match, the events are valid.

**This is actually correct** -- the TOCTOU check at line 294 ensures the head hasn't changed since the BFS was done.

**Severity:** Not a bug

---

## 15. High: `StrictAscends`-to-`DivergedSince` Transformation Produces Empty `other_chain`, Breaking Layer Computation for Events Below the Meet

**File:** `/Users/daniel/ak/ankurah-201/core/src/event_dag/comparison.rs`, lines 115-148

When `compare_unstored_event` transforms a `StrictAscends` result, the `other_chain` is left empty:

```rust
AbstractCausalRelation::DivergedSince {
    meet,
    subject: vec![],
    other,
    subject_chain: vec![event.id()],
    other_chain: vec![],  // <-- empty
}
```

The comment says "LWW resolution handles this conservatively." But `compute_layers` uses the events from `acc_navigator.get_events()`, not the chains. Let me trace what events the navigator accumulates in this case.

`compare_unstored_event` calls `compare(navigator, event.parent(), comparison, budget)`. For the StrictAscends case, this means `compare(event.parent, entity_head)` returned StrictAscends. The BFS traversed from event.parent() backward and from entity_head backward, finding that event.parent() is strictly before entity_head.

The accumulated events include all events encountered during this BFS. For StrictAscends, the comparison side (entity_head) traversal goes deeper than the subject side (event.parent()). The subject side stops when all its heads are seen by the comparison. So the accumulated events include the chain from entity_head back to event.parent() (approximately).

Then in `apply_event`, `acc_navigator.get_events()` returns these events, and the incoming event is inserted. `compute_layers` is called with meet = event.parent().members() and current_head_ancestry computed from head.

**The problem arises here:** The meet is the event's parent clock. If the event's parent is [A] and the entity head is [E] where A -> B -> C -> D -> E, then meet = [A]. `compute_layers` starts with children of A in the events map. Children of A = {B} (and maybe the incoming event, if its parent is [A]). B is in current_ancestry. The incoming event is NOT in current_ancestry.

Layer 0: already_applied=[B], to_apply=[incoming_event]. Then the LWW resolution in this layer tries to compare the incoming event against stored values. The stored value for property x might have event_id = D (the most recent writer of x). `layer.compare(incoming_event.id, D.id)` -- D should be in the events map (accumulated during BFS).

Wait, but D is between A and E. Was D accumulated? The BFS traversed from entity_head [E] backward: E -> D -> C -> B -> A. All of these were accumulated. So D IS in the events map. Good.

But `is_descendant(events, incoming_event_id, D_id)` -- does the incoming event descend from D? The incoming event's parent is [A]. A does NOT descend from D. So `is_descendant` returns false for both directions. Result: Concurrent.

Then tiebreak by event_id. If incoming_event.id > D.id, incoming wins. Otherwise, D (stored value) wins.

**This is wrong!** D is causally AFTER A. The incoming event branches from A, which is before D. In a sane LWW resolution, D should win because it's more recent (deeper in the causal chain). But the comparison returns "Concurrent" because the events map shows no path from incoming_event to D.

Actually, wait. `is_descendant(events, incoming_event_id, D_id)` traces from incoming_event backward through parents. incoming_event's parent is [A]. A's parent is... is A in the events map? Let me check.

The events map from `acc_navigator.get_events()` plus the manually inserted incoming event. The BFS traversed from [A] and [E]. Events accumulated: E, D, C, B, A (from the comparison side), and possibly A from the subject side too (they overlap at A). So A IS in the events map, and its parent is [] (genesis).

`is_descendant(events, incoming_event_id, D_id)`:
- frontier = [incoming_event_id]
- Process incoming_event: parents = [A]. A != D. Push A.
- Process A: parents = []. No more frontier.
- Return false (D not found).

`is_descendant(events, D_id, incoming_event_id)`:
- frontier = [D_id]
- Process D: parents = [C]. C != incoming_event. Push C.
- Process C: parents = [B]. B != incoming_event. Push B.
- Process B: parents = [A]. A != incoming_event. Push A.
- Process A: parents = []. No more frontier.
- Return false (incoming_event not found).

Both return false -> Concurrent -> tiebreak by event_id.

**This means an event from a very old branch point (parent=[A]) can WIN the LWW tiebreak against an event from deep in the current history (D), purely based on the random EventId hash. This violates the spec's "depth precedence" rule.**

Looking at the spec/intent: the StrictAscends transformation comment says "Head has more recent events that should take precedence." The code relies on the convention that `other_chain` being empty means "current values win." But that convention is implemented incorrectly in the LWW backend.

Looking at LWW `apply_layer` more carefully: the stored value has event_id D. D is in the events map. The layer comparison returns Concurrent (because D and incoming_event have no causal path through each other in the events map). So the winner is based on `candidate.event_id > current.event_id`, which is based on SHA256 hashes -- essentially random.

**The correct behavior:** When an event arrives from an old branch point (StrictAscends transformed to DivergedSince), and the entity has progressed far beyond that point, the entity's current values should always win (they're causally later). But the code treats them as concurrent and uses random tiebreak.

**Severity:** High (violates depth-precedence invariant, produces non-deterministic results based on hash values)

---

## 16. Low: `Clock::from(Vec<EventId>)` Does Not Sort

**File:** `/Users/daniel/ak/ankurah-201/proto/src/clock.rs`, line 63-65

```rust
impl From<Vec<EventId>> for Clock {
    fn from(ids: Vec<EventId>) -> Self { Self(ids.into_iter().collect()) }
}
```

This creates a Clock from a Vec without sorting. But `Clock::contains` uses `binary_search`, which requires the internal Vec to be sorted. If a Clock is created from an unsorted Vec, `contains` will return incorrect results.

`Clock::from_strings` sorts (line 23), and `Clock::insert` maintains sorted order (line 30). But `Clock::from(vec)` does not sort.

**Impact:** If any code path creates a Clock from an unsorted Vec, `contains` and `binary_search` will produce incorrect results. Searching the codebase, `From<Vec<EventId>>` is used in test helper code and potentially in deserialization paths.

**Severity:** Low (likely only affects test code, but could cause hard-to-diagnose bugs)

---

## Confidence-Rated Findings

| # | Finding | Severity | Confidence | Details |
|---|---------|----------|------------|---------|
| 1 | `build_forward_chain` produces incorrect chains for multi-meet DAGs: reversed BFS order is not a topological sort, and the position-based trim only skips the first meet member | Medium | High | comparison.rs lines 447-460. The chain may contain meet nodes and have events in wrong order. Mitigated because `compute_layers` does not use these chains, but they are in the public API `AbstractCausalRelation::DivergedSince`. |
| 2 | Head update in DivergedSince removes meet IDs instead of superseded head tips | Low | High | entity.rs lines 320-326. Works correctly by accident: when meet members ARE in the head they SHOULD be removed (event supersedes them), and when they are NOT in the head the remove is a no-op. But the code's intent is unclear and fragile for future changes. |
| 3 | LWW `apply_layer` fails with `InsufficientCausalInfo` when stored value's event_id is below the meet point | Critical | High | lww.rs lines 203-207, layers.rs lines 201-230. When a property was last written by an event ancestral to the meet, and a concurrent branch writes the same property, `layer.compare()` tries to find the old event in the layer's events map. If the old event was accumulated during BFS it works, but if the meet is the event itself (e.g., meet=[B] and stored event_id=A where A is B's parent), A won't be in the map, causing a hard error. |
| 4 | `children_of` linear scan creates O(N^2) behavior in `compute_layers` | High | High | layers.rs lines 159-166. For N events, each layer advancement scans all N events to find children. An attacker can craft a long-chain DAG that forces N layer iterations, each scanning N events, for O(N^2) total work. |
| 5 | StrictAscends-to-DivergedSince transformation violates depth precedence: old-branch events can win LWW tiebreak against causally-later head events | High | High | comparison.rs lines 115-148, lww.rs apply_layer. When an event from an old branch (parent deep in history) arrives, the transformation produces a DivergedSince where the layer comparison treats the old event and the current head's events as "Concurrent" (since neither descends from the other in the DAG). This falls through to lexicographic tiebreak by EventId hash, making the result non-deterministic rather than always favoring the deeper/newer event. |
| 6 | TOCTOU retry loop (MAX_RETRIES=5) with no backoff can exhaust under moderate contention | Medium | High | entity.rs lines 243-347. With 5+ concurrent writers on the same entity, each doing expensive BFS comparisons, the probability of 5 consecutive TOCTOU failures is non-trivial. The test `test_rapid_concurrent_transactions` accepts failures gracefully, but in production this means lost events. |
| 7 | Dual entity creation events produce permanently corrupted multi-root head | Medium | Medium | entity.rs lines 224-239. If two creation events (empty parent) race for the same entity_id, the second one gets DivergedSince with empty meet, resulting in both genesis events in the head. Any future comparison encounters two roots and may return Disjoint. |
| 8 | Budget counts BFS rounds not events, allowing unbounded event fetching per round | Low | High | retrieval.rs line 79, comparison.rs line 283. The budget tracks expansion steps (always 1 per round), not events fetched. A wide DAG frontier can cause thousands of events to be fetched in a single round for budget cost 1. |
| 9 | `Clock::from(Vec<EventId>)` does not sort, but `Clock::contains` uses binary search | Low | High | proto/src/clock.rs lines 26, 63-65. Creating a Clock from an unsorted Vec will cause `contains` to return incorrect results. |
| 10 | `build_forward_chain` chains in DivergedSince are not topologically sorted | Medium | High | comparison.rs lines 447-460. BFS visit order reversed is not a valid topological order for DAGs with branching. Any consumer of the `subject_chain`/`other_chain` fields that expects causal ordering will get incorrect results. Same root cause as #1 but emphasizing the ordering issue rather than the trim issue. |
| 11 | `compute_ancestry` and `compute_layers` have no upper bound on memory usage | Low | High | layers.rs lines 100-154, 172-199. For an entity with N events in its history, the ancestry set and events clone use O(N) memory. No cap is enforced. |
| 12 | No validation of DAG acyclicity or parent existence during event application | Low | Medium | entity.rs apply_event. Malformed events with cyclic parent references or non-existent parent IDs are not validated before being added to the entity. The `is_descendant` DFS in layers.rs has cycle protection (visited set), but cycles in the DAG could cause unexpected behavior in other code paths. |

---

## Summary

The most severe finding is **#3 / #5 combined**: the LWW backend's `apply_layer` can fail with a hard error when stored values reference events below the meet point, AND when events DO compare, the StrictAscends transformation produces "Concurrent" results that should be "Ascends", leading to incorrect LWW tiebreaks. Together, these mean that (a) some legitimate concurrent updates will hard-fail, and (b) those that don't fail may produce wrong values.

The second most impactful finding is **#4**: the O(N^2) `children_of` scan in `compute_layers` is a straightforward DoS vector that should be fixed with a pre-computed children index.

Finding **#7** (dual creation events) is a structural integrity issue that could cause long-term corruption, though it requires a specific race condition to trigger.
