# Review Matrix A2: LWW Register Backend and Layer Comparison Logic

**Reviewer focus:** CRDT/replication specialist -- LWW property backend and causal layer comparison
**Branch:** `concurrent-updates-event-dag`
**Date:** 2026-02-05
**Files reviewed:**
- `/Users/daniel/ak/ankurah-201/core/src/property/backend/lww.rs`
- `/Users/daniel/ak/ankurah-201/core/src/property/backend/mod.rs`
- `/Users/daniel/ak/ankurah-201/core/src/property/backend/yrs.rs`
- `/Users/daniel/ak/ankurah-201/core/src/event_dag/layers.rs`
- `/Users/daniel/ak/ankurah-201/core/src/event_dag/relation.rs`
- `/Users/daniel/ak/ankurah-201/core/src/event_dag/comparison.rs`
- `/Users/daniel/ak/ankurah-201/core/src/event_dag/navigator.rs`
- `/Users/daniel/ak/ankurah-201/core/src/event_dag/traits.rs`
- `/Users/daniel/ak/ankurah-201/core/src/entity.rs`
- `/Users/daniel/ak/ankurah-201/proto/src/data.rs`
- `/Users/daniel/ak/ankurah-201/proto/src/clock.rs`

---

## 1. Committed / Pending / Uncommitted State Machine

### The Three States

The `ValueEntry` enum in `lww.rs:21-25` defines three states:

```rust
enum ValueEntry {
    Uncommitted { value: Option<Value> },
    Pending { value: Option<Value> },
    Committed { value: Option<Value>, event_id: EventId },
}
```

### Transition Analysis

**Transition 1: User writes a value** (`LWWBackend::set`, line 70-73)
- Any state -> `Uncommitted { value }`
- This is unconditional. Correct.

**Transition 2: Operations collected for commit** (`to_operations`, lines 137-157)
- `Uncommitted { value }` -> `Pending { value }` (lines 142-147)
- Only `Uncommitted` entries transition. `Pending` and `Committed` entries are skipped. Correct.

**Transition 3: Event applied with tracking** (`apply_operations_internal` with `Some(event_id)`, lines 280-312)
- Any state -> `Committed { value, event_id }` (line 292)
- This occurs during `apply_operations_with_event`. Correct.

**Transition 4: Event applied without tracking** (`apply_operations_internal` with `None`, lines 280-312)
- Any state -> `Pending { value }` (line 293)
- This occurs during `apply_operations` (no event_id). Correct.

**Transition 5: Layer applied** (`apply_layer`, lines 169-254)
- Winning `to_apply` candidate -> `Committed { value, event_id }` (line 239)
- Non-winning or already-applied entries: unchanged.

### Stuck State Analysis

**Can `Pending` get stuck?** `Pending` is created by `to_operations()` (transition 2) and by untracked `apply_operations()` (transition 4). The `Pending` state has no `event_id`, which means:
- `to_state_buffer()` (line 112-127) will **fail** with an error if any entry is `Pending` (line 117-121). This is the intended guard: you cannot serialize state that has not been fully committed.
- `apply_layer()` will also **fail** for any stored `Pending` entry because `entry.event_id()` returns `None` (line 183-186).

The `Pending` -> `Committed` transition happens when the transaction commit flow calls `apply_operations_with_event` on the upstream entity (not the transaction fork). In the transaction fork itself, values may remain `Pending` after `to_operations()` is called, but the fork is discarded after commit. This is architecturally sound.

**Can `Uncommitted` get stuck?** If `to_operations()` is never called (e.g., the transaction is dropped without committing), `Uncommitted` entries remain. Since the transaction fork is discarded, this is a non-issue. On the primary entity, `set()` is only called on transaction forks (the primary is read-only per `is_writable()` in entity.rs:110-115).

**Verdict:** The state machine is correct. There is no stuck-state bug. The guard in `to_state_buffer()` correctly prevents serialization of incomplete state.

---

## 2. Per-Property Resolution with DivergedSince

### The Core Question

When `DivergedSince` produces two chains, does the LWW resolver correctly pick the winner for EACH property independently?

### Tracing the Flow

1. **entity.rs:277-318** (DivergedSince handling):
   - Accumulated events from navigator + incoming event are collected into a `BTreeMap<EventId, Event>`.
   - `compute_ancestry` computes which events are ancestors of the current head.
   - `compute_layers` partitions events into layers by causal depth from meet.
   - Each layer is applied to **all** backends.

2. **lww.rs:169-254** (apply_layer):
   - Seeds `winners` map with ALL currently stored values (lines 180-189).
   - Iterates through `already_applied` and `to_apply` events.
   - For each LWW operation in each event, extracts per-property changes.
   - For each property, uses `layer.compare()` to determine if the candidate beats the current winner.
   - Only writes winning values from `to_apply` events.

**Per-property independence:** Yes, the `winners` map is keyed by `PropertyName`. Each property's winner is computed independently. Different properties can have different winning events.

### The Known Bug: Stored Values vs. Layer Events

The seeding of the winners map (lines 180-189) uses ALL stored values, reading their `event_id` from the `Committed` entry. This is the partial fix from commit 4c41099 that the digest describes.

**The problem:** When `layer.compare()` is called between a stored value's `event_id` and a layer event's `event_id`, the stored value's event may NOT be in the layer's `events` map (the `Arc<BTreeMap<Id, E>>` inside `EventLayer`). The `is_descendant` function in `layers.rs:201-230` will return `Err(RetrievalError::Other("missing event for ancestry lookup"))` if it encounters an event ID not in the map.

This error is mapped to `MutationError::InsufficientCausalInfo` at line 203-207, which causes `apply_layer` to fail entirely.

**Concrete scenario:**
```
    A (genesis)
   / \
  B   C
  |
  D
```
- Entity at head [D], with `title` last written by event B.
- Event C arrives. DivergedSince with meet [A].
- Layer 1 contains {already_applied: [B], to_apply: [C]}.
- Stored value for `title` has `event_id = B`.
- `layer.compare(C_id, B_id)` traverses from C -> A and from B -> A. Both are in the events map (accumulated during BFS). This works.

But consider:
```
    A (genesis)
   / \
  B   C
  |
  D
  |
  E (head)
```
- Entity at head [E], with `title` last written by event B (events D, E did not touch `title`).
- Event C arrives. DivergedSince with meet [A].
- Layer 1 contains {already_applied: [B], to_apply: [C]}.
- Stored value for `title` has `event_id = B`.
- The events accumulated during comparison BFS from [E] toward [A] will include E, D, B, A and from C toward A will include C, A. So B IS in the events map.

Actually, wait -- the BFS traversal from the subject side starts at the incoming event's parent, not the entity's head. Let me re-trace:

For `compare_unstored_event(navigator, event_C, head=[E])`:
- Compares C's parent [A] against [E].
- Subject frontier: {A}. Comparison frontier: {E}.
- Step 1: Fetch A and E. A has no parents (genesis). E has parent [D].
- A is seen from subject. E is seen from comparison.
- Subject frontier: {} (A has no parents). Comparison frontier: {D}.
- Step 2: Fetch D. D has parent [B].
- Comparison frontier: {B}.
- Step 3: Fetch B. B has parent [A].
- A is now seen from both sides. Meet = [A].
- Subject_visited: [A]. Other_visited: [E, D, B].

The events map contains: {A, E, D, B} (accumulated via AccumulatingNavigator). C is added manually at entity.rs:282.

Now `compute_layers(&events, &[A], &current_ancestry_of_E)`:
- current_ancestry = {A, B, D, E}
- Children of A in events: B and C (C was added).
- Layer 0: already_applied = [B] (in ancestry), to_apply = [C] (not in ancestry).
- Children of B: D. Children of C: none.
- Layer 1: already_applied = [D], to_apply = [] (no work). Skipped.
- Children of D: E.
- Layer 2: already_applied = [E], to_apply = [] (no work). Skipped.

So only Layer 0 is generated with B and C. The stored value for `title` has event_id = B. In `apply_layer`, `layer.compare(C_id, B_id)` works because both C and B are in the events map.

**But now consider a deeper scenario:**
```
    A (genesis)
   / \
  B   X
  |   |
  C   Y  (X and Y don't touch title; B wrote title)
  |
  D (head)
```
- Entity at head [D], `title` last written by B.
- Event Y arrives.
- `compare_unstored_event(navigator, Y, head=[D])`: compares Y's parent [X] against [D].
- BFS from [X] and [D] backward. Subject (X side) visits X, then A. Comparison (D side) visits D, C, B, A.
- Meet = [A]. Events accumulated: {X, A, D, C, B}. Y is added manually.
- Layers from meet A: children of A are B and X.
- Layer 0: already_applied = [B], to_apply = [X].
- Layer 1: already_applied = [C], to_apply = [Y].
- Layer 2: already_applied = [D], to_apply = []. Skipped.

In Layer 0, stored value `title` has event_id = B. `layer.compare(X_id, B_id)`: X's events map contains {X, A, D, C, B, Y}. X has parent A. B has parent A. Neither is a descendant of the other. Result: `Concurrent`. Then lexicographic tiebreak on event IDs. This is correct -- X and B are indeed concurrent.

In Layer 1, stored value `title` now has event_id = winner of {B, X}. Let's say B won. `layer.compare(Y_id, B_id)`: Y has parent X. X has parent A. B has parent A. Y -> X -> A, B -> A. Is Y a descendant of B? No. Is B a descendant of Y? No. They are `Concurrent`. Lexicographic tiebreak. But Y is NOT concurrent with B in the DAG -- Y descends from X which is concurrent with B. The layer model treats same-depth events as concurrent, so Y competes with C (the already_applied at that depth). But `title` was written by B, not C. The `winners` map still holds B's value. So `layer.compare(Y_id, B_id)` is comparing across layers.

Wait -- the `winners` map persists across layers. After Layer 0, if B won, `winners["title"] = B`. In Layer 1, events are {C (already), Y (to_apply)}. Y has LWW ops (if it writes `title`). If Y writes `title`, it competes against the current winner B. `layer.compare(Y_id, B_id)` uses the Layer 1 events map, which contains the same full event set `{X, A, D, C, B, Y}`. `is_descendant(events, Y, B)`: Y -> X -> A. Never reaches B. So not a descendant. `is_descendant(events, B, Y)`: B -> A. Never reaches Y. So `Concurrent`. Lexicographic tiebreak.

**This is actually wrong!** Y is causally after X (Y descends from X). If B beat X in Layer 0, and now Y (which is after X) competes with B, Y should NOT be treated as concurrent with B from a causal perspective. Y is on the same branch as X, so if B beat X, B should also beat Y (since Y didn't independently decide to write title -- it inherited X's branch context). But lexicographic tiebreak might give Y the win if Y's event ID happens to be larger.

However, this is a subtle point: in a pure LWW model, the question is "who wrote this value most recently?" If Y writes `title`, Y is a fresh write regardless of branch history. The LWW semantics say: the last writer wins. Y IS concurrent with B (neither causally precedes the other in terms of the DAG -- Y descends from X, B does not descend from X, and Y does not descend from B). So `Concurrent` with lexicographic tiebreak is defensible.

The more serious issue is the one identified in the digest: **when a property was last written by a non-head, non-layer event.** The current code seeds from stored values, and `layer.compare()` works because the BFS traversal accumulates enough context. But there is a scenario where this breaks:

If the stored value's `event_id` points to an event that was NOT traversed by the BFS (e.g., it was written far back in history and the BFS meet point is more recent), then `is_descendant` will fail with a missing-event error.

**Example:**
```
    G (genesis, writes title="foo")
    |
    A
   / \
  B   C (meet)
  |   |
  D   E
  |
  F (head)
```
- Entity at head [F]. `title` was last written by G (genesis).
- Event E arrives. Meet = [C].
- Events accumulated during BFS: {A was NOT necessarily traversed -- depends on how far back BFS went}.

Actually, the BFS continues until both frontiers are empty. In this case, comparing E's parent [C] against [F]:
- Subject frontier: {C}. Comparison frontier: {F}.
- Fetch C (parent A) and F (parent D).
- Subject frontier: {A}. Comparison frontier: {D}.
- Fetch A (parent G) and D (parent B).
- Subject frontier: {G}. Comparison frontier: {B}.
- Fetch G (parent []) and B (parent A).
- G is genesis. B has parent A. A was seen from subject.
- Subject frontier: {}. Comparison frontier: {A}.
- A is already visited from subject side. Now visited from comparison too. Meet = some analysis...

Actually the BFS should find meet = [A] or potentially [C] depending on the structure. The key point is that the BFS traverses backward from BOTH sides until it finds common ancestors, so it will traverse through all events between the tips and the meet. The accumulated events will include everything from F back to meet and from E's parent back to meet.

If `title` was written by G (genesis), and meet is [A], then G is NOT in the accumulated events (the BFS stops at the meet, not at genesis... actually no, the BFS continues until frontiers are empty, it doesn't stop at meet). Let me re-read the BFS logic.

Looking at comparison.rs, the BFS continues until `check_result` returns `Some`. The `check_result` function checks:
1. Taint (line 491)
2. `unseen_comparison_heads == 0` -> StrictDescends (line 543)
3. `unseen_subject_heads == 0` -> StrictAscends (line 553)
4. Both frontiers empty -> compute meet and return DivergedSince (line 558)
5. Budget exhausted (line 602)

For divergence, the BFS continues until both frontiers are empty. The frontiers extend by adding parents of processed events. So the BFS DOES traverse all the way back to genesis. All events in the DAG will be accumulated.

**Correction:** The events accumulated will be EVERYTHING the navigator returns. If the navigator is backed by local storage (which has all events), then yes, all events are accumulated. The `AccumulatingNavigator` stores everything returned from `expand_frontier`.

So the concern about missing events in the `is_descendant` lookup is mitigated by the fact that BFS traverses all the way to genesis. The events map used by `EventLayer::compare()` should contain all events from both branches.

**However**, there is a subtlety: the events map in the EventLayer is `Arc::new(events.clone())` where `events` is the accumulated events BTreeMap from the navigator PLUS the incoming event. If the navigator's underlying storage is complete (has all events for this entity), then the events map is complete. If storage is incomplete (e.g., ephemeral node with partial history), then `is_descendant` can fail.

---

## 3. Depth-Based Precedence Analysis

The layer model does NOT use explicit "depth" values. Instead, it uses structural position in the DAG via frontier expansion from meet. Let me trace through each case:

### Case 1: Property written at depth 3 on branch X, depth 20 on branch Y

```
    M (meet)
   / \
  X1  Y1
  |   Y2
  X2  ...
  |   Y19
  X3  Y20  <-- writes title
```

- Layers are computed by frontier expansion from M.
- Layer 0: {X1, Y1}
- Layer 1: {X2, Y2}
- Layer 2: {X3, Y3}
- ...
- Layer 19: {Y20} (X branch has ended)

The `winners` map accumulates across layers. If `title` is written by X3 (layer 2, depth 3) and Y20 (layer 19, depth 20):
- After Layer 2: winners["title"] = X3 (no other competitor at that layer).
- Layers 3-18: no `title` writes, winners unchanged.
- Layer 19: Y20 writes `title`. `layer.compare(Y20, X3)`: `is_descendant(events, Y20, X3)` traces Y20 -> Y19 -> ... -> Y1 -> M. Never reaches X3. `is_descendant(events, X3, Y20)` traces X3 -> X2 -> X1 -> M. Never reaches Y20. Result: `Concurrent`. Lexicographic tiebreak on event IDs.

**Issue:** The spec says "further from meet wins," but the code does NOT implement depth-based precedence. The code uses `layer.compare()` which determines causal ordering (Descends/Ascends/Concurrent). For truly concurrent events (different branches from meet), the result is always `Concurrent`, regardless of depth. The tiebreak is always lexicographic on event ID.

**This means depth does NOT determine the winner.** The depth-20 event on branch Y does NOT automatically beat the depth-3 event on branch X. The winner is determined by lexicographic event ID comparison. This is actually the correct LWW behavior -- "last write" is determined by a deterministic total order (here, lexicographic event ID), not by depth. Depth only matters for layered application order, ensuring that within a single branch, later writes overwrite earlier ones (because later events descend from earlier ones, so `Descends` kicks in).

### Case 2: Property written on only one branch

```
    M (meet)
   / \
  B   C (writes title)
```

- Layer 0: {B (already_applied), C (to_apply)}
- C writes `title`. The `winners` map may have a stored value from before M. Stored value's event_id is compared against C via `layer.compare()`.
- If stored value's event is an ancestor of both B and C (i.e., it is at or before M), then `is_descendant(events, C, stored_event_id)` will return `true` (C descends from meet and meet descends from or equals stored event). Result: `Descends`. C wins. Correct.
- If stored value has no competitor in the layer, C is inserted as the new winner. Correct.

### Case 3: Property written at equal depth on both branches

```
    M (meet)
   / \
  B   C (both write title at depth 1)
```

- Layer 0: {B (already_applied), C (to_apply)}
- Both write `title`. Stored value might be from before M.
- B processed first (from `already_applied`). B competes against stored value. B descends from stored value -> B wins.
- C processed second (from `to_apply`). C competes against B. `layer.compare(C, B)`: C -> M, B -> M. Neither descends from the other. Result: `Concurrent`. Lexicographic tiebreak. Correct.

### Case 4: Property written multiple times on one branch

```
    M (meet)
   / \
  B   C
  |   |
  D   E (only branch has title writes: C and E both write title)
```

- Layer 0: {B, C}. C writes `title`. C wins its layer competition.
- Layer 1: {D, E}. E writes `title`. E competes against current winner (C). `layer.compare(E, C)`: E -> C -> M. E descends from C. Result: `Descends`. E replaces C. Correct -- only the latest write matters.

### Verdict on Depth Precedence

The implementation does NOT use "further from meet wins" as a rule. It uses causal dominance (`Descends`/`Ascends`) within layers and lexicographic event ID for truly concurrent events. This is correct LWW semantics. The layer ordering ensures that writes within a branch are applied in causal order (earlier layers first), so the latest write on a branch naturally wins by `Descends` in subsequent layers.

---

## 4. Cross-Replica Convergence

### The Critical Question

If replica R1 sees events in order [A, B, C] and replica R2 sees [C, A, B], do they converge?

### Scenario Setup

Consider:
```
    G (genesis)
   / \
  A   B
       \
        C
```
All three events write property `title`. Entity initially at head [G].

**Replica R1 receives events in order: A, B, C**

1. **R1 receives A:** `compare_unstored_event(A, head=[G])`. A's parent is [G], head is [G]. `Equal` -> `StrictDescends{chain:[A]}`. Apply A's operations. Head becomes [A]. title = A's value.

2. **R1 receives B:** `compare_unstored_event(B, head=[A])`. B's parent is [G]. `compare([G], [A])` -> `StrictAscends` (G is before A). Transformed to `DivergedSince{meet=[G], other=[A]}`. Layer computation with events {G, A, B}: Layer 0 = {already_applied: [A], to_apply: [B]}. Both concurrent from G. `apply_layer`: B's title competes with stored title (event_id = A). `layer.compare(B, A)`: Neither descends. `Concurrent`. Lexicographic tiebreak. Head becomes [A, B].

3. **R1 receives C:** `compare_unstored_event(C, head=[A, B])`. C's parent is [B]. `compare([B], [A, B])` -> `StrictAscends`. Transformed to `DivergedSince{meet=[B], other=[A]}`. subject_chain = [C], other_chain = []. Layer computation with events accumulated during BFS + C. Since other_chain is empty, the events from A's branch may or may not be in the accumulated set.

   Actually, let me trace more carefully. `compare([B], [A, B])`:
   - Subject: {B}. Comparison: {A, B}.
   - B is in both frontiers. Process B: seen from subject and comparison. B is in original_comparison, so `unseen_comparison_heads` decremented. B has parent [G].
   - After processing B: subject_frontier = {G}. comparison_frontier = {A} (B was removed from comparison).
   - A is in original_comparison. Fetch G and A.
   - G: subject, parent []. Genesis root. Subject frontier = {} (no parents).
   - A: comparison, parent [G]. G already seen from subject. A seen from comparison. Comparison frontier = {G}. But G already processed... hmm.

   Actually the frontier adds G again. But G was already processed. Let me look at the code more carefully.

   `process_event` (line 299-382): it calls `self.subject_frontier.remove(&id)` and `self.comparison_frontier.remove(&id)`. So the frontier will have G added by A's parents. G is re-fetched.

   Wait, the `expand_frontier` call includes ALL frontier IDs from both sides. G was already removed from subject_frontier but now added to comparison_frontier. So it gets fetched again. `process_event` for G: `from_subject = subject_frontier.remove(G) = false` (G not in subject frontier anymore). `from_comparison = comparison_frontier.remove(G) = true`. G seen from comparison now too. G was already seen from subject (first pass). So G becomes common. Meet candidate.

   unseen_subject_heads: initially 1 (B in subject). B was processed, seen from comparison, but the check is `original_subject.contains(&id)` on the comparison side (line 379). B was processed from both sides simultaneously, so line 379 checks if B is in original_subject = {B}. Yes. `unseen_subject_heads` decremented to 0.

   After processing G second time: both frontiers empty. Meet = [G] (or refined). But `unseen_subject_heads == 0` is checked at line 553 before frontiers-empty at line 558. So result is `StrictAscends`.

   Then in `compare_unstored_event`, `StrictAscends` is transformed to `DivergedSince`:
   - meet = C's parent = [B]
   - other = head tips not in parent = [A] (since head is [A, B] and parent is [B])
   - subject_chain = [C]
   - other_chain = []

   Now in entity.rs DivergedSince handler:
   - events = accumulated_events + C. Accumulated events from BFS of [B] vs [A, B]: {B, G, A}. Plus C. So events = {B, G, A, C}.
   - current_ancestry for head [A, B]: ancestry in events = {A, G, B}. (A->G, B->G).
   - compute_layers from meet [B]: children of B in events = {C} (C has parent [B]).
   - Layer 0: C is not in ancestry, so to_apply = [C]. No already_applied.

   `apply_layer` for Layer 0: Stored title has event_id = winner of step 2 (lexicographic winner of A and B). C is in to_apply. `layer.compare(C, stored_winner)`: events map is {B, G, A, C}. `is_descendant(C, stored_winner)`: If stored_winner is B, then C -> B. Yes, descendant. C wins by `Descends`. If stored_winner is A, then C -> B -> G. A -> G. C does not descend from A. A does not descend from C. `Concurrent`. Lexicographic tiebreak.

   Head update: remove meet [B] from head [A, B] -> [A]. Insert C -> [A, C].

**Final R1 state:** Head = [A, C]. title = ?
- If A > B lexicographically: After step 2, title = A's value. After step 3, C competes with A. Concurrent. If C > A, title = C's value. If A > C, title = A's value.
- If B > A lexicographically: After step 2, title = B's value. After step 3, C descends from B, so C wins by `Descends`. title = C's value.

**Replica R2 receives events in order: C, A, B**

1. **R2 receives C:** `compare_unstored_event(C, head=[G])`. C's parent is [B]. `compare([B], [G])` -- but B is not stored yet! The navigator cannot fetch B. This fails with a retrieval error.

**Key insight:** The protocol assumes causal delivery (parent before child). C cannot arrive before B because C's parent is [B]. So the order [C, A, B] violates the causal delivery assumption. This is documented in the spec and entity.rs:256-266 discussion in the digest.

### Valid Cross-Replica Scenario

Let me construct a valid scenario where two replicas see events in different (but causally valid) orders:

```
    G (genesis)
   / \
  A   B
```

R1 sees [A, B]. R2 sees [B, A]. Both are causally valid.

**R1: [A, B]**
1. Apply A: StrictDescends from G. Head = [A]. title = A.
2. Apply B: B's parent [G] vs head [A]. StrictAscends -> DivergedSince{meet=[G], other=[A]}. Layer 0: {already:[A], to_apply:[B]}. `compare(B, A)`: Concurrent. Lexicographic tiebreak. Head = [A, B].

**R2: [B, A]**
1. Apply B: StrictDescends from G. Head = [B]. title = B.
2. Apply A: A's parent [G] vs head [B]. StrictAscends -> DivergedSince{meet=[G], other=[B]}. Layer 0: {already:[B], to_apply:[A]}. `compare(A, B)`: Concurrent. Lexicographic tiebreak. Head = [B, A] (= [A, B] since Clock is sorted).

**Convergence check:**
- Both replicas have head [A, B].
- For title: In R1, the winner is max(A_id, B_id) by lexicographic comparison. In R2, the winner is also max(A_id, B_id). Same winner. **Converges.**

But wait -- there is a subtlety. In R1 step 2, the stored value is A's title (event_id = A_id). The apply_layer competes B against stored A. If B wins (B_id > A_id), the value is updated to B's title. If A wins (A_id > B_id), the value stays as A's title (B is from to_apply but loses, so no mutation).

In R2 step 2, the stored value is B's title (event_id = B_id). The apply_layer competes A against stored B. If A wins (A_id > B_id), the value is updated to A's title. If B wins (B_id > A_id), the value stays as B's title.

In both cases, the final value is the one with max(A_id, B_id). **Convergence confirmed for this case.**

### Three-Event Convergence

```
    G (genesis)
   /|\
  A B C
```

R1 sees [A, B, C]. R2 sees [C, B, A]. R3 sees [B, A, C].

Each application of the second and third events triggers DivergedSince with layers. The key is that `apply_layer` uses `layer.compare()` which returns `Concurrent` for all pairs (they all have parent [G]). The tiebreaker is lexicographic event ID, which is deterministic.

After all three events are applied on any replica:
- Head = [A, B, C].
- For each property, the winner is the event with the highest event ID among those that wrote that property. This is deterministic regardless of application order.

**But there is a complication:** Events are applied one at a time via `apply_event`. The second event triggers a 2-way merge, and the third event triggers another merge against the 2-way head. The layer computation and winners are computed fresh each time. Does the sequential accumulation converge?

Let me trace R1 [A, B, C] vs R2 [C, B, A] more carefully:

**R1: [A, B, C]**
1. A applied. Head = [A]. title = A's value.
2. B applied. DivergedSince meet=[G]. Layer: {already:[A], to_apply:[B]}. Winner = max(A, B). Head = [A, B].
3. C applied. C's parent [G] vs head [A, B]. compare([G], [A, B]) -> StrictAscends. DivergedSince{meet=[G], other=[A, B]}.
   - Events accumulated: BFS from [G] vs [A, B] traverses G, A, B. Plus C.
   - Layers from meet [G]: children of G = {A, B, C}. ancestry of head [A, B] = {A, B, G}.
   - Layer 0: already_applied = [A, B], to_apply = [C].
   - `apply_layer`: Stored title has event_id = winner of step 2. C competes against stored winner. Both have parent G. `Concurrent`. Lexicographic tiebreak.
   - Head: remove G from [A, B] -> [A, B] (G is not in head!). Insert C -> [A, B, C].

   **Wait -- the head update is wrong!** The meet is [G], but G is not in the current head [A, B]. `state.head.remove(&G)` does nothing. Then `state.head.insert(C)` adds C. Final head = [A, B, C]. This is actually correct -- the meet is the common ancestor, not necessarily a head member. The head should have all three concurrent tips.

   Actually, there is an issue here. The code at entity.rs:323-326 removes meet IDs from head and adds the new event. Meet = [G]. G is not in head [A, B], so nothing is removed. C is added. Head = [A, B, C]. This is correct behavior.

   But what about the value? Stored title event_id is max(A_id, B_id). C competes with that. Winner = max(max(A_id, B_id), C_id) = max(A_id, B_id, C_id). Correct.

**R2: [C, B, A]**
1. C applied. Head = [C]. title = C's value.
2. B applied. DivergedSince meet=[G]. Layer: {already:[C], to_apply:[B]}. Winner = max(C_id, B_id). Head = [C, B] = [B, C].
3. A applied. A's parent [G] vs head [B, C]. StrictAscends -> DivergedSince{meet=[G], other=[B, C]}.
   - Layer 0: already_applied = [B, C], to_apply = [A].
   - Stored title event_id = max(C_id, B_id). A competes. `Concurrent`. Tiebreak.
   - Winner = max(max(C_id, B_id), A_id) = max(A_id, B_id, C_id). Same as R1. **Converges.**
   - Head = [B, C] - {} + [A] = [A, B, C]. Same as R1. **Converges.**

### Convergence Verdict

For events that are all concurrent (same parent), cross-replica convergence holds because:
1. The lexicographic event ID comparison is a total order.
2. The `apply_layer` logic accumulates winners correctly.
3. The `Concurrent` relation always falls back to the deterministic tiebreaker.

For events with causal relationships (one descends from another), convergence also holds because `layer.compare()` correctly identifies `Descends`/`Ascends` and the causally later event always wins.

**Caveat:** Convergence depends on the causal delivery assumption. If events arrive out of causal order, the system may fail (retrieval errors for missing parent events).

---

## 5. Layer Comparison API (layers.rs)

### compute_layers Analysis

The function at `layers.rs:100-154`:

1. **Input:** events map, meet point, current_head_ancestry.
2. **Algorithm:** Frontier expansion from meet children.
3. **Output:** Layers in causal order.

**Correctness of frontier advancement:**
- Line 138-150: Next frontier = children of current frontier whose parents are ALL processed.
- This correctly handles merge events (diamond reconvergence) -- a merge event only enters a layer once all its parents have been processed.

**Correctness of partitioning:**
- `already_applied`: event_id in `current_head_ancestry`.
- `to_apply`: event_id NOT in `current_head_ancestry`.
- This is correct. Events in the current head's ancestry have already been applied to the entity state.

**Layer skipping:**
- Line 130: Only create layer if `!to_apply.is_empty()`.
- This is an optimization but has an important side effect: if a layer has only `already_applied` events, those events are NOT presented to `apply_layer`. This means the `winners` map in LWW apply_layer does not get updated for events in skipped layers.
- **Is this a problem?** Only if a skipped layer contains events that wrote properties that later need to compete. But since those events are already in the stored state (their values are in the `values` map with correct `event_id`), the seeding of `winners` from stored values handles this correctly. The stored value already reflects the latest write from the already-applied branch.

**Actually, this is NOT necessarily correct.** Consider:

```
    M (meet)
   / \
  B   C
  |
  D
```
Head = [D]. Events B and D are already applied. C is to_apply.
- Layer 0: already_applied = [B], to_apply = [C]. Layer created.
- Layer 1: already_applied = [D], to_apply = []. Layer SKIPPED.

The stored value for `title` might have event_id = D (if D wrote title). In apply_layer for Layer 0, C competes against stored value (event_id = D). `layer.compare(C, D)`: C -> M, D -> B -> M. C does not descend from D. D does not descend from C. `Concurrent`. Lexicographic tiebreak.

But wait -- D is causally AFTER B. C is concurrent with B. Is C concurrent with D? In the DAG, C has parent M, D has parent B, B has parent M. C does not descend from D, D does not descend from C. So yes, `Concurrent` is correct. They are on different branches and neither causally precedes the other.

**The issue**: If the stored value's event_id points to an event at a deeper layer (D at layer 1), and the current layer is layer 0, the comparison uses the full events map which includes D. `is_descendant(events, C, D)` correctly returns false. So the comparison works. The fact that D is "deeper" does not matter -- what matters is causal relationship, which is correctly determined.

### EventLayer::compare Analysis

The `compare` method at `layers.rs:57-70`:

```rust
pub fn compare(&self, a: &Id, b: &Id) -> Result<CausalRelation, RetrievalError> {
    if a == b { return Ok(CausalRelation::Descends); }
    if is_descendant(&self.events, a, b)? { return Ok(CausalRelation::Descends); }
    if is_descendant(&self.events, b, a)? { return Ok(CausalRelation::Ascends); }
    Ok(CausalRelation::Concurrent)
}
```

**Issue with a == b:** When `a == b`, the method returns `Descends`. This is semantically questionable -- an event does not "descend from" itself. It would be more correct to return `Equal` or a separate variant. However, in the LWW context, this case means "same event, so current winner stays" which is the same behavior as `Descends` (no replacement). Functionally harmless.

**is_descendant correctness:** The function at `layers.rs:201-230` does a BFS backward from `descendant` looking for `ancestor`. It returns `false` when `descendant == ancestor` (line 207-209), which is correct (an event is not a proper descendant of itself). The `compare` method handles the equality case before calling `is_descendant`.

**Error propagation:** `is_descendant` returns `Err` if an event in the BFS path is not in the events map (line 221). This error propagates to `apply_layer` as `InsufficientCausalInfo`. This is the correct behavior -- if the DAG context is incomplete, the resolution cannot proceed safely.

---

## 6. LWW vs Yrs Backend Consistency

### LWW apply_layer (lww.rs:169-254)

- Seeds winners from stored values.
- Iterates through already_applied and to_apply events.
- Uses `layer.compare()` for causal comparison.
- Only writes winners from `to_apply`.
- Tracks `event_id` per property.

### Yrs apply_layer (yrs.rs:186-209)

- Ignores `already_applied` entirely.
- Applies all operations from `to_apply` events.
- Relies on Yrs CRDT internal idempotency.

### Consistency Analysis

The two backends handle layers differently, which is correct:

1. **LWW needs comparison context** (`already_applied`) to determine winners. Yrs does not.
2. **LWW is NOT idempotent** -- applying the same event twice would overwrite the event_id tracking. Yrs IS idempotent (Yrs internally deduplicates operations by state vector).
3. **LWW changes are applied selectively** (only winners from to_apply). Yrs changes are applied unconditionally.

**Potential issue with the entity-level layer loop:** In entity.rs:300-317, ALL backends receive ALL layers:

```rust
for layer in &layers {
    for (_backend_name, backend) in state.backends.iter() {
        backend.apply_layer(layer)?;
    }
}
```

This means the Yrs backend receives layers it may not need, and the LWW backend receives layers that may contain only Yrs operations. Both handle this gracefully:
- LWW: checks for `operations.get(&"lww")` and skips events without LWW operations.
- Yrs: checks for `operations.get(&"yrs")` and skips events without Yrs operations.

**Consistency concern:** The LWW backend seeds `winners` from stored values at the START of each `apply_layer` call (lines 180-189). Since layers are applied sequentially, the second layer call will re-seed from the state as modified by the first layer. This is correct -- each layer sees the accumulated state from all prior layers.

**However**, there is a subtle issue: the LWW backend seeds winners from ALL stored values, not just the values relevant to the current layer's events. If a stored value's `event_id` is not in the `events` map of the current layer, the `layer.compare()` call will fail.

Wait -- all layers share the same `events` map (it is `Arc::new(events.clone())` in compute_layers, line 107, and all layers get `Arc::clone(&events)` at line 131). So all layers have access to the same complete set of events. This means `layer.compare()` should always work, as long as the overall events map is complete.

---

## 7. Head Update Correctness in DivergedSince

At entity.rs:320-326:

```rust
for parent_id in &meet {
    state.head.remove(parent_id);
}
state.head.insert(event.id());
```

The `meet` here comes from the `DivergedSince` variant. This is the common ancestor(s).

**Issue:** The meet is the GCA (greatest common ancestor), NOT the event's parent. The incoming event's parent is its direct parent in the DAG, while the meet is the deepest common ancestor between the incoming event's lineage and the current head.

For the StrictAscends-transformed-to-DivergedSince case (comparison.rs:127-148), meet = event's parent clock. So removing meet from head makes sense -- we're removing the tips that this event's parent covers.

For the genuine DivergedSince case (comparison.rs:150-155), meet is the GCA, which is typically NOT in the current head. Removing GCA IDs from the head does nothing (they are not head members). Then inserting the new event adds a new tip. This leaves the original head tips intact AND adds the new event, resulting in a multi-head.

**But is this correct?** If the head is [D] and event C arrives with meet at [A]:

```
    A (meet)
   / \
  B   C (incoming)
  |
  D (head)
```

- Remove A from head [D]: no-op (A not in head).
- Insert C: head becomes [D, C].

Is [D, C] correct? D descends from B which descends from A. C descends from A. D and C are concurrent. Head [D, C] correctly represents two concurrent tips. **Correct.**

For the StrictAscends case:
```
    G
   / \
  A   B (head member)
  |
  C (incoming, parent = [A])
```
Head = [A, B]. Event C arrives. Meet = [A] (C's parent). Other = [B].
- Remove A from head [A, B]: head becomes [B].
- Insert C: head becomes [B, C].

Is [B, C] correct? C extends A (which was a head tip). B is unchanged. New head tips are [B, C]. **Correct.**

---

## 8. Critical: The Stored-Value/Layer-Event Causal Comparison Gap

I want to trace one more scenario very carefully to verify the known bug (or confirm it's mitigated in the current code).

**Scenario from the digest:**
```
Branch B: B1 writes title = X. Then B2 happens, does not touch title. Head = [B2].
Branch C: C1 writes title = Y.
Genesis: G.
```

```
    G
   / \
  B1  C1 (writes title=Y)
  |
  B2 (head, does not touch title)
```

Entity at head [B2]. Stored title has event_id = B1.

Event C1 arrives. `compare_unstored_event(C1, head=[B2])`:
- C1's parent is [G]. compare([G], [B2]).
- BFS from [G] and [B2].
- Fetch G and B2. G: subject, parent []. B2: comparison, parent [B1].
- Subject frontier: {}. Comparison frontier: {B1}.
- Subject frontier empty but comparison not. BFS continues.
- Fetch B1. B1: comparison, parent [G]. G already seen from subject. B1 seen from comparison.
- G is now common (seen from both). Meet = [G].
- Both frontiers empty. Result: DivergedSince{meet=[G]}.
- Transformed in compare_unstored_event: subject_chain has C1 appended.
- Events accumulated: {G, B2, B1}. Plus C1 added at entity.rs:282.

`compute_ancestry(&events, &[B2])` = {B2, B1, G}.
`compute_layers(&events, &[G], &{B2, B1, G})`:
- Children of G: B1, C1.
- Layer 0: B1 in ancestry -> already_applied. C1 not in ancestry -> to_apply. Layer created.
- Children of B1: B2. Children of C1: none.
- Layer 1: B2 in ancestry -> already_applied. to_apply empty. Layer SKIPPED.

`apply_layer` for Layer 0:
- Seed winners from stored values: title -> Candidate{value="X", event_id=B1, from_to_apply=false}.
- Process already_applied [B1]: B1 has LWW operations for title. Candidate{value="X", event_id=B1_id, from_to_apply=false}. Compare B1 vs stored B1: same event_id. `layer.compare(B1, B1)` returns `Descends` (equality case). No change.
- Process to_apply [C1]: C1 has LWW operations for title. Candidate{value="Y", event_id=C1_id, from_to_apply=true}. Compare C1 vs current winner B1: `layer.compare(C1_id, B1_id)`. Events = {G, B2, B1, C1}. `is_descendant(C1, B1)`: C1 -> G. Not B1. `is_descendant(B1, C1)`: B1 -> G. Not C1. Result: `Concurrent`. Lexicographic tiebreak.

**This is correct!** B1 and C1 are indeed concurrent (both have parent G). The lexicographic tiebreak picks the deterministic winner. B1 DOES participate in the competition because it is seeded from stored values AND it appears in the already_applied list.

**So the bug described in the digest is actually FIXED in the current code.** The seeding of winners from stored values (lines 180-189) ensures that B1's value competes even though B1 is not a head member. The `layer.compare()` call works because B1 is in the events map (it was accumulated during BFS).

**But wait -- what if B1 is NOT in the events map?** This would happen if the BFS did not traverse through B1. In the scenario above, the BFS from [B2] goes B2 -> B1 -> G, so B1 IS accumulated. But could there be a scenario where it isn't?

The BFS traverses from the comparison head backward until it meets the subject head's ancestor frontier. Since B1 is an ancestor of B2 (the head), and the BFS starts from B2, B1 will always be traversed. **The stored value's event is always an ancestor of the current head, and the BFS always traverses the head's full ancestry until it reaches the meet.** Therefore, B1 will always be in the accumulated events.

**Potential exception:** If the BFS budget runs out before reaching B1, the comparison returns `BudgetExceeded` rather than `DivergedSince`, so the layer logic is never invoked. This is handled correctly.

**Remaining gap:** The stored value's event_id could theoretically point to an event on a DIFFERENT branch than the current head if the entity went through a prior merge that resolved in favor of that branch's value. In that case:

```
    G
   /|\
  X  Y  Z (incoming)
```
Head was [X], then Y arrived, Y won for title (Y_id > X_id). Head becomes [X, Y]. Stored title event_id = Y.

Now Z arrives. Meet = [G]. Events = {G, X, Y, Z}. Layer 0: already_applied = [X, Y], to_apply = [Z]. Stored title event_id = Y. `layer.compare(Z_id, Y_id)`: Y -> G. Z -> G. Concurrent. Tiebreak. This works.

But what if there was a deeper history?

```
    G
   / \
  A   B
  |   |
  X   Y
```
Head was [X], then Y arrived. DivergedSince meet=[G]. Layer 0: {A, B}. Layer 1: {X, Y}. After Layer 0, title winner is max(A,B). After Layer 1, title winner is max(X,Y) (both descend from their respective Layer 0 parents, so they beat the Layer 0 winner via `Descends`).

Now, later, Z arrives with parent [G]:
```
    G
   / | \
  A  B  Z
  |  |
  X  Y (head = [X, Y])
```
Meet = [G]. Events = {G, A, B, X, Y, Z}.
- Layer 0: already_applied = [A, B], to_apply = [Z].
- Layer 1: already_applied = [X, Y], to_apply = []. Skipped.

In apply_layer for Layer 0: Stored title event_id = winner(X,Y). Z competes against winner(X,Y). `layer.compare(Z, winner(X,Y))`: `is_descendant(Z, X)` = Z -> G, never reaches X. `is_descendant(X, Z)` = X -> A -> G, never reaches Z. `Concurrent`. Tiebreak.

**This is correct!** Z IS concurrent with X and Y (they are on different branches from G). The lexicographic tiebreak is the right resolution.

But the semantic concern from the digest is: should Z be able to beat X/Y even though X/Y are "deeper" in the DAG (further from G)? In a pure LWW model, the answer is yes -- "last write wins" is determined by event ID, not by depth. The depth-based layer application ensures WITHIN-BRANCH ordering (later events override earlier ones on the same branch), but ACROSS-BRANCH resolution is always by event ID tiebreak. This is a valid design choice.

---

## 9. Additional Observations

### 9.1 All Backends Receive All Layers (entity.rs:300-306)

Every layer is sent to every backend, even if only one backend has operations in that layer. The backends filter internally. This is wasteful but correct. For a small number of backends (currently 2: LWW and Yrs), this is negligible overhead.

### 9.2 Backend Creation During Layer Application (entity.rs:308-317)

If an event in `to_apply` uses a backend not yet in `state.backends`, a new backend is created and receives the layer. But this new backend is created EMPTY -- it has no stored values to seed the `winners` map. This is correct for the first time a backend is used, since there are no prior values.

However, there is a potential issue: the new backend receives the SAME layer that was already applied to existing backends. The `apply_layer` call on the new backend (line 313) processes the same layer. Since the new backend is empty, it correctly processes only the events that have operations for its backend type.

### 9.3 LWW apply_layer Fails When Stored Values Lack event_id (lww.rs:183-186)

If any stored value is in `Uncommitted` or `Pending` state, `apply_layer` returns an error. This should never happen in practice because:
- `apply_layer` is called on the primary entity (not a transaction fork).
- The primary entity only has `Committed` values (set via `apply_operations_with_event`).
- The transaction fork may have `Uncommitted`/`Pending` values, but `apply_layer` is not called on forks.

If this invariant is somehow violated (e.g., a genesis event is applied via `apply_operations` without event tracking), the error correctly prevents silent data loss.

### 9.4 Head Pruning Uses Meet, Not Event Parent

At entity.rs:323-326, the head is pruned by removing meet IDs. For the StrictAscends-to-DivergedSince transformation, meet = event's parent clock, which is correct (removing the tips that the event extends). For genuine DivergedSince, meet is the GCA which is typically not in the head, so the pruning is a no-op and the event is simply added as a new head tip. This is correct -- in genuine DivergedSince, the current head tips remain (they are still concurrent with the new event's branch).

### 9.5 is_descendant Performance in EventLayer::compare

The `is_descendant` function does a full BFS from `descendant` backward through the events map. In the worst case (comparing two events at the tips of a deep linear chain), this is O(n) where n is the chain length. The `compare` method calls `is_descendant` twice (once in each direction), so it is O(n) per call.

In `apply_layer`, `compare` is called once per property per event in the layer. For a layer with k events and p properties, the total cost is O(k * p * n). This is acceptable for typical workloads but could be expensive for large event maps with many properties.

---

## Confidence-Rated Findings

| # | Finding | Severity | Confidence | Details |
|---|---------|----------|------------|---------|
| 1 | **Stored value competes correctly after seeding fix** | Info (previously Critical) | High | The digest's CRITICAL bug (non-head last-write candidates ignored) is mitigated in the current code. `apply_layer` seeds the `winners` map from all stored values (lww.rs:180-189), and the BFS traversal accumulates all ancestor events including the stored value's event. `layer.compare()` can therefore compare stored values against layer events. The seeding fix from commit 4c41099 appears sufficient for the DAG configurations tested. |
| 2 | **InsufficientCausalInfo error when stored value's event is not in accumulated event map** | High | Medium | If the stored value's `event_id` points to an event NOT accumulated during the BFS traversal (e.g., due to partial storage, budget exhaustion on a prior comparison, or a bug in the navigator), `is_descendant` at layers.rs:221 will return `Err`, causing `apply_layer` to fail with `InsufficientCausalInfo`. The current code assumes complete event accumulation, which depends on: (a) the navigator having complete storage, and (b) the BFS not being budget-limited for DivergedSince results. If either assumption is violated, layer application fails entirely rather than degrading gracefully. Currently, BudgetExceeded returns before layers are computed, so (b) is safe. But (a) depends on the retriever implementation. |
| 3 | **Layer skipping when to_apply is empty loses already_applied context for LWW** | Medium | Medium | At layers.rs:130, layers with empty `to_apply` are skipped. This means the LWW backend never processes these already_applied events through `apply_layer`. Since the stored values are seeded from the current state (which already incorporates these events), this is functionally correct. However, it means the `winners` map may hold a stored value from a deep layer while competing against an event from layer 0, causing a cross-layer causal comparison. This works because all layers share the same events map, but it violates the spec's statement that "events in a layer are mutually concurrent." The stored value is NOT concurrent with layer 0 events -- it may be causally after them. The `layer.compare()` correctly returns `Descends`/`Ascends` for such cases, so the behavior is correct despite the semantic mismatch. |
| 4 | **EventLayer::compare returns Descends for equality (a == b)** | Low | High | At layers.rs:58-59, `compare(a, b)` where `a == b` returns `CausalRelation::Descends`. Semantically, an event does not "descend from" itself. A separate `Equal` variant would be cleaner. In practice, this only occurs when the stored value's event is also in the layer (e.g., already_applied), and the `Descends` result correctly means "no replacement needed." Functionally harmless but semantically imprecise. |
| 5 | **Head pruning in DivergedSince uses meet instead of event parent** | Medium | High | At entity.rs:323-324, the code removes `meet` IDs from the head. For the StrictAscends-to-DivergedSince transformation (comparison.rs:142), meet = event's parent clock, so this correctly removes the extended tip. For genuine DivergedSince, meet is the GCA (e.g., genesis), which is NOT in the head, so no pruning occurs and the event is just added. This means the head grows monotonically in genuine divergence scenarios. For example, if head is [D] and concurrent event C arrives with meet [A], head becomes [D, C] -- correct. But if a LATER event E arrives that extends C (parent = [C]), `compare_unstored_event` returns StrictAscends -> DivergedSince{meet=[C], other=[D]}. Head pruning removes C from [D, C] -> [D], then inserts E -> [D, E]. Correct. However, if the event's parent is a true multi-parent merge (parent = [C, D]), it would be StrictDescends (parent equals head), and the head update goes through the StrictDescends branch, not DivergedSince. So the pruning logic is consistent. |
| 6 | **All backends receive all layers regardless of relevance** | Low | High | At entity.rs:300-306, every backend receives every layer. Each backend internally filters for its operations. For 2 backends, the overhead is negligible. For future extensibility with many backends, this could be optimized by pre-filtering layers per backend. |
| 7 | **Lexicographic EventId tiebreak is deterministic but not intuitive** | Low | High | The EventId is a SHA-256 hash of (entity_id, operations, parent_clock). The lexicographic ordering of these hashes has no semantic meaning -- it does not correlate with wall-clock time, branch depth, or any user-visible ordering. This is correct for convergence (deterministic total order), but may surprise users who expect "last write" to mean "most recent in wall-clock time." This is a fundamental LWW design choice, not a bug. |
| 8 | **Cross-replica convergence holds for causally-delivered events** | Info | High | Through tracing of multiple scenarios (2-way, 3-way concurrency with different delivery orders), the system converges to the same final state on all replicas when events are delivered in causal order. The convergence relies on: (1) deterministic lexicographic tiebreak for concurrent events, (2) correct causal dominance detection via `is_descendant`, and (3) correct layer partitioning via `compute_layers`. |
| 9 | **to_operations() transition from Uncommitted to Pending is not reversible** | Low | Medium | If `to_operations()` is called but the transaction is then dropped (not committed), the values on the transaction fork remain `Pending`. Since the fork is discarded, this is not a problem. But if `to_operations()` were called twice on the same fork (e.g., partial commit retry), the second call would find no `Uncommitted` entries and return `None`, losing the changes. The current architecture prevents this because `to_operations()` is called exactly once during `generate_commit_event()`, and the fork is consumed. |
| 10 | **Yrs backend apply_layer ignores already_applied events, relying on CRDT idempotency** | Info | High | The Yrs backend (yrs.rs:186-209) only processes `to_apply` events, ignoring `already_applied`. This is correct because Yrs updates are idempotent -- applying an already-applied update is a no-op internally (Yrs deduplicates by state vector). If Yrs idempotency were ever broken (e.g., by a Yrs library bug), double-application would corrupt state. The current approach is the standard pattern for CRDT backends. |
| 11 | **compute_layers children_of is O(n) per parent, O(n^2 * p) total** | Low | High | `children_of` at layers.rs:159-166 scans all events for each parent lookup. For m events in the map and p parents per layer, this is O(m) per call. Total cost across all layers: O(m^2) in the worst case. For typical entity histories (tens to hundreds of events), this is acceptable. For large event maps (thousands), a parent-to-children index should be pre-built. This is a known performance concern documented in the digest. |
| 12 | **StrictAscends-to-DivergedSince transformation produces empty other_chain** | Medium | High | At comparison.rs:142-148, the transformation produces `other_chain: vec![]`. This means the other branch (from meet to head) has no event chain. The layer computation relies on the accumulated events (from BFS), not the chain itself, so layers are computed correctly. However, the empty `other_chain` is semantically misleading -- it suggests there are no events on the other branch, when in fact there are. The comment at lines 135-141 explains this, but it could confuse future maintainers. The actual events on the other branch ARE in the accumulated events map and are correctly classified as `already_applied` by `compute_ancestry`. |
| 13 | **Potential double-application when new backend is created during layer loop** | Medium | Medium | At entity.rs:308-317, when a new backend is created for an event's operations, it receives the CURRENT layer via `apply_layer`. But this layer may have already been processed by an existing backend of the same type that was created in a previous layer iteration. The check `!state.backends.contains_key(backend_name)` prevents this for existing backends. However, if TWO events in the SAME layer use a new backend, the first event triggers backend creation and layer application, and the second event's backend_name check sees the backend already exists (just created). So the new backend receives the layer once, which is correct. No double-application issue. |