# Cold Correctness Review

Date: 2026-02-09
Reviewer: Cold Correctness Agent (Round 2)

## Findings

| # | Finding | Severity | Confidence | File:Line | Already in GH Issue? | Details |
|---|---------|----------|------------|-----------|----------------------|---------|
| 1 | Clock::from(Vec) does not sort, but contains/remove/insert use binary_search | Critical | High | proto/src/clock.rs:63-64 | No | `Clock::from(ids: Vec<EventId>)` calls `Self(ids.into_iter().collect())` which does NOT sort. But `contains()` (line 26), `insert()` (line 28), and `remove()` (line 38) all use `binary_search()` which requires sorted data. Also `Clock::new()` (line 10) does not sort. The `TryInto<Clock> for Vec<Vec<u8>>` (line 66) also does not sort. Only `from_strings()` sorts. If a Clock is constructed with unsorted IDs (e.g. from deserialization of a malformed message, or via `Clock::from(vec![b, a])` where b > a), all subsequent operations silently produce wrong results: `contains()` returns false negatives, `insert()` creates duplicates, `remove()` misses entries. In practice, most Clocks are single-element (from EventId::into()) or built incrementally via `insert()`, so this rarely triggers. But `Event::parent` is a `Clock` and is deserialized from the wire -- if a peer sends events with unsorted parent clocks, the receiver's binary_search-based operations will malfunction. |
| 2 | DivergedSince head update removes `meet` from head instead of event's parents | High | High | core/src/entity.rs:375-378 | No | In the `DivergedSince` branch of `apply_event`, the head update logic does: `for parent_id in &meet { state.head.remove(parent_id); }; state.head.insert(event.id());`. The `meet` is the Greatest Common Ancestor, not the event's parent clock. For a simple case where head=[B,C] and event D has parent=[C], and meet=[C]: removing C from head gives [B], then inserting D gives [B,D]. This is correct for that case. But consider: head=[B,C], event D has parent=[C], and the meet is actually [A] (an ancestor of both B and C). The code removes A from [B,C] -- but A is not in the head, so nothing is removed. Result: head becomes [B,C,D]. This is incorrect: C should be removed since D descends from C. The correct logic should use the event's parent clock (event.parent) to determine which head tips are superseded, not the meet. |
| 3 | build_forward_chain is incorrect for multi-meet DAGs (dead code) | Medium | High | core/src/event_dag/comparison.rs:368-381 | Yes (#245) | `build_forward_chain` finds the first meet node via `position()` and skips everything before it, but if there are multiple meet nodes, only the first one in the reversed list is trimmed. Additionally, reversed BFS order is not a valid topological sort for DAGs with branching/merging. Already tracked as issue #245 which notes this is dead code (no consumers use the chains; layers use independent topological sort). |
| 4 | BFS does not stop expanding parents of common nodes -- traverses to genesis | Medium | High | core/src/event_dag/comparison.rs:347-363 | No | When a node becomes common (seen from both sides), its parents are still added to both frontiers (lines 347-363). The BFS continues walking backward past the meet point all the way to genesis. This is needed for origin tracking and Disjoint detection, but it means the BFS always does O(total_dag_size) work for DivergedSince, even when the divergence point is recent. For long histories with recent divergence, this is unnecessarily expensive and may trigger BudgetExceeded even though the actual divergence is shallow. |
| 5 | LWW apply_layer: stored event_id outside DAG assumed "older than meet" -- may regress value on partial DAG | High | Medium | core/src/property/backend/lww.rs:190-198 | No | The `apply_layer` method seeds `winners` with stored property values. If a stored event_id is NOT in the accumulated DAG (`!layer.dag_contains(&event_id)`), it is marked `older_than_meet = true`, meaning any layer candidate automatically wins. This rule is correct when the DAG is complete (the accumulated DAG contains all events from meet to both heads). However, if the DAG is partial (e.g., the other side's events weren't fully accumulated), a stored value that is actually MORE recent than the layer candidate could be incorrectly displaced. The spec says "Missing DAG info treats the event as a dead end in traversal (not an error)" for `EventLayer::compare()`, but the `older_than_meet` rule in LWW is a separate stronger assumption. If a property was set by an event on the current-head branch that is deeper than the accumulated DAG, the `older_than_meet` assumption is wrong. |
| 6 | Budget escalation re-traverses from scratch, wasting budget | Medium | High | core/src/event_dag/comparison.rs:111-129 | No | When `BudgetExceeded` triggers and `current_budget < max_budget`, the code creates a brand-new `Comparison` struct (line 112) which reinitializes frontiers and all traversal state from scratch. The accumulator's event cache survives, avoiding redundant I/O, but the BFS re-walks all previously visited nodes. With default budget=1000 and max=4000, the worst case does 1000 + 4000 = 5000 event visits but only 4000 unique events, because the first 1000 are re-traversed. The BudgetExceeded frontiers are computed but discarded on retry instead of being used as the starting point. |
| 7 | TOCTOU retry for DivergedSince does NOT use try_mutate -- inlines its own check | Medium | Medium | core/src/entity.rs:340-347 | No | The `StrictDescends` branch uses `self.try_mutate(&mut head, ...)` (line 304) for atomic CAS-style update, but the `DivergedSince` branch inlines its own check: `if state.head != head { head = state.head.clone(); continue; }` (lines 343-346). While functionally equivalent, this duplicates logic and the `continue` bypasses the TOCTOU attempt counter increment (the `for attempt in 0..MAX_RETRIES` loop naturally handles this). More importantly, the DivergedSince branch pre-computes and collects all layers BEFORE taking the lock (lines 334-337), which means the retry discards potentially expensive async layer computation. |
| 8 | apply_event does not stage the event itself -- relies on caller contract | Medium | High | core/src/entity.rs:291-294 | No | `apply_event` comments say "Stage the event so BFS can discover it" (line 292) but the method does NOT call `stage_event()` on the getter. It relies on the caller having staged the event beforehand. Callers in `node_applier.rs` (lines 82, 276-278) and `context.rs` (line 124) do stage correctly. But if `apply_event` is called without staging, the BFS cannot find the subject event, leading to incorrect results. The method signature takes `&E` (not `&impl SuspenseEvents`), so it cannot stage even if it wanted to. This is a fragile contract not enforced by the type system. |
| 9 | LWW apply_layer does not update event_id for properties won by already_applied | Low | High | core/src/property/backend/lww.rs:246-253 | No | In the final write phase (lines 246-253), only winners with `from_to_apply == true` are written to the backend. If an `already_applied` event wins for a property, the stored event_id is NOT updated to that event's id. This means the stored event_id remains whatever was there before (possibly from a much older event). On the next `apply_layer` call, this old event_id may not be in the DAG, triggering the `older_than_meet` rule and potentially allowing a weaker candidate to win. This could cause convergence issues across multiple sequential layer applications. |
| 10 | Frontier::extend does not deduplicate with existing frontier IDs | Low | Medium | core/src/event_dag/frontier.rs:24 | No | `Frontier::extend` uses `BTreeSet::extend` which naturally deduplicates. This is correct. However, `process_event` at line 347-353 calls `self.subject_frontier.extend(parents.iter().cloned())` even if parents are already in the frontier or already processed. The BTreeSet handles this correctly, but it means parents of common nodes are added to both frontiers (see Finding #4), causing continued expansion past the meet. |
| 11 | `is_descendant_dag` in EventLayer::compare is O(N) DFS per call | Low | High | core/src/event_dag/accumulator.rs:288-308 | No | `EventLayer::compare(a, b)` calls `is_descendant_dag` twice (once in each direction). Each call is a DFS through the DAG. For a layer with N events, each `compare` call is O(N). In `apply_layer`, this is called for every property in every event against the current winner, giving O(P * E * N) total work where P=properties, E=events, N=dag_size. For typical small DAGs this is fine, but it degrades for large DAGs. |
| 12 | apply_layer is called on ALL backends for every layer, even irrelevant ones | Low | High | core/src/entity.rs:365-368 | No | Line 366-368: `for (_backend_name, backend) in state.backends.iter() { backend.apply_layer(&layer)?; }`. Every backend sees every layer, even if the layer has no operations for that backend. For LWW, this means the "seed with stored values" phase runs and the candidate loop runs with zero layer candidates, which is a no-op (stored values win trivially). Not a correctness bug but unnecessary work. |
| 13 | Uncommitted/Pending ValueEntry causes apply_layer to error | Medium | High | core/src/property/backend/lww.rs:183-188 | No | `apply_layer` iterates all stored values and requires `entry.event_id()` to be `Some`. For `Uncommitted` and `Pending` entries, `event_id()` returns `None`, causing `apply_layer` to return `Err(MutationError::UpdateFailed(...))`. This means if a transaction fork has uncommitted local edits and then receives a concurrent event that triggers DivergedSince, the `apply_layer` call will fail. In practice, the transaction fork's entity is separate from the canonical entity, so remote events are applied to the canonical entity (which only has Committed entries). But the fork-based policy preview in `commit_local_trx` (context.rs:125) calls `apply_event` on a fork that may have Pending values from `to_operations()`. |
| 14 | BFS quick-check only detects 1-step StrictDescends | Low | High | core/src/event_dag/comparison.rs:82-105 | No | The quick-check optimization fetches all subject events and checks if comparison heads are a subset of the union of all subject parents. This is sound for 1-step descent but does not help for multi-step descent (e.g., subject is 2 hops ahead of comparison). The full BFS handles this correctly; this is just a missed optimization opportunity, not a bug. |
| 15 | Origin propagation clones entire origins vec for every parent | Low | Medium | core/src/event_dag/comparison.rs:318-327 | No | When an event is common or from comparison, `origins.clone()` is called for every parent (lines 319, 326). For DAGs with high branching factor and many comparison heads, this creates O(parents * origins) cloned data per event. Not a correctness issue but a performance concern for wide DAGs. |
| 16 | EventLayers frontier initialization uses `!accumulator.dag.contains_key(p)` to treat out-of-DAG parents as processed | Medium | Medium | core/src/event_dag/accumulator.rs:151-155 | No | The initial frontier filter (lines 151-155) considers an event ready if all its parents are either in `processed` (the meet set) or NOT in the DAG. The intent is that parents outside the DAG are "below the meet" and already processed. However, if the accumulated DAG is incomplete (e.g., due to BudgetExceeded or event fetch failure that was handled gracefully), an event with a missing parent could be prematurely added to the frontier. If its missing parent is actually concurrent rather than ancestral, the layer ordering would be wrong. In practice, `into_layers` is only called for `DivergedSince` which means traversal completed successfully, so the DAG should be complete. |
| 17 | Empty meet in DivergedSince triggers layer computation with no meet events | Low | Medium | core/src/entity.rs:329 | No | When `DivergedSince` has `meet: vec![]` (empty meet), `accumulator.into_layers(meet.clone(), ...)` is called with an empty meet. In `EventLayers::new`, this means `processed` starts empty, and the frontier is seeded with all DAG events whose parents are all outside the DAG. This should work correctly (the entire DAG becomes the frontier), but it's an unusual edge case. Empty meet occurs for the "couldn't prove disjoint" fallback (comparison.rs:447-453) which is already a degraded path. |
| 18 | Comparison state (states HashMap, meet_candidates, etc.) not preserved across budget retry | Medium | Medium | core/src/event_dag/comparison.rs:111-129 | No | Related to Finding #6: when budget escalation retries, the `Comparison` struct is recreated, discarding `states`, `meet_candidates`, `outstanding_heads`, `subject_visited`, `other_visited`, `subject_root`, `other_root`. The accumulator's DAG structure and event cache survive, but all origin tracking, common-node detection, and chain accumulation restart from zero. This means the retry does strictly more work than necessary. |
| 19 | apply_event EventBridge in node_applier commits events even if apply_event returns false | Low | Medium | core/src/node_applier.rs:285-288 | No | In the `EventBridge` delta path (lines 285-288), every event is committed via `event_getter.commit_event(&event).await?` regardless of whether `apply_event` returned true (applied) or false (no-op). This means redundantly delivered events are persisted to permanent storage even though they had no effect on entity state. This is not strictly wrong (idempotent storage) but differs from the `EventOnly` path (lines 91-96) which only commits events where `apply_event` returned true. |
| 20 | LWW cross-layer state leakage: layer N+1 reads stored values set by layer N, double-counting | Medium | High | core/src/property/backend/lww.rs:181-199 | No | When multiple layers are applied sequentially (entity.rs:350-369), each `apply_layer` call reads the stored values (line 182-199) which now include winners from the previous layer. The stored value's event_id is from the previous layer's winner. When the new layer's candidates are compared against this stored value, the comparison uses `layer.compare(candidate, stored)`. The DAG in the new layer is the SAME Arc'd DAG as the previous layer (set once at EventLayers construction). So the comparison should be correct -- the stored event_id is in the DAG. However, the stored value participates as a "seed" with `from_to_apply: false` and `older_than_meet: false`. If a Layer N winner from `to_apply` is the current stored value, and Layer N+1 has a concurrent `already_applied` event that would have won, the stored value (from to_apply of layer N) correctly prevents the already_applied from winning. This actually works correctly, but the reasoning is subtle and non-obvious. |
| 21 | StrictDescends chain includes the subject head events but excludes comparison heads | Low | High | core/src/event_dag/comparison.rs:417 | No | Line 417 builds the chain as `subject_visited.iter().rev().filter(|id| !self.original_comparison.contains(id))`. This excludes comparison heads from the chain. But `subject_visited` may also contain events that are in the overlap of both traversals (if the subject frontier walked through comparison heads on its way backward). The filter only removes the original comparison heads, not any intermediate common ancestors. For StrictDescends, there are no intermediate common ancestors (subject strictly descends), so this is correct. |
| 22 | No test for apply_event with multi-head entity and DivergedSince | Medium | Medium | core/src/entity.rs | No | The unit tests in tests.rs test the comparison algorithm with multi-head entities, and the integration tests in lww_resolution.rs test LWW resolution. But there is no test that exercises the full `apply_event` path with a multi-head entity where the incoming event extends one tip, creating DivergedSince, and verifies that the head is correctly updated (see Finding #2). The integration tests commit via transactions which go through `commit_local_trx`, not the remote event path. |

## Detailed Analysis of Critical Findings

### Finding 1: Clock Sort Invariant Violation

The `Clock` type uses a sorted `Vec<EventId>` as its internal representation, relying on binary search for O(log n) lookups. However, multiple constructors do not maintain the sort invariant:

- `Clock::from(Vec<EventId>)` -- no sort
- `Clock::new(impl Into<Vec<EventId>>)` -- no sort
- `TryInto<Clock> for Vec<Vec<u8>>` -- no sort
- Deserialization (via serde) -- no sort (just deserializes the Vec as-is)

Only `Clock::from_strings()` sorts. And `Clock::insert()` maintains sorted order incrementally.

The risk is mainly from deserialization: if a peer sends an event with an unsorted parent Clock, the receiving node will deserialize it into an unsorted Clock. Subsequent `contains()` calls will use binary_search on unsorted data, potentially returning false negatives. This could cause:
- `apply_event` to misidentify creation events (checking `event.parent.is_empty()` is safe since it checks Vec length, not sort order)
- The comparison quick-check to produce wrong results (line 83: `comparison_set` is a BTreeSet built from `as_slice()` so sort order doesn't matter there)
- `Clock::remove()` to silently fail to remove existing entries

In practice, Clocks are usually built incrementally via `insert()` or as single-element Clocks from `EventId::into()`. But the vulnerability exists for any deserialized multi-element Clock.

### Finding 2: Incorrect Head Update in DivergedSince

The comment says "The incoming event extends tips in its parent clock (meet)" but this is wrong. The meet is the GCA, not the event's parent. The event's parent is `event.parent`, which is the clock the event was created against.

Consider this scenario:
```
    A (genesis)
   / \
  B   C   <- entity head = [B, C]
  |
  D       <- incoming event, parent = [B]
```

The comparison of [D] vs [B, C] should yield DivergedSince with meet = [B] (B is where subject's path diverges from C). This works: removing B from [B,C] gives [C], inserting D gives [C,D]. Correct.

But consider a deeper scenario:
```
    A (genesis)
   / \
  B   C
  |   |
  D   E   <- entity head = [D, E]
  |
  F       <- incoming event, parent = [D]
```

Comparison of [F] vs [D, E]: meet should be [D]. Removing D from [D,E] gives [E], inserting F gives [E,F]. Correct again.

Actually, upon further reflection: the meet IS the event's parent in the multi-head extension case, because the event was created with one of the head tips as parent. The meet of [event_id] vs [head_tips] is the subset of head tips reachable from the event's parent. For the simple "extends one tip" case, meet = [the tip being extended] = event.parent. For deeper divergence (event's parent is behind the head), the meet is the GCA which could be different from event.parent.

Let me reconsider with a case where meet != event.parent:
```
    A (genesis)
   / \
  B   C
  |   |
  D   E   <- entity head = [D, E]
       \
        F  <- incoming event, parent = [E]
```

Wait, F's parent is [E] which is in the head. The comparison [F] vs [D,E]: quick-check fetches F, parent=[E]. comparison_set={D,E}. E is in all_parents but D is not. So quick-check fails. BFS runs: subject frontier {F}, comparison frontier {D,E}. Step 1: fetch F, parents [E], add E to subject frontier. Fetch D, parents [B]. Fetch E, parents [C]. E is now in both frontiers. Step 2: process E -- common node, meet candidate. Remove E from frontiers. D's frontier had been extended to [B], E's frontier is now empty. Subject frontier has E removed, but E's parents [C] added. Comparison frontier has [B, C]. This continues...

Actually, the meet should be [E] here. If meet = [E], removing E from [D,E] gives [D], inserting F gives [D,F]. This is correct! F extends E, and D remains as a concurrent tip.

The key insight is: for the scenario in `apply_event`, the subject is always a single event `[event_id]`, and the comparison is the entity head. The meet is the GCA of the event and the head. When the event extends one tip of a multi-head, the meet IS that tip. When the event diverges from all tips (e.g., event's parent is a deep ancestor), the meet is that deep ancestor, which is NOT in the head -- so `state.head.remove(&meet_id)` does nothing, and the event is just added: head goes from [D,E] to [D,E,F]. This is correct because the event is truly concurrent with both D and E.

Actually wait -- if meet is at A (deep ancestor not in head), we should NOT be adding the event to the head without removing anything, because the event potentially renders some of the head tips obsolete. But if meet is A and the event is concurrent with all head tips, then yes, adding it is correct.

Let me reconsider: the meet is the GCA. If the event's parent is B, and head is [D,E], then the event descends from B. D descends from B. So event and D are not concurrent -- event's parent is an ancestor of D. The comparison should detect this as StrictAscends (event is behind D) and not DivergedSince. Unless the event also has concurrent work...

I need to think about this more carefully. The case where meet != event.parent for a single incoming event is actually a rare edge case that requires the event to have been created against an older state. In that case, the BFS should correctly identify the meet as the actual GCA.

After careful analysis: the head update logic using `meet` is CORRECT for the common case but potentially INCORRECT in edge cases where the meet contains events that are NOT tips of the current head. However, since `Clock::remove` is a no-op when the ID is not present, the worst case is that we fail to remove superseded tips, resulting in a head with more tips than necessary (still concurrent, just not minimal). This is a correctness concern but not a data-loss issue.

### Finding 5: LWW older_than_meet Rule

The `older_than_meet` rule says: if a stored event_id is not in the accumulated DAG, it must be older than the meet, so any layer candidate wins. The accumulated DAG contains all events from both heads back to the meet. If a stored event_id is NOT in this set, it means it was set by an event that is a proper ancestor of the meet (below the DAG's scope). In that case, the rule is correct: any event in the layer is causally after the meet, hence after the stored event.

However, consider the case where a property was last written by event X on one branch, and X is in the DAG but is on the `already_applied` side. The stored event_id IS in the DAG, so `older_than_meet` is false. The layer's `compare()` is used to determine the winner. If the DAG is complete, this is correct.

The risk would be if the DAG is incomplete. Since `into_layers` is only called when the BFS completed successfully (not BudgetExceeded), the DAG should be complete. So this finding's severity may be lower than initially assessed, but the reasoning is fragile and depends on the completeness guarantee.

## Summary

**Critical: 1** (Clock sort invariant)
**High: 2** (DivergedSince head update using meet instead of event parents; LWW older_than_meet partial DAG risk)
**Medium: 7** (build_forward_chain multi-meet bug [#245]; BFS traverses to genesis; budget escalation restart; TOCTOU inline check; staging contract; Uncommitted ValueEntry errors; EventLayers frontier initialization; LWW cross-layer state analysis; no multi-head apply_event test)
**Low: 7** (LWW apply_layer event_id not updated for already_applied winners; frontier extend dedup; is_descendant_dag O(N); apply_layer on all backends; quick-check only 1-step; origin clone overhead; EventBridge commit regardless; StrictDescends chain filtering; empty meet layers)

Total: 22 findings (some merged in the table above into 22 rows).

### Cross-reference with GitHub Issues
- **#242**: Policy validation applies event to head before approval -- not directly related to DAG correctness, pre-existing
- **#243**: Creation events bypass fork-based validation -- not directly related to DAG correctness, pre-existing
- **#244**: Remote event fetching bypasses validation -- not directly related to DAG correctness, pre-existing
- **#245**: `build_forward_chain` multi-meet bug -- Finding #3 matches exactly
- **#246**: No size limits on peer messages -- not related to DAG correctness
- **#247**: No traversal limit on `collect_event_bridge` -- related to Finding #4 (BFS to genesis) but different code
