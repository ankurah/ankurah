# Informed Correctness Review

Date: 2026-02-09
Reviewer: Informed Correctness Agent (Round 2)

## Prior Finding Reconfirmation

### H1: LWW stale event_id across layers

**CONFIRMED** with high confidence.

`apply_layer` in `lww.rs:169-265` only writes back winners where `from_to_apply == true` (line 249). When a winner from an earlier layer came from an `already_applied` event, the stored `event_id` in `self.values` is NOT updated. The next layer's seeding (line 182-200) re-reads `self.values` and gets the stale `event_id`.

**Concrete trace:**
- Entity stores property `title` with `event_id=X` (old, pre-meet).
- Layer 1: `already_applied=[A (writes title, id=A)]`, `to_apply=[]`.
- Layer 1 processing: seed reads X from `self.values`. A competes against X. If A descends from X (or X is `older_than_meet`), A wins. But `from_to_apply=false`, so nothing is written to `self.values`.
- Layer 2: `to_apply=[B (writes title, id=B)]`.
- Layer 2 processing: seed reads X (stale!) from `self.values`. B competes against X instead of A. If B < A by lexicographic order but B > X, B incorrectly wins against A's write. The correct comparison should be B vs A.

This is a real multi-layer convergence bug. It requires: (1) at least 2 layers, (2) the winning event for a property in layer N being from `already_applied`, and (3) a different event in layer N+1 writing the same property.

**Not tracked in any existing GitHub issue.**

### H2: `Clock::from(Vec<EventId>)` doesn't sort

**CONFIRMED** but severity downgraded from Medium to Low.

Code evidence: `proto/src/clock.rs:63-65`:
```rust
impl From<Vec<EventId>> for Clock {
    fn from(ids: Vec<EventId>) -> Self { Self(ids.into_iter().collect()) }
}
```

`contains()` at line 26 uses `binary_search`, which requires sorted input. The `From<Vec<EventId>>` impl does NOT sort. Similarly, the `TryInto<Clock>` from `Vec<Vec<u8>>` at line 66-78 does not sort.

However, I verified that the postgres (line 129-141) and wasm (line 44-53) deserialization paths DO use binary-search-based sorted insertion. The main risk vectors are:
1. Serde `Deserialize` on `Clock` (it derives `Deserialize`) will deserialize directly into the inner `Vec` without sorting. A malicious peer could craft a bincode payload with unsorted clock entries.
2. Test code using `Clock::from(vec![...])` with multiple IDs.

In practice, the comparison algorithm primarily uses `as_slice()` (not `contains()`), and entity heads are built via `insert()` (which maintains sort order). The `contains()` method is used at `changes.rs:30` for head validation, but heads are always constructed in sorted order through normal code paths.

The bug exists but is unlikely to be triggered in production unless a malicious peer sends a crafted unsorted clock. **Reclassified: Low severity, Medium confidence for practical impact.**

**Not tracked in any existing GitHub issue.**

### H6: `apply_event` caller contract fragility

**CONFIRMED** with nuance.

The `apply_event` method in `entity.rs:229` takes `getter: &E` where `E: GetEvents`. The staging protocol requires callers to stage events (via `SuspenseEvents::stage_event`) before calling `apply_event`, so that BFS can discover the event. However, `apply_event`'s signature only requires `GetEvents`, not `SuspenseEvents`.

Code evidence from `entity.rs:292-294`:
```rust
let subject_clock: Clock = event.id().into();
let comparison_result = crate::event_dag::compare(getter, &subject_clock, &head, DEFAULT_BUDGET).await?;
```

The comparison will look up the event itself via `accumulator.get_event(id)`. If the event wasn't staged, this lookup will fail with `EventNotFound`, which is returned as an error. So the contract violation manifests as an error, not silent corruption.

However, looking at `node_applier.rs:271-288` (EventBridge path), events are staged at line 276-278 before `apply_event` is called at line 286. The `apply_update` paths at lines 77-97 and 104-145 also stage before applying. The `commit_local_trx` path in `context.rs:124` also stages before applying.

**All call sites in production code correctly stage events before calling `apply_event`.** The fragility is real (no compile-time enforcement that staging happened), but there is no current violation. The trait split (`GetEvents` vs `SuspenseEvents`) prevents `apply_event` from staging itself, which is by design.

**Not tracked in any existing GitHub issue.** Severity: Medium (design fragility, not current bug).

### M2: Partial layer application on deser error

**CONFIRMED** with specific code evidence.

In `entity.rs:350-369`:
```rust
for layer in all_layers {
    // ... backend creation ...
    for (_backend_name, backend) in state.backends.iter() {
        backend.apply_layer(&layer)?;
    }
    applied_layers.push(layer);
}
```

If `apply_layer` fails on layer N (e.g., due to `bincode::deserialize` failure in lww.rs:206), layers 1 through N-1 have already been applied to the backends. The `?` operator returns the error, which causes the `state` write guard to be dropped without updating the head. This leaves backends mutated but the head unchanged.

On the next attempt (retry loop or re-delivery), BFS comparison re-runs against the old head, producing the same `DivergedSince` result. Layers 1 through N-1 are re-applied to the already-mutated backends. For LWW, re-application with the same events should produce the same winners (idempotent within a layer). For Yrs CRDT, re-application of already-applied updates should also be safe (CRDTs handle duplicates).

**Practical impact: Low.** The partial application is not rolled back, but re-application on retry should converge to the same state. The real concern is if the deserialization error is persistent (malformed event) -- then the entity is stuck with partially-applied state forever. **This is a correctness risk in the persistent error case.**

**Not tracked in any existing GitHub issue.**

### M9: Self-referential parent causes budget exhaustion

**CONFIRMED** with caveat.

The BFS at `comparison.rs:281-283` processes events and extends frontiers with parent IDs:
```rust
let from_subject = self.subject_frontier.remove(&id);
// ...
if from_subject {
    self.subject_frontier.extend(parents.iter().cloned());
}
```

If an event E has `parent = [E]` (self-referential), then:
1. E is removed from frontier.
2. E's parent (E itself) is added back to frontier.
3. Next step: E is fetched again, removed, re-added, etc.

The `accumulator.accumulate()` is called at line 272 before `process_event`, and `get_event` returns the cached event. So each iteration burns one budget unit (line 273: `self.remaining_budget = self.remaining_budget.saturating_sub(1)`).

With default budget 1000 and 4x escalation to 4000, this would waste 4000 iterations before returning `BudgetExceeded`. This is a denial-of-service vector if a malicious peer can inject a self-referential event.

**However**, self-referential events are impossible in practice because `EventId` is a content hash of `(entity_id, operations, parent_clock)`. An event with `parent=[E]` where `E` is the event's own hash would require a hash collision. SHA-256 makes this computationally infeasible.

**Severity: Low (theoretical only).** The only practical concern is corrupted storage, not malicious construction.

**Not tracked in any existing GitHub issue.**

### L1: `build_forward_chain` multi-meet bug

**CONFIRMED** -- dead data, correctly deferred.

`comparison.rs:368-381`:
```rust
fn build_forward_chain(&self, visited: &[EventId], meet: &BTreeSet<EventId>) -> Vec<EventId> {
    let mut chain: Vec<_> = visited.iter().rev().cloned().collect();
    if !meet.is_empty() {
        if let Some(pos) = chain.iter().position(|id| meet.contains(id)) {
            chain = chain.into_iter().skip(pos + 1).collect();
        }
    }
    chain
}
```

With multi-node meet `{M1, M2}`, `position()` finds only the first meet node. Other meet nodes leak into the chain. The chains (`subject_chain`, `other_chain`) are carried in the `DivergedSince` variant but are NOT consumed by any production code. Layer computation uses `EventLayers` which performs its own correct topological traversal from the meet point.

**Tracked as #245.** Status: Open.

### L8: Quick-check doesn't decrement budget

**CONFIRMED** as benign.

`comparison.rs:82-105`: The quick-check fetches each subject event via `accumulator.get_event(id)` and checks if comparison heads are a subset of the union of parents. This loop runs at most `subject.len()` iterations (typically 1-2 for normal entity heads) and does NOT decrement the remaining budget.

This is correct because: (1) the quick-check is bounded by the subject clock size, which is small, (2) if the quick-check succeeds, the algorithm returns immediately without entering BFS, (3) if it fails, the full BFS begins with the original budget intact.

**Severity: None (benign by design).**

**Not tracked in any existing GitHub issue.**

---

## New Findings

| # | Finding | Severity | Confidence | File:Line | Details |
|---|---------|----------|------------|-----------|---------|
| N1 | **EventBridge path skips `validate_received_event`** | HIGH | HIGH | `node_applier.rs:271-288` | The `EventBridge` delta path stages and applies events without calling `validate_received_event()`. Compare to `EventOnly` (line 81) and `StateAndEvent` (line 109) which both validate. A peer can bypass attestation checks by sending events via the Fetch/QuerySubscribed delta path instead of the subscription path. **Already tracked as #244 (pre-existing, not introduced by this branch).** |
| N2 | **EventBridge commits events unconditionally after apply** | MEDIUM | HIGH | `node_applier.rs:285-288` | In the EventBridge path, `commit_event` is called for every event regardless of whether `apply_event` returned `true` or `false`. Compare to the EventOnly path (lines 91-96) where `commit_event` is called only when `apply_event` returns `true`. This means rejected or no-op events get persisted to permanent storage anyway. While not immediately harmful (idempotent delivery is safe), it wastes storage and commits events that may have been rejected by the entity's DAG comparison. |
| N3 | **Budget escalation does NOT warm-start -- Comparison state is recreated** | LOW | HIGH | `comparison.rs:111-129` | On BudgetExceeded, the retry loop creates a brand new `Comparison` struct (`Comparison::new()`). The `EventAccumulator` retains its DAG and LRU cache (warm start for event fetching), but all BFS state (frontiers, visited lists, meet candidates, unseen counters) is recreated from the original clocks. Events already in the accumulator's DAG will be found immediately in cache (no storage round-trips), but they must be re-processed through `process_event`. This means budget escalation from 1000 to 4000 effectively wastes the first ~1000 events' worth of budget on re-traversal (they are fetched from cache at budget cost 1 per step). The effective additional budget is approximately 3000, not 4000. This is documented as a known behavior ("only LRU cache survives") but the doc at `event-dag.md:121-123` says "reusing the accumulated DAG structure" which is misleading -- the DAG structure is reused for cache hits, not for BFS state. |
| N4 | **`DivergedSince` head update uses `meet` instead of event parents** | LOW | HIGH | `entity.rs:374-378` | The head update removes `meet` members from head and inserts the new event. The `meet` is the GCA of `event.parent` and entity head. For the common `StrictAscends`-transformed case, `meet` = event's parent members, which correctly supersedes the extended tips. For deep divergence, `meet` may be an ancient ancestor NOT in the head, so `head.remove(meet_id)` is a no-op. This is CORRECT behavior (verified by trace), but the comment at line 372-374 says "remove superseded tips" which is misleading. In the deep-divergence case, no tips are removed -- the new event is simply added, growing the head. This is correct because the new event IS concurrent with existing tips. |
| N5 | **`compute_ancestry_from_dag` stops at DAG boundary -- may undercount for incomplete DAGs** | LOW | MEDIUM | `accumulator.rs:268-284` | The `compute_ancestry_from_dag` function walks backward through the accumulated DAG. If the DAG is incomplete (BFS terminated before reaching genesis), the ancestry set will be incomplete. Events beyond the DAG boundary are treated as having no parents. This affects the `current_head_ancestry` computation at line 139, which determines the `already_applied` vs `to_apply` partition. If an event that IS in the current head's ancestry is NOT in the accumulated DAG (because it was beyond the meet), it would be misclassified as `to_apply`. However, such events should also not be in the layer frontier (they are below the meet), so in practice this is not triggered for correctly-computed meet points. |
| N6 | **Layer frontier initialization counts events outside the DAG as having all parents processed** | LOW | MEDIUM | `accumulator.rs:150-159` | The initial frontier computation at lines 147-159 includes events whose parents are either processed (in the meet set) or not in the DAG at all (`!accumulator.dag.contains_key(p)`). The second condition treats events whose parents are outside the accumulated DAG as "ready" for processing. This is correct for the common case where parents outside the DAG are below the meet, but could produce incorrect layer ordering if the DAG is incomplete for some other reason. This is guarded by the invariant "never compute layers from an incomplete traversal" (BudgetExceeded check). |
| N7 | **`apply_event` DivergedSince path does NOT use `try_mutate`** | LOW | HIGH | `entity.rs:340-347` | The DivergedSince path manually acquires the write lock and checks the head, rather than using the `try_mutate` helper that the `StrictDescends` path uses (line 304). Both implementations are functionally equivalent and correct. However, any future changes to `try_mutate` (logging, metrics, deadlock detection) would not be reflected in the DivergedSince path. This was noted in the holistic review. |
| N8 | **Empty-clock comparison returns DivergedSince instead of Equal** | LOW | HIGH | `comparison.rs:56-68` | When both clocks are empty, the early exit returns `DivergedSince` with all-empty fields. Two empty clocks represent the same lattice point (bottom element) and should semantically be `Equal`. However, the callers handle this: `apply_event` guards against empty-head entities (line 283), so this path is not reachable in production for the entity use case. It could confuse future API consumers. |
| N9 | **Redundant delivery of historical non-head events** | MEDIUM | HIGH | `entity.rs:291-394` (interaction with comparison.rs) | If event B (already applied, superseded by C; head=[C]) is re-delivered, the comparison `compare([B_id], [C])` will find B via BFS. Since B is an ancestor of C, the comparison returns `StrictAscends` (line 316-319), which is a no-op. **This is now CORRECTLY handled.** The prior B1 review identified this as a critical bug with the old `compare_unstored_event` approach that compared `event.parent` vs head. The new approach compares `event.id` vs head via staging. Since B is staged and then compared by its own ID against head C, BFS discovers B is an ancestor of C and returns `StrictAscends`. **The B1 review finding (Critical: "Redundant delivery of historical events creates invalid multi-heads") is RESOLVED by the current implementation.** The key change is at entity.rs:293 where `subject_clock = event.id().into()` (not `event.parent`) is used as the subject. |

---

## Summary

### Prior Findings

| Finding | Status | Issue |
|---------|--------|-------|
| H1: LWW stale event_id across layers | **CONFIRMED** -- real multi-layer convergence bug | Not tracked |
| H2: `Clock::from(Vec)` doesn't sort | **CONFIRMED** but severity downgraded to LOW | Not tracked |
| H6: `apply_event` caller contract fragility | **CONFIRMED** -- design fragility, no current violation | Not tracked |
| M2: Partial layer application on deser error | **CONFIRMED** -- benign on retry, problematic for persistent errors | Not tracked |
| M9: Self-referential parent exhaustion | **CONFIRMED** -- theoretical only (SHA-256 prevents) | Not tracked |
| L1: `build_forward_chain` multi-meet bug | **CONFIRMED** -- dead data | Tracked as #245 |
| L8: Quick-check doesn't decrement budget | **CONFIRMED** -- benign by design | Not tracked |

### New Findings

| # | Severity | Summary |
|---|----------|---------|
| N1 | HIGH | EventBridge skips validate_received_event (tracked as #244) |
| N2 | MEDIUM | EventBridge commits events unconditionally |
| N3 | LOW | Budget escalation re-traverses (not a warm start of BFS state) |
| N4 | LOW | DivergedSince head update comment misleading (code correct) |
| N5 | LOW | compute_ancestry_from_dag undercount on incomplete DAGs |
| N6 | LOW | Layer frontier treats DAG-external parents as processed |
| N7 | LOW | DivergedSince path doesn't use try_mutate |
| N8 | LOW | Empty-clock comparison returns DivergedSince |
| N9 | MEDIUM (RESOLVED) | Historical re-delivery was critical in B1 review but is now fixed by staging pattern |

### Key Correctness Assessment

**The BFS comparison algorithm is sound.** Termination is correct for all traced topologies. The dual-frontier approach correctly identifies all six relationship types. Meet computation via `common_child_count == 0` correctly produces the GCA. The `StrictAscends`/`StrictDescends` early termination via unseen counters is correct and cannot underflow in practice.

**The staging pattern resolves the historical re-delivery bug.** The B1 review identified a critical issue where re-delivery of historical non-head events created invalid multi-heads. This is RESOLVED by the current implementation which stages events and compares by `event.id()` rather than `event.parent`.

**H1 (LWW stale event_id across layers) is the most significant untracked correctness issue.** It can cause incorrect LWW resolution across multiple layers when a winning write comes from an `already_applied` event. The fix would be to update `self.values` for all winners (not just `from_to_apply`) or to maintain the winners map across layers.

**Cross-replica convergence is upheld** for the single-layer case (lexicographic tiebreak is commutative and associative). For the multi-layer case, convergence depends on H1 being fixed -- the stale seed can cause different replicas to pick different winners depending on their event application order.

### Severity Tally

- **HIGH**: 1 confirmed (H1: LWW stale event_id), 1 pre-existing (N1/EventBridge #244)
- **MEDIUM**: 2 confirmed (M2: partial layer, H6: caller fragility), 1 new (N2: unconditional commit)
- **LOW**: 7 (H2, M9, L1, L8, N3-N8)
- **RESOLVED**: 1 (N9: historical re-delivery, fixed by staging pattern)
