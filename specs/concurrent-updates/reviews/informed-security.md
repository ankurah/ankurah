# Informed Security Review

Date: 2026-02-09
Reviewer: Informed Security Agent (Round 2)
Branch: `concurrent-updates-event-dag`
Worktree: `/Users/daniel/ak/ankurah-201`

---

## Prior Finding Reconfirmation

### CRITICAL: EventBridge skips `validate_received_event` (tracked as #244)

**CONFIRMED** with specific code evidence.

In `core/src/node_applier.rs`, the `apply_delta_inner` method handles `DeltaContent::EventBridge` at lines 271-295. The events are converted to `Attested<Event>` (line 272-273), staged (line 276-278), and then applied directly via `entity.apply_event` and `commit_event` (lines 285-288). At no point is `node.policy_agent.validate_received_event(...)` called.

In contrast, `apply_update` (same file) validates every event fragment in both `UpdateContent::EventOnly` (line 81) and `UpdateContent::StateAndEvent` (line 109) paths via:
```rust
node.policy_agent.validate_received_event(node, from_peer_id, &attested_event)?;
```

This bypass allows a malicious durable peer to inject events through the EventBridge delta path (used for Fetch and QuerySubscribed responses) without any attestation validation. The attack surface is the `apply_deltas` call chain triggered from `context.rs:385` (`fetch_from_peer`) and any subscription establishment flow.

**Severity**: CRITICAL. This is a pre-existing issue (not introduced by this branch). Tracked as #244.

---

### H3: Unbounded `collect_event_bridge` (tracked as #247)

**CONFIRMED** with specific code evidence.

In `core/src/node.rs:654-712`, the `collect_event_bridge` method performs:
1. A `compare()` call with budget `100000` (line 671) -- already 100x the normal `DEFAULT_BUDGET` of 1000.
2. If the result is `StrictDescends`, a backward walk from `current_head` to `known_head` with NO traversal limit (lines 686-702). The `while !frontier.is_empty()` loop has no counter, no budget, and no maximum event count.

A malicious peer could send a `known_head` pointing to genesis (or even a fabricated ID). If the entity has N events in its history, the durable node would walk backward through all N events and collect them into memory. For a very active entity (e.g., 1M events), this could exhaust memory.

Additionally, since `known_head` comes from the requesting peer (via `known_matches` in `Fetch` or `SubscribeQuery`), the attacker controls the input. Even a `known_head` value of an empty clock would cause the compare to return a non-`StrictDescends` result (triggering a fallback to StateSnapshot, which is safe), but a known_head pointing to a very old but valid event would trigger the full backward walk.

**Severity**: HIGH. Tracked as #247.

---

### H4: No size/count limits on peer event messages (tracked as #246)

**CONFIRMED** with specific code evidence.

The `proto::SubscriptionUpdateItem` (in `proto/src/update.rs`) contains:
- `UpdateContent::EventOnly(Vec<EventFragment>)` -- unbounded Vec
- `UpdateContent::StateAndEvent(StateFragment, Vec<EventFragment>)` -- unbounded Vec + unbounded state

Similarly, `proto::DeltaContent::EventBridge { events: Vec<EventFragment> }` (checked via grep) carries an unbounded events Vec.

The `apply_updates` method in `node_applier.rs:21-53` iterates all items with no cardinality check. The `apply_deltas` method (line 171-215) likewise processes all deltas without a count limit, though it uses `ReadyChunks` for batching -- this is a throughput optimization, not a security limit.

The `NodeRequest::CommitTransaction` in `node.rs:418-427` accepts `events: Vec<Attested<proto::Event>>` with no cardinality validation.

On the deserialization side, `bincode::deserialize::<proto::Message>(&data)` at `connectors/websocket-client/src/client.rs:352` and `connectors/websocket-server/src/server.rs:14` deserialize with no size limit. bincode's default `Options` have no maximum size for sequences. A crafted message with a Vec length prefix of `u64::MAX` would cause bincode to attempt allocation of that many elements.

**Severity**: HIGH. Tracked as #246.

---

### H5: Unchecked bincode deser sizes

**CONFIRMED** with specific code evidence.

All `bincode::deserialize` call sites use the default configuration without `with_limit()`. Key entry points:

1. **Websocket message deserialization** (`connectors/websocket-client/src/client.rs:352`, `connectors/websocket-server/src/server.rs:14`, `connectors/websocket-client-wasm/src/connection.rs:129`): Raw binary from the wire is deserialized into `proto::Message` with no size cap. A crafted binary payload with large length prefixes for nested Vecs could trigger massive allocation.

2. **LWW operation deserialization** (`core/src/property/backend/lww.rs:206,209,295,298`): Operation diffs are deserialized without size limits. A malicious event with a crafted `operation.diff` field could contain a huge `BTreeMap<PropertyName, Option<Value>>`.

3. **Storage deserialization** (`storage/postgres/src/lib.rs:500+`, `storage/sqlite/src/engine.rs:405+`, `storage/sled/src/collection.rs:318+`): These are from local storage so the trust boundary is weaker, but a corrupted database could still trigger OOM.

The specific attack: A malicious peer sends a valid `proto::Message` envelope where inner fields contain enormous length-prefixed bincode data. bincode's default `deserialize` will allocate before validating content.

**Severity**: HIGH. Not yet tracked in a GitHub issue.

---

### M1: Entity RwLock `.unwrap()` -- 4+ sites in entity.rs

**CONFIRMED** with specific code evidence.

Sites in `core/src/entity.rs` that use bare `.unwrap()` on RwLock operations:

| Line | Code | Risk |
|------|------|------|
| 118 | `self.state.read().unwrap()` | `head()` -- called frequently |
| 197 | `self.state.write().unwrap()` | `commit_head()` |
| 210 | `self.state.write().unwrap()` | `try_mutate()` |
| 264 | `self.state.write().unwrap()` | `apply_event()` creation path |
| 341 | `self.state.write().unwrap()` | `apply_event()` DivergedSince path |
| 505, 509 | `.downcast::<P>().unwrap()` | `get_backend()` -- different issue (downcast) |
| 609 | `self.0.state.read().unwrap()` | `Display` impl |
| 619, 669, 684, 703, 712 | `.0.write().unwrap()` / `.0.read().unwrap()` | `WeakEntitySet` operations |

In contrast, `retrieval.rs` already uses `.unwrap_or_else(|e| e.into_inner())` for poison recovery (lines 105, 127, 133, 177, 230, 236). The entity.rs sites were NOT updated to use this pattern.

If any thread panics while holding an entity's state write lock (e.g., due to a deserialization error in `apply_operations_from_event`), the lock becomes poisoned. Subsequent calls to `head()` (line 118) or `try_mutate()` (line 210) would propagate the panic to all threads accessing that entity, causing cascading failures.

The `LWWBackend` also uses bare `.unwrap()` on its `values` RwLock at lines 71, 76, 87, 99, 106, 114, 138, 182, 247, 287, 300.

**Severity**: MEDIUM. A single panic poisons the lock and all subsequent operations on that entity panic.

---

### M3: Creation events bypass fork-based validation (tracked as #243)

**CONFIRMED** with specific code evidence.

In `core/src/node.rs:566-577` (`commit_remote_transaction`):

```rust
let (entity_before, entity_after, already_applied) = if event.payload.is_entity_create() && entity.head().is_empty() {
    // Create: apply to entity directly, use as both before/after
    entity.apply_event(&event_getter, &event.payload).await?;
    (entity.clone(), entity.clone(), true)
} else {
    // Update: snapshot, apply to fork for validation
    let trx_alive = Arc::new(AtomicBool::new(true));
    let forked = entity.snapshot(trx_alive);
    forked.apply_event(&event_getter, &event.payload).await?;
    (entity.clone(), forked, false)
};
```

For creation events on an entity with empty head:
1. The event is applied directly to the real entity (line 568), **mutating it before policy validation**.
2. The `check_event` call on line 580 receives `entity_before == entity_after` (both are the same entity after mutation).
3. If the policy check fails (or returns an error), the entity is already mutated with no rollback path.

For non-creation events, a fork (snapshot) is used for validation (line 574-575), and the real entity is only mutated after policy passes (line 589).

**Severity**: MEDIUM. Pre-existing. Tracked as #243.

Also note: In `context.rs:112-125` (`commit_local_trx`), the equivalent code stages the event and applies to a fork (`forked.apply_event`), which is correct. But then on line 125, `forked.apply_event` is called after staging but the `entity_before` is the upstream entity, which is also a correct pattern. The vulnerability is specific to `commit_remote_transaction`.

---

### M4: TOCTOU retry no backoff

**CONFIRMED** with specific code evidence.

In `core/src/entity.rs:289-398`, the retry loop:
```rust
const MAX_RETRIES: usize = 5;
for attempt in 0..MAX_RETRIES {
    // ... expensive async BFS comparison ...
    // ... TOCTOU check ...
}
Err(MutationError::TOCTOUAttemptsExhausted)
```

No `tokio::time::sleep()`, no exponential backoff, no jitter between retries. Each retry immediately re-runs the full BFS comparison (which may traverse up to 4000 events with budget escalation).

The BFS comparison (line 294) does `compare(getter, &subject_clock, &head, DEFAULT_BUDGET)` where `DEFAULT_BUDGET = 1000`. With 4x escalation, each retry could traverse up to 4000 events. Five retries = up to 20,000 event fetches with no delay between them.

An attacker with 6+ concurrent writers to the same entity could reliably exhaust all retries by timing cheap head-advancing events during the victim's BFS phase. The BFS is async (may yield), providing a natural window for the attacker.

The `apply_state` method (line 414-474) has the same pattern.

**Severity**: MEDIUM.

---

### L2: `saturating_sub` masks cycle double-decrement

**CONFIRMED** as defense-in-depth, correct.

In `core/src/event_dag/comparison.rs:352,361`:
```rust
self.unseen_comparison_heads = self.unseen_comparison_heads.saturating_sub(1);
self.unseen_subject_heads = self.unseen_subject_heads.saturating_sub(1);
```

And line 273:
```rust
self.remaining_budget = self.remaining_budget.saturating_sub(1);
```

The `saturating_sub` prevents underflow (which would be a debug panic or release wrap-around). For cycles in the event graph, a node could be processed from the same frontier multiple times, but the frontier uses a `BTreeSet` (deduplication) and `frontier.remove(&id)` returns false if already removed. So in practice, double-processing is unlikely for non-cyclic data. The `saturating_sub` is a belt-and-suspenders defense.

However, the prior A-track review (Finding #5) correctly identified that with malicious cyclic data, the BFS's `frontier.extend(parents.iter().cloned())` call can RE-INSERT a parent ID that was previously removed and processed, because `BTreeSet::extend` will insert it again after `remove` took it out. This would cause the node to be re-processed, double-decrementing the counter. The `saturating_sub` correctly prevents underflow in this case.

**Severity**: LOW. Defense-in-depth is correct.

---

### L3: Staging shared across entities in batch

**CONFIRMED** as by-design with minor info-leak risk.

In `core/src/node_applier.rs:44-48`, a single `CachedEventGetter` (or `LocalEventGetter` in `commit_remote_transaction`) is created per batch of updates. Its staging map is an `Arc<RwLock<HashMap<EventId, Event>>>` shared across all events in the batch.

For `apply_updates` (line 46), the `event_getter` is shared across all items. Events staged for entity A are visible to BFS traversal for entity B. This is by design -- it enables cross-entity causal discovery when events reference each other. The "info leak" risk is minimal since events are semi-public within a subscription context anyway.

For `commit_remote_transaction` (node.rs:551-599), each event gets its own `LocalEventGetter` (line 555), so staging is NOT shared across entities in this path.

**Severity**: LOW. By design.

---

## New Findings

### N1: `commit_local_trx` applies event to forked entity but then applies AGAIN to upstream -- double application

**File**: `core/src/context.rs:120-155`

In `commit_local_trx`:
1. Line 124: `event_getter.stage_event(event.clone());`
2. Line 125: `forked.apply_event(&event_getter, &event).await?;` -- applies to the fork for validation
3. Line 131: `event_getter.commit_event(&attested).await?;` -- commits to storage
4. Line 138-139: `entity.commit_head(Clock::new([attested_event.payload.id()]));` -- updates head directly, skipping comparison
5. Line 154-155: `upstream.apply_event(&event_getter, &attested_event.payload).await?;` -- applies AGAIN to the upstream/canonical entity

The `commit_head` on line 139 sets the head on the **forked** entity (since `entity` iterates over `trx.entities` which are all forked). Then on line 155, `upstream.apply_event(...)` is called on the canonical entity. Since the event is now in storage, BFS will find it and the comparison should return `StrictDescends` (event extends the upstream's current head). This would apply the operations again to the upstream entity.

However, there is a subtle issue: `commit_head` is called for ALL transaction entities BEFORE any upstream `apply_event` calls (line 138-139 is in a separate loop). This means the forked entity's head is updated but the upstream entity's head is not yet updated. If two concurrent transactions both call `commit_local_trx` for the same entity, the second transaction's `upstream.apply_event` could see a stale head, requiring TOCTOU retry.

This is not a double-application bug per se (forked and upstream are separate entities), but the `commit_head` call (line 139) on the forked entity is redundant since the forked entity is about to be dropped (transaction completed). The real update happens via `upstream.apply_event`.

**Severity**: LOW. No actual data corruption, but the redundant `commit_head` and the timing gap between head updates could cause confusion in concurrent scenarios.

---

### N2: `is_descendant_dag` treats missing DAG entries as dead ends -- incorrect `Concurrent` for sub-meet events

**File**: `core/src/event_dag/accumulator.rs:286-308`

The `is_descendant_dag` function treats missing entries as dead ends (`continue` at line 299):
```rust
let Some(parents) = dag.get(&id) else {
    continue;
};
```

When the accumulated DAG is incomplete (which it always is -- it only contains events from the BFS traversal), ancestry lookups for events below the meet point will hit missing entries and return false (not a descendant).

Combined with the `EventLayer::compare` method (line 250-261), this means:
- If event A's ancestry goes through an event not in the DAG, `is_descendant_dag(A, B)` returns false even if A truly descends from B.
- Both `is_descendant_dag(A, B)` and `is_descendant_dag(B, A)` return false -> `Concurrent` result.

The `older_than_meet` rule in `lww.rs:193-197` mitigates this for stored values whose event_id is NOT in the DAG at all (`dag_contains` returns false). But for events that ARE in the DAG but whose full ancestry is not, the comparison silently returns `Concurrent` instead of the true causal relation. This leads to lexicographic EventId tiebreaking where causal ordering should be used.

**Example**: DAG accumulated during BFS: `{A, B, C, D, E}` where `A <- B <- C <- D <- E`. If event F (in the DAG) has parent G (NOT in the DAG), and we compare F against E, `is_descendant_dag(F, E)` walks F -> G -> missing -> false. `is_descendant_dag(E, F)` walks E -> D -> C -> B -> A -> no parents -> false. Result: Concurrent. But F may actually descend from E through G -> ... -> E in the real DAG.

This is the CORRECT behavior for the `older_than_meet` design: if the event is in the DAG but its full ancestry isn't, the comparison is "best effort" -- it may undercount causal relationships and fall through to deterministic tiebreaking. This is documented as intentional.

However, the prior A-track review (Finding #1 and Section 7) identified a more severe version where the OLD `is_descendant` (in the prior `layers.rs`) would return a HARD ERROR for missing events. The new `is_descendant_dag` (in `accumulator.rs`) returns false instead, which is a significant improvement. The old hard-error `InsufficientCausalInfo` bug has been FIXED by this refactor.

**Severity**: LOW. The "Concurrent" fallback is deterministic (lexicographic tiebreak) and converges across nodes. Not a correctness issue for the LWW semantics since all nodes use the same DAG and same tiebreak.

---

### N3: `collect_event_bridge` uses budget 100,000 for compare but no limit on the backward walk

**File**: `core/src/node.rs:671`

```rust
let comparison_result = compare(&event_getter, current_head, known_head, 100000).await?;
```

The `compare` function is called with a budget of 100,000 -- 100x the normal `DEFAULT_BUDGET` of 1000. With 4x internal escalation, this becomes up to 400,000 event traversals. This alone could be a CPU exhaustion vector: a malicious peer requesting a bridge with a fabricated `known_head` could force the durable node to traverse up to 400,000 events.

Even when compare returns `StrictDescends` quickly, the subsequent backward walk (lines 686-702) has NO limit at all. A stale `known_head` pointing to genesis would cause all N events to be collected into memory.

This is worse than H3 suggests: the budget for the compare itself is also excessive.

**Severity**: HIGH (already tracked as part of #247, but the compare budget of 100,000 is an additional concern).

---

### N4: `Clock::from(Vec<EventId>)` does not sort -- `contains()` uses binary search

**File**: `proto/src/clock.rs:63-65`

```rust
impl From<Vec<EventId>> for Clock {
    fn from(ids: Vec<EventId>) -> Self { Self(ids.into_iter().collect()) }
}
```

`Vec::into_iter().collect()` just creates a Vec from the iterator -- no sorting. But `Clock::contains` (line 26) uses `self.0.binary_search(id)` which requires sorted data.

Usage sites for `Clock::from(Vec<EventId>)`:
- `core/src/event_dag/tests.rs` uses this pattern extensively for test clocks
- `proto/src/clock.rs:76`: `TryInto<Clock> for Vec<Vec<u8>>` also does NOT sort (line 76: `Clock(ids.into_iter().collect())`)

The `From<EventId>` impl (line 83-85) creates a single-element Vec (always sorted). The `from_strings` impl (line 20-23) DOES sort. `insert` (line 28-33) maintains sorted order.

If deserialization produces a `Clock` with unsorted members (e.g., from an older serialization format or a malicious peer), `contains()` would return incorrect results. This could cause the BFS comparison to produce wrong results (e.g., not detecting that a head member is in the subject).

Looking at the deserialization path: `Clock` derives `Deserialize`. bincode deserialization would reconstruct the inner `Vec<EventId>` exactly as serialized. If the serialized data was from a properly maintained Clock, it would be sorted. But a malicious peer could send an unsorted Clock.

**Severity**: MEDIUM. A malicious peer could craft an unsorted Clock in an event's parent field or in a state's head field, causing `contains()` to return false negatives. This could corrupt the BFS comparison results. The fix is trivial: sort in `From<Vec<EventId>>` and in the `Deserialize` impl (or add a post-deserialization validation step).

---

### N5: Policy validation applies event to head before approval in `commit_remote_transaction`

**File**: `core/src/node.rs:566-569`

Already covered under M3 reconfirmation. Adding additional detail:

The creation path applies the event directly to the entity BEFORE `check_event` (line 580). If `check_event` returns an error, the entity is left with the applied creation event and its head is set to the event's ID. There is no rollback.

Worse, the `check_event` call receives `entity_before == entity_after` (line 569: both are `entity.clone()`), so the policy agent cannot actually detect what changed -- it sees identical before/after states.

**Severity**: MEDIUM (tracked as #243 and #242 combined).

---

### N6: Partial layer application on error leaves entity in inconsistent state

**File**: `core/src/entity.rs:349-369`

```rust
for layer in all_layers {
    // Create backends for new operations...
    // Apply to all backends
    for (_backend_name, backend) in state.backends.iter() {
        backend.apply_layer(&layer)?;
    }
    applied_layers.push(layer);
}
```

If `backend.apply_layer` fails on layer N (e.g., unknown LWW operation version, deserialization error), layers 0..N-1 have already been applied to the backends, but the head is NOT updated (the head update happens at lines 375-378, after the loop). The `?` operator propagates the error, and the write lock is dropped without updating the head.

This leaves the entity in an inconsistent state: backends contain data from layers 0..N-1 but the head still points to the old clock. If `apply_event` is retried (or another event arrives), the comparison will re-do the same BFS (since the head hasn't changed), re-compute the same layers, and try to re-apply them. The `already_applied` partitioning is based on `current_head_ancestry`, which hasn't changed, so the same events would be in `to_apply` again.

For LWW backends, re-applying the same operations would overwrite with the same values (idempotent for the applied portion). For Yrs backends, `apply_layer` only processes `to_apply` events, and Yrs handles duplicate operations via its internal state vector. So re-application is likely safe but not guaranteed.

**Severity**: MEDIUM. A malicious event with a deliberately corrupted operation in a later layer could cause partial application that leaves the entity in an intermediate state.

---

### N7: Remote event fetching via `CachedEventGetter` stores peer events without validation

**File**: `core/src/retrieval.rs:194-208`

```rust
proto::NodeResponseBody::GetEvents(peer_events) => {
    // Store locally for future access
    for event in peer_events.iter() {
        self.collection.add_event(event).await?;
    }
    peer_events.into_iter().next().map(|e| e.payload).ok_or_else(...)
}
```

When the `CachedEventGetter` (used by ephemeral nodes) fetches events from a durable peer, the peer's response events are stored directly to local storage via `collection.add_event(event)` WITHOUT calling `validate_received_event`. The events are then used by BFS comparison.

This is the event-fetching variant of the #244 bypass. While #244 covers the EventBridge delta path, this covers individual event fetches during BFS traversal on ephemeral nodes. A malicious durable peer could respond to `GetEvents` requests with fabricated events that are stored and trusted.

**Severity**: HIGH. Related to #244 but distinct path. The `GetEvents` response is not validated.

---

### N8: `WeakEntitySet` RwLock uses bare `.unwrap()` -- potential cascading panic

**File**: `core/src/entity.rs:619,669,684,703,712`

The `WeakEntitySet` wraps `Arc<std::sync::RwLock<BTreeMap<EntityId, WeakEntity>>>`. All access uses `.unwrap()`:

```rust
let entities = self.0.read().unwrap();       // line 619
let mut entities = self.0.write().unwrap();  // line 669, 684, 703, 712
```

If any thread panics while holding this lock (which would be unusual but possible if e.g., `Entity::from_state` at line 719 triggers a panic during state reconstruction), the `WeakEntitySet` becomes permanently poisoned. All subsequent entity lookups, retrievals, and creations would panic, effectively killing the node.

**Severity**: MEDIUM. Unlike per-entity locks (where poisoning affects one entity), this is a node-wide shared lock.

---

## Summary Table: All Findings

### Prior Finding Reconfirmation

| # | Finding | Status | Severity | GH Issue |
|---|---------|--------|----------|----------|
| CRITICAL | EventBridge skips `validate_received_event` | CONFIRMED | CRITICAL | #244 |
| H3 | Unbounded `collect_event_bridge` | CONFIRMED | HIGH | #247 |
| H4 | No size/count limits on peer messages | CONFIRMED | HIGH | #246 |
| H5 | Unchecked bincode deser sizes | CONFIRMED | HIGH | No |
| M1 | Entity RwLock `.unwrap()` | CONFIRMED | MEDIUM | No |
| M3 | Creation events bypass fork-based validation | CONFIRMED | MEDIUM | #243 |
| M4 | TOCTOU retry no backoff | CONFIRMED | MEDIUM | No |
| L2 | `saturating_sub` masks cycle double-decrement | CONFIRMED (correct) | LOW | No |
| L3 | Staging shared across entities in batch | CONFIRMED (by design) | LOW | No |

### New Findings

| # | Finding | Severity | Confidence | File:Line | GH Issue? | Details |
|---|---------|----------|------------|-----------|-----------|---------|
| N1 | `commit_local_trx` redundant `commit_head` on fork + timing gap | LOW | Medium | `core/src/context.rs:138-139` | No | Forked entity head updated before upstream apply; redundant but harmless. |
| N2 | `is_descendant_dag` treats missing entries as dead ends -- Concurrent fallback | LOW | High | `core/src/event_dag/accumulator.rs:286-308` | No | By design; significant improvement over prior hard-error behavior. Falls through to deterministic tiebreak. |
| N3 | `collect_event_bridge` uses 100,000 budget + unbounded backward walk | HIGH | High | `core/src/node.rs:671,686-702` | Partially #247 | Compare budget 100x normal + no limit on event collection. Server-side DoS. |
| N4 | `Clock::from(Vec<EventId>)` does not sort; `contains()` uses binary search | MEDIUM | High | `proto/src/clock.rs:63-65,26` | No | Malicious peer can craft unsorted Clock causing false negatives in BFS. |
| N5 | Creation event applied before policy check (same as M3 with added detail) | MEDIUM | High | `core/src/node.rs:566-569` | #243 | Entity mutated before check_event; no rollback on failure. |
| N6 | Partial layer application on error leaves inconsistent backend state | MEDIUM | High | `core/src/entity.rs:349-369` | No | If apply_layer fails on layer N, layers 0..N-1 are applied but head is not updated. |
| N7 | Remote event fetching stores peer events without validation | HIGH | High | `core/src/retrieval.rs:194-208` | Related to #244 | CachedEventGetter stores GetEvents responses to local storage without validate_received_event. Distinct from EventBridge path. |
| N8 | `WeakEntitySet` RwLock uses bare `.unwrap()` -- node-wide poison risk | MEDIUM | High | `core/src/entity.rs:619,669,684,703,712` | No | Node-wide shared lock; poison cascades to all entity operations. |

### Previously Reported Findings Now Challenged or Refined

| Prior Finding | Status | Notes |
|---------------|--------|-------|
| A4 Finding #1: LWW `layer.compare()` fails with InsufficientCausalInfo | **RESOLVED by this branch** | The new `is_descendant_dag` (accumulator.rs:288) returns false for missing entries instead of erroring. The `older_than_meet` rule (lww.rs:193-197) handles sub-meet stored values. The hard-error scenario identified in the A4 review no longer exists in the current code. |
| A4 Finding #4: Events with multi-parent clocks spanning meet boundary orphaned | **MITIGATED** | The new `EventLayers::new` (accumulator.rs:151-155) checks `!accumulator.dag.contains_key(p)` for parents -- treating out-of-DAG parents as satisfied. This prevents the orphaning. |
| B4 Finding #5: StrictAscends transformation depth-precedence violation | **MITIGATED** | No longer applicable. The current code uses `compare(getter, &subject_clock, &head, ...)` where `subject_clock` is the event's own ID (not its parent). The BFS discovers the event via staging and correctly determines the relationship. The old `compare_unstored_event` transformation is gone. |
| B4 Finding #3/A4 Finding #1: `is_descendant` hard error | **RESOLVED** | Replaced by `is_descendant_dag` which treats missing entries as dead ends (returns false). No more hard errors. |

## Summary

**Finding counts by severity:**
- CRITICAL: 1 (EventBridge validation bypass -- confirmed pre-existing, tracked)
- HIGH: 4 (unbounded collect_event_bridge, no message size limits, unchecked bincode deser, remote event fetch bypass)
- MEDIUM: 6 (entity RwLock unwrap, creation bypass, TOCTOU no backoff, Clock sort, partial layer application, WeakEntitySet poison)
- LOW: 4 (staging shared, saturating_sub correct, commit_head redundant, is_descendant_dag dead-end)

**Key positive finding**: Several of the most severe findings from the A4 and B4 adversarial reviews (InsufficientCausalInfo hard error, orphaned multi-parent events, StrictAscends depth-precedence violation) have been RESOLVED or MITIGATED by the refactored `EventAccumulator` and `is_descendant_dag` implementations. The `older_than_meet` rule is the key design decision that makes the LWW resolution robust against incomplete DAG context.

**Most actionable new finding**: N7 (remote event fetch stores without validation) is a distinct validation bypass path related to but not covered by #244. It should be tracked as a separate sub-issue or added to #244's scope.
