# StateAndEvent Divergence Bug Investigation

## Summary

When a `StateAndEvent` update arrives and the incoming state diverges from the local state, the events are saved to storage but **NOT applied** to the entity. This is a critical gap in PR #201's concurrent update handling.

## Investigation Findings

### Two Separate Event Delivery Mechanisms

We discovered there are **two separate paths** for event delivery between nodes:

#### Path 1: CommitTransaction (WORKS)

```
Local commit → relay_to_required_peers() → CommitTransaction request
                                                    ↓
                              commit_remote_transaction() → apply_event()
```

- Used when: Direct transaction commits between nodes
- Works correctly: `apply_event` handles `DivergedSince` with layer-based resolution
- Code path: `node.rs:524` → `node.rs:542-594`

#### Path 2: StateAndEvent (BROKEN)

```
Subscription update → SubscriptionUpdate message → node_applier::apply_update()
                                                           ↓
                                           StateAndEvent → with_state() → apply_state()
                                                                              ↓
                                                           DivergedSince → Ok(false) ← EVENTS NOT APPLIED!
```

- Used when: Subscription updates, initial fetch, reactor changes
- BROKEN: Events are saved to storage but NOT applied to entity
- Code path: `node_applier.rs:96-125`

### Why Existing Tests Pass

The test `test_multi_head_single_tip_extension_cross_node` passes because:
1. It uses local transaction commits
2. These go through Path 1 (`CommitTransaction`)
3. Events are properly applied via `apply_event`

The `StateAndEvent` path is NOT tested for divergence scenarios.

### The Bug in Detail

**node_applier.rs:96-125:**
```rust
proto::UpdateContent::StateAndEvent(state_fragment, event_fragments) => {
    // Step 1: Events ARE saved to storage
    let events = Self::save_events(...).await?;

    // Step 2: Try to apply state only (NOT events)
    let (changed, entity) = node.entities.with_state(...).await?;

    // Step 3: If divergence, changed == Some(false), this block is SKIPPED
    if matches!(changed, Some(true) | None) {
        Self::save_state(...);
        changes.push(EntityChange::new(entity, events)?);
    }
    // BUG: Events are NOT applied via apply_event!
}
```

**entity.rs:388-401 (apply_state for DivergedSince):**
```rust
AbstractCausalRelation::DivergedSince { meet, .. } => {
    // Just returns Ok(false) with a warning!
    // Does NOT apply events from the forward chains
    warn!("... State not applied; events required for proper merge.");
    return Ok(false);
}
```

### When StateAndEvent is Used

1. **Initial subscription** - When a peer subscribes and doesn't have the entity yet
2. **Fetch requests** - When explicitly fetching entities
3. **Subscription updates via reactor** - When entities change and subscribers are notified

All subscription updates go through `StateAndEvent` (see `peer_subscription/server.rs:152`):
```rust
let content = proto::UpdateContent::StateAndEvent(attested_state.into(), attested_events.into_iter().map(|e| e.into()).collect());
```

### Impact

If a node receives a `StateAndEvent` where:
- The node already has the entity with head [B]
- The incoming state has head [C] (where B and C diverge from common ancestor A)

Then:
- Event C is saved to storage ✓
- Entity in-memory state is NOT updated ✗
- Entity state is NOT persisted ✗
- EntityChange is NOT emitted ✗
- Subscribers are NOT notified ✗

The entity becomes inconsistent between storage (has event C) and in-memory state (doesn't reflect C).

## The Fix

When `apply_state` returns `Ok(false)` for `StateAndEvent`, fall back to applying events via `apply_event`:

```rust
proto::UpdateContent::StateAndEvent(state_fragment, event_fragments) => {
    let events = Self::save_events(...).await?;
    let state = ...;

    let (changed, entity) = node.entities.with_state(...).await?;

    if matches!(changed, Some(true) | None) {
        // State applied successfully
        Self::save_state(node, &entity, &collection).await?;
        changes.push(EntityChange::new(entity, events)?);
    } else {
        // State not applied (divergence or older) - fall back to event-by-event application
        let mut applied_events = Vec::new();
        for event in events {
            if entity.apply_event(retriever, &event.payload).await? {
                applied_events.push(event);
            }
        }
        if !applied_events.is_empty() {
            Self::save_state(node, &entity, &collection).await?;
            changes.push(EntityChange::new(entity, applied_events)?);
        }
    }
}
```

## Test Plan

Create a test that specifically triggers `StateAndEvent` with divergence:
1. Use subscription/fetch path instead of direct commit
2. Ensure the receiving node already has a divergent state
3. Verify that events are applied and state converges correctly
