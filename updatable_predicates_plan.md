# Updatable Predicates Implementation Plan

## Overview

Implement the ability to update predicates on LiveQuery instances to support virtual scrolling and other dynamic filtering use cases. The predicate update will propagate to both local reactor and remote durable peers while maintaining the same predicate_id.

## Progress Summary

- ✅ Protocol updates completed (version fields added)
- ✅ Struct updates completed (LiveQuery, ReactorUpdate, Status enum)
- ✅ Method stubs added with detailed TODOs
- ✅ Identified unified approach for predicate management
- ✅ Designed ResultSetTrx for atomic updates

## Key Design Decisions (REVISED)

### 1. Unified Predicate Model - No Special "Initial" Case

**Key Insight**: All predicate changes are transitions from an old state (possibly empty) to a new state.

- Version 0 is just a transition from "no predicate" to "predicate"
- Updates are transitions from "old predicate" to "new predicate"
- The logic is identical - compute diff and apply changes

**Current Problem**: Two-phase activation creates confusion

- `add_predicate` creates inactive predicate (initialized=false)
- Predicate is ignored by `notify_change` until `initialize()` is called
- This intermediate state serves no purpose

**Solution**: Single `set_predicate` method that:

- Takes predicate, collection_id, included_entities, and version
- Makes predicate immediately active (no intermediate state)
- Handles both initial and update cases uniformly

### 2. Version Number Handling

- Separate u32 field in the request (not part of PredicateId)
- Process whichever version arrives first regardless of gaps
- Discard any non-monotonic versions (older than current)

### 3. Node Type Behavior

The key distinction is the presence or absence of `client_relay` (subscription_relay):

- **Ephemeral nodes** have `subscription_relay: Some(SubscriptionRelay)`
- **Durable nodes** have `subscription_relay: None`

#### Durable Nodes (no client_relay)

- Update predicate immediately (no remote dependencies)
- Simply change the predicate on the reactor
- Existing entities get re-filtered against new predicate
- Send Add/Remove updates for changes

#### Ephemeral Nodes (with client_relay)

- **LiveQuery** holds new predicate in suspense (not the reactor or relay)
- Reactor only tracks current predicate (not pending)
- LiveQuery manages the suspension/coordination via its Inner struct
- Update local reactor only after remote confirmation

### 4. Update Flow for Ephemeral Nodes

```
1. LiveQuery.update_predicate(new_predicate) called
   - Set is_initialized = false
   - Store new predicate as pending in LiveQuery's Inner struct with version number

2. Send SubscribePredicate(predicate_id, new_predicate, version) to remote
   - Via node.subscribe_remote_predicate (gets new rx channel)
   - Relay tracks Status::Updating(version)

3. Remote server processes update
   - Computes diff between old and new predicate matches
   - Sends only new entities (MembershipChange::Initial) that match new but not old
   - Does NOT send removals (local reactor handles those)
   - Sends SubscriptionUpdate with initialized_predicate=(predicate_id, version)

4. Node receives update, applies to reactor, filters by version for LiveQuery
   - Update always applied to local reactor (keeps it in sync)
   - Version filtering happens in apply_updates() for pending_predicate_sub
   - Only resolves pending_predicate_sub if version matches
   - Old versions are silently dropped for LiveQuery but still applied to reactor

5. LiveQuery processes the update (if version matched)
   - Receives entities via rx channel
   - Updates local predicate to pending_predicate
   - Sets is_initialized = true

6. Reactor.set_predicate() processes update
   - Updates the predicate in PredicateState
   - Uses ResultSetBatch for atomic changes:
     - Adds NEW entities (not already in resultset)
     - Removes entities that no longer match new predicate
   - Sends single ReactorUpdate with initialized_predicate=(id, version)

7. Set is_initialized = true
```

### 5. Reactor Changes (REVISED)

#### Single Method: `reactor.set_predicate()` (replaces both `add_predicate` and `initialize`)

- Takes: subscription_id, predicate_id, collection_id, predicate, included_entities, version
- If predicate exists: compute diff from old state
- If new predicate: old state is empty (version 0)
- Uses `ResultSetBatch` for atomic changes:
  ```rust
  let mut batch = resultset.batch();
  // As we iterate through entities, apply changes one by one
  for entity in entities_from_storage {
      if !old_resultset.contains(entity.id) {
          batch.add(entity); // NEW match
      }
  }
  for old_id in old_resultset.keys() {
      if !new_predicate.matches(old_id) {
          batch.remove(old_id); // No longer matches
      }
  }
  // Automatic notification on drop if changed
  ```
- Issues single ReactorUpdate with initialized_predicate=(predicate_id, version)

#### ResultSetBatch - Atomic Updates

- Batch operation object (NOT a transaction - no rollback)
- Holds mutex guard for direct state mutation
- Single broadcast notification on Drop (if changed)
- Prevents multiple notifications during batch
- Ensures observers see atomic state transition

### 6. Protocol Changes

#### SubscribePredicate Request

```rust
NodeRequestBody::SubscribePredicate {
    predicate_id: PredicateId,
    collection: CollectionId,
    predicate: ast::Predicate,
    version: u32,  // NEW FIELD - 0 for initial subscription
}
```

#### SubscriptionUpdate Response

```rust
NodeUpdateBody::SubscriptionUpdate {
    items: Vec<SubscriptionUpdateItem>,
    initialized_predicate: Option<(PredicateId, u32)>,  // CHANGED - now includes version
}
```

Note: `initialized_predicate` now contains both PredicateId AND version number as a tuple. This allows the Node to properly filter updates and only send the correctly-versioned ones to the appropriate rx channel.

### 7. LiveQuery API

#### New Methods

```rust
impl LiveQuery {
    /// Update the predicate for this query
    pub fn update_predicate(&self, new_predicate: ankql::ast::Predicate) -> Result<(), Error> {
        // Synchronous - initiates update but doesn't wait
    }

    /// Update predicate and wait for initialization
    pub async fn update_predicate_wait(&self, new_predicate: ankql::ast::Predicate) -> Result<(), Error> {
        self.update_predicate(new_predicate)?;
        self.wait_initialized().await;
        Ok(())
    }
}
```

### 8. Error Handling

- If predicate update fails on remote, leave in failed state (same as initialization errors)
- Track error in LiveQuery's error field
- is_initialized remains false if update fails

### 9. Subscription Relay Changes (SIMPLIFIED)

- Status enum now includes version in all states:
  - `Requested(peer_id, version)`
  - `Established(peer_id, version)`
- No separate `Updating` state needed - updates are just new requests
- Track version numbers for each predicate
- Handle version ordering/dropping of old versions

## Implementation Steps

1. **Protocol Updates**

   - Add version field to SubscribePredicate request
   - Add version field to SubscriptionUpdate response
   - Update proto serialization/deserialization

2. **Reactor Changes**

   - Implement `reactor.reinitialize()` method
   - Modify PredicateState to track current version
   - Update watcher management for predicate changes

3. **LiveQuery Updates**

   - Add `update_predicate()` method
   - Add version tracking to Inner struct
   - **LiveQuery's Inner struct holds the pending predicate and version**
   - Manage pending predicate during updates (suspension state)
   - Handle rx channel for update responses
   - Coordinate with Node to get correctly-versioned updates

4. **Server-side Changes**

   - Modify `SubscriptionHandler::add_predicate()` to handle versions
   - Compute diff for predicate updates vs full initialization
   - Send only additions (new entities) for updates

5. **Client Relay Updates**

   - Add version tracking to SubscriptionRelay
   - Handle `Status::Updating(version)` state
   - Manage version ordering

6. **Node Integration**

   - Update `node.subscribe_remote_predicate()` to handle versions
   - **Node is responsible for filtering SubscriptionUpdate messages by version**
   - Only send updates to rx channel if version matches the pending version
   - Drop/ignore updates with non-matching or old versions
   - Coordinate between LiveQuery and reactor updates

7. **Testing**
   - Test version ordering/dropping
   - Test diff computation for predicate updates
   - Test virtual scrolling use case
   - Test error scenarios

## Key Invariants to Maintain

1. **Single Source of Truth**: Reactor only ever tracks one predicate per predicate_id
2. **Atomic Updates**: Predicate change and entity updates happen in single ReactorUpdate
3. **No Data Loss**: Never clear resultset during updates, only add/remove delta
4. **Version Monotonicity**: Only accept increasing version numbers
5. **Initialization State**: is_initialized = false during updates, true when complete

## Notes

- For virtual scrolling, the client controls the window via predicate updates
- Date-based windows for now (not offset/limit)
- The system doesn't need to understand the "window" semantics, just apply predicates
