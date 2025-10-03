# Phase 1: Known Matches Implementation Plan

## Overview

This document outlines the implementation plan for Phase 1 of the lineage attestation feature, focusing solely on the `known_matches` mechanism for optimizing subscription initialization and fetch operations. The actual lineage attestation/assertion functionality is deferred to a later phase.

## Goals

1. **Optimize initial data transfer** by allowing clients to specify entities they already have (`known_matches`)
2. **Support event bridges** for deltas between known and current state (no size limit)
3. **Maintain correctness** of reactive updates and subscription management
4. **Ensure backward compatibility** with existing test suite
5. **Consolidate all node update logic** into a single, reliable module

## Non-Goals for Phase 1

- No `CausalRelation` or `CausalAssertion` implementation for wire protocol
- No `DeltaContent::StateAndRelation` variant usage
- No attestation validation beyond existing state validation
- No event bridge size limits or thresholds

## Current State

The proto changes are complete and satisfactory:

- `NodeRequestBody::Fetch` and `SubscribeQuery` include `known_matches: Vec<KnownEntity>`
- `NodeResponseBody::QuerySubscribed` returns `initial: Vec<EntityDelta>`
- `EntityDelta` supports `StateSnapshot` and `EventBridge` content variants

## Remaining Work

### 1. Fix Compilation Errors

**Location**: `core/src/context.rs:326` and `core/src/livequery.rs:119`

**Issues**:

- `apply_state` expects `State` but receives `StateFragment`
- Version comparison type mismatch in LiveQuery

**Actions**:

- Extract `state.state` from `StateFragment` when calling `apply_state`
- Fix atomic integer type consistency for version tracking

### 2. Implement Event Gap Retrieval

**Location**: Modify `core/src/lineage.rs`

**Implementation Steps**:

1. Create `EventAccumulator<Id>` struct with `events: Vec<Id>` and `maximum: Option<usize>`
2. Add `event_accumulator: Option<EventAccumulator<G::Id>>` to `Comparison` struct
3. Add constructor `Comparison::new_with_accumulator()` that accepts optional accumulator
4. Modify `process_event()` to accumulate event IDs when traversing from subject frontier
5. Add `take_accumulated_events()` method to extract collected events
6. Create helper function `collect_event_bridge()` that wraps the comparison process

**Key Design**:

```rust
pub struct EventAccumulator<Event> {
    events: Vec<Event>,  // Accumulates actual events, not just IDs
    maximum: Option<usize>,  // None for unlimited (Phase 1)
}

// Usage pattern:
let accumulator = EventAccumulator::new(None);
let mut comparison = Comparison::new_with_accumulator(
    getter, current_head, known_head, budget, Some(accumulator)
);
// Run comparison...
let events = comparison.take_accumulated_events();
```

**Key Points**:

- Only accumulate events seen from the subject side (current_head direction)
- Ignore events from the other side (known_head direction) of the traversal
- Set `maximum` to `None` for Phase 1 (unlimited collection)
- Return both `Ordering` and event list for complete information

**Benefits**:

- Reuses proven DAG traversal logic
- Provides `Ordering` for free (useful for Phase 2's `CausalAssertion`)
- Single implementation for both event bridge and lineage comparison
- Clean separation of concerns with optional accumulator

### 3. Implement Server-Side Delta Generation

**Location**: `core/src/peer_subscription/server.rs::subscribe_query`

**Current**: Always returns `StateSnapshot` for all matching entities

**Required Logic**:

```
For each entity in fetch_states result:
  if entity_id in known_matches:
    if entity.head == known_head:
      -> Omit from initial response (client already has current state)
    else:
      -> Build EventBridge using lineage-based gap retrieval
      -> Return EventBridge { events }
  else:
    -> Return StateSnapshot { state } (client doesn't have this entity)
```

**Key Considerations**:

- No event bridge size limit - always prefer EventBridge when possible
- Use lineage comparison to get both ordering and event list
- Handle entities that match the predicate but aren't in initial fetch (removed entities)

### 4. Implement Fetch Request Delta Generation

**Location**: `core/src/node.rs::handle_request` (Fetch branch)

**Current**: Placeholder FIXMEs, always returns StateSnapshot

**Required**: Same logic as subscription initialization (see #3)

### 5. Refactor and Consolidate Node Update Logic

**Location**: Move `core/src/peer_subscription/applier.rs` to `core/src/node_applier.rs`

**Rename**: `UpdateApplier` -> `NodeApplier`

**New Methods**:

- `apply_update()` - existing, handles `SubscriptionUpdateItem`
- `apply_delta()` - new, handles `EntityDelta` from Fetch/QuerySubscribed
- `apply_events()` - extracted common logic for event application
- `save_events()` - existing helper
- `save_state()` - existing helper

**Goal**: Single source of truth for all node state mutations from remote sources

### 6. Implement EventBridge Application

**Location**: `core/src/node_applier.rs::apply_delta` (new method)

**Current**: Only handles `StateSnapshot`, `EventBridge` is unimplemented

**Required Logic**:

```
EventBridge { events } =>
  1. Get entity from local storage or create new
  2. Validate and save events to storage
  3. Apply events to entity sequentially
  4. Return updated entity for resultset refresh
```

**Key Considerations**:

- Reuse logic from existing `apply_update` for event processing
- Entity may not be in resultset yet (fetch from storage)
- Ensure proper event validation and storage

### 7. Context and Client Relay Updates

**Location**: Multiple files need updating to use `NodeApplier`

**Changes Required**:

- `core/src/context.rs::fetch` - Use `NodeApplier::apply_delta`
- `core/src/peer_subscription/client_relay.rs::apply_initial_deltas` - Use `NodeApplier::apply_delta`
- Both should then re-fetch from local storage to populate resultsets

**Critical Design Consideration - Reactor Notification**:

When `QuerySubscribed` response arrives with initial deltas:

1. **Apply to Storage**: `NodeApplier::apply_delta` saves entities to storage ✓
2. **Update Query's Resultset**: The subscribing query's resultset must be populated with the entities
3. **Notify Reactor**: The reactor must be notified so OTHER queries can react to these new entities
4. **Emit Correct Changes**: The subscribing query should emit `ItemChange::Initial` for these entities

**Change Emission Pattern**:

- `ItemChange::Initial`: First time an entity matches (initial subscription or after predicate change)
- `ItemChange::Add`: Entity newly matches after a predicate change (wasn't matching before)
- `ItemChange::Remove`: Entity no longer matches after a predicate change (was matching before)
- `ItemChange::Update`: Entity still matches but its data changed

**Implementation Approach (Finalized for Phase 1)**:

- Make `NodeApplier::apply_delta` private and return `Option<EntityChange>`.
- Add `NodeApplier::apply_deltas(deltas: Vec<EntityDelta>)` which:

  - Applies deltas in parallel using a `FuturesUnordered` (do we actually need this to do the below? seems like we should implement our own stream that yields the chunks)
  - On each wake, repeatedly drain all immediately-ready futures to gather the full batch of `Ready` results for this poll cycle (do not block on stragglers).
  - For each drained batch, call `reactor.notify_change(batch: Vec<EntityChange>)` to notify ALL local subscriptions.
  - Continue draining until all futures complete.
  - Errors are logged and skipped; do not abort the whole batch.

- Phase 1 EntityChange payload:

  - Build `EntityChange` with the updated `entity` and an empty `events` list initially (the reactor’s membership logic does not depend on events).

- Subscription path behavior:

  - For non-cached initial queries (query_wait): after apply_deltas completes, re-fetch from local storage and overwrite the subscribing query’s resultset for the current predicate.
  - For cached initial queries and selection updates: still re-fetch and overwrite the subscribing query’s resultset for the new predicate after apply_deltas. This preserves correctness if updates race with QuerySubscribed.

- Resultset overwrite policy:
  - The subscribing LiveQuery is authoritative for its own resultset based on a local re-fetch after initialization or predicate change.
  - `apply_deltas` is for side-effects (storage + notifying other subscriptions), not the final content of the subscribing query’s resultset.

### 8. Simplified Response Handling

**Key Insight**: Version validation is less critical than originally thought

**Approach**:

- **Always apply** `QuerySubscribed` and `Fetch` responses to local storage
- **Always re-fetch** from local storage after applying deltas
- Event DAG monotonicity prevents regression
- No need to track versions for correctness (only for optimization)

**Benefits**:

- Simpler, more robust implementation
- No race condition handling needed
- Consistent with "local storage as source of truth" principle

### 9. Testing Strategy

**Test Coverage Required**:

1. **Known matches with equal heads**: Entities omitted from response
2. **Small event gaps**: EventBridge generation and application
3. **Large event gaps**: Falls back to StateSnapshot
4. **Unknown entities**: Always get StateSnapshot
5. **Selection updates**: Version validation and proper re-initialization
6. **Reactive updates**: Ensure reactor notifications still work correctly

**Existing Tests to Validate**:

- `test_sled` - Basic functionality
- `test_websocket_predicate_subscription` - Remote subscription flow
- `test_local_subscription` - Local subscription behavior
- `test_inter_node` - Node-to-node communication

## Implementation Status: ✅ FULLY COMPLETED

**Phase 1 Known Matches has been successfully implemented** as part of the broader Reactor and lineage refactoring work.

## ✅ **Completed Components**

### **Core Protocol & Infrastructure**

- ✅ **Proto changes**: `known_matches` in `Fetch`/`SubscribeQuery`, `EntityDelta` responses with `StateSnapshot`/`EventBridge` content
- ✅ **EventAccumulator in lineage.rs**: Accumulates events during DAG traversal for gap bridging
- ✅ **NodeApplier consolidation**: Single module for all remote update logic with `apply_deltas` batching
- ✅ **Server-side delta generation**: Smart `EntityDelta` creation (omit equal heads, EventBridge, StateSnapshot)

### **Client Integration**

- ✅ **apply_deltas implementation**: Drain-ready batching with `reactor.notify_change` per batch
- ✅ **Client relay integration**: Uses `apply_deltas` then re-fetches and overwrites resultset
- ✅ **LiveQuery integration**: Proper initialization flow with resultset overwrite and emission policies

### **Key Behaviors**

- ✅ **EventBridge application**: Handles `DeltaContent::EventBridge` with event staging and sequential application
- ✅ **StateSnapshot application**: Handles `DeltaContent::StateSnapshot` with state validation
- ✅ **Gap retrieval**: Uses lineage comparison to collect events for bridges (no size limits)
- ✅ **Error handling**: Proper error mapping for budget exceeded vs storage misses

## ✅ **Non-Goals (Correctly Deferred)**

- No `CausalRelation`/`CausalAssertion` in wire protocol ✅
- No `DeltaContent::StateAndRelation` usage ✅
- No attestation validation beyond existing state validation ✅
- No event bridge size limits or thresholds ✅

## ✅ **Quick Checklist - All Completed**

- [x] apply_delta private → Option<EntityChange> with empty events list
- [x] apply_deltas: parallelize, drain ALL Ready futures per wake, batch notify_change
- [x] Client relay: apply_deltas, then fetch_entities_from_local, then overwrite resultset
- [x] Lineage error handling verified (budget exceeded vs storage misses)

## 📋 **Open Discussions (Future Work)**

The following items were identified as Phase 2 considerations (not Phase 1 scope):

- Reactor.add_query correctness and re-screening logic
- Cancellation of background gap-filling operations
- Applied-events provenance in EntityChange
- Executor implementation details and backpressure
- Initial broadcast policy formalization

## 🎯 **Success Criteria Met**

- ✅ All existing tests pass without modification
- ✅ Network traffic reduced for subscriptions with known entities
- ✅ Event bridges used when delta is small (no size limits as planned)
- ✅ No changes to public API or breaking changes to protocol
- ✅ Clear path forward for Phase 2 (actual lineage attestation)

## 🚀 **Phase 2 Path Forward**

When implementing actual lineage attestation:

- Add `CausalRelation` comparison logic to lineage traversal
- Implement `DeltaContent::StateAndRelation` generation and validation
- Add attestation signature validation in PolicyAgent
- Consider budget/effort limits for lineage traversal
- Handle non-descending relations appropriately
- Consider storage engine optimizations for event bridge collection
