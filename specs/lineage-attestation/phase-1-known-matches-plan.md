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

## Implementation Order

1. **Fix compilation errors** - Unblock testing
2. **Refactor to NodeApplier** - Consolidate update logic
3. **Implement event gap retrieval** - Adapt lineage comparison
4. **Implement EventBridge application** - Complete delta handling
5. **Implement server delta generation** - Smart response construction
6. **Implement Fetch delta generation** - Consistency with subscriptions
7. **Update Context and ClientRelay** - Use new NodeApplier
8. **Run and fix tests** - Ensure no regressions

## Success Criteria

- All existing tests pass without modification
- Network traffic reduced for subscriptions with known entities
- Event bridges used when delta is small (configurable threshold)
- No changes to public API or breaking changes to protocol
- Clear path forward for Phase 2 (actual lineage attestation)

## Design Decisions

1. **No Event Bridge Size Limit**: EventBridge is always preferred over StateSnapshot when we have a known_head, regardless of gap size.

2. **Lineage-Based Gap Retrieval**: Adapt existing `lineage::Comparison` to collect events during traversal, providing both ordering and event list.

3. **NodeApplier Consolidation**: All remote update logic moves to a single module for consistency and maintainability.

4. **Always Apply, Always Refetch**: Responses are always applied to local storage and resultsets are always refreshed from local storage, ensuring consistency.

5. **Removed Entity Handling**: Entities in known_matches that no longer match predicate are simply omitted from response (implicit removal).

## Implementation Decisions Made

1. **EventAccumulator Approach**: Modify `Comparison::step()` to accumulate events as it traverses, using an optional `EventAccumulator` that collects only events from the subject side.

2. **Collection Strategy**: Collect all events seen from the subject side until reaching the known_head or determining incomparability.

3. **Storage Optimization**: Deferred to future phase. Added TODO comment for potential storage engine optimizations of Clock comparison process.

## Notes for Phase 2

When implementing actual lineage attestation:

- Add `CausalRelation` comparison logic
- Implement `DeltaContent::StateAndRelation` generation and validation
- Add attestation signature validation in PolicyAgent
- Consider budget/effort limits for lineage traversal
- Handle non-descending relations appropriately
- Consider storage engine optimizations for event bridge collection
