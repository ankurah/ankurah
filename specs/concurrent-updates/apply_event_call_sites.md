# Entity.apply_event() Call Site Analysis

## Overview

This document analyzes all call sites of `Entity::apply_event()` to understand when it's acceptable to apply events to entities with empty heads, particularly in the context of locally created uncommitted entities.

## Call Sites

### 1. `node_applier.rs:89` - Remote Update (EventOnly)

**Context:** Handling incoming remote updates with events only (no state fragment).

```rust
let entity = node.entities.get_retrieve_or_create(retriever, &collection_id, &entity_id, &mut entity_trx).await?;
for event in events {
    if entity.apply_event(retriever, &event.payload).await? {
        applied_events.push(event);
    }
}
```

**Scenarios:**

a) **Server echo of locally created entity:**

- Local: Entity created with `commit_state`, `head = []`
- Server echo arrives with same genesis event (`parent = []`)
- **Protection:** `get_retrieve_or_create` awaits commit/rollback
- After await:
  - If committed: `head = [event_id]` → lineage check → `Equal` → no-op ✅
  - If rolled back: entity removed → creates new entity → accepts genesis ✅

b) **Genuine remote creation (from another peer):**

- No local entity exists
- `get_retrieve_or_create` creates new entity with `head = []`
- `is_entity_create() && head.is_empty()` → accepts ✅

c) **Duplicate genesis (malicious/buggy peer):**

- Entity already exists with `head = [existing_genesis]`
- Remote genesis arrives (`parent = []`)
- `is_entity_create()` true, but `head.is_empty()` false
- Falls through to lineage check: `compare_unstored_event([], [existing])`
- Returns `Disjoint` (NULL semantics)
- **Rejected** ✅

### 2. `node_applier.rs:276` - Remote Delta (EventBridge)

**Context:** Handling incoming deltas with event bridges.

```rust
retriever.stage_events(attested_events.clone());
let entity = node.entities.get_retrieve_or_create(retriever, &delta.collection, &delta.entity_id, &mut entity_trx).await?;
for event in attested_events.into_iter() {
    entity.apply_event(retriever, &event.payload).await?;
    retriever.mark_event_used(&event.payload.id());
}
// Save updated state
Self::save_state(node, &entity, &collection).await?;
// Commit any entities created
entity_trx.commit();
```

**Analysis:** Same as scenario #1 above - protected by await in `get_retrieve_or_create`.

**⚠️ CRITICAL ISSUE (Task #11):**

- Events are staged in retriever via `stage_events()` and marked via `mark_event_used()`
- `entity_trx.commit()` is called at line 284, making entities visible
- BUT: staged events are not persisted until `retriever.store_used_events()` is called by the caller

**Call sites of `apply_deltas()`:**

1. `client_relay.rs:544` - CALLS `store_used_events()` after (line 545) ⚠️ window between commit and persist
2. `context.rs:333` - NEVER calls `store_used_events()` ❌ **events are lost!**

**Risks:**

- If process crashes between `entity_trx.commit()` and `store_used_events()`, entities have heads pointing to non-existent events
- In `context.rs` path, events are NEVER persisted to storage!

**Solutions:**

1. **Best:** Call `retriever.store_used_events()` inside `apply_deltas()` BEFORE `entity_trx.commit()`
2. **Alternative:** Make `EntityTransaction::commit()` return a rollback handle, call `store_used_events()`, then finalize commit
3. **Simplest:** Save events immediately in `stage_events()` or `retrieve_event()` instead of batching

### 3. `node.rs:562` - Remote Transaction Commit

**Context:** Handling transactions from remote peers (other clients).

```rust
let entity = self.entities.get_retrieve_or_create(&retriever, &event.payload.collection, &event.payload.entity_id, &mut entity_trx).await?;
if entity.apply_event(&retriever, &event.payload).await? {
    collection.add_event(event).await?;
    collection.set_state(attested).await?;
}
```

**Analysis:** Same as scenario #1 above - protected by await in `get_retrieve_or_create`.

### 4. `context.rs:268` - Local Transaction Commit (Branch → Primary)

**Context:** Applying changes from a Branch entity to its upstream Primary during local transaction commit.

```rust
if let crate::entity::EntityKind::Branch { upstream, .. } = &entity.kind {
    upstream.apply_event(&retriever, &attested_event.payload).await?;
}
```

**Analysis:**

- Branch was created from Primary via `snapshot()`
- Primary always has non-empty head when Branch is created
- No empty head scenario here ✅

## Entity Creation Guard

The `is_entity_create()` guard in `Entity::apply_event` (lines 266-295):

```rust
if event.is_entity_create() {  // checks parent.is_empty()
    let mut state = self.state.write().unwrap();
    if state.head.is_empty() {
        // Accept as creation event, bypass lineage check
        // ...apply operations...
        state.head = event.id().into();
        return Ok(true);
    }
}
// Fall through to lineage comparison
```

**Current behavior:**

- Allows ANY event with `parent = []` to be applied to an entity with `head = []`
- This is correct for genuine remote creations
- Protected from local creation race by await in `get_retrieve_or_create`

**Potential tightening (Task #4):**

- Also check if `event.id()` is already in `state.head`
- Would handle edge case where server echo arrives exactly after commit
- Would make the guard more robust

## Empty Clock Semantics (NULL Semantics)

With the new `Disjoint` semantics for empty clocks:

- `compare([], [])` → `Disjoint { subject_root: None, other_root: None }`
- `compare([x], [])` → `Disjoint { subject_root: Some(x), other_root: None }`
- Genesis events bypass lineage check via `is_entity_create()` guard
- Duplicate genesis events are rejected by `Disjoint` in lineage check

## Conclusion

**Current protection is sound:**

1. ✅ **Locally created uncommitted entities** are protected by await in `get_retrieve_or_create`
2. ✅ **Genuine remote creations** are allowed via `is_entity_create() && head.is_empty()`
3. ✅ **Duplicate genesis events** are rejected by `Disjoint` check after falling through guard
4. ✅ **Rolled back local creations** correctly accept server echo

**No operational issues found** - the combination of:

- `get_retrieve_or_create` awaiting uncommitted entities
- `is_entity_create()` guard for genesis events
- `Disjoint` rejection for duplicate genesis

...ensures that locally created entities are never overwritten by remote echoes.

Task #4 (tightening `is_entity_create()`) remains a good hardening measure but is not strictly necessary for correctness.
