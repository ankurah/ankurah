# LWWCausalRegister Fix: Claude's Implementation Proposal

## Spec Interpretation & Amendments

### Core Problem Agreement
The spec correctly identifies that head supersession ≠ per-property supersession. A property's last-write event may not be a head tip if subsequent events on the same branch didn't touch that property.

### Amendment 1: Simplify Causal Comparison

The spec proposes `EventLayer::compare(a, b) -> Result<CausalRelation, RetrievalError>`.

**My amendment:** Instead of exposing a general comparison API, provide a simpler **ancestry check**:

```rust
/// Check if `descendant` causally descends from `ancestor` using accumulated DAG.
fn is_descendant(&self, descendant: &EventId, ancestor: &EventId) -> Option<bool>;
```

Returns:
- `Some(true)` - confirmed descendant
- `Some(false)` - confirmed NOT descendant (concurrent or ancestor)
- `None` - cannot determine with available DAG info

**Rationale:** Full causal comparison requires knowing both directions. For LWW, we only need to know if one candidate supersedes another. This is a simpler API that's harder to misuse.

### Amendment 2: Candidate Reduction Algorithm

The spec says "reduce by repeated compare calls." I propose a clearer algorithm:

For each property with candidates `[C1, C2, ..., Cn]`:
1. **Filter dominated:** Remove any candidate that is an ancestor of another candidate
2. **Remaining are concurrent:** All survivors are mutually concurrent
3. **Lexicographic tiebreak:** Pick max EventId among survivors

If any ancestry check returns `None` when we need it, bail out.

### Amendment 3: CausalContext Trait

Rather than modifying `EventLayer` directly, introduce a `CausalContext` trait that can be passed to backends:

```rust
pub trait CausalContext: Send + Sync {
    /// Check if `descendant` is a causal descendant of `ancestor`.
    /// Returns None if relationship cannot be determined.
    fn is_descendant(&self, descendant: &EventId, ancestor: &EventId) -> Option<bool>;

    /// Check if event_id is in the accumulated DAG (known to us).
    fn contains(&self, event_id: &EventId) -> bool;
}
```

This keeps the interface minimal and composable.

### Amendment 4: No Legacy Format Support

The spec says "no legacy format." I agree but propose a **migration path**:

- `from_state_buffer` with legacy format should return `Err(MigrationRequired)`
- A separate migration function can upgrade legacy state
- This prevents silent data loss during upgrade

## API Shape

### CausalContext Trait

```rust
/// Provides causal relationship information from accumulated DAG.
pub trait CausalContext: Send + Sync {
    /// Check if `descendant` is a causal descendant of `ancestor`.
    /// - Some(true): descendant > ancestor causally
    /// - Some(false): descendant does NOT descend from ancestor
    /// - None: cannot determine (insufficient DAG info)
    fn is_descendant(&self, descendant: &EventId, ancestor: &EventId) -> Option<bool>;

    /// Check if an event is known in the accumulated DAG.
    fn contains(&self, event_id: &EventId) -> bool;
}
```

### Modified PropertyBackend Trait

```rust
/// Apply a layer of concurrent events with causal context.
fn apply_layer_with_context(
    &self,
    already_applied: &[&Event],
    to_apply: &[&Event],
    current_head: &[EventId],
    context: &dyn CausalContext,
) -> Result<(), MutationError>;

// Deprecate the old apply_layer or make it call apply_layer_with_context with a no-op context
```

### EventAccumulator as CausalContext

```rust
impl<R: Retrieve> CausalContext for EventAccumulator<R> {
    fn is_descendant(&self, descendant: &EventId, ancestor: &EventId) -> Option<bool> {
        if descendant == ancestor {
            return Some(false); // Equal, not strictly descends
        }

        // Walk backward from descendant looking for ancestor
        let mut frontier = vec![*descendant];
        let mut visited = BTreeSet::new();

        while let Some(id) = frontier.pop() {
            if visited.contains(&id) {
                continue;
            }
            visited.insert(id);

            if id == *ancestor {
                return Some(true); // Found ancestor
            }

            // Get parents from DAG
            match self.dag.get(&id) {
                Some(parents) => frontier.extend(parents.iter().cloned()),
                None => return None, // Unknown event - can't determine
            }
        }

        Some(false) // Exhausted search without finding ancestor
    }

    fn contains(&self, event_id: &EventId) -> bool {
        self.dag.contains_key(event_id)
    }
}
```

## High-Level Algorithm

### LWW Winner Selection with Causal Context

```rust
fn select_winner(
    candidates: &[(Option<Value>, EventId)],
    context: &dyn CausalContext,
) -> Result<Option<(Option<Value>, EventId)>, MutationError> {
    if candidates.is_empty() {
        return Ok(None);
    }
    if candidates.len() == 1 {
        return Ok(Some(candidates[0].clone()));
    }

    // Phase 1: Filter out dominated candidates
    let mut dominated = BTreeSet::new();
    for (i, (_, id_a)) in candidates.iter().enumerate() {
        for (j, (_, id_b)) in candidates.iter().enumerate() {
            if i == j {
                continue;
            }
            // Check if id_a is dominated by id_b (id_b descends from id_a)
            match context.is_descendant(id_b, id_a) {
                Some(true) => {
                    // id_b > id_a causally, id_a is dominated
                    dominated.insert(i);
                }
                Some(false) => {
                    // id_b does not descend from id_a - might be concurrent or id_a > id_b
                }
                None => {
                    // Cannot determine - bail out
                    return Err(MutationError::InsufficientCausalInfo {
                        event_a: *id_a,
                        event_b: *id_b,
                    });
                }
            }
        }
    }

    // Phase 2: Collect non-dominated (concurrent) candidates
    let concurrent: Vec<_> = candidates
        .iter()
        .enumerate()
        .filter(|(i, _)| !dominated.contains(i))
        .map(|(_, c)| c.clone())
        .collect();

    // Phase 3: Lexicographic tiebreak among concurrent candidates
    let winner = concurrent
        .into_iter()
        .max_by_key(|(_, id)| *id)
        .unwrap(); // At least one survivor

    Ok(Some(winner))
}
```

### Modified apply_layer_with_context

```rust
fn apply_layer_with_context(
    &self,
    already_applied: &[&Event],
    to_apply: &[&Event],
    current_head: &[EventId],
    context: &dyn CausalContext,
) -> Result<(), MutationError> {
    // Build candidate set per property
    let mut candidates: BTreeMap<PropertyName, Vec<(Option<Value>, EventId)>> = BTreeMap::new();

    // Add layer events
    for event in already_applied.iter().chain(to_apply.iter()) {
        if let Some(operations) = event.operations.get("lww") {
            for op in operations {
                let changes = decode_lww_operation(op)?;
                for (prop, value) in changes {
                    candidates.entry(prop).or_default().push((value, event.id()));
                }
            }
        }
    }

    // Add stored per-property last-write values
    {
        let values = self.values.read().unwrap();
        for (prop, entry) in values.iter() {
            if let Some(eid) = &entry.event_id {
                // Skip if this event is already in layer events
                if candidates.get(prop).map(|c| c.iter().any(|(_, id)| id == eid)).unwrap_or(false) {
                    continue;
                }
                // Add as candidate
                candidates.entry(prop.clone()).or_default().push((entry.value.clone(), *eid));
            }
        }
    }

    // Determine winners using causal context
    let to_apply_ids: BTreeSet<EventId> = to_apply.iter().map(|e| e.id()).collect();
    let mut changed_fields = Vec::new();

    {
        let mut values = self.values.write().unwrap();
        for (prop, prop_candidates) in candidates {
            let winner = select_winner(&prop_candidates, context)?;
            if let Some((value, winner_id)) = winner {
                // Only apply if winner is from to_apply
                if to_apply_ids.contains(&winner_id) {
                    values.insert(prop.clone(), ValueEntry {
                        value,
                        committed: true,
                        event_id: Some(winner_id)
                    });
                    changed_fields.push(prop);
                }
            }
        }
    }

    // Notify subscribers
    self.notify_changed_fields(&changed_fields);
    Ok(())
}
```

## Failure Modes & Error Semantics

### New Error Type

```rust
#[derive(Debug, Clone)]
pub enum MutationError {
    // ... existing variants ...

    /// Causal relationship between events could not be determined.
    /// This indicates the accumulated DAG is incomplete.
    InsufficientCausalInfo {
        event_a: EventId,
        event_b: EventId,
    },

    /// State buffer missing required event_id for property.
    MigrationRequired {
        property: PropertyName,
    },
}
```

### Error Handling Strategy

1. **InsufficientCausalInfo**: Bubble up to caller. Caller should:
   - Fetch missing events from peer
   - Re-seed accumulator
   - Retry layer application

2. **MigrationRequired**: Reject state buffer load. Caller should:
   - Use migration utility to upgrade state
   - Or reject the state entirely

### Guarantees

- If `apply_layer_with_context` returns `Ok(())`, the winner selection was correct
- If it returns `Err(InsufficientCausalInfo)`, no state was modified
- Legacy state buffers are rejected, not silently corrupted

## Test Plan

### Test 1: `test_lww_last_write_not_head_competes`
Scenario from the bug report - B1's title value competes with D1 even though B1 is not in head.

### Test 2: `test_lww_concurrent_lexicographic_fallback`
Two truly concurrent writes, higher EventId wins.

### Test 3: `test_lww_causal_descends_wins`
Causal descendant wins even if lexicographically lower.

### Test 4: `test_lww_insufficient_dag_bails`
Missing event in DAG causes InsufficientCausalInfo error.

### Test 5: `test_lww_multiple_properties_independent`
Different properties have different winners.

### Test 6: `test_lww_stored_value_superseded_by_layer`
Stored value is causally dominated by layer event.

## Integration with EventAccumulator Refactor

The `EventAccumulator` from the refactor plan naturally implements `CausalContext`:
- It has the `dag: BTreeMap<EventId, Vec<EventId>>` for ancestry lookups
- It accumulates events during comparison
- It's available when `EventLayers` is iterated

The integration point is:
```rust
impl<R: Retrieve> EventLayers<R> {
    pub async fn next(&mut self) -> Result<Option<EventLayerWithContext<'_>>, RetrievalError> {
        // ... compute layer ...
        Ok(Some(EventLayerWithContext {
            already_applied,
            to_apply,
            context: &self.accumulator, // Borrow accumulator as CausalContext
        }))
    }
}
```

This keeps the accumulator's DAG available for causal queries during layer application.
