# Entity Apply Event/State TODOs

Notes preserved from `entity.rs` during event_dag migration (2026-01-05).

## Event Replay for Multi-Hop Descends

When applying an event that descends the entity head by more than one event, we currently just apply the latest event's operations. This is incorrect - we need to play forward all events from the current entity/backend state to the new event.

**Current behavior**: Apply only the final event's operations
**Correct behavior**: Replay all events in the lineage gap

The challenge is that lineage comparison looks backward (from new event to head), but we need to replay events forward. Options:
1. Have `StrictDescends` return the list of event IDs connecting current head to new event
2. Use an LRU cache on event retrieval to make forward replay efficient
3. Stream events rather than materializing potentially large event lists

## NotDescends Differentiation

The `NotDescends` case currently combines two distinct situations:
1. **Ascends**: The incoming event is an ancestor of the current head (older event) - should be ignored
2. **Concurrent**: The incoming event diverges from the current head - should be merged

Current code applies both as concurrent, augmenting the head. This is correct for concurrent events but wrong for ascends (older events should be no-ops).

## LWW Backend Strategy

For concurrent events, LWW (Last-Write-Wins) backends need a deterministic strategy to resolve conflicts. Suggested approach: lexicographic comparison of event IDs to ensure all nodes converge to the same value.

## Security Note (apply_state)

When applying state directly (vs events), there's a potential vulnerability: a malicious peer could send a state buffer that disagrees with the actual event operations. Playing forward events would ensure the state always matches the verified event lineage.
