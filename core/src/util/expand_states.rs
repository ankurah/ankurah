use crate::error::RetrievalError;
use ankurah_proto as proto;

/// Expand initial_states to include additional entities that weren't in the predicate results.
/// This ensures we can generate proper deltas for entities that may no longer match the predicate.
///
/// When a client has knowledge of entities that don't appear in the current predicate results,
/// we need to fetch their current state individually to generate proper deltas (including removals).
pub async fn expand_states(
    mut states: Vec<proto::Attested<proto::EntityState>>,
    additional_entity_ids: impl IntoIterator<Item = proto::EntityId>,
    collection: &crate::storage::StorageCollectionWrapper,
) -> Result<Vec<proto::Attested<proto::EntityState>>, RetrievalError> {
    let mut entity_map: std::collections::HashSet<_> = states.iter().map(|s| s.payload.entity_id).collect();

    for entity_id in additional_entity_ids {
        if !entity_map.contains(&entity_id) {
            // Fetch the current state of this entity (even though it doesn't match the predicate)
            match collection.get_state(entity_id).await {
                Ok(state) => {
                    states.push(state);
                    entity_map.insert(entity_id);
                }
                Err(RetrievalError::EntityNotFound(_)) => {
                    // Entity was deleted - silently ignore
                }
                Err(e) => {
                    // Other errors (storage failures, etc.) should be propagated
                    return Err(e);
                }
            }
        }
    }

    Ok(states)
}
