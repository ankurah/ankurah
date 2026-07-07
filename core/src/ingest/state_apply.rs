//! The shared state-apply: one function for every state-bearing feed
//! (StateSnapshot deltas, the StateAndEvent fast path, Get responses).
//!
//! with_state mediates through the resident entity, so the comparison
//! machinery decides what the incoming snapshot may do: fresh adoption and
//! strict descent advance, while equal, older, and divergent states are
//! no-ops (a stale fetch can never regress a newer resident, and a
//! divergent snapshot waits for its events). Events that arrived with the
//! state commit BEFORE the buffer persists: a crash must never yield
//! persisted state referencing uncommitted events. Persistence is
//! advance-gated, so a no-op apply writes nothing (the redundant-write
//! elision the bridge arm adopted at M2, uniform here). The caller builds
//! an advance-only change and notifies; validation is NOT here, because
//! each arm keeps its own admission gate (unifying WHICH validation runs
//! is #274's jurisdiction).

use ankurah_proto::{Attested, CollectionId, EntityId, Event, State};

use super::executor::PersistState;
use crate::entity::{Entity, WeakEntitySet};
use crate::error::MutationError;
use crate::retrieval::{GetState, SuspenseEvents};

/// What a state feed did to one entity.
pub(crate) struct StateApplied {
    pub entity: Entity,
    /// True when the entity advanced (fresh adoption or strict descent);
    /// false when the incoming state was equal, older, or divergent. The
    /// advance-only notification rule rides this, as does persistence.
    pub advanced: bool,
}

/// Apply one state (plus any events that traveled with it) to one entity.
pub(crate) async fn apply_state_feed<S, E>(
    entities: &WeakEntitySet,
    state_getter: &S,
    event_getter: &E,
    entity_id: EntityId,
    collection_id: CollectionId,
    state: State,
    events_to_commit: &[Attested<Event>],
    persist: &dyn PersistState,
) -> Result<StateApplied, MutationError>
where
    S: GetState + Send + Sync,
    E: SuspenseEvents + Send + Sync,
{
    let (changed, entity) = entities
        .with_state(state_getter, event_getter, entity_id, collection_id, state)
        .await
        .map_err(|e| super::type_comparison_error(e.into()))?;
    let advanced = !matches!(changed, Some(false));
    if advanced {
        for event in events_to_commit {
            event_getter.commit_event(event).await?;
        }
        persist.persist(&entity).await?;
    }
    Ok(StateApplied { entity, advanced })
}
