use ankql::ast::{Predicate, Resolved};
use ankurah_proto::{Attested, EntityId, EntityState, Event, GetResult};

use crate::{entity::TemporaryEntity, error::RetrievalError, policy::AccessDenied, selection::filter::evaluate_predicate};

use super::StorageEngine;

/// One requested entity's state, absence, or failed predicate check.
#[derive(Debug)]
pub enum GetStateResult {
    Found(Attested<EntityState>),
    NotFound(EntityId),
    PredicateMismatch(EntityId),
}

impl GetStateResult {
    pub fn matching(state: Attested<EntityState>, predicate: &Predicate<Resolved>) -> Result<Self, RetrievalError> {
        let matches = match predicate {
            Predicate::True => true,
            Predicate::False => false,
            _ => evaluate_predicate(&TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?, predicate).unwrap_or(false),
        };
        Ok(if matches { Self::Found(state) } else { Self::PredicateMismatch(state.payload.entity_id) })
    }
}

impl From<GetStateResult> for GetResult {
    fn from(result: GetStateResult) -> Self {
        match result {
            GetStateResult::Found(state) => Self::Found(state),
            GetStateResult::NotFound(id) => Self::NotFound(id),
            GetStateResult::PredicateMismatch(id) => Self::AccessDenied(id),
        }
    }
}

impl TryFrom<GetStateResult> for Attested<EntityState> {
    type Error = RetrievalError;

    fn try_from(result: GetStateResult) -> Result<Self, Self::Error> {
        match result {
            GetStateResult::Found(state) => Ok(state),
            GetStateResult::NotFound(id) => Err(RetrievalError::EntityNotFound(id)),
            GetStateResult::PredicateMismatch(_) => Err(AccessDenied::ByPolicy("Entity is outside the retrieval predicate").into()),
        }
    }
}

/// Fallback for engines that check entity visibility after loading events rather than joining during retrieval.
pub async fn filter_events<SE: StorageEngine + ?Sized>(
    storage: &SE,
    mut events: Vec<Attested<Event>>,
    predicate: &Predicate<Resolved>,
) -> Result<Vec<Attested<Event>>, RetrievalError> {
    if *predicate != Predicate::True && !events.is_empty() {
        let ids = events.iter().map(|event| event.payload.entity_id).collect::<std::collections::BTreeSet<_>>();
        let allowed: std::collections::BTreeSet<_> =
            storage.filter_entity_ids(&ids.into_iter().collect::<Vec<_>>(), predicate).await?.into_iter().collect();
        events.retain(|event| allowed.contains(&event.payload.entity_id));
    }
    Ok(events)
}
