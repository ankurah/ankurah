use ankql::ast::{Predicate, Resolved};
use ankurah_proto::{Attested, EntityId, EntityState, GetResult};

use crate::{entity::TemporaryEntity, error::RetrievalError, policy::AccessDenied, selection::filter::evaluate_predicate};

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
