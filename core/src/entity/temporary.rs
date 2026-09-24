use super::state::EntityInnerState;
use crate::{
    error::RetrievalError,
    selection::filter::Filterable,
    value::Value,
};
use ankql::ast::PropertyId;
use ankurah_proto::{EntityId, ModelId, State};

/// Reconstituted state for predicate evaluation, not registered with a node.
pub struct TemporaryEntity {
    id: EntityId,
    state: EntityInnerState,
}

impl TemporaryEntity {
    pub fn new(id: EntityId, state: &State) -> Result<Self, RetrievalError> {
        Ok(Self { id, state: EntityInnerState::from_state(state)? })
    }

    pub fn values(&self) -> Vec<(PropertyId, Option<Value>)> {
        self.state.backends.values().flat_map(|backend| backend.property_values()).collect()
    }
}

impl Filterable for TemporaryEntity {
    fn is_member_of(&self, model: &ModelId) -> Result<bool, crate::selection::filter::Error> {
        Ok(self.state.memberships.contains(model))
    }

    fn value(&self, property: &PropertyId) -> Option<Value> {
        if *property == PropertyId::Id {
            Some(Value::EntityId(self.id))
        } else {
            self.state.backends.values().find_map(|backend| backend.property_value(property))
        }
    }
}

impl std::fmt::Display for TemporaryEntity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TemporaryEntity({}) = {}", self.id, self.state.head)
    }
}
