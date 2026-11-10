use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    attestation::{AttestationSet, Attested},
    clock::Clock,
    id::EntityId,
};
use ankurah_core_types::ModelId;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct StateFragment {
    pub state: State,
    pub attestations: AttestationSet,
}

impl From<Attested<EntityState>> for StateFragment {
    fn from(attested: Attested<EntityState>) -> Self { Self { state: attested.payload.state, attestations: attested.attestations } }
}
impl From<(EntityId, StateFragment)> for Attested<EntityState> {
    fn from(value: (EntityId, StateFragment)) -> Self {
        let entity_state = EntityState { entity_id: value.0, state: value.1.state };
        Attested { payload: entity_state, attestations: value.1.attestations }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct EntityState {
    pub entity_id: EntityId,
    pub state: State,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct State {
    /// The current accumulated state of the entity inclusive of all events up to this point
    pub state_buffers: StateBuffers,
    /// Model-backed memberships established by this entity's causal history.
    pub memberships: BTreeSet<ModelId>,
    /// The materialized head: the concurrent tips (usually one) applied to the state above, each with its
    /// generation; empty before the genesis applies.
    pub head: Clock,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct StateBuffers(pub BTreeMap<String, Vec<u8>>);

impl std::ops::Deref for StateBuffers {
    type Target = BTreeMap<String, Vec<u8>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "State({:#} buffers {})",
            self.head,
            self.state_buffers.iter().map(|(backend, buf)| format!("{} => {}b", backend, buf.len())).collect::<Vec<_>>().join(" ")
        )
    }
}

impl std::fmt::Display for StateFragment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StateFragment(state {} attestations: {})", self.state, self.attestations.len())
    }
}

impl std::fmt::Display for EntityState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EntityState({} {})", self.entity_id.to_base64_short(), self.state)
    }
}

impl From<EntityState> for Attested<EntityState> {
    fn from(val: EntityState) -> Self { Attested { payload: val, attestations: AttestationSet::default() } }
}
