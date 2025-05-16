use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{auth::Attested, clock::Clock, collection::CollectionId, id::EntityId, AttestationSet, DecodeError};

#[derive(Clone, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct EventId([u8; 32]);

impl std::fmt::Debug for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "EventId({})", self.to_base64()) }
}

impl EventId {
    /// Generate an EventID from the parts of an Event
    /// notably, we are not including the collection in the hash because collection is getting excised from identity
    pub fn from_parts(entity_id: &EntityId, operations: &OperationSet, parent: &Clock) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bincode::serialize(&entity_id).unwrap());
        hasher.update(bincode::serialize(&operations).unwrap());
        hasher.update(bincode::serialize(&parent).unwrap());
        Self(hasher.finalize().into())
    }
    pub fn to_base64(&self) -> String {
        use base64::{engine::general_purpose, Engine as _};
        general_purpose::URL_SAFE_NO_PAD.encode(self.0)
    }
    pub fn to_base64_short(&self) -> String {
        // take the last 6 characters of the base64 encoded string
        let value = self.to_base64();
        value[value.len() - 6..].to_string()
    }
    pub fn from_base64<T: AsRef<[u8]>>(input: T) -> Result<Self, DecodeError> {
        use base64::{engine::general_purpose, Engine as _};
        let decoded = general_purpose::URL_SAFE_NO_PAD.decode(input)?;
        let v: [u8; 32] = decoded.try_into().map_err(|_| DecodeError::InvalidLength)?;

        Ok(Self(v))
    }
    pub fn to_bytes(self) -> [u8; 32] { self.0 }
    pub fn from_bytes(bytes: [u8; 32]) -> Self { Self(bytes) }
    pub fn as_bytes(&self) -> &[u8] { &self.0 }
}

impl std::fmt::Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.to_base64()) }
}

impl TryFrom<String> for EventId {
    type Error = DecodeError;

    fn try_from(s: String) -> Result<Self, Self::Error> { Self::from_base64(&s) }
}

impl From<[u8; 32]> for EventId {
    fn from(bytes: [u8; 32]) -> Self { Self(bytes) }
}
impl TryFrom<Vec<u8>> for EventId {
    type Error = DecodeError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let v: [u8; 32] = bytes.try_into().map_err(|_| DecodeError::InvalidLength)?;
        Ok(Self(v))
    }
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Event {
    pub collection: CollectionId,
    pub entity_id: EntityId,
    pub operations: OperationSet,
    /// The set of concurrent events (usually only one) which is the precursor of this event
    pub parent: Clock,
}

impl Event {
    // TODO: figure out how we actually want to signify entity creation. This is a hack for now
    pub fn is_entity_root(&self) -> bool { self.parent.is_empty() }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct EventFragment {
    pub operations: OperationSet,
    pub parent: Clock,
    pub attestations: AttestationSet,
}

impl From<Attested<Event>> for EventFragment {
    fn from(attested: Attested<Event>) -> Self {
        Self { operations: attested.payload.operations, parent: attested.payload.parent, attestations: attested.attestations }
    }
}

impl From<(EntityId, CollectionId, EventFragment)> for Attested<Event> {
    fn from(value: (EntityId, CollectionId, EventFragment)) -> Self {
        let event = Event { entity_id: value.0, collection: value.1, operations: value.2.operations, parent: value.2.parent };
        Attested { payload: event, attestations: value.2.attestations }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct StateFragment {
    pub state: State,
    pub attestations: AttestationSet,
}

impl From<Attested<EntityState>> for StateFragment {
    fn from(attested: Attested<EntityState>) -> Self { Self { state: attested.payload.state, attestations: attested.attestations } }
}
impl From<(EntityId, CollectionId, StateFragment)> for Attested<EntityState> {
    fn from(value: (EntityId, CollectionId, StateFragment)) -> Self {
        let entity_state = EntityState { entity_id: value.0, collection: value.1, state: value.2.state };
        Attested { payload: entity_state, attestations: value.2.attestations }
    }
}

impl Event {
    pub fn id(&self) -> EventId { EventId::from_parts(&self.entity_id, &self.operations, &self.parent) }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct OperationSet(pub BTreeMap<String, Vec<Operation>>);

impl std::fmt::Display for OperationSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OperationSet({})",
            self.0
                .iter()
                .map(|(backend, ops)| format!("{} => {}b", backend, ops.iter().map(|op| op.diff.len()).sum::<usize>()))
                .collect::<Vec<_>>()
                .join(" ")
        )
    }
}

impl std::ops::Deref for OperationSet {
    type Target = BTreeMap<String, Vec<Operation>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub struct Operation {
    pub diff: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct EntityState {
    pub entity_id: EntityId,
    pub collection: CollectionId,
    pub state: State,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct State {
    /// The current accumulated state of the entity inclusive of all events up to this point
    pub state_buffers: StateBuffers,
    /// The set of concurrent events (usually only one) which have been applied to the entity state above
    pub head: Clock,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct StateBuffers(pub BTreeMap<String, Vec<u8>>);

impl std::ops::Deref for StateBuffers {
    type Target = BTreeMap<String, Vec<u8>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Event({} {}/{} {}{} {})",
            self.id().to_base64_short(),
            self.collection,
            self.entity_id.to_base64_short(),
            if self.is_entity_root() { "(create) " } else { "" },
            self.parent.to_base64_short(),
            self.operations
                .iter()
                .map(|(backend, ops)| format!("{} => {}b", backend, ops.iter().map(|op| op.diff.len()).sum::<usize>()))
                .collect::<Vec<_>>()
                .join(" ")
        )
    }
}

impl std::fmt::Display for EventFragment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EventFragment(parent {} operations {})", self.parent, self.operations)
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "State(clock {} buffers {})",
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

impl Attested<Event> {
    pub fn collection(&self) -> &CollectionId { &self.payload.collection }
}

impl From<Event> for Attested<Event> {
    fn from(val: Event) -> Self { Attested { payload: val, attestations: AttestationSet::default() } }
}

impl From<EntityState> for Attested<EntityState> {
    fn from(val: EntityState) -> Self { Attested { payload: val, attestations: AttestationSet::default() } }
}

impl Attested<EntityState> {
    pub fn to_parts(self) -> (EntityId, CollectionId, StateFragment) {
        (self.payload.entity_id, self.payload.collection, StateFragment { state: self.payload.state, attestations: self.attestations })
    }
    pub fn from_parts(entity_id: EntityId, collection: CollectionId, fragment: StateFragment) -> Self {
        Self { payload: EntityState { entity_id, collection, state: fragment.state }, attestations: fragment.attestations }
    }
}

impl Attested<Event> {
    pub fn from_parts(entity_id: EntityId, collection: CollectionId, frag: EventFragment) -> Self {
        Self { payload: Event { entity_id, collection, operations: frag.operations, parent: frag.parent }, attestations: frag.attestations }
    }
}
