use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{auth::Attested, clock::Clock, collection::CollectionId, id::EntityID, DecodeError};

#[derive(Debug, Clone, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct EventID([u8; 32]);

impl EventID {
    /// Generate an EventID from the parts of an Event
    /// notably, we are not including the collection in the hash because collection is getting excised from identity
    pub fn from_parts(entity_id: &EntityID, operations: &BTreeMap<String, Vec<Operation>>, parent: &Clock) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bincode::serialize(&entity_id).unwrap());
        hasher.update(bincode::serialize(&operations).unwrap());
        hasher.update(bincode::serialize(&parent).unwrap());
        Self(hasher.finalize().try_into().unwrap())
    }
    pub fn to_base64(&self) -> String {
        use base64::{engine::general_purpose, Engine as _};
        general_purpose::URL_SAFE_NO_PAD.encode(self.0)
    }
    pub fn from_base64(s: &str) -> Result<Self, DecodeError> {
        use base64::{engine::general_purpose, Engine as _};
        let decoded = general_purpose::URL_SAFE_NO_PAD.decode(s)?;
        let v: [u8; 32] = decoded.try_into().map_err(|_| DecodeError::InvalidLength)?;

        Ok(Self(v))
    }
    pub fn to_bytes(self) -> [u8; 32] { self.0 }
    pub fn from_bytes(bytes: [u8; 32]) -> Self { Self(bytes) }
    pub fn as_bytes(&self) -> &[u8] { &self.0 }
}

impl std::fmt::Display for EventID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.to_base64()) }
}

impl TryFrom<String> for EventID {
    type Error = DecodeError;

    fn try_from(s: String) -> Result<Self, Self::Error> { Self::from_base64(&s) }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Event {
    pub id: EventID,
    pub collection: CollectionId,
    pub entity_id: EntityID,
    pub operations: BTreeMap<String, Vec<Operation>>,
    /// The set of concurrent events (usually only one) which is the precursor of this event
    pub parent: Clock,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub struct Operation {
    pub diff: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EntityState {
    pub entity_id: EntityID,
    pub collection: CollectionId,
    pub state: State,
    pub head: Clock,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct State {
    /// The current accumulated state of the entity inclusive of all events up to this point
    pub state_buffers: BTreeMap<String, Vec<u8>>,
    /// The set of concurrent events (usually only one) which have been applied to the entity state above
    pub head: Clock,
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Event({} {}/{} {} {})",
            self.id,
            self.collection,
            self.entity_id,
            self.parent,
            self.operations
                .iter()
                .map(|(backend, ops)| format!("{} => {}b", backend, ops.iter().map(|op| op.diff.len()).sum::<usize>()))
                .collect::<Vec<_>>()
                .join(" ")
        )
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

impl std::fmt::Display for EntityState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "EntityState({} {})", self.entity_id, self.state) }
}

impl Attested<Event> {
    pub fn collection(&self) -> &CollectionId { &self.payload.collection }
}

impl Into<Attested<Event>> for Event {
    fn into(self) -> Attested<Event> { Attested { payload: self, attestations: vec![] } }
}
