use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{auth::Attested, clock::Clock, AttestationSet, DecodeError, EntityId};

/// A value carried under a model usage context.
///
/// The model describes how the value is being accessed, authorized, or
/// materialized. It is deliberately outside canonical entity state and event
/// payloads, which have no intrinsic model.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ModelContext<T> {
    /// The model through which `value` is being used.
    pub model: crate::ModelId,
    /// The model-independent value.
    pub value: T,
}

impl<T> ModelContext<T> {
    /// Attach an explicit model usage context to `value`.
    pub fn new(model: crate::ModelId, value: T) -> Self { Self { model, value } }

    /// Split the usage context into its model and model-independent value.
    pub fn into_parts(self) -> (crate::ModelId, T) { (self.model, self.value) }
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct EventId([u8; 32]);

impl std::fmt::Debug for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "EventId({})", self.to_base64()) }
}

impl EventId {
    /// Generate an event identity from its complete model-independent payload.
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            write!(f, "{}", self.to_base64_short())
        } else {
            write!(f, "{}", self.to_base64())
        }
    }
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

impl Serialize for EventId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        if serializer.is_human_readable() {
            // Use base64 for human-readable formats like JSON
            serializer.serialize_str(&self.to_base64())
        } else {
            // Use raw bytes as a fixed-size array for binary formats like bincode
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for EventId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        if deserializer.is_human_readable() {
            // Deserialize from base64 string for human-readable formats
            let s = String::deserialize(deserializer)?;
            EventId::from_base64(s).map_err(serde::de::Error::custom)
        } else {
            // Deserialize from raw bytes as a fixed-size array for binary formats
            let bytes = <[u8; 32]>::deserialize(deserializer)?;
            Ok(EventId::from_bytes(bytes))
        }
    }
}

/// A canonical, model-independent entity event.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Event {
    /// The entity changed by this event.
    pub entity_id: EntityId,
    /// Backend operations carried by the event.
    pub operations: OperationSet,
    /// The set of concurrent events (usually only one) which is the precursor of this event
    pub parent: Clock,
}

impl Event {
    // TODO: figure out how we actually want to signify entity creation. This is a hack for now
    pub fn is_entity_create(&self) -> bool { self.parent.is_empty() }
}

/// Event data with the entity identity factored out for entity-scoped wire
/// envelopes.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct EventFragment {
    /// Backend operations carried by the event.
    pub operations: OperationSet,
    /// Causal parents of the event.
    pub parent: Clock,
    /// Attestations over the complete event.
    pub attestations: AttestationSet,
}

impl From<Attested<Event>> for EventFragment {
    fn from(attested: Attested<Event>) -> Self {
        Self { operations: attested.payload.operations, parent: attested.payload.parent, attestations: attested.attestations }
    }
}

impl From<(EntityId, EventFragment)> for Attested<Event> {
    fn from(value: (EntityId, EventFragment)) -> Self {
        let event = Event { entity_id: value.0, operations: value.1.operations, parent: value.1.parent };
        Attested { payload: event, attestations: value.1.attestations }
    }
}

/// Attested state with the entity identity factored out for entity-scoped wire
/// envelopes.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct StateFragment {
    /// Accumulated backend state and causal head.
    pub state: State,
    /// Attestations over the complete entity state.
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

impl Event {
    /// Derive the event identity from the complete model-independent payload.
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

/// Canonical accumulated state for one entity, independent of every model
/// through which the entity is used.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct EntityState {
    /// The durable entity identity.
    pub entity_id: EntityId,
    /// Accumulated backend state and causal head.
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
            "Event({} {} {}{} {})",
            self.id().to_base64_short(),
            self.entity_id.to_base64_short(),
            if self.is_entity_create() { "(create) " } else { "" },
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

impl From<Event> for Attested<Event> {
    fn from(val: Event) -> Self { Attested { payload: val, attestations: AttestationSet::default() } }
}

impl From<EntityState> for Attested<EntityState> {
    fn from(val: EntityState) -> Self { Attested { payload: val, attestations: AttestationSet::default() } }
}

impl Attested<EntityState> {
    /// Factor the entity identity out of an attested canonical state.
    pub fn to_parts(self) -> (EntityId, StateFragment) {
        (self.payload.entity_id, StateFragment { state: self.payload.state, attestations: self.attestations })
    }
    /// Reconstitute an attested canonical state from an entity identity and
    /// state fragment.
    pub fn from_parts(entity_id: EntityId, fragment: StateFragment) -> Self {
        Self { payload: EntityState { entity_id, state: fragment.state }, attestations: fragment.attestations }
    }
}

impl Attested<Event> {
    /// Reconstitute an attested canonical event from an entity identity and
    /// event fragment.
    pub fn from_parts(entity_id: EntityId, frag: EventFragment) -> Self {
        Self { payload: Event { entity_id, operations: frag.operations, parent: frag.parent }, attestations: frag.attestations }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_id_json_serialization() {
        let id = EventId::from_bytes([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
        ]);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA\"");
        assert_eq!(id, serde_json::from_str(&json).unwrap());
    }

    #[test]
    fn test_event_id_bincode_serialization() {
        let id = EventId::from_bytes([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
        ]);
        let bytes = bincode::serialize(&id).unwrap();
        assert_eq!(
            bytes,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
        );
        assert_eq!(id, bincode::deserialize(&bytes).unwrap());
    }

    #[test]
    fn event_id_is_derived_from_the_model_independent_event() {
        let event = Event { entity_id: EntityId::new(), operations: OperationSet(BTreeMap::new()), parent: Clock::default() };
        assert_eq!(event.id(), EventId::from_parts(&event.entity_id, &event.operations, &event.parent));
    }
}
