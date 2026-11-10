use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{author::AuthorId, clock::Clock, id::EntityId, operation::OperationSet, DecodeError};

/// Domain tag for genesis event ids. Separates the two preimage shapes so no
/// genesis preimage can ever equal an update preimage, and versions the
/// derivation scheme.
pub const GENESIS_TAG: &[u8] = b"org.ankurah.genesis.v0";

/// Domain tag for update event ids. See [`GENESIS_TAG`].
pub const EVENT_TAG: &[u8] = b"org.ankurah.event.v0";

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct EventId([u8; 32]);

impl std::fmt::Debug for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "EventId({})", self.to_base64()) }
}

impl EventId {
    /// The id of a genesis event: `SHA-256(GENESIS_TAG || bincode(system,
    /// nonce, timestamp, author, operations))`. Membership is carried in the
    /// operations and therefore contributes to the entity's identity.
    pub fn from_genesis_parts(
        system: &Option<EntityId>,
        nonce: &[u8; 32],
        timestamp: u64,
        author: &AuthorId,
        operations: &OperationSet,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(GENESIS_TAG);
        hasher.update(bincode::serialize(system).unwrap());
        hasher.update(bincode::serialize(nonce).unwrap());
        hasher.update(bincode::serialize(&timestamp).unwrap());
        hasher.update(bincode::serialize(author).unwrap());
        hasher.update(bincode::serialize(operations).unwrap());
        Self(hasher.finalize().into())
    }

    /// The id of an update event: `SHA-256(EVENT_TAG || bincode(entity_id,
    /// author, nonce, timestamp, operations, parent))`. Model projections stay
    /// outside the canonical event and its identity. The nonce is what makes an
    /// event id unguessable to anyone who was not sent the event.
    pub fn from_update_parts(
        entity_id: &EntityId,
        author: &AuthorId,
        nonce: &[u8; 32],
        timestamp: u64,
        operations: &OperationSet,
        parent: &Clock,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(EVENT_TAG);
        hasher.update(bincode::serialize(entity_id).unwrap());
        hasher.update(bincode::serialize(author).unwrap());
        hasher.update(bincode::serialize(nonce).unwrap());
        hasher.update(bincode::serialize(&timestamp).unwrap());
        hasher.update(bincode::serialize(operations).unwrap());
        hasher.update(bincode::serialize(parent).unwrap());
        Self(hasher.finalize().into())
    }
    pub fn to_base64(&self) -> String {
        use base64::{engine::general_purpose, Engine as _};
        general_purpose::URL_SAFE_NO_PAD.encode(self.0)
    }
    /// The first six characters of the base64 form. For compact diagnostics,
    /// never durable identity.
    pub fn to_base64_short(&self) -> String {
        let value = self.to_base64();
        value[..6].to_string()
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

/// An entity id IS the id of its genesis event. This conversion is the whole
/// of that equality; nothing else turns one into the other.
impl From<EventId> for EntityId {
    fn from(id: EventId) -> Self { EntityId::from_bytes(id.0) }
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
}
