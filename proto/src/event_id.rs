use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{clock::Clock, domain_tag::DomainTag, id::EntityId, identity::Identity, operation::CarriedOperations, DecodeError};

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct EventId([u8; 32]);

impl std::fmt::Debug for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "EventId({})", self.to_base64()) }
}

impl EventId {
    /// The id of a genesis event: SHA-256 over [`DomainTag::Genesis`], then the
    /// minter, nonce and timestamp, the operation count and each operation's
    /// digest. The minter and the memberships carried in the operations are
    /// part of the entity's identity.
    pub fn from_genesis_parts(minter: &Identity, nonce: &[u8; 32], timestamp: u64, operations: &CarriedOperations) -> Self {
        Self::from_preimage(DomainTag::Genesis, &(minter, nonce, timestamp, operations.digests()))
    }

    /// The id of an update event: SHA-256 over [`DomainTag::Event`], then the
    /// entity id, nonce and timestamp, the operation count and each operation's
    /// digest, and the parent clock. Model projections stay outside the
    /// canonical event and its identity. The nonce is what makes an event id
    /// unguessable to anyone who was not sent the event.
    pub fn from_update_parts(
        entity_id: &EntityId,
        nonce: &[u8; 32],
        timestamp: u64,
        operations: &CarriedOperations,
        parent: &Clock,
    ) -> Self {
        Self::from_preimage(DomainTag::Event, &(entity_id, nonce, timestamp, operations.digests(), parent))
    }

    /// SHA-256 over `tag`, then the bincode of `fields`.
    fn from_preimage(tag: DomainTag, fields: &impl Serialize) -> Self {
        let mut preimage = tag.encoded();
        // Cannot fail: bincode encodes every id field, and writing to a vector cannot fail.
        bincode::serialize_into(&mut preimage, fields).expect("event id fields encode");
        Self(Sha256::digest(&preimage).into())
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
    use crate::operation::{BackendOperation, CarriedOperation, Membership, Operation};
    use ankurah_core_types::{ModelId, SystemModel};

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

    /// The id vectors of specs/protocol/bytes.md section 5: a genesis and an update over zero, one and two
    /// operations, and over the one operation erased, which leaves each id unchanged.
    #[test]
    fn event_ids_match_the_byte_specification_vectors() {
        let membership = CarriedOperation::encode(&Operation::Membership(Membership::Add(ModelId::System(SystemModel::Model))));
        let backend = CarriedOperation::encode(&Operation::Backend {
            backend: "lww".to_owned(),
            operations: vec![BackendOperation { diff: vec![1, 2, 3] }],
        });
        let erased = CarriedOperation::Erased(membership.digest());
        let minter = Identity::Ed25519([0x11; 32]);
        let (nonce, timestamp) = ([0x22; 32], 1_720_000_000_000);
        let entity_id = EntityId::from_bytes([0x33; 32]);
        let parent = Clock::new(vec![(1, EventId::from_bytes([0x44; 32]))]).unwrap();

        for (operations, genesis, update) in [
            (
                vec![],
                "133fdae5349217e57d706cfb395b259cc94e718562a71779dfd1f0270d8bb649",
                "ea0c549736a30f74f83fc27165938ccd17ac57c5b955fe4a1347f74b32372aa8",
            ),
            (
                vec![membership.clone()],
                "9b8f62522e532428eae37ef651a7a0f495fdbf2b1c5d153a1ee79660fbbd358a",
                "e865a554005226a6dc65c5a8dc039f7d40cd2e658833e741bb6d3a149c0dfe7c",
            ),
            (
                vec![membership, backend],
                "0395ecaf7e93f57b19cf2098a8161c6c0c72b182ff1f5dfc8a0525359cd927a3",
                "76c6835c83eb2aba276df5ad184fca20075a513a9140db37cc00e43e6cb63623",
            ),
            (
                vec![erased],
                "9b8f62522e532428eae37ef651a7a0f495fdbf2b1c5d153a1ee79660fbbd358a",
                "e865a554005226a6dc65c5a8dc039f7d40cd2e658833e741bb6d3a149c0dfe7c",
            ),
        ] {
            let operations = CarriedOperations(operations);
            assert_eq!(hex::encode(EventId::from_genesis_parts(&minter, &nonce, timestamp, &operations).as_bytes()), genesis);
            assert_eq!(hex::encode(EventId::from_update_parts(&entity_id, &nonce, timestamp, &operations, &parent).as_bytes()), update);
        }
    }
}
