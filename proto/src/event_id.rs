use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{clock::Clock, domain_tag::DomainTag, encoding, id::EntityId, operation::CarriedOperations, DecodeError};

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct EventId([u8; 32]);

impl std::fmt::Debug for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "EventId({})", self.to_base64()) }
}

impl EventId {
    /// The id of a genesis event (specs/protocol/bytes.md section 4.2). The memberships its
    /// operations carry are part of the entity's identity.
    pub fn from_genesis_parts(nonce: &[u8; 32], timestamp: u64, operations: &CarriedOperations) -> Self {
        Self::from_preimage(DomainTag::Genesis, &(nonce, timestamp, operations.digests()))
    }

    /// The id of an update event (specs/protocol/bytes.md section 4.3). Its nonce makes the id
    /// unguessable to anyone who was not sent the event.
    pub fn from_update_parts(
        entity_id: &EntityId,
        nonce: &[u8; 32],
        timestamp: u64,
        operations: &CarriedOperations,
        parent: &Clock,
    ) -> Self {
        Self::from_preimage(DomainTag::Update, &(entity_id, nonce, timestamp, operations.digests(), parent))
    }

    /// SHA-256 over `tag`, then the encoding of `fields`.
    fn from_preimage(tag: DomainTag, fields: &impl Serialize) -> Self {
        let mut preimage = tag.encoded().to_vec();
        encoding::encode_into(&mut preimage, fields);
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
    use crate::encoding::{decode_exact, encode};
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
        let bytes = encode(&id);
        assert_eq!(
            bytes,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
        );
        assert_eq!(id, decode_exact(&bytes).unwrap());
    }

    /// The id vectors of specs/protocol/bytes.md section 5: a genesis and an update over zero, one and two
    /// operations, each carried under the salt 00 to 0f, and over the one operation erased, which leaves
    /// each id unchanged.
    #[test]
    fn event_ids_match_the_byte_specification_vectors() {
        let salt = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let membership =
            CarriedOperation::encode_with_salt(&Operation::Membership(Membership::Add(ModelId::System(SystemModel::Model))), salt);
        let backend = CarriedOperation::encode_with_salt(
            &Operation::Backend { backend: "lww".to_owned(), operations: vec![BackendOperation { diff: vec![1, 2, 3] }] },
            salt,
        );
        let erased = CarriedOperation::Erased(membership.digest());
        let (nonce, timestamp) = ([0x22; 32], 1_720_000_000_000);
        let entity_id = EntityId::from_bytes([0x33; 32]);
        let parent = Clock::new(vec![(1, EventId::from_bytes([0x44; 32]))]).unwrap();

        for (operations, genesis, update) in [
            (
                vec![],
                "7142250172e85b3d6ea61e537ad3980c29e3f2ac3854f8c0f5d788124e3141ac",
                "036ff81f4ba2285f3ccd38e0ad3232855a466bb6c0b9d6f0fd254a202180364e",
            ),
            (
                vec![membership.clone()],
                "847e97d2ea2977d148a604d1b5f309be86e9209a0a9ef386ea3b80eb37c7a5ca",
                "760a5569fdfd623e4c1bdf40482c77f57e747f00a38a4389c067916d94886e1f",
            ),
            (
                vec![membership, backend],
                "333012998d0558ffb741500c36420083fcf3a41b4a0f51c1c8629cd554bcc640",
                "f6068afad6389a9d0c6ee253f24965dd9f9a481fa0648a1a08007c171d24ac72",
            ),
            (
                vec![erased],
                "847e97d2ea2977d148a604d1b5f309be86e9209a0a9ef386ea3b80eb37c7a5ca",
                "760a5569fdfd623e4c1bdf40482c77f57e747f00a38a4389c067916d94886e1f",
            ),
        ] {
            let operations = CarriedOperations(operations);
            assert_eq!(hex::encode(EventId::from_genesis_parts(&nonce, timestamp, &operations).as_bytes()), genesis);
            assert_eq!(hex::encode(EventId::from_update_parts(&entity_id, &nonce, timestamp, &operations, &parent).as_bytes()), update);
        }
    }
}
