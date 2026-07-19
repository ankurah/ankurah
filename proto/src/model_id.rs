use serde::{Deserialize, Serialize};
use std::fmt;

use crate::id::EntityId;

/// The durable address of a model carried by entity-data envelopes.
///
/// Registered models are addressed by the real entity id of their catalog
/// definition. Built-in models have no catalog definition (describing the
/// catalog through itself would create a bootstrap cycle), so their durable
/// identity is their collection name. The two cases are encoded as distinct
/// enum arms; a system model is never represented by an invented `EntityId`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ModelId {
    /// A registered model's catalog entity id.
    EntityId(EntityId),
    /// A built-in model's collection name.
    System { name: String },
}

impl ModelId {
    /// Construct the address of a registered model.
    pub const fn entity_id(id: EntityId) -> Self { Self::EntityId(id) }

    /// Construct a named system-model address.
    ///
    /// This constructor does not decide which names are trusted built-ins;
    /// ingress validates that policy in core, where the built-in collection
    /// vocabulary lives.
    pub fn system(name: impl Into<String>) -> Self { Self::System { name: name.into() } }

    /// Return the catalog entity id for a registered model.
    pub const fn as_entity_id(&self) -> Option<&EntityId> {
        match self {
            Self::EntityId(id) => Some(id),
            Self::System { .. } => None,
        }
    }

    /// Return the collection name carried by a system-model address.
    pub fn system_name(&self) -> Option<&str> {
        match self {
            Self::EntityId(_) => None,
            Self::System { name } => Some(name),
        }
    }
}

impl From<EntityId> for ModelId {
    fn from(id: EntityId) -> Self { Self::EntityId(id) }
}

impl fmt::Display for ModelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EntityId(id) => fmt::Display::fmt(id, f),
            Self::System { name } => f.write_str(name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_encoding_distinguishes_system_names_from_entity_ids() {
        let id = EntityId::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
        let allocated = ModelId::EntityId(id);
        let system = ModelId::system("_ankurah_system");

        let allocated_bytes = bincode::serialize(&allocated).unwrap();
        let system_bytes = bincode::serialize(&system).unwrap();
        assert_eq!(allocated_bytes, [0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
        assert_eq!(
            system_bytes,
            [
                1, 0, 0, 0, // System variant
                15, 0, 0, 0, 0, 0, 0, 0, // string length
                b'_', b'a', b'n', b'k', b'u', b'r', b'a', b'h', b'_', b's', b'y', b's', b't', b'e', b'm',
            ]
        );
        assert_eq!(bincode::deserialize::<ModelId>(&allocated_bytes).unwrap(), allocated);
        assert_eq!(bincode::deserialize::<ModelId>(&system_bytes).unwrap(), system);

        assert_eq!(serde_json::to_string(&allocated).unwrap(), format!(r#"{{"EntityId":"{}"}}"#, id.to_base64()));
        assert_eq!(serde_json::to_string(&system).unwrap(), r#"{"System":{"name":"_ankurah_system"}}"#);
    }

    #[test]
    fn every_entity_id_pattern_stays_an_entity_id() {
        for bytes in [[0u8; 16], [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], [0xff; 16]] {
            let id = EntityId::from_bytes(bytes);
            let model = ModelId::from(id);
            assert_eq!(model, ModelId::EntityId(id));
            assert_eq!(bincode::deserialize::<ModelId>(&bincode::serialize(&model).unwrap()).unwrap(), model);
            assert_eq!(serde_json::from_str::<ModelId>(&serde_json::to_string(&model).unwrap()).unwrap(), model);
        }
    }

    #[test]
    fn bare_entity_bytes_are_not_a_model_id_codec() {
        let mut bare_id = [0u8; 16];
        bare_id[15] = 1;
        assert!(bincode::deserialize::<ModelId>(&bare_id).is_err());
    }
}
