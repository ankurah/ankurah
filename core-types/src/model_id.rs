use serde::{Deserialize, Serialize};
use std::fmt;

use crate::EntityId;

/// A built-in model's logical identity. Variant order is part of the bincode
/// contract; append variants, never reorder them without a protocol bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SystemModel {
    /// The singleton system-configuration model.
    System,
    /// Catalog entities that define registered models.
    Model,
    /// Catalog entities that define registered properties.
    Property,
    /// Catalog entities that associate properties with models.
    ModelProperty,
}

impl fmt::Display for SystemModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::System => "system",
            Self::Model => "model",
            Self::Property => "property",
            Self::ModelProperty => "model-property",
        })
    }
}

/// The durable address of a model. Registered models use their real catalog
/// entity id; built-ins use a closed logical identity, never a magic id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ModelId {
    /// A user-registered model, identified by its catalog entity.
    EntityId(EntityId),
    /// A built-in model with a closed logical identity.
    System(SystemModel),
}

impl ModelId {
    /// Construct the model identity for a registered catalog entity.
    pub const fn entity_id(id: EntityId) -> Self { Self::EntityId(id) }
    /// Construct the identity for a built-in system model.
    pub const fn system(model: SystemModel) -> Self { Self::System(model) }

    /// Return the catalog entity identity for a registered model.
    pub const fn as_entity_id(&self) -> Option<&EntityId> {
        match self {
            Self::EntityId(id) => Some(id),
            Self::System(_) => None,
        }
    }

    /// Return the built-in identity when this is a system model.
    pub const fn system_model(&self) -> Option<SystemModel> {
        match self {
            Self::EntityId(_) => None,
            Self::System(model) => Some(*model),
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
            Self::System(model) => fmt::Display::fmt(model, f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_encoding_and_variant_order_are_pinned() {
        let id = EntityId::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
        assert_eq!(bincode::serialize(&ModelId::EntityId(id)).unwrap(), [0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);

        let variants = [SystemModel::System, SystemModel::Model, SystemModel::Property, SystemModel::ModelProperty];
        for (ordinal, variant) in variants.into_iter().enumerate() {
            assert_eq!(bincode::serialize(&variant).unwrap(), (ordinal as u32).to_le_bytes());
            assert_eq!(
                bincode::serialize(&ModelId::System(variant)).unwrap(),
                [1u32.to_le_bytes(), (ordinal as u32).to_le_bytes()].concat()
            );
        }
    }

    #[test]
    fn entity_ids_never_decode_as_system_models() {
        for bytes in [[0u8; 16], [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], [0xff; 16]] {
            let id = EntityId::from_bytes(bytes);
            let model = ModelId::EntityId(id);
            assert_eq!(bincode::deserialize::<ModelId>(&bincode::serialize(&model).unwrap()).unwrap(), model);
        }
    }
}
