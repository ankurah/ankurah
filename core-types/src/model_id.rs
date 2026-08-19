use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::{DecodeError, EntityId};

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

impl SystemModel {
    /// The Rust variant identifier. `#[derive(Model)]` writes a built-in
    /// identity as `#[model(system = "ModelProperty")]`, so the derive both
    /// parses and re-emits this spelling; keeping it beside the enum means
    /// the macro reads the vocabulary rather than restating it.
    pub const fn variant_name(self) -> &'static str {
        match self {
            Self::System => "System",
            Self::Model => "Model",
            Self::Property => "Property",
            Self::ModelProperty => "ModelProperty",
        }
    }

    /// Parse a variant identifier, the form [`Self::variant_name`] renders.
    pub fn from_variant_name(name: &str) -> Option<Self> {
        Some(match name {
            "System" => Self::System,
            "Model" => Self::Model,
            "Property" => Self::Property,
            "ModelProperty" => Self::ModelProperty,
            _ => return None,
        })
    }

    /// The canonical rendering of a built-in identity, the form
    /// [`Display`](fmt::Display) writes and [`Self::from_rendering`] reads.
    /// Storage engines name a built-in's physical table with it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Model => "model",
            Self::Property => "property",
            Self::ModelProperty => "model-property",
        }
    }

    /// Parse a canonical rendering, the form [`Self::as_str`] writes.
    pub fn from_rendering(rendering: &str) -> Option<Self> {
        Some(match rendering {
            "system" => Self::System,
            "model" => Self::Model,
            "property" => Self::Property,
            "model-property" => Self::ModelProperty,
            _ => return None,
        })
    }
}

impl fmt::Display for SystemModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.as_str()) }
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

/// The exact inverse of [`Display`](fmt::Display): every variant parses back
/// to itself, which is what lets a storage engine read an identity off a
/// physical name it wrote. The arms cannot collide -- no built-in rendering
/// decodes to the 32 bytes an entity id requires (the longest,
/// `model-property`, is 14 characters and decodes to 10).
impl FromStr for ModelId {
    type Err = DecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if let Some(model) = SystemModel::from_rendering(input) {
            return Ok(Self::System(model));
        }
        EntityId::from_base64(input).map(Self::EntityId).map_err(|_| DecodeError::InvalidFormat)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_encoding_and_variant_order_are_pinned() {
        let id = EntityId::from_bytes([
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        ]);
        assert_eq!(bincode::serialize(&ModelId::EntityId(id)).unwrap(), [0u32.to_le_bytes().to_vec(), id.to_bytes().to_vec()].concat());

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
    fn display_round_trips_every_variant() {
        let entity = EntityId::from_bytes([9; 32]);
        let mut cases = vec![ModelId::EntityId(entity)];
        cases.extend([SystemModel::System, SystemModel::Model, SystemModel::Property, SystemModel::ModelProperty].map(ModelId::System));

        for model in cases {
            let rendering = model.to_string();
            let parsed =
                rendering.parse::<ModelId>().unwrap_or_else(|e| panic!("{rendering:?} must parse back to the id that wrote it: {e}"));
            assert_eq!(parsed, model, "{rendering:?} round-tripped to the wrong identity");
        }

        assert_eq!(ModelId::System(SystemModel::ModelProperty).to_string(), "model-property");
        assert_eq!(ModelId::EntityId(entity).to_string(), entity.to_base64());
        assert!("albums".parse::<ModelId>().is_err(), "a source-level label is not a rendering");
    }

    #[test]
    fn entity_ids_never_decode_as_system_models() {
        let mut low_bit_set = [0u8; 32];
        low_bit_set[31] = 1;
        for bytes in [[0u8; 32], low_bit_set, [0xff; 32]] {
            let id = EntityId::from_bytes(bytes);
            let model = ModelId::EntityId(id);
            assert_eq!(bincode::deserialize::<ModelId>(&bincode::serialize(&model).unwrap()).unwrap(), model);
        }
    }
}
