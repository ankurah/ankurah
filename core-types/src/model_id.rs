use serde::{de::VariantAccess, Deserialize, Serialize};
use std::fmt;

use crate::EntityId;

/// A built-in model's logical identity. Variant order is part of the bincode
/// contract; append variants, never reorder them without a protocol bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, strum::IntoStaticStr)]
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
    /// Parse a Rust variant identifier.
    pub fn from_variant_name(name: &str) -> Option<Self> {
        Some(match name {
            "System" => Self::System,
            "Model" => Self::Model,
            "Property" => Self::Property,
            "ModelProperty" => Self::ModelProperty,
            _ => return None,
        })
    }
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
/// Human-readable serialization uses Display/FromStr; binary serialization retains the enum tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, strum::Display)]
pub enum ModelId {
    /// A user-registered model, identified by its catalog entity.
    #[strum(transparent)]
    EntityId(EntityId),
    /// A built-in model with a closed logical identity.
    #[strum(to_string = "system:{0}")]
    System(SystemModel),
}

impl Serialize for ModelId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            // These variant indices are part of the binary wire contract.
            match self {
                Self::EntityId(id) => serializer.serialize_newtype_variant("ModelId", 0, "EntityId", id),
                Self::System(model) => serializer.serialize_newtype_variant("ModelId", 1, "System", model),
            }
        }
    }
}

impl<'de> Deserialize<'de> for ModelId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        if deserializer.is_human_readable() {
            deserializer.deserialize_str(ModelIdVisitor)
        } else {
            deserializer.deserialize_enum("ModelId", &["EntityId", "System"], ModelIdVisitor)
        }
    }
}

struct ModelIdVisitor;

impl<'de> serde::de::Visitor<'de> for ModelIdVisitor {
    type Value = ModelId;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result { formatter.write_str("a model ID") }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<ModelId, E> { value.parse().map_err(E::custom) }

    fn visit_enum<A: serde::de::EnumAccess<'de>>(self, data: A) -> Result<ModelId, A::Error> {
        // Accept both numeric and named variant identifiers, as Serde's enum derive does.
        #[derive(Deserialize)]
        #[serde(variant_identifier)]
        enum Variant {
            EntityId,
            System,
        }

        let (variant, payload) = data.variant()?;
        match variant {
            Variant::EntityId => payload.newtype_variant().map(ModelId::EntityId),
            Variant::System => payload.newtype_variant().map(ModelId::System),
        }
    }
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

impl std::str::FromStr for ModelId {
    type Err = crate::DecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "system:system" => Self::System(SystemModel::System),
            "system:model" => Self::System(SystemModel::Model),
            "system:property" => Self::System(SystemModel::Property),
            "system:model-property" => Self::System(SystemModel::ModelProperty),
            _ => Self::EntityId(value.parse()?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_uses_display_strings_and_roundtrips_all_variants() {
        let id = EntityId::from_bytes([0; 32]);
        for (model, text) in [
            (ModelId::EntityId(id), "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            (ModelId::System(SystemModel::System), "system:system"),
            (ModelId::System(SystemModel::Model), "system:model"),
            (ModelId::System(SystemModel::Property), "system:property"),
            (ModelId::System(SystemModel::ModelProperty), "system:model-property"),
        ] {
            assert_eq!(model.to_string(), text);
            let json = format!("\"{text}\"");
            assert_eq!(serde_json::to_string(&model).unwrap(), json);
            assert_eq!(serde_json::from_str::<ModelId>(&json).unwrap(), model);
        }
    }

    #[test]
    fn memberships_serialize_as_an_array_of_strings() {
        let memberships =
            std::collections::BTreeSet::from([ModelId::EntityId(EntityId::from_bytes([0; 32])), ModelId::System(SystemModel::Model)]);
        let json = r#"["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","system:model"]"#;
        assert_eq!(serde_json::to_string(&memberships).unwrap(), json);
        assert_eq!(serde_json::from_str::<std::collections::BTreeSet<ModelId>>(json).unwrap(), memberships);
    }

    #[test]
    fn json_rejects_invalid_model_ids() {
        for json in [r#""system:unknown""#, r#""not-an-entity-id""#, r#""""#, "null", "42"] {
            assert!(serde_json::from_str::<ModelId>(json).is_err(), "{json}");
        }
    }

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
            assert_eq!(
                bincode::deserialize::<ModelId>(&[1u32.to_le_bytes(), (ordinal as u32).to_le_bytes()].concat()).unwrap(),
                ModelId::System(variant)
            );
        }
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
