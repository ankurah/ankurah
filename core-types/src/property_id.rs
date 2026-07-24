use serde::{Deserialize, Serialize};
use std::fmt;

use crate::EntityId;

/// A built-in property's logical identity. Variant order is part of the
/// bincode contract; append variants, never reorder them without a protocol
/// bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SystemProperty {
    /// The system configuration item key.
    Item,
    /// The registered model's source-level label.
    Label,
    /// A model or property's registered display name.
    Name,
    /// The model in whose scope a property identity was minted.
    MintedFor,
    /// A property's state backend identifier.
    Backend,
    /// A property's registered logical value type.
    ValueType,
    /// The target model of an entity-reference property.
    TargetModel,
    /// The model side of a model-property membership.
    Model,
    /// The property side of a model-property membership.
    Property,
    /// Whether a property is optional in a model contract.
    Optional,
}

impl SystemProperty {
    /// Return the canonical source-level spelling of this property.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Item => "item",
            Self::Label => "label",
            Self::Name => "name",
            Self::MintedFor => "minted_for",
            Self::Backend => "backend",
            Self::ValueType => "value_type",
            Self::TargetModel => "target_model",
            Self::Model => "model",
            Self::Property => "property",
            Self::Optional => "optional",
        }
    }

    /// Parse a canonical system-property name.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "item" => Self::Item,
            "label" => Self::Label,
            "name" => Self::Name,
            "minted_for" => Self::MintedFor,
            "backend" => Self::Backend,
            "value_type" => Self::ValueType,
            "target_model" => Self::TargetModel,
            "model" => Self::Model,
            "property" => Self::Property,
            "optional" => Self::Optional,
            _ => return None,
        })
    }
}

impl fmt::Display for SystemProperty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.as_str()) }
}

/// The durable address of a property. Storage engines key their private
/// physical-address registries on this identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PropertyId {
    /// Every entity's primary-key pseudo-property.
    Id,
    /// A registered property's real catalog entity id.
    EntityId(EntityId),
    /// A closed built-in property identity.
    System(SystemProperty),
}

impl fmt::Display for PropertyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Id => f.write_str("id"),
            Self::EntityId(id) => fmt::Display::fmt(id, f),
            Self::System(property) => fmt::Display::fmt(property, f),
        }
    }
}

impl From<EntityId> for PropertyId {
    fn from(id: EntityId) -> Self { Self::EntityId(id) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_order_and_entity_payload_are_pinned() {
        assert_eq!(bincode::serialize(&PropertyId::Id).unwrap(), 0u32.to_le_bytes());
        let entity = EntityId::from_bytes([7; 16]);
        assert_eq!(
            bincode::serialize(&PropertyId::EntityId(entity)).unwrap(),
            [1u32.to_le_bytes().as_slice(), entity.to_bytes().as_slice()].concat()
        );

        let variants = [
            SystemProperty::Item,
            SystemProperty::Label,
            SystemProperty::Name,
            SystemProperty::MintedFor,
            SystemProperty::Backend,
            SystemProperty::ValueType,
            SystemProperty::TargetModel,
            SystemProperty::Model,
            SystemProperty::Property,
            SystemProperty::Optional,
        ];
        for (ordinal, variant) in variants.into_iter().enumerate() {
            assert_eq!(bincode::serialize(&variant).unwrap(), (ordinal as u32).to_le_bytes());
            assert_eq!(
                bincode::serialize(&PropertyId::System(variant)).unwrap(),
                [2u32.to_le_bytes(), (ordinal as u32).to_le_bytes()].concat()
            );
        }
    }
}
