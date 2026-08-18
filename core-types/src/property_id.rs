use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::{DecodeError, EntityId};

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

    /// The Rust variant identifier. A system model's `#[derive(Model)]`
    /// fields pin their identities at compile time, so the macro re-emits
    /// this spelling as a path; keeping it beside the enum means the macro
    /// reads the vocabulary rather than restating it.
    pub const fn variant_name(self) -> &'static str {
        match self {
            Self::Item => "Item",
            Self::Label => "Label",
            Self::Name => "Name",
            Self::MintedFor => "MintedFor",
            Self::Backend => "Backend",
            Self::ValueType => "ValueType",
            Self::TargetModel => "TargetModel",
            Self::Model => "Model",
            Self::Property => "Property",
            Self::Optional => "Optional",
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

/// The exact inverse of [`Display`](fmt::Display): every variant parses back
/// to itself. The arms cannot collide -- `"id"` is not a system name, and no
/// system name decodes to the 32 bytes an entity id requires (the longest,
/// `target_model`, is 12 characters and decodes to 9).
impl FromStr for PropertyId {
    type Err = DecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input == "id" {
            return Ok(Self::Id);
        }
        if let Some(property) = SystemProperty::from_name(input) {
            return Ok(Self::System(property));
        }
        EntityId::from_base64(input).map(Self::EntityId).map_err(|_| DecodeError::InvalidFormat)
    }
}

impl From<EntityId> for PropertyId {
    fn from(id: EntityId) -> Self { Self::EntityId(id) }
}

/// Lets a `PropertyId` be handed straight to APIs keyed by `Arc<str>`, such as
/// the Yjs root accessors.
impl From<PropertyId> for std::sync::Arc<str> {
    fn from(id: PropertyId) -> Self { id.to_string().into() }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `SystemProperty`, in declaration order.
    const ALL_SYSTEM_PROPERTIES: [SystemProperty; 10] = [
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

    #[test]
    fn display_round_trips_every_variant() {
        let mut cases = vec![PropertyId::Id, PropertyId::EntityId(EntityId::from_bytes([9; 32]))];
        cases.extend(ALL_SYSTEM_PROPERTIES.map(PropertyId::System));

        for property in cases {
            let key = property.to_string();
            let parsed = key.parse::<PropertyId>().unwrap_or_else(|e| panic!("{key:?} must parse back to the id that wrote it: {e}"));
            assert_eq!(parsed, property, "{key:?} round-tripped to the wrong identity");
        }

        assert_eq!(PropertyId::Id.to_string(), "id");
        assert_eq!(PropertyId::System(SystemProperty::TargetModel).to_string(), "target_model");
        let entity = EntityId::from_bytes([9; 32]);
        assert_eq!(PropertyId::EntityId(entity).to_string(), entity.to_base64());
    }

    #[test]
    fn unparseable_string_is_refused() {
        for input in [
            "",
            "title",                                        // a plausible display name, but no system property
            "Name",                                         // system names are lowercase and exact
            "id ",                                          // no trimming
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",   // valid base64, decodes to 31 bytes, not 32
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", // valid base64, decodes to 33 bytes
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",  // an entity id's width, but not base64
        ] {
            assert!(input.parse::<PropertyId>().is_err(), "{input:?} must not parse as a PropertyId");
        }
    }

    #[test]
    fn variant_order_and_entity_payload_are_pinned() {
        assert_eq!(bincode::serialize(&PropertyId::Id).unwrap(), 0u32.to_le_bytes());
        let entity = EntityId::from_bytes([7; 32]);
        assert_eq!(
            bincode::serialize(&PropertyId::EntityId(entity)).unwrap(),
            [1u32.to_le_bytes().as_slice(), entity.to_bytes().as_slice()].concat()
        );

        for (ordinal, variant) in ALL_SYSTEM_PROPERTIES.into_iter().enumerate() {
            assert_eq!(bincode::serialize(&variant).unwrap(), (ordinal as u32).to_le_bytes());
            assert_eq!(
                bincode::serialize(&PropertyId::System(variant)).unwrap(),
                [2u32.to_le_bytes(), (ordinal as u32).to_le_bytes()].concat()
            );
        }
    }
}
