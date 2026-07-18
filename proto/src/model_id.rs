use serde::{Deserialize, Serialize};
use std::fmt;

use crate::collection::CollectionId;
use crate::id::EntityId;

/// Canonical names of Ankurah's built-in collections. These live in proto
/// because [`WellKnownModel`] binds each reserved model id to its collection;
/// core re-exports them under its historical paths.
pub const SYSTEM_COLLECTION_ID: &str = "_ankurah_system";
pub const MODEL_COLLECTION_ID: &str = "_ankurah_model";
pub const PROPERTY_COLLECTION_ID: &str = "_ankurah_property";
pub const MODEL_PROPERTY_COLLECTION_ID: &str = "_ankurah_model_property";

/// The built-in collections addressable before any catalog data exists
/// (#330, #397). The wire envelope carries a model id, not a collection
/// name, so a cold node must be able to route these collections' events
/// before it holds any catalog data. This moves the bootstrap base case from
/// static names to static model addresses. No catalog entity describes them (the
/// self-description ouroboros stays forbidden); the set is closed, so the
/// arms carry no string and the type stays a cheap Copy map key.
///
/// Each variant's reserved id is 15 zero bytes plus its [`Self::ordinal`].
/// These are valid ULID values; the guarantee is about generation, not the
/// value space: a minted `EntityId::new()` carries the current unix-time
/// milliseconds in its leading bytes, so no honestly generated id (in
/// particular, no catalog-allocated model id) ever lands in the zero-prefix
/// range. Wire deserialization will accept a crafted zero-prefix id, but
/// that does not collide with anything: a known ordinal merely names a
/// built-in collection, whose writes the protected-collections guard
/// polices regardless of how the id was produced, and an unknown ordinal
/// stays a [`ModelId::Entity`] for ingress resolution to reject.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WellKnownModel {
    /// `_ankurah_system` (replicates via the Presence handshake and is not
    /// part of the metadata catalog).
    System,
    /// `_ankurah_model`
    Model,
    /// `_ankurah_property`
    Property,
    /// `_ankurah_model_property`
    ModelProperty,
}

impl WellKnownModel {
    pub const ALL: [WellKnownModel; 4] = [Self::System, Self::Model, Self::Property, Self::ModelProperty];

    /// The final byte of this model's reserved id.
    pub const fn ordinal(self) -> u8 {
        match self {
            Self::System => 1,
            Self::Model => 2,
            Self::Property => 3,
            Self::ModelProperty => 4,
        }
    }

    pub const fn from_ordinal(ordinal: u8) -> Option<Self> {
        match ordinal {
            1 => Some(Self::System),
            2 => Some(Self::Model),
            3 => Some(Self::Property),
            4 => Some(Self::ModelProperty),
            _ => None,
        }
    }

    pub const fn collection_str(self) -> &'static str {
        match self {
            Self::System => SYSTEM_COLLECTION_ID,
            Self::Model => MODEL_COLLECTION_ID,
            Self::Property => PROPERTY_COLLECTION_ID,
            Self::ModelProperty => MODEL_PROPERTY_COLLECTION_ID,
        }
    }

    /// The collection this reserved model id routes to.
    pub fn collection_id(self) -> CollectionId { CollectionId::fixed_name(self.collection_str()) }

    /// The built-in model bound to `name`, if `name` is a built-in
    /// collection. User collections resolve their model id through the
    /// catalog instead.
    pub fn from_collection(name: &str) -> Option<Self> { Self::ALL.into_iter().find(|model| model.collection_str() == name) }

    /// Whether this is one of the three metadata catalog collections (not
    /// the system collection, which has its own replication and trust
    /// story). Catalog definition states are the ones that ride the wire
    /// `schema` envelope field.
    pub const fn is_catalog(self) -> bool { !matches!(self, Self::System) }

    /// This model's reserved id: 15 zero bytes plus the ordinal. An address
    /// in id space with no entity behind it; see the type-level invariants.
    pub fn to_entity_id(self) -> EntityId {
        let mut bytes = [0u8; 16];
        bytes[15] = self.ordinal();
        EntityId::from_bytes(bytes)
    }
}

/// The model address carried on wire envelopes (#397): either one of the
/// built-in collections' reserved ids or an allocated model-definition
/// entity id. The sum makes the reserved carve-out visible to the type
/// system. Code that used to prefix-check a bare `EntityId` against the
/// reserved range matches on the arm instead.
///
/// Wire encoding is byte-identical to the bare `EntityId` this field
/// carried before the enum existed (both arms collapse to the 16-byte id),
/// so introducing it required no protocol version bump and old stored rows
/// decode unchanged. The classifying [`From<EntityId>`] is the canonical
/// constructor; building `ModelId::Entity` directly around a reserved id
/// creates a non-canonical value that a serde roundtrip normalizes to
/// `WellKnown`.
///
/// The derived `Ord` orders `WellKnown` before `Entity` by variant. That
/// matches the byte order of every honestly generated id (minted ids never
/// have the zero prefix) and is not a wire concern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ModelId {
    WellKnown(WellKnownModel),
    Entity(EntityId),
}

impl ModelId {
    /// The 16-byte wire form: the reserved id for a well-known, the raw id
    /// for an entity. The inverse of the classifying `From<EntityId>`.
    pub fn to_entity_id(&self) -> EntityId {
        match self {
            Self::WellKnown(model) => model.to_entity_id(),
            Self::Entity(id) => *id,
        }
    }

    /// The allocated catalog entity id, or `None` for a built-in model.
    pub const fn entity(self) -> Option<EntityId> {
        match self {
            Self::Entity(id) => Some(id),
            Self::WellKnown(_) => None,
        }
    }

    /// The built-in model, or `None` for a catalog-allocated model.
    pub const fn well_known(self) -> Option<WellKnownModel> {
        match self {
            Self::WellKnown(model) => Some(model),
            Self::Entity(_) => None,
        }
    }

    pub fn to_base64(&self) -> String { self.to_entity_id().to_base64() }

    pub fn to_base64_short(&self) -> String { self.to_entity_id().to_base64_short() }
}

impl From<WellKnownModel> for ModelId {
    fn from(model: WellKnownModel) -> Self { ModelId::WellKnown(model) }
}

impl From<EntityId> for ModelId {
    /// Classify an id into the arm its bytes name: the zero-prefix reserved
    /// range with a known ordinal is `WellKnown`, everything else `Entity`.
    fn from(id: EntityId) -> Self {
        let bytes = id.to_bytes();
        if bytes[..15] == [0u8; 15] {
            if let Some(model) = WellKnownModel::from_ordinal(bytes[15]) {
                return ModelId::WellKnown(model);
            }
        }
        ModelId::Entity(id)
    }
}

/// Mirrors `EntityId`'s Display: base64 of the 16-byte form, short form
/// under `{:#}`.
impl fmt::Display for ModelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "{}", self.to_base64_short())
        } else {
            write!(f, "{}", self.to_base64())
        }
    }
}

impl Serialize for ModelId {
    /// Byte-identical to the bare `EntityId` encoding this field carried
    /// before the enum existed: both arms serialize as the 16-byte id
    /// (base64 string in human-readable formats). Delegating to `EntityId`'s
    /// own two-format serde makes the equivalence structural rather than
    /// maintained by hand.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        self.to_entity_id().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ModelId {
    /// Classify, never reject: a zero-prefix id whose ordinal names a
    /// built-in becomes `WellKnown`; everything else, including a
    /// zero-prefix id with an unknown ordinal, becomes `Entity`. An
    /// unknown ordinal is deliberately not a decode error: rejection
    /// belongs to ingress resolution (`Node::resolve_model`), which is loud
    /// but retryable; a decode error here would poison the whole envelope
    /// and lose that semantics.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        Ok(EntityId::deserialize(deserializer)?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reserved_entity_id(ordinal: u8) -> EntityId {
        let mut bytes = [0u8; 16];
        bytes[15] = ordinal;
        EntityId::from_bytes(bytes)
    }

    /// The compatibility proof for the no-version-bump claim: each arm's
    /// bincode encoding equals the bare EntityId encoding of the same 16
    /// bytes, so a pre-enum peer and a post-enum peer read each other's
    /// envelopes (and old stored rows decode) unchanged.
    #[test]
    fn well_known_bincode_bytes_equal_bare_entity_id() {
        for model in WellKnownModel::ALL {
            let bare = reserved_entity_id(model.ordinal());
            assert_eq!(
                bincode::serialize(&ModelId::WellKnown(model)).unwrap(),
                bincode::serialize(&bare).unwrap(),
                "{model:?} must encode as its reserved EntityId bytes"
            );
        }
    }

    #[test]
    fn entity_bincode_bytes_equal_bare_entity_id() {
        let id = EntityId::new();
        assert_eq!(bincode::serialize(&ModelId::Entity(id)).unwrap(), bincode::serialize(&id).unwrap());
    }

    /// The human-readable arm of the same proof: JSON output equals the
    /// bare EntityId's base64 string form.
    #[test]
    fn json_form_equals_bare_entity_id() {
        for model in WellKnownModel::ALL {
            let bare = reserved_entity_id(model.ordinal());
            assert_eq!(
                serde_json::to_string(&ModelId::WellKnown(model)).unwrap(),
                serde_json::to_string(&bare).unwrap(),
                "{model:?} must serialize to the reserved id's base64 form"
            );
        }
        let id = EntityId::new();
        assert_eq!(serde_json::to_string(&ModelId::Entity(id)).unwrap(), serde_json::to_string(&id).unwrap());
    }

    #[test]
    fn bincode_roundtrip_both_arms() {
        for model in WellKnownModel::ALL {
            let original = ModelId::WellKnown(model);
            let decoded: ModelId = bincode::deserialize(&bincode::serialize(&original).unwrap()).unwrap();
            assert_eq!(original, decoded);
        }
        let original = ModelId::Entity(EntityId::new());
        let decoded: ModelId = bincode::deserialize(&bincode::serialize(&original).unwrap()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn json_roundtrip_both_arms() {
        for model in WellKnownModel::ALL {
            let original = ModelId::WellKnown(model);
            let decoded: ModelId = serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
            assert_eq!(original, decoded);
        }
        let original = ModelId::Entity(EntityId::new());
        let decoded: ModelId = serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
        assert_eq!(original, decoded);
    }

    /// A zero-prefix id with an ordinal outside the built-in set is neither a
    /// decode error nor a well-known model. It classifies as `Entity`, so it
    /// still reaches ingress resolution (`Node::resolve_model`) where an
    /// unknown model is rejected loudly but remains retryable.
    #[test]
    fn unknown_ordinal_classifies_as_entity() {
        let crafted = reserved_entity_id(5);
        let decoded: ModelId = bincode::deserialize(&bincode::serialize(&crafted).unwrap()).unwrap();
        assert_eq!(decoded, ModelId::Entity(crafted));
        // The all-zero id (ordinal 0) is likewise no built-in.
        let zero = EntityId::from_bytes([0u8; 16]);
        assert_eq!(ModelId::from(zero), ModelId::Entity(zero));
    }

    /// Every reserved ordinal classifies to its variant, from both the
    /// classifying From and a wire decode.
    #[test]
    fn reserved_ordinals_classify_as_well_known() {
        for model in WellKnownModel::ALL {
            let bare = reserved_entity_id(model.ordinal());
            assert_eq!(ModelId::from(bare), ModelId::WellKnown(model));
            let decoded: ModelId = bincode::deserialize(&bincode::serialize(&bare).unwrap()).unwrap();
            assert_eq!(decoded, ModelId::WellKnown(model));
        }
    }

    #[test]
    fn collection_binding_roundtrips() {
        for model in WellKnownModel::ALL {
            assert_eq!(WellKnownModel::from_collection(model.collection_str()), Some(model));
            assert_eq!(model.collection_id().as_str(), model.collection_str());
        }
        assert_eq!(WellKnownModel::from_collection("albums"), None);
    }
}
