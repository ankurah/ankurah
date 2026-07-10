//! The metadata catalog (specs/model-property-metadata/rfc.md).
//!
//! ORIENTATION: one model/property/membership exists in THREE
//! representations, one per layer, and the parallel type triplets map to
//! them:
//!
//! - WIRE (a registration request): `ModelDescriptor` / `PropertyDescriptor`
//!   / `MembershipDescriptor` (ankurah-proto). Language-agnostic, id-free
//!   except explicit bindings; the durable executor allocates or resolves
//!   ids for them and returns the resolved definitions
//!   (`SchemaRegistered`).
//! - PARSED CATALOG (the replicated entities, projected into the in-memory
//!   map): `ModelDef` / `PropertyDef` / `MembershipDef` ([`catalog`]). The
//!   definitive schema; keyed by the allocated entity ids.
//! - COMPILED LOCAL (one binary's `derive(Model)` output): [`ModelSchema`] /
//!   [`FieldSchema`] ([`local`]). A static, per-binary BINDING to the
//!   catalog, never the definitive schema (RFC section 3).

pub mod catalog;
pub mod local;
pub mod registration;
pub mod resolve;

pub use local::{registration_request, FieldSchema, ModelSchema};

use crate::property::PropertyError;
use crate::value::ValueType;
use ankql::ast::PathExpr;
use ankurah_proto::{CollectionId, EntityId};

/// Trait for providing schema information about collections
pub trait CollectionSchema {
    /// Get the ValueType for a given field path
    fn field_type(&self, path: &PathExpr) -> Result<ValueType, PropertyError>;
}

/// The metadata catalog collections (specs/model-property-metadata/rfc.md
/// section 4). Catalog entities are SYSTEM MODELS: raw Entity/backend
/// access only, like SysRoot; deriving a Model for one of these is the
/// self-description ouroboros the RFC expressly forbids.
pub const MODEL_COLLECTION_ID: &str = "_ankurah_model";
pub const PROPERTY_COLLECTION_ID: &str = "_ankurah_property";
pub const MODEL_PROPERTY_COLLECTION_ID: &str = "_ankurah_model_property";

/// Collection ids reserved for the system: user models may not use this
/// prefix (enforced at derive time and at CollectionSet::get).
pub const RESERVED_COLLECTION_PREFIX: &str = "_ankurah_";

pub fn model_collection() -> CollectionId { CollectionId::fixed_name(MODEL_COLLECTION_ID) }
pub fn property_collection() -> CollectionId { CollectionId::fixed_name(PROPERTY_COLLECTION_ID) }
pub fn model_property_collection() -> CollectionId { CollectionId::fixed_name(MODEL_PROPERTY_COLLECTION_ID) }

/// Whether `id` is one of the three metadata catalog collections (NOT the
/// system collection, which replicates via the Presence handshake and has
/// its own trust story).
pub fn is_catalog_collection(id: &CollectionId) -> bool {
    let s = id.as_str();
    s == MODEL_COLLECTION_ID || s == PROPERTY_COLLECTION_ID || s == MODEL_PROPERTY_COLLECTION_ID
}

/// Well-known (reserved) model-definition entity ids for the system and
/// catalog collections (#330). The wire envelope carries a model id, not a
/// collection name, so a cold node must be able to route THESE collections'
/// events before it holds any catalog data -- the bootstrap base case moves
/// from static names to static ids. No catalog entity describes them (the
/// self-description ouroboros stays forbidden); the ids exist only for
/// routing and the protected-collections guard.
///
/// Reserved ids are 15 zero bytes + a one-byte ordinal. A minted
/// `EntityId::new()` is a ULID whose leading bytes carry the current
/// timestamp, so the all-zero prefix is unmintable and the ranges are
/// disjoint by construction.
const WELL_KNOWN_MODELS: &[(u8, &str)] =
    &[(1, crate::system::SYSTEM_COLLECTION_ID), (2, MODEL_COLLECTION_ID), (3, PROPERTY_COLLECTION_ID), (4, MODEL_PROPERTY_COLLECTION_ID)];

fn well_known_id(ordinal: u8) -> EntityId {
    let mut bytes = [0u8; 16];
    bytes[15] = ordinal;
    EntityId::from_bytes(bytes)
}

/// The reserved model id for a system/catalog collection, if `collection` is
/// one. User collections resolve their model id through the catalog instead.
pub fn well_known_model_id(collection: &str) -> Option<EntityId> {
    WELL_KNOWN_MODELS.iter().find(|(_, name)| *name == collection).map(|(ordinal, _)| well_known_id(*ordinal))
}

/// The collection a reserved model id routes to, if `id` is one. The inverse
/// of [`well_known_model_id`].
pub fn well_known_collection(id: &EntityId) -> Option<CollectionId> {
    let bytes = id.to_bytes();
    if bytes[..15] != [0u8; 15] {
        return None;
    }
    WELL_KNOWN_MODELS.iter().find(|(ordinal, _)| *ordinal == bytes[15]).map(|(_, name)| CollectionId::fixed_name(name))
}
