//! The metadata catalog (specs/model-property-metadata/rfc.md).
//!
//! ORIENTATION: one model/property/membership exists in THREE
//! representations, one per layer, and the parallel type triplets map to
//! them:
//!
//! - WIRE (a registration request): `ModelDescriptor` / `PropertyDescriptor`
//!   / `MembershipDescriptor` (ankurah-proto). Language-agnostic, id-free
//!   except explicit bindings; the durable executor derives ids from them.
//! - PARSED CATALOG (the replicated entities, projected into the in-memory
//!   map): `ModelDef` / `PropertyDef` / `MembershipDef` ([`catalog`]). The
//!   definitive schema; keyed by derived entity ids.
//! - COMPILED LOCAL (one binary's `derive(Model)` output): [`ModelSchema`] /
//!   [`FieldSchema`] ([`local`]). A static, per-binary BINDING to the
//!   catalog, never the definitive schema (RFC section 3).

pub mod catalog;
pub mod genesis;
pub mod local;
pub mod registration;
pub mod resolve;

pub use local::{registration_request, FieldSchema, ModelSchema};

use crate::property::PropertyError;
use crate::value::ValueType;
use ankql::ast::PathExpr;
use ankurah_proto::CollectionId;

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
