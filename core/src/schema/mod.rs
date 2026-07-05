pub mod genesis;
pub mod registration;

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
