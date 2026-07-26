//! The schema catalog: what the system knows about models and properties.
//!
//! Ankurah stores schema as DATA. Every model and every property is an
//! ordinary entity in one of three reserved collections (`_ankurah_model`,
//! `_ankurah_property`, `_ankurah_model_property` for the property-to-model
//! memberships), with a durable id minted by the system's one durable
//! allocator. Those entities replicate, persist, and survive renames like
//! any other data, which is the point: a property's identity is its entity
//! id, not its current display name, so renaming a field someday does not
//! orphan the data written under it.
//!
//! A Rust struct with `#[derive(Model)]` is not the schema; it is one
//! binary's DECLARATION of a schema, compiled into a static
//! ([`ModelSchema`], in [`local`]). The first time a binary uses a model --
//! explicitly via `Context::register`, or implicitly on create/fetch -- that
//! declaration is sent to the durable node, whose registration executor
//! ([`registration`]) looks each piece up, allocates ids for anything new,
//! checks that a re-declaration is compatible with what the catalog already
//! holds, and returns the resolved ids. Two binaries with the same struct
//! get the same ids; a binary whose declaration conflicts is refused.
//!
//! Each node keeps an in-memory index of the catalog entities
//! ([`catalog::CatalogManager`]) so lookups don't touch storage: parsed into
//! `ModelDef`/`PropertyDef`/`MembershipDef`, warmed from local storage on
//! durable nodes and from registration responses on ephemeral ones. The
//! wire request/response types live in ankurah-proto. The full design
//! record is specs/model-property-metadata/rfc.md.

pub mod catalog;
pub mod local;
pub mod registration;

pub use local::{registration_request, FieldSchema, ModelSchema};

use ankurah_proto::{ModelId, SystemModel};

use crate::property::PropertyError;
use crate::value::ValueType;
use ankql::ast::PathExpr;

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

/// Labels reserved for built-in models: user models may not use this prefix
/// (enforced at derive time and at schema registration).
pub const RESERVED_COLLECTION_PREFIX: &str = "_ankurah_";

pub const fn model_collection() -> ModelId { ModelId::System(SystemModel::Model) }
pub const fn property_collection() -> ModelId { ModelId::System(SystemModel::Property) }
pub const fn model_property_collection() -> ModelId { ModelId::System(SystemModel::ModelProperty) }

/// Whether `id` is one of the three metadata catalog collections (NOT the
/// system collection, which replicates via the Presence handshake and has
/// its own trust story).
pub fn is_catalog_collection(id: &ModelId) -> bool {
    matches!(id, ModelId::System(SystemModel::Model | SystemModel::Property | SystemModel::ModelProperty))
}

/// Whether `id` names one of Ankurah's built-in collections. Built-ins are
/// the only collections permitted under [`RESERVED_COLLECTION_PREFIX`] and
/// cannot be mutated through ordinary user transactions.
pub fn is_protected_collection(id: &ModelId) -> bool { matches!(id, ModelId::System(_)) }

/// The logical protocol model for today's built-in storage key. This mapping
/// is deliberately core-local: protocol identity must not depend on the
/// current materialization name.
pub fn system_model_id(collection: &str) -> Option<ModelId> {
    let model = match collection {
        crate::system::SYSTEM_COLLECTION_ID => SystemModel::System,
        MODEL_COLLECTION_ID => SystemModel::Model,
        PROPERTY_COLLECTION_ID => SystemModel::Property,
        MODEL_PROPERTY_COLLECTION_ID => SystemModel::ModelProperty,
        _ => return None,
    };
    Some(ModelId::System(model))
}

/// Today's declared collection label for a built-in model. This is schema and
/// query-qualifier metadata only; storage engines assign private names.
pub const fn system_collection_label(model: SystemModel) -> &'static str {
    match model {
        SystemModel::System => crate::system::SYSTEM_COLLECTION_ID,
        SystemModel::Model => MODEL_COLLECTION_ID,
        SystemModel::Property => PROPERTY_COLLECTION_ID,
        SystemModel::ModelProperty => MODEL_PROPERTY_COLLECTION_ID,
    }
}

#[cfg(test)]
mod model_mapping_tests {
    use super::*;

    #[test]
    fn every_system_model_maps_to_the_current_storage_key_and_back() {
        let pairs = [
            (SystemModel::System, crate::system::SYSTEM_COLLECTION_ID),
            (SystemModel::Model, MODEL_COLLECTION_ID),
            (SystemModel::Property, PROPERTY_COLLECTION_ID),
            (SystemModel::ModelProperty, MODEL_PROPERTY_COLLECTION_ID),
        ];
        for (system_model, collection) in pairs {
            let model = ModelId::System(system_model);
            assert_eq!(system_collection_label(system_model), collection);
            assert_eq!(system_model_id(collection), Some(model));
        }
        assert_eq!(system_model_id("albums"), None);
    }
}
