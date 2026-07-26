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
mod resolver;

pub use local::{registration_request, FieldSchema, ModelSchema};
pub use resolver::CatalogResolver;

use ankurah_proto::{ModelId, SystemModel};

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
