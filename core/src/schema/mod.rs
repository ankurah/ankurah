//! The boundary between source-level model declarations and a system's
//! durable model and property identities.
//!
//! Here, **schema** means the facts needed to translate model and property
//! names and Rust field types into stable identities and value types. The
//! durable facts are data: application model definitions, property
//! definitions, and model-property memberships are entities in three
//! reserved collections. Their entity ids survive renames, so stored data is
//! addressed by identity rather than by its current source spelling.
//!
//! [`compiled`] is one binary's static declaration of those facts;
//! [`registration`] is the durable allocator operation that reconciles a
//! declaration with catalog data; [`catalog`] is each node's runtime service
//! and materialized projection of the resulting definitions. The first use
//! of a derived model, or an explicit `Context::register_model`, sends its
//! [`ModelStructDescriptor`] through that path. Compatible declarations
//! receive the same ids and incompatible declarations fail.
//!
//! This module root owns vocabulary shared by those three stages: the closed
//! mapping between built-in [`SystemModel`]
//! identities and their current collection labels, protection of the
//! `_ankurah_` namespace, and [`CollectionSchema`], the minimal field-type
//! interface used when query literals must be cast. Wire request and response
//! types remain in `ankurah-proto`.

pub mod catalog;
pub mod compiled;
pub mod registration;

pub use compiled::{ModelStructDescriptor, StructProperty};

use ankurah_proto::{ModelId, SystemModel};

use crate::property::PropertyError;
use crate::value::ValueType;
use ankql::ast::PathExpr;

/// Trait for providing schema information about collections
pub trait CollectionSchema {
    /// Get the ValueType for a given field path
    fn field_type(&self, path: &PathExpr) -> Result<ValueType, PropertyError>;
}

/// The metadata catalog collections. Their typed row models are SYSTEM
/// MODELS: compile-time identities whose derives never consult the catalog
/// they describe. Ordinary transactions may read them through typed Views,
/// but only schema registration may mutate them.
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
