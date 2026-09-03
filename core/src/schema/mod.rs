//! Durable model and property identities.
//!
//! Catalog entities persist the schema; compiled descriptors cache its IDs
//! per system epoch so names are resolved before storage or evaluation.

pub mod catalog;
pub mod cell;
pub mod compiled;
pub mod registration;
pub use catalog::resolver;

pub use cell::{SchemaEpoch, SchemaOnceCell};
pub use compiled::{ModelStructDescriptor, StructProperty};

use ankurah_proto::{ModelId, SystemModel};

pub const MODEL_COLLECTION_ID: &str = "_ankurah_model";
pub const PROPERTY_COLLECTION_ID: &str = "_ankurah_property";
pub const MODEL_PROPERTY_COLLECTION_ID: &str = "_ankurah_model_property";

pub const RESERVED_COLLECTION_PREFIX: &str = "_ankurah_";

pub const fn model_collection() -> ModelId { ModelId::System(SystemModel::Model) }
pub const fn property_collection() -> ModelId { ModelId::System(SystemModel::Property) }
pub const fn model_property_collection() -> ModelId { ModelId::System(SystemModel::ModelProperty) }

pub fn is_catalog_collection(id: &ModelId) -> bool {
    matches!(id, ModelId::System(SystemModel::Model | SystemModel::Property | SystemModel::ModelProperty))
}

/// Catalog reads bypass policy so nodes can resolve names before authorization.
pub fn reads_bypass_policy(collection: &ankurah_proto::CollectionId) -> bool {
    matches!(collection.as_str(), MODEL_COLLECTION_ID | PROPERTY_COLLECTION_ID | MODEL_PROPERTY_COLLECTION_ID)
}

pub fn is_protected_collection(id: &ModelId) -> bool { matches!(id, ModelId::System(_)) }

pub fn is_reserved_collection(collection: &ankurah_proto::CollectionId) -> bool {
    collection.as_str().starts_with(RESERVED_COLLECTION_PREFIX)
}

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
