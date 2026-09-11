//! Durable model and property identities.
//!
//! Catalog entities persist the schema; compiled descriptors cache its IDs
//! per system epoch so names are resolved before storage or evaluation.

pub mod catalog;
pub mod cell;
pub mod compiled;
pub mod registration;
pub use catalog::resolver;

pub use cell::{PerSystemOnceCell, SystemEpoch};
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

/// Whether this request is a policy-exempt catalog read.
pub(crate) fn request_bypasses_policy(request: &ankurah_proto::NodeRequestBody) -> bool {
    use ankurah_proto::NodeRequestBody;
    match request {
        NodeRequestBody::Fetch { collection, .. }
        | NodeRequestBody::Get { collection, .. }
        | NodeRequestBody::GetEvents { collection, .. }
        | NodeRequestBody::SubscribeQuery { collection, .. } => reads_bypass_policy(collection),
        NodeRequestBody::CommitTransaction { .. } | NodeRequestBody::RegisterSchema { .. } => false,
    }
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

#[cfg(test)]
mod request_policy_tests {
    use super::*;
    use ankql::ast::Predicate;
    use ankurah_proto::{CollectionId, NodeRequestBody, QueryId, RegisterModel, TransactionId};

    #[test]
    fn only_catalog_reads_bypass_policy() {
        for (label, exempt) in [
            (MODEL_COLLECTION_ID, true),
            (PROPERTY_COLLECTION_ID, true),
            (MODEL_PROPERTY_COLLECTION_ID, true),
            (crate::system::SYSTEM_COLLECTION_ID, false),
            ("_ankurah_other", false),
            ("albums", false),
        ] {
            let collection = CollectionId::fixed_name(label);
            let requests = [
                NodeRequestBody::Get { collection: collection.clone(), ids: Vec::new() },
                NodeRequestBody::GetEvents { collection: collection.clone(), event_ids: Vec::new() },
                NodeRequestBody::Fetch { collection: collection.clone(), selection: Predicate::True.into(), known_matches: Vec::new() },
                NodeRequestBody::SubscribeQuery {
                    query_id: QueryId::new(),
                    collection,
                    selection: Predicate::True.into(),
                    version: 1,
                    known_matches: Vec::new(),
                },
            ];
            for request in requests {
                assert_eq!(request_bypasses_policy(&request), exempt, "{request:?}");
            }
        }
        assert!(!request_bypasses_policy(&NodeRequestBody::CommitTransaction { id: TransactionId::new(), events: Vec::new() }));
        assert!(!request_bypasses_policy(&NodeRequestBody::RegisterSchema {
            model: RegisterModel {
                label: MODEL_COLLECTION_ID.into(),
                name: "Model".into(),
                explicit_id: None,
                build_id: [0; 16],
                properties: Vec::new(),
            },
        }));
    }
}
