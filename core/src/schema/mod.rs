//! Durable model and property identities.
//!
//! Catalog entities persist the schema; compiled descriptors cache its IDs
//! per system epoch so names are resolved before storage or evaluation.

pub mod catalog;
pub mod cell;
pub mod compiled;
pub mod registration;
pub use crate::storage::CatalogResolver;
pub use catalog::resolver;

pub use cell::{PerSystemOnceCell, SystemEpoch, UninitializedCell};
pub use compiled::{ModelStructDescriptor, StructProperty};

use ankurah_proto::{ModelId, SystemModel};

pub const MODEL_COLLECTION_ID: &str = "_ankurah_model";
pub const PROPERTY_COLLECTION_ID: &str = "_ankurah_property";
pub const MODEL_PROPERTY_COLLECTION_ID: &str = "_ankurah_model_property";

pub const RESERVED_COLLECTION_PREFIX: &str = "_ankurah_";

pub(crate) const CATALOG_MODELS: [ModelId; 3] = [
    ModelId::System(SystemModel::Model),
    ModelId::System(SystemModel::Property),
    ModelId::System(SystemModel::ModelProperty),
];

pub fn is_catalog_model(id: &ModelId) -> bool { CATALOG_MODELS.contains(id) }

/// Catalog reads bypass policy so nodes can resolve names before authorization.
pub fn reads_bypass_policy(model: &ModelId) -> bool { is_catalog_model(model) }

/// Whether every matching entity must be a catalog row, so this read can bootstrap policy.
pub(crate) fn is_catalog_read<S: ankql::ast::Stage<ModelId = ModelId>>(predicate: &ankql::ast::Predicate<S>) -> bool {
    use ankql::ast::Predicate;
    match predicate {
        Predicate::MemberOf(model) => reads_bypass_policy(model),
        Predicate::And(left, right) => is_catalog_read(left) || is_catalog_read(right),
        Predicate::Or(left, right) => is_catalog_read(left) && is_catalog_read(right),
        _ => false,
    }
}

/// Whether this request is a policy-exempt catalog read.
pub(crate) fn request_bypasses_policy(request: &ankurah_proto::NodeRequestBody) -> bool {
    use ankurah_proto::NodeRequestBody;
    match request {
        NodeRequestBody::Fetch { selection, .. } | NodeRequestBody::SubscribeQuery { selection, .. } => {
            is_catalog_read(&selection.predicate)
        }
        NodeRequestBody::Get { .. } | NodeRequestBody::GetEvents { .. }
        | NodeRequestBody::CommitTransaction { .. } | NodeRequestBody::RegisterSchema { .. } => false,
    }
}

pub fn is_reserved_model(model: &ModelId) -> bool { matches!(model, ModelId::System(_)) }

pub fn system_model_id(label: &str) -> Option<ModelId> {
    let model = match label {
        crate::system::SYSTEM_COLLECTION_ID => SystemModel::System,
        MODEL_COLLECTION_ID => SystemModel::Model,
        PROPERTY_COLLECTION_ID => SystemModel::Property,
        MODEL_PROPERTY_COLLECTION_ID => SystemModel::ModelProperty,
        _ => return None,
    };
    Some(ModelId::System(model))
}

pub const fn system_model_label(model: SystemModel) -> &'static str {
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
            assert_eq!(system_model_label(system_model), collection);
            assert_eq!(system_model_id(collection), Some(model));
        }
        assert_eq!(system_model_id("albums"), None);
    }
}

#[cfg(test)]
mod request_policy_tests {
    use super::*;
    use ankql::ast::Predicate;
    use ankurah_proto::{NodeRequestBody, QueryId, RegisterModel, TransactionId};

    #[test]
    fn only_catalog_reads_bypass_policy() {
        for (model, exempt) in [
            (ModelId::System(SystemModel::Model), true),
            (ModelId::System(SystemModel::Property), true),
            (ModelId::System(SystemModel::ModelProperty), true),
            (ModelId::System(SystemModel::System), false),
            (ModelId::EntityId(ankurah_proto::EntityId::random()), false),
        ] {
            let requests = [
                NodeRequestBody::Fetch { selection: Predicate::MemberOf(model).into(), known_matches: Vec::new() },
                NodeRequestBody::SubscribeQuery {
                    query_id: QueryId::new(),
                    selection: Predicate::MemberOf(model).into(),
                    version: 1,
                    known_matches: Vec::new(),
                },
            ];
            for request in requests {
                assert_eq!(request_bypasses_policy(&request), exempt, "{request:?}");
            }
        }
        assert!(!request_bypasses_policy(&NodeRequestBody::Get { ids: Vec::new() }));
        assert!(!request_bypasses_policy(&NodeRequestBody::GetEvents { event_ids: Vec::new() }));
        assert!(!request_bypasses_policy(&NodeRequestBody::CommitTransaction {
            id: TransactionId::new(),
            events: Vec::new()
        }));
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
