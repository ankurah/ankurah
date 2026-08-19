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
//! ([`ModelStructDescriptor`], in [`compiled`]). The first time a binary uses a model --
//! explicitly via `Context::register`, or implicitly on create/fetch -- that
//! declaration is sent to the durable node, whose registration executor
//! ([`registration`]) looks each piece up, allocates ids for anything new,
//! checks that a re-declaration is compatible with what the catalog already
//! holds, and returns the resolved ids. Two binaries with the same struct
//! get the same ids; a binary whose declaration conflicts is refused.
//!
//! Each node keeps an in-memory index of the catalog entities
//! ([`catalog::CatalogManager`]) so lookups don't touch storage: the catalog's
//! own typed rows ([`catalog::rows`]), keyed by catalog entity id and derived
//! from the projection livequeries — every node from its own storage, and an
//! ephemeral node additionally from the durable peer it subscribes to. The
//! wire request/response types live in ankurah-proto. The full design
//! record lives with the design documents for this subsystem.

pub mod catalog;
pub mod cell;
pub mod compiled;
pub mod registration;
pub mod resolver;

pub use cell::{SchemaEpoch, SchemaOnceCell};
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

/// The metadata catalog collections. Catalog entities are SYSTEM MODELS:
/// their model and property identities are built-ins fixed at compile time
/// ([`catalog::rows`]), never allocated. That is what keeps them out of the
/// self-description ouroboros -- reading a catalog row asks the catalog
/// nothing -- and it is also what lets them have an ordinary derived Model,
/// which is how the catalog projects itself into every node's map.
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

/// Whether READS of `collection` bypass the [`crate::policy::PolicyAgent`].
///
/// The catalog is what turns a name into an identity, so every node needs the
/// whole of it before it can run any query at all -- including the query that
/// would ask the agent whether it may read. Making catalog reads answerable
/// without a credential is what breaks that circle, and it is a documented
/// 0.10 property: the catalog is readable to any connected peer.
///
/// The exemption is symmetric and complete. A node skips its own admission
/// checks, sends the request with NO credential, and the serving node neither
/// authenticates the request nor authorizes what it returns. Authentication
/// is included deliberately: an agent that checks credentials per request
/// would refuse the empty one, and the peer would never obtain the catalog it
/// needs in order to authenticate anything. Connection is the gate.
///
/// Temporary posture, scoped to exactly these three collections: the policy
/// re-derivation work (https://github.com/ankurah/ankurah/pull/426) is the
/// intended replacement for this carve.
///
/// It covers READS only. Catalog writes stay exactly as protected as every
/// other write: registration is the only writer, its requests are signed and
/// checked like any other, and each event it emits still passes `check_event`.
pub fn reads_bypass_policy(collection: &ankurah_proto::CollectionId) -> bool {
    matches!(collection.as_str(), MODEL_COLLECTION_ID | PROPERTY_COLLECTION_ID | MODEL_PROPERTY_COLLECTION_ID)
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
