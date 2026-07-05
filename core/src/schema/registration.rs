//! The durable-side executor for the RegisterSchema protocol operation
//! (specs/model-property-metadata/rfc.md section 5.2).
//!
//! A durable node needs no model code to serve registration: the request
//! carries language-agnostic descriptors, and the executor derives the
//! ids, mints frozen genesis events, writes follow-up metadata, and runs
//! every event through the ordinary policy-checked commit pipeline
//! (PolicyAgent::check_event is the gate on who may define schema).
//! Execution is idempotent because derivation and the genesis encoding
//! are deterministic: re-issued or concurrently-issued registrations of
//! the same definitions converge on identical events, including across
//! independent durable executors.

use std::collections::BTreeMap;

use ankurah_proto::{
    self as proto, schema_id, Attested, CollectionId, EntityId, MembershipDescriptor, ModelDescriptor, PropertyDescriptor, PropertyRef,
    TransactionId,
};

use crate::error::{MutationError, RetrievalError};
use crate::node::Node;
use crate::policy::PolicyAgent;
use crate::property::backend::{LWWBackend, PropertyBackend};
use crate::storage::StorageEngine;
use crate::value::Value;

use super::{model_collection, model_property_collection, property_collection};

#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    #[error("registration executes on durable nodes; this node is ephemeral")]
    NotDurable,
    #[error("system is not ready")]
    SystemNotReady,
    #[error("membership references anchor '{0}' which is not declared in this request for collection '{1}'")]
    UnresolvedPropertyRef(String, String),
    #[error(
        "anchor reuse: '{name}' derives property {property}, which currently carries the display name '{current}'; \
         if this is the renamed property, anchor it expressly, otherwise pin a fresh anchor for the new field (RFC 5.8)"
    )]
    AnchorReuse { property: EntityId, name: String, current: String },
    #[error("explicit property id {property} does not exist in the catalog; explicit binding never mints (RFC 5.9)")]
    ExplicitIdNotFound { property: EntityId },
    #[error("explicit property id {property} is ({found_backend}, {found_value_type}); binder declares ({backend}, {value_type})")]
    ExplicitIdMismatch { property: EntityId, found_backend: String, found_value_type: String, backend: String, value_type: String },
    #[error(transparent)]
    Mutation(#[from] MutationError),
    #[error(transparent)]
    Retrieval(#[from] RetrievalError),
}

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Execute a RegisterSchema request: derive ids, mint frozen genesis
    /// events for unknown definitions, write follow-up metadata, and
    /// commit the lot through the policy-checked pipeline.
    pub async fn execute_schema_registration(
        &self,
        cdata: &PA::ContextData,
        models: Vec<ModelDescriptor>,
        properties: Vec<PropertyDescriptor>,
        memberships: Vec<MembershipDescriptor>,
    ) -> Result<(), RegistrationError> {
        if !self.durable {
            return Err(RegistrationError::NotDurable);
        }
        let root = self.system.root().ok_or(RegistrationError::SystemNotReady)?.payload.entity_id;

        let mut events: Vec<Attested<proto::Event>> = Vec::new();
        let mut push = |event: proto::Event| events.push(Attested::opt(event, None));

        // Models: genesis sets `collection`; the display name is follow-up.
        for m in &models {
            let genesis = super::genesis::model_genesis(&root, &m.collection);
            let (model_id, genesis_id) = (genesis.entity_id, genesis.id());
            push(genesis);
            if m.name != m.collection {
                push(follow_up(model_collection(), model_id, genesis_id, [("name", Value::String(m.name.clone()))]));
            }
        }

        // Properties: derived ids under the minting model's scope, or an
        // explicit binding to an existing entity (which never mints).
        // Resolved ids are keyed by (minting collection, anchor) for
        // membership references within this request.
        let mut property_ids: BTreeMap<(String, String), EntityId> = BTreeMap::new();
        for p in &properties {
            let property_id = match p.explicit_id {
                Some(id) => {
                    self.verify_explicit_binding(id, p).await?;
                    id
                }
                None => {
                    let scope = schema_id::model_entity_id(&root, &p.minting_collection);
                    let genesis = super::genesis::property_genesis(&root, &scope, &p.anchor, &p.backend, &p.value_type);
                    let (id, genesis_id) = (genesis.entity_id, genesis.id());
                    self.check_anchor_reuse(id, p).await?;
                    push(genesis);
                    if p.name != p.anchor {
                        push(follow_up(property_collection(), id, genesis_id.clone(), [("name", Value::String(p.name.clone()))]));
                    }
                    if let Some(target) = p.target_model {
                        push(follow_up(property_collection(), id, genesis_id.clone(), [("target_model", Value::EntityId(target))]));
                    }
                    id
                }
            };
            property_ids.insert((p.minting_collection.clone(), p.anchor.clone()), property_id);
        }

        // Memberships: the contract edges. `optional` is always follow-up
        // (readers treat a membership with no optional flag as optional,
        // so required-ness must arrive as data; RFC 5.4).
        for ms in &memberships {
            let model_id = schema_id::model_entity_id(&root, &ms.collection);
            let property_id = match &ms.property {
                PropertyRef::Id(id) => *id,
                PropertyRef::Anchor(anchor) => *property_ids
                    .get(&(ms.collection.clone(), anchor.clone()))
                    .ok_or_else(|| RegistrationError::UnresolvedPropertyRef(anchor.clone(), ms.collection.clone()))?,
            };
            let genesis = super::genesis::membership_genesis(&root, &model_id, &property_id);
            let (membership_id, genesis_id) = (genesis.entity_id, genesis.id());
            push(genesis);
            push(follow_up(model_property_collection(), membership_id, genesis_id, [("optional", Value::Bool(ms.optional))]));
        }

        // The ordinary remote-commit pipeline: policy check (check_event),
        // attest, persist, apply, reactor notify. Redelivered events (same
        // content hash) are no-ops, which is what makes this idempotent.
        self.commit_remote_transaction(cdata, TransactionId::new(), events).await?;
        Ok(())
    }

    /// RFC 5.9: an explicit binding references a definition authored
    /// elsewhere. Absence is a hard failure (cold start), and a
    /// (backend, value_type) mismatch means the definition was retyped:
    /// breaking for binders BY DESIGN.
    async fn verify_explicit_binding(&self, id: EntityId, p: &PropertyDescriptor) -> Result<(), RegistrationError> {
        let Some(values) = self.catalog_entity_values(property_collection(), id).await? else {
            return Err(RegistrationError::ExplicitIdNotFound { property: id });
        };
        let get_string = |field: &str| match values.get(field) {
            Some(Some(Value::String(s))) => s.clone(),
            _ => String::new(),
        };
        let (found_backend, found_value_type) = (get_string("backend"), get_string("value_type"));
        if found_backend != p.backend || found_value_type != p.value_type {
            return Err(RegistrationError::ExplicitIdMismatch {
                property: id,
                found_backend,
                found_value_type,
                backend: p.backend.clone(),
                value_type: p.value_type.clone(),
            });
        }
        Ok(())
    }

    /// RFC 5.8: refuse to mint a genesis for a field whose derived id
    /// already exists in the catalog under a DIFFERENT current display
    /// name, unless the author expressly anchored (anchor != name). This
    /// is what keeps a retired display name from being silently re-minted
    /// into an unrelated lineage.
    async fn check_anchor_reuse(&self, id: EntityId, p: &PropertyDescriptor) -> Result<(), RegistrationError> {
        if p.anchor != p.name {
            return Ok(()); // expressly anchored: this IS the rename case
        }
        if let Some(values) = self.catalog_entity_values(property_collection(), id).await? {
            if let Some(Some(Value::String(current))) = values.get("name") {
                if *current != p.name {
                    return Err(RegistrationError::AnchorReuse { property: id, name: p.name.clone(), current: current.clone() });
                }
            }
        }
        Ok(())
    }

    /// Read a catalog entity's LWW values straight from storage (catalog
    /// entities are system models: raw backend access, never a View).
    async fn catalog_entity_values(
        &self,
        collection: CollectionId,
        id: EntityId,
    ) -> Result<Option<BTreeMap<String, Option<Value>>>, RetrievalError> {
        let storage = self.collections.get(&collection).await?;
        let state = match storage.get_state(id).await {
            Ok(state) => state,
            Err(RetrievalError::EntityNotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let Some(buffer) = state.payload.state.state_buffers.0.get("lww") else {
            return Ok(None);
        };
        let backend = LWWBackend::from_state_buffer(buffer)?;
        Ok(Some(backend.property_values()))
    }
}

/// A follow-up event carrying non-identity metadata. These merge as
/// ordinary LWW updates and may use the node's current encoding; catalog
/// collections stay name-keyed at the backend layer permanently (the
/// bootstrap exemption, RFC 4), which today coincides with the only
/// encoding there is.
fn follow_up<const N: usize>(
    collection: CollectionId,
    entity_id: EntityId,
    parent: proto::EventId,
    fields: [(&str, Value); N],
) -> proto::Event {
    let backend = LWWBackend::new();
    for (name, value) in fields {
        backend.set(name.to_string(), Some(value));
    }
    let operations = backend.to_operations().expect("LWW encoding of scalar values is infallible").expect("fields are non-empty");
    proto::Event {
        collection,
        entity_id,
        operations: proto::OperationSet(BTreeMap::from([("lww".to_string(), operations)])),
        parent: proto::Clock::new([parent]),
    }
}
