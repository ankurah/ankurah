//! The durable-side executor for the RegisterSchema protocol operation
//! (specs/model-property-metadata/rfc.md section 5.2, rev 4).
//!
//! Registration is an UPSERT: the executor looks each definition up by its
//! lookup key (model by collection; property by (model, name, backend,
//! value_type); membership by (model, property)), ALLOCATES a fresh
//! `EntityId::new()` -- a true ULID -- on miss, and emits ordinary events
//! through the policy-checked commit pipeline. The whole execution
//! serializes on a process-local mutex, and the executor upserts the
//! resolved definitions into the catalog map synchronously after commit,
//! BEFORE releasing that mutex, so consecutive registrations can never
//! race the reactor-fed map into double-allocation (RFC 5.1 executor
//! discipline). The resolved definitions are returned to the requester via
//! `NodeResponseBody::SchemaRegistered`.
//!
//! Policy gates the execution twice (RFC 5.7): the resolved plan -- what
//! this request will actually create and update -- goes through
//! `PolicyAgent::check_schema_registration` before anything is emitted,
//! and every emitted event still passes `check_event` inside the ordinary
//! commit pipeline. A durable node needs no model code to serve
//! registration: the request carries language-agnostic descriptors.
//! Idempotence is the upsert's: a repeat registration finds every key,
//! emits zero events, and returns the same ids.

use std::collections::BTreeMap;

use ankurah_proto::{
    self as proto, Attested, CollectionId, EntityId, MembershipDescriptor, ModelDescriptor, PropertyDescriptor, PropertyRef,
    RegisteredMembership, RegisteredModel, RegisteredProperty, TransactionId,
};

use crate::error::{MutationError, RetrievalError};
use crate::node::Node;
use crate::policy::{AccessDenied, PlannedMembership, PlannedUpdate, PolicyAgent, RegistrationPlan};
use crate::property::backend::{LWWBackend, PropertyBackend};
use crate::storage::StorageEngine;
use crate::value::Value;

use super::{model_collection, model_property_collection, property_collection};

/// The full resolved output of one registration: what SchemaRegistered
/// carries back to the requester.
pub type RegisteredDefs = (Vec<RegisteredModel>, Vec<RegisteredProperty>, Vec<RegisteredMembership>);

#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    #[error("registration executes on durable nodes; this node is ephemeral")]
    NotDurable,
    #[error("system is not ready")]
    SystemNotReady,
    #[error(
        "collection '{0}' has never been registered on this node and no durable peer is connected; \
         connect to the system once first (RFC 5.2 strict offline rule)"
    )]
    NoDurablePeer(String),
    #[error("membership references property '{0}' which is not declared in this request for collection '{1}'")]
    UnresolvedPropertyRef(String, String),
    #[error("descriptor references collection '{0}' which is neither declared in this request nor present in the catalog")]
    UnknownMintingCollection(String),
    #[error("explicit property id {property} does not exist in the catalog; explicit binding never mints (RFC 5.9)")]
    ExplicitIdNotFound { property: EntityId },
    #[error("explicit model id {model} does not exist in the catalog; explicit binding never mints (RFC 5.9)")]
    ExplicitModelIdNotFound { model: EntityId },
    #[error("explicit model id {model} is bound to collection '{found_collection}'; binder declares '{collection}'")]
    ExplicitModelIdMismatch { model: EntityId, found_collection: String, collection: String },
    #[error("explicit property id {property} is ({found_backend}, {found_value_type}); binder declares ({backend}, {value_type})")]
    ExplicitIdMismatch { property: EntityId, found_backend: String, found_value_type: String, backend: String, value_type: String },
    #[error("registration refused by policy: {0}")]
    PolicyDenied(#[from] AccessDenied),
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
    /// Execute a RegisterSchema request as the system's allocator: upsert
    /// every definition by its lookup key, allocate fresh ids for misses,
    /// emit ordinary creation events and difference-only follow-ups through
    /// the policy-checked pipeline, and return the resolved definitions.
    pub async fn execute_schema_registration(
        &self,
        cdata: &PA::ContextData,
        models: Vec<ModelDescriptor>,
        properties: Vec<PropertyDescriptor>,
        memberships: Vec<MembershipDescriptor>,
    ) -> Result<RegisteredDefs, RegistrationError> {
        if !self.durable {
            return Err(RegistrationError::NotDurable);
        }
        if self.system.root().is_none() {
            return Err(RegistrationError::SystemNotReady);
        }
        // The catalog map is the executor's lookup source; the storage warm
        // must have landed before the first lookup, or a cold-started
        // allocator could double-allocate existing definitions.
        self.catalog.wait_catalog_ready().await;

        // RFC 5.1 executor discipline: the whole upsert -- lookups,
        // allocation, commit, and the synchronous map update -- serializes
        // on the allocator mutex. The reactor-fed map alone lags commit and
        // must never be raced by the next request in line.
        let _allocator = self.catalog.lock_allocator().await;

        let mut plan = RegistrationPlan::default();
        // Creation events carry the FULL definition state (no frozen
        // encoder, no identity/metadata split; RFC 5.1); follow-ups carry
        // only fields that differ, parented at the entity's current head
        // (provenance-ordered, plan decision 18).
        let mut events: Vec<Attested<proto::Event>> = Vec::new();
        let mut push = |event: proto::Event| events.push(Attested::opt(event, None));

        // -- models: looked up by collection --------------------------------
        let mut model_ids: BTreeMap<String, EntityId> = BTreeMap::new();
        let mut out_models: Vec<RegisteredModel> = Vec::new();
        for m in &models {
            let (model_id, resolved_name) = match m.explicit_id {
                Some(id) => {
                    // RFC 5.9: verify, never mint, never mutate the bound
                    // entity's fields; the catalog's own display name stands.
                    let values = self.verify_explicit_model_binding(id, m).await?;
                    let name = string_field(&values, "name").unwrap_or_else(|| m.collection.clone());
                    plan.existing.push(id);
                    (id, name)
                }
                None => match self.catalog.model_by_collection(&m.collection) {
                    Some(def) => {
                        // Display names follow the most recent registration
                        // (plan decision 18); emit only on difference.
                        if def.name != m.name {
                            let (_, head) = self
                                .catalog_entity_snapshot(model_collection(), def.id)
                                .await?
                                .ok_or_else(|| RetrievalError::Other(format!("catalog map holds model {} absent from storage", def.id)))?;
                            plan.updates.push(PlannedUpdate {
                                collection: model_collection(),
                                entity: def.id,
                                field: "name".into(),
                                from: Some(Value::String(def.name.clone())),
                                to: Value::String(m.name.clone()),
                            });
                            push(follow_up(model_collection(), def.id, head, vec![("name", Value::String(m.name.clone()))]));
                        } else {
                            plan.existing.push(def.id);
                        }
                        (def.id, m.name.clone())
                    }
                    None => {
                        let id = EntityId::new();
                        plan.creates_models.push((id, m.clone()));
                        push(creation(
                            model_collection(),
                            id,
                            vec![("collection", Value::String(m.collection.clone())), ("name", Value::String(m.name.clone()))],
                        ));
                        (id, m.name.clone())
                    }
                },
            };
            model_ids.insert(m.collection.clone(), model_id);
            out_models.push(RegisteredModel { id: model_id, collection: m.collection.clone(), name: resolved_name });
        }

        // Resolve a collection reference to a model id, allocating a stub
        // model on full miss (RFC 5.2: target-model references and
        // circular references resolve executor-side).
        macro_rules! resolve_model {
            ($collection:expr) => {{
                let c: &str = $collection;
                match model_ids.get(c) {
                    Some(id) => *id,
                    None => match self.catalog.model_by_collection(c) {
                        Some(def) => {
                            model_ids.insert(c.to_string(), def.id);
                            def.id
                        }
                        None => {
                            let id = EntityId::new();
                            let stub = ModelDescriptor { collection: c.to_string(), name: c.to_string(), explicit_id: None };
                            plan.creates_models.push((id, stub));
                            push(creation(
                                model_collection(),
                                id,
                                vec![("collection", Value::String(c.to_string())), ("name", Value::String(c.to_string()))],
                            ));
                            model_ids.insert(c.to_string(), id);
                            out_models.push(RegisteredModel { id, collection: c.to_string(), name: c.to_string() });
                            id
                        }
                    },
                }
            }};
        }

        // -- properties: looked up by (model, name, backend, value_type) ----
        let mut property_ids: BTreeMap<(String, String), EntityId> = BTreeMap::new();
        let mut out_properties: Vec<RegisteredProperty> = Vec::new();
        for p in &properties {
            match p.explicit_id {
                Some(id) => {
                    // RFC 5.9: verify (backend, value_type), never mint. The
                    // bound entity's name and metadata are authoritative.
                    let values = self.verify_explicit_binding(id, p).await?;
                    plan.existing.push(id);
                    let scope = resolve_model!(&p.minting_collection);
                    out_properties.push(RegisteredProperty {
                        id,
                        model: entity_id_field(&values, "minted_for").unwrap_or(scope),
                        name: string_field(&values, "name").unwrap_or_else(|| p.name.clone()),
                        backend: p.backend.clone(),
                        value_type: p.value_type.clone(),
                        target_model: entity_id_field(&values, "target_model"),
                    });
                    property_ids.insert((p.minting_collection.clone(), p.name.clone()), id);
                    continue;
                }
                None => {}
            }

            let scope = resolve_model!(&p.minting_collection);
            // Resolve the target-model reference first so both the create
            // and the diff paths can use it.
            let target = match &p.target_collection {
                Some(tc) => Some(resolve_model!(tc)),
                None => None,
            };

            let current = self.catalog.property_by_key(&scope, &p.name, &p.backend, &p.value_type);

            // RFC 5.8 rename-hint pre-pass, GUARDED: only when the
            // current-name lookup misses and the hinted lookup hits. The
            // hint is an ordinary name follow-up; the property keeps its id.
            let renamed = match (&current, &p.renamed_from) {
                (None, Some(old)) => self.catalog.property_by_key(&scope, old, &p.backend, &p.value_type),
                _ => None,
            };

            let property_id = match (&current, &renamed) {
                (Some(def), _) => {
                    // Plain hit: name matches by construction; only the
                    // target reference can differ.
                    let mut fields: Vec<(&str, Value)> = Vec::new();
                    if let Some(t) = target {
                        if def.target_model != Some(t) {
                            plan.updates.push(PlannedUpdate {
                                collection: property_collection(),
                                entity: def.id,
                                field: "target_model".into(),
                                from: def.target_model.map(Value::EntityId),
                                to: Value::EntityId(t),
                            });
                            fields.push(("target_model", Value::EntityId(t)));
                        }
                    }
                    if fields.is_empty() {
                        plan.existing.push(def.id);
                    } else {
                        let (_, head) = self
                            .catalog_entity_snapshot(property_collection(), def.id)
                            .await?
                            .ok_or_else(|| RetrievalError::Other(format!("catalog map holds property {} absent from storage", def.id)))?;
                        push(follow_up(property_collection(), def.id, head, fields));
                    }
                    def.id
                }
                (None, Some(def)) => {
                    // The rename hint applies: update `name` on the existing
                    // lineage, plus any target change, in one follow-up.
                    let mut fields: Vec<(&str, Value)> = vec![("name", Value::String(p.name.clone()))];
                    plan.updates.push(PlannedUpdate {
                        collection: property_collection(),
                        entity: def.id,
                        field: "name".into(),
                        from: Some(Value::String(def.name.clone())),
                        to: Value::String(p.name.clone()),
                    });
                    if let Some(t) = target {
                        if def.target_model != Some(t) {
                            plan.updates.push(PlannedUpdate {
                                collection: property_collection(),
                                entity: def.id,
                                field: "target_model".into(),
                                from: def.target_model.map(Value::EntityId),
                                to: Value::EntityId(t),
                            });
                            fields.push(("target_model", Value::EntityId(t)));
                        }
                    }
                    let (_, head) = self
                        .catalog_entity_snapshot(property_collection(), def.id)
                        .await?
                        .ok_or_else(|| RetrievalError::Other(format!("catalog map holds property {} absent from storage", def.id)))?;
                    push(follow_up(property_collection(), def.id, head, fields));
                    def.id
                }
                (None, None) => {
                    // Miss: allocate. The creation event carries the full
                    // definition state.
                    let id = EntityId::new();
                    plan.creates_properties.push((id, p.clone()));
                    let mut fields: Vec<(&str, Value)> = vec![
                        ("minted_for", Value::EntityId(scope)),
                        ("name", Value::String(p.name.clone())),
                        ("backend", Value::String(p.backend.clone())),
                        ("value_type", Value::String(p.value_type.clone())),
                    ];
                    if let Some(t) = target {
                        fields.push(("target_model", Value::EntityId(t)));
                    }
                    push(creation(property_collection(), id, fields));
                    id
                }
            };

            property_ids.insert((p.minting_collection.clone(), p.name.clone()), property_id);
            out_properties.push(RegisteredProperty {
                id: property_id,
                model: scope,
                name: p.name.clone(),
                backend: p.backend.clone(),
                value_type: p.value_type.clone(),
                target_model: target.or_else(|| current.as_ref().and_then(|d| d.target_model)),
            });
        }

        // -- memberships: looked up by (model, property) ---------------------
        let mut out_memberships: Vec<RegisteredMembership> = Vec::new();
        for ms in &memberships {
            let model_id = match model_ids.get(&ms.collection) {
                Some(id) => *id,
                None => self
                    .catalog
                    .model_by_collection(&ms.collection)
                    .map(|def| def.id)
                    .ok_or_else(|| RegistrationError::UnknownMintingCollection(ms.collection.clone()))?,
            };
            let property_id = match &ms.property {
                PropertyRef::Id(id) => *id,
                PropertyRef::Name(name) => *property_ids
                    .get(&(ms.collection.clone(), name.clone()))
                    .ok_or_else(|| RegistrationError::UnresolvedPropertyRef(name.clone(), ms.collection.clone()))?,
            };

            let membership_id = match self.catalog.membership(&model_id, &property_id) {
                Some(def) => {
                    if def.optional != Some(ms.optional) {
                        let (_, head) = self
                            .catalog_entity_snapshot(model_property_collection(), def.id)
                            .await?
                            .ok_or_else(|| RetrievalError::Other(format!("catalog map holds membership {} absent from storage", def.id)))?;
                        plan.updates.push(PlannedUpdate {
                            collection: model_property_collection(),
                            entity: def.id,
                            field: "optional".into(),
                            from: def.optional.map(Value::Bool),
                            to: Value::Bool(ms.optional),
                        });
                        push(follow_up(model_property_collection(), def.id, head, vec![("optional", Value::Bool(ms.optional))]));
                    } else {
                        plan.existing.push(def.id);
                    }
                    def.id
                }
                None => {
                    let id = EntityId::new();
                    plan.creates_memberships.push(PlannedMembership { id, model: model_id, property: property_id, optional: ms.optional });
                    push(creation(
                        model_property_collection(),
                        id,
                        vec![
                            ("model", Value::EntityId(model_id)),
                            ("property", Value::EntityId(property_id)),
                            ("optional", Value::Bool(ms.optional)),
                        ],
                    ));
                    id
                }
            };
            out_memberships.push(RegisteredMembership { id: membership_id, model: model_id, property: property_id, optional: ms.optional });
        }

        // A re-registration of unchanged definitions is a pure no-op:
        // nothing to gate, nothing to commit, nothing to relay -- but the
        // requester still gets the full resolved definitions.
        if plan.is_noop() {
            return Ok((out_models, out_properties, out_memberships));
        }

        // RFC 5.7 / plan decision 26: the exists-aware policy gate judges
        // the resolved plan before anything is emitted. All-or-nothing.
        self.policy_agent.check_schema_registration(self, cdata, &plan)?;

        // The ordinary remote-commit pipeline: policy check (check_event),
        // attest, persist, apply, reactor notify.
        self.commit_remote_transaction(cdata, TransactionId::new(), events).await?;

        // Synchronous map upsert BEFORE the allocator mutex releases: the
        // next registration in line must observe these allocations even if
        // the reactor has not delivered them yet (RFC 5.1).
        self.catalog.upsert_registered(&out_models, &out_properties, &out_memberships);

        Ok((out_models, out_properties, out_memberships))
    }

    /// RFC 5.9: an explicit binding references a definition authored
    /// elsewhere. Absence is a hard failure (cold start), and a
    /// (backend, value_type) mismatch means the definition was retyped:
    /// breaking for binders BY DESIGN. Returns the bound entity's current
    /// values for response building.
    async fn verify_explicit_binding(
        &self,
        id: EntityId,
        p: &PropertyDescriptor,
    ) -> Result<BTreeMap<String, Option<Value>>, RegistrationError> {
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
        Ok(values)
    }

    /// RFC 5.9 for models: an explicit binding references a model entity
    /// authored elsewhere. Absence is a hard failure (never mints), and a
    /// collection mismatch means the binder points at the wrong contract.
    /// Returns the bound entity's current values for response building.
    async fn verify_explicit_model_binding(
        &self,
        id: EntityId,
        m: &ModelDescriptor,
    ) -> Result<BTreeMap<String, Option<Value>>, RegistrationError> {
        let Some((values, _)) = self.catalog_entity_snapshot(model_collection(), id).await? else {
            return Err(RegistrationError::ExplicitModelIdNotFound { model: id });
        };
        let found_collection = string_field(&values, "collection").unwrap_or_default();
        if found_collection != m.collection {
            return Err(RegistrationError::ExplicitModelIdMismatch { model: id, found_collection, collection: m.collection.clone() });
        }
        Ok(values)
    }

    /// Read a catalog entity's LWW values and head straight from storage
    /// (catalog entities are system models: raw backend access, never a
    /// View). `None` when the entity does not exist yet.
    async fn catalog_entity_snapshot(
        &self,
        collection: CollectionId,
        id: EntityId,
    ) -> Result<Option<(BTreeMap<String, Option<Value>>, proto::Clock)>, RetrievalError> {
        let storage = self.collections.get(&collection).await?;
        let state = match storage.get_state(id).await {
            Ok(state) => state,
            Err(RetrievalError::EntityNotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let head = state.payload.state.head.clone();
        let Some(buffer) = state.payload.state.state_buffers.0.get("lww") else {
            return Ok(None);
        };
        let backend = LWWBackend::from_state_buffer(buffer)?;
        Ok(Some((backend.property_values(), head)))
    }

    /// Values-only convenience over [`Self::catalog_entity_snapshot`].
    async fn catalog_entity_values(
        &self,
        collection: CollectionId,
        id: EntityId,
    ) -> Result<Option<BTreeMap<String, Option<Value>>>, RetrievalError> {
        Ok(self.catalog_entity_snapshot(collection, id).await?.map(|(values, _)| values))
    }
}

fn string_field(values: &BTreeMap<String, Option<Value>>, field: &str) -> Option<String> {
    match values.get(field) {
        Some(Some(Value::String(s))) => Some(s.clone()),
        _ => None,
    }
}

fn entity_id_field(values: &BTreeMap<String, Option<Value>>, field: &str) -> Option<EntityId> {
    match values.get(field) {
        Some(Some(Value::EntityId(id))) => Some(*id),
        _ => None,
    }
}

/// A creation event: full definition state, empty parent clock. Ordinary
/// in every respect (RFC 5.1: no frozen encoder; catalog collections stay
/// name-keyed at the backend layer permanently, the bootstrap exemption).
fn creation(collection: CollectionId, entity_id: EntityId, fields: Vec<(&str, Value)>) -> proto::Event {
    follow_up(collection, entity_id, proto::Clock::default(), fields)
}

/// A follow-up event carrying changed metadata, parented at the entity's
/// current head (provenance-ordered, plan decision 18: it must DESCEND the
/// metadata it supersedes so LWW recency decides, not the concurrent
/// tiebreak).
fn follow_up(collection: CollectionId, entity_id: EntityId, parent: proto::Clock, fields: Vec<(&str, Value)>) -> proto::Event {
    let backend = LWWBackend::new();
    for (name, value) in fields {
        backend.set(name.to_string(), Some(value));
    }
    let operations = backend.to_operations().expect("LWW encoding of scalar values is infallible").expect("fields are non-empty");
    proto::Event { collection, entity_id, operations: proto::OperationSet(BTreeMap::from([("lww".to_string(), operations)])), parent }
}
