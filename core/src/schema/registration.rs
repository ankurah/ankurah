//! The durable-side executor for the RegisterSchema protocol operation
//! (specs/model-property-metadata/rfc.md section 5.2, rev 4).
//!
//! Registration is an UPSERT: the executor looks each definition up by its
//! lookup key (model by collection; property by (model, name); membership by
//! (model, property)), ALLOCATES a fresh
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
//!
//! A property's (backend, value_type) is CANONICAL: fixed at allocation and
//! never changed by registration (rfc.md 5.6 as amended 2026-07-10). A hit
//! whose descriptor declares a different value_type is admitted only when the
//! two types are mutually castable per `Value::cast_to` (the binary writes
//! and reads through the cast); a different backend, or a non-castable type
//! pair, refuses the registration loudly. Changing a canonical type is a
//! deliberate migration (#303), never a model-struct edit.

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
use crate::value::{Value, ValueType};

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
    #[error(
        "explicit property id {property} is canonically ({found_backend}, {found_value_type}); binder declares ({backend}, {value_type}), which is not castable to/from the canonical type (backend must match; value types must be mutually castable)"
    )]
    ExplicitIdMismatch { property: EntityId, found_backend: String, found_value_type: String, backend: String, value_type: String },
    #[error(
        "property '{name}' in '{collection}' is canonically ({found_backend}, {found_value_type}); this binary declares ({backend}, {value_type}), which is not castable to/from the canonical type (backend must match; value types must be mutually castable per Value::cast_to). The canonical type is fixed at allocation; changing it is a deliberate migration (#303), never a model-struct edit"
    )]
    NonCastable { collection: String, name: String, found_backend: String, found_value_type: String, backend: String, value_type: String },
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
        // Schema knowledge follows collection access (RFC 5.7 addendum,
        // maintainer ruling 2026-07-06): every collection this request
        // names must pass can_access_collection for the requesting
        // principal. Without this, a catalog-unprivileged principal could
        // use the no-op upsert -- which skips the policy verb by design --
        // as an existence oracle and harvest definition ids from the
        // response. Denied access and a denied registration are
        // indistinguishable to the requester, so the probe learns nothing.
        {
            let mut named: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
            for m in &models {
                named.insert(m.collection.as_str());
            }
            for p in &properties {
                named.insert(p.minting_collection.as_str());
                if let Some(tc) = &p.target_collection {
                    named.insert(tc.as_str());
                }
            }
            for ms in &memberships {
                named.insert(ms.collection.as_str());
            }
            for collection in named {
                self.policy_agent.can_access_collection(cdata, &CollectionId::fixed_name(collection))?;
            }
        }
        // The catalog map is the executor's primary lookup source; wait for
        // the warm so the common case looks up against a full map. The map
        // is NOT trusted alone on a miss: every miss double-checks durable
        // storage before minting (see the *_lookup_checked helpers), so a
        // lagging or cold map (partial-commit abort skipped the fold, or a
        // lazily-warming engine, #310) can never fork identity.
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
            // A duplicate descriptor in ONE request must not re-mint: the
            // catalog map only learns this request's allocations at the
            // post-commit fold, so the in-flight table is the authority for
            // keys this request already resolved. First occurrence wins.
            if model_ids.contains_key(&m.collection) {
                continue;
            }
            let (model_id, resolved_name) = match m.explicit_id {
                Some(id) => {
                    // RFC 5.9: verify, never mint, never mutate the bound
                    // entity's fields; the catalog's own display name stands.
                    let values = self.verify_explicit_model_binding(id, m).await?;
                    let name = string_field(&values, "name").unwrap_or_else(|| m.collection.clone());
                    plan.existing.push(id);
                    (id, name)
                }
                None => match self.model_lookup_checked(&m.collection).await? {
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
                    None => match self.model_lookup_checked(c).await? {
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
            // Duplicate descriptor in one request: first occurrence wins
            // (same in-flight rule as models).
            if property_ids.contains_key(&(p.minting_collection.clone(), p.name.clone())) {
                continue;
            }
            match p.explicit_id {
                Some(id) => {
                    // RFC 5.9: verify (backend, value_type), never mint. The
                    // bound entity's name and metadata are authoritative.
                    let values = self.verify_explicit_binding(id, p).await?;
                    plan.existing.push(id);
                    let scope = resolve_model!(&p.minting_collection);
                    // The response carries the bound entity's CANONICAL
                    // (backend, value_type), not the binder's declaration.
                    out_properties.push(RegisteredProperty {
                        id,
                        model: entity_id_field(&values, "minted_for").unwrap_or(scope),
                        name: string_field(&values, "name").unwrap_or_else(|| p.name.clone()),
                        backend: string_field(&values, "backend").unwrap_or_else(|| p.backend.clone()),
                        value_type: string_field(&values, "value_type").unwrap_or_else(|| p.value_type.clone()),
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

            let current = self.property_lookup_checked(&scope, &p.name).await?;

            // RFC 5.8 rename-hint pre-pass, GUARDED: only when the
            // current-name lookup misses and the hinted lookup hits. The
            // hint is an ordinary name follow-up; the property keeps its id.
            let renamed = match (&current, &p.renamed_from) {
                (None, Some(old)) => self.property_lookup_checked(&scope, old).await?,
                _ => None,
            };

            // The canonical-type compatibility gate (rfc.md 5.6 as amended
            // 2026-07-10): a hit never mutates (backend, value_type) and
            // never forks a second identity. Refuses loudly on a different
            // backend or a non-castable type pair; a castable drift is
            // admitted (the binary writes and reads through the cast) and
            // logged, and the response below carries the CANONICAL types so
            // the requester's catalog map holds its cast target.
            let canonical = current.as_ref().or(renamed.as_ref()).map(|def| (def.backend.clone(), def.value_type.clone()));
            if let Some(def) = current.as_ref().or(renamed.as_ref()) {
                check_property_compat(def, p)?;
            }

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
            let (backend, value_type) = canonical.unwrap_or_else(|| (p.backend.clone(), p.value_type.clone()));
            out_properties.push(RegisteredProperty {
                id: property_id,
                model: scope,
                name: p.name.clone(),
                backend,
                value_type,
                target_model: target.or_else(|| current.as_ref().and_then(|d| d.target_model)),
            });
        }

        // -- memberships: looked up by (model, property) ---------------------
        let mut out_memberships: Vec<RegisteredMembership> = Vec::new();
        let mut membership_seen: Vec<(EntityId, EntityId)> = Vec::new();
        for ms in &memberships {
            let model_id = match model_ids.get(&ms.collection) {
                Some(id) => *id,
                None => self
                    .model_lookup_checked(&ms.collection)
                    .await?
                    .map(|def| def.id)
                    .ok_or_else(|| RegistrationError::UnknownMintingCollection(ms.collection.clone()))?,
            };
            let property_id = match &ms.property {
                PropertyRef::Id(id) => *id,
                PropertyRef::Name(name) => *property_ids
                    .get(&(ms.collection.clone(), name.clone()))
                    .ok_or_else(|| RegistrationError::UnresolvedPropertyRef(name.clone(), ms.collection.clone()))?,
            };
            // Duplicate descriptor in one request: first occurrence wins.
            if membership_seen.contains(&(model_id, property_id)) {
                continue;
            }
            membership_seen.push((model_id, property_id));

            let membership_id = match self.membership_lookup_checked(&model_id, &property_id).await? {
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
        // the resolved plan before anything is emitted; refusal fails the
        // request before any write. Underneath, check_event still gates
        // each event individually inside the commit pipeline, and the batch
        // is NOT transactional: a mid-batch event denial leaves the earlier
        // catalog events durable (maintainer ruling 2026-07-06: registration
        // does not need to be atomic; #313 tracks the transactional
        // upgrade). Identity survives such partials because every allocator
        // lookup double-checks storage on a map miss, so a retry converges
        // on the stored ids instead of re-minting.
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

    /// Allocator lookup for a model: the catalog map first, then durable
    /// STORAGE on a miss (rev 4 hardening). The in-memory map can lag
    /// durable truth -- a partial-commit abort skips the post-commit fold,
    /// and non-sled engines warm lazily (#310) -- and minting from a cold
    /// map would fork identity for a key that already exists. A storage hit
    /// is folded into the map so the rest of the request (and the next one)
    /// sees it; ordinary first sightings miss both and pay one bounded
    /// fetch under the allocator mutex.
    async fn model_lookup_checked(&self, collection: &str) -> Result<Option<super::catalog::ModelDef>, RetrievalError> {
        if let Some(def) = self.catalog.model_by_collection(collection) {
            return Ok(Some(def));
        }
        let Some((id, values)) = self.catalog_row_by_key(model_collection(), field_eq_str("collection", collection)).await? else {
            return Ok(None);
        };
        let def = super::catalog::ModelDef {
            id,
            collection: collection.to_string(),
            name: string_field(&values, "name").unwrap_or_else(|| collection.to_string()),
        };
        self.catalog.upsert_registered(
            &[RegisteredModel { id: def.id, collection: def.collection.clone(), name: def.name.clone() }],
            &[],
            &[],
        );
        Ok(Some(def))
    }

    /// Allocator lookup for a property by its full key (model, name,
    /// backend, value_type): map first, storage on a miss (see
    /// [`Self::model_lookup_checked`]).
    async fn property_lookup_checked(&self, model: &EntityId, name: &str) -> Result<Option<super::catalog::PropertyDef>, RetrievalError> {
        if let Some(def) = self.catalog.property_by_name(model, name) {
            return Ok(Some(def));
        }
        let predicate = and(field_eq_id("minted_for", *model), field_eq_str("name", name));
        let Some((id, values)) = self.catalog_row_by_key(property_collection(), predicate).await? else {
            return Ok(None);
        };
        // The stored row is authoritative for the canonical (backend,
        // value_type); the requester's declaration is compatibility-checked
        // against it by the caller, never written over it.
        let def = super::catalog::PropertyDef {
            id,
            minted_for: Some(*model),
            name: name.to_string(),
            backend: string_field(&values, "backend").unwrap_or_default(),
            value_type: string_field(&values, "value_type").unwrap_or_default(),
            target_model: entity_id_field(&values, "target_model"),
        };
        self.catalog.upsert_registered(
            &[],
            &[RegisteredProperty {
                id: def.id,
                model: *model,
                name: def.name.clone(),
                backend: def.backend.clone(),
                value_type: def.value_type.clone(),
                target_model: def.target_model,
            }],
            &[],
        );
        Ok(Some(def))
    }

    /// Allocator lookup for a membership by (model, property): map first,
    /// storage on a miss (see [`Self::model_lookup_checked`]).
    async fn membership_lookup_checked(
        &self,
        model: &EntityId,
        property: &EntityId,
    ) -> Result<Option<super::catalog::MembershipDef>, RetrievalError> {
        if let Some(def) = self.catalog.membership(model, property) {
            return Ok(Some(def));
        }
        let predicate = and(field_eq_id("model", *model), field_eq_id("property", *property));
        let Some((id, values)) = self.catalog_row_by_key(model_property_collection(), predicate).await? else {
            return Ok(None);
        };
        let optional = bool_field(&values, "optional");
        let def = super::catalog::MembershipDef { id, model: *model, property: *property, optional };
        // Fold only when the flag is present: a flag-less row is TREATED as
        // optional (never defaulted, catalog.rs MembershipDef), and the
        // executor's diff arm emits the repairing follow-up either way.
        if let Some(optional) = optional {
            self.catalog.upsert_registered(&[], &[], &[RegisteredMembership { id: def.id, model: *model, property: *property, optional }]);
        }
        Ok(Some(def))
    }

    /// Fetch the catalog row matching `predicate` straight from durable
    /// storage (no map, no policy: allocator-internal, under the mutex).
    /// Returns the lowest entity id on multiple matches so repeated calls
    /// are deterministic even over historical duplicates.
    async fn catalog_row_by_key(
        &self,
        collection: CollectionId,
        predicate: ankql::ast::Predicate,
    ) -> Result<Option<(EntityId, BTreeMap<String, Option<Value>>)>, RetrievalError> {
        let storage = self.collections.get(&collection).await?;
        let selection = ankql::ast::Selection { predicate, order_by: None, limit: None };
        let mut best: Option<(EntityId, BTreeMap<String, Option<Value>>)> = None;
        for state in storage.fetch_states(&selection).await? {
            let id = state.payload.entity_id;
            if best.as_ref().is_some_and(|(b, _)| *b <= id) {
                continue;
            }
            let Some(buffer) = state.payload.state.state_buffers.0.get("lww") else {
                continue;
            };
            best = Some((id, crate::property::name_keyed(LWWBackend::from_state_buffer(buffer)?.property_values())));
        }
        Ok(best)
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
        // Same compatibility bar as the name-keyed upsert (rfc.md 5.6 as
        // amended 2026-07-10): the backend must match, and a drifted
        // value_type is admitted only when mutually castable with the
        // canonical one. The binding never mutates the bound definition.
        if found_backend != p.backend || !value_types_compatible(&found_value_type, &p.value_type) {
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
        Ok(Some((crate::property::name_keyed(backend.property_values()), head)))
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

/// Whether a declared value_type is admissible against a canonical one:
/// equal, or mutually castable per the `Value::cast_to` relation. A type
/// string this build cannot parse (a newer fleet's type) is compatible only
/// when equal.
fn value_types_compatible(canonical: &str, declared: &str) -> bool {
    canonical == declared
        || match (ValueType::from_property_str(canonical), ValueType::from_property_str(declared)) {
            (Some(a), Some(b)) => ValueType::mutually_castable(a, b),
            _ => false,
        }
}

/// The canonical-type compatibility gate (rfc.md 5.6 as amended 2026-07-10)
/// for a name-keyed upsert hit. Never mutates the found definition.
fn check_property_compat(def: &super::catalog::PropertyDef, p: &PropertyDescriptor) -> Result<(), RegistrationError> {
    if def.backend != p.backend || !value_types_compatible(&def.value_type, &p.value_type) {
        return Err(RegistrationError::NonCastable {
            collection: p.minting_collection.clone(),
            name: p.name.clone(),
            found_backend: def.backend.clone(),
            found_value_type: def.value_type.clone(),
            backend: p.backend.clone(),
            value_type: p.value_type.clone(),
        });
    }
    if def.value_type != p.value_type {
        tracing::warn!(
            "property '{}' in '{}' is canonically '{}'; this binary declares '{}' and will write and read through casts. \
             The canonical type is fixed at allocation (rfc.md 5.6); changing it is a deliberate migration (#303)",
            p.name,
            p.minting_collection,
            def.value_type,
            p.value_type
        );
    }
    Ok(())
}

fn bool_field(values: &BTreeMap<String, Option<Value>>, field: &str) -> Option<bool> {
    match values.get(field) {
        Some(Some(Value::Bool(b))) => Some(*b),
        _ => None,
    }
}

// -- allocator storage-lookup predicate builders ------------------------------

fn field_eq_str(field: &str, value: &str) -> ankql::ast::Predicate {
    ankql::ast::Predicate::Comparison {
        left: Box::new(ankql::ast::Expr::Path(ankql::ast::PathExpr { steps: vec![field.to_string()] })),
        operator: ankql::ast::ComparisonOperator::Equal,
        right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::String(value.to_string()))),
    }
}

fn field_eq_id(field: &str, id: EntityId) -> ankql::ast::Predicate {
    ankql::ast::Predicate::Comparison {
        left: Box::new(ankql::ast::Expr::Path(ankql::ast::PathExpr { steps: vec![field.to_string()] })),
        operator: ankql::ast::ComparisonOperator::Equal,
        right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::EntityId(id.to_ulid()))),
    }
}

fn and(a: ankql::ast::Predicate, b: ankql::ast::Predicate) -> ankql::ast::Predicate { ankql::ast::Predicate::And(Box::new(a), Box::new(b)) }

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
        backend.set(crate::property::PropertyKey::Name(name.to_string()), Some(value));
    }
    let operations = backend.to_operations().expect("LWW encoding of scalar values is infallible").expect("fields are non-empty");
    // Catalog collections have well-known model ids by construction (#330);
    // this helper is only ever called for them.
    let model = crate::schema::well_known_model_id(collection.as_str())
        .expect("registration events target the catalog collections, which have well-known model ids");
    proto::Event { model, entity_id, operations: proto::OperationSet(BTreeMap::from([("lww".to_string(), operations)])), parent }
}
