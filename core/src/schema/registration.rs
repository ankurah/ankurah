//! The durable-side executor for the RegisterSchema protocol operation
//!.
//!
//! Registration is an UPSERT: the executor looks each definition up by its
//! lookup key (model by source label; property by (model, name);
//! model-property membership by (model, property)), ALLOCATES a fresh
//! `EntityId::new()` -- a true ULID -- on miss, and emits ordinary events
//! through the policy-checked commit pipeline. The whole execution
//! serializes on a process-local mutex, and the executor upserts the
//! resolved definitions into the catalog map synchronously after commit,
//! BEFORE releasing that mutex, so consecutive registrations can never
//! race the reactor-fed map into double-allocation. The resolved definitions are returned to the requester via
//! `NodeResponseBody::SchemaRegistered`.
//!
//! Policy gates the execution at two complementary boundaries:
//! request authentication decides whether the principal may submit schema
//! descriptors, the resolved plan -- what this request will actually create
//! and update -- goes through `PolicyAgent::check_schema_registration` before
//! anything is emitted, and every emitted event still passes `check_event` inside the ordinary
//! commit pipeline. A durable node needs no model code to serve
//! registration: the request carries language-agnostic descriptors.
//! Idempotence is the upsert's: a repeat registration finds every key,
//! emits zero events, and returns the same ids.
//!
//! A property's (backend, value_type) is CANONICAL: fixed at allocation and
//! never changed by registration. A hit
//! whose descriptor declares a different value_type is admitted only when the
//! two types are mutually castable per `Value::cast_to` (the binary writes
//! and reads through the cast); a different backend, or a non-castable type
//! pair, refuses the registration loudly. Changing a canonical type is a
//! deliberate migration (#303), never a model-struct edit.

use std::collections::BTreeMap;

use ankurah_proto::{
    self as proto, Attested, EntityId, Membership, Operation, OperationSet, RegisterModel, RegisterProperty, RegisteredModel,
    RegisteredProperty, SystemProperty, TransactionId,
};

use crate::error::{MutationError, RetrievalError};
use crate::policy::{AccessDenied, PolicyAgent};
use crate::property::backend::{LWWBackend, PropertyBackend};
use crate::storage::StorageEngine;
use crate::value::Value;
use crate::ModelId;
use ankurah_core_types::ValueType;

use super::{model_collection, model_property_collection, property_collection};

/// A durable schema-registration request could not be completed.
#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    /// Registration was attempted on a node without durable storage.
    #[error("registration executes on durable nodes; this node is ephemeral")]
    NotDurable,
    /// The node has not established a system root or catalog epoch.
    #[error("system is not ready")]
    SystemNotReady,
    /// Strict offline registration could not consult an allocator.
    #[error(
        "collection '{0}' has never been registered on this node and no durable peer is connected; \
         connect to the system once first"
    )]
    NoDurablePeer(
        /// The source-level collection label that could not be registered.
        String,
    ),
    /// A prior registration exists, but this binary's compiled schema could
    /// not be confirmed against it without a durable peer (changed or
    /// unbound fields need the allocator).
    #[error(
        "collection '{0}' is registered, but this binary's schema for it is unconfirmed \
         and no durable peer is connected; connect to the system once first"
    )]
    UnconfirmedSchema(
        /// The source-level collection label whose binding is unconfirmed.
        String,
    ),
    /// A descriptor attempted to claim the protected system namespace.
    #[error(
        "collection '{0}' starts with the reserved prefix '{prefix}'; the system and catalog collections are never registered through this executor",
        prefix = crate::schema::RESERVED_COLLECTION_PREFIX
    )]
    ReservedCollection(
        /// The rejected source-level collection label.
        String,
    ),
    /// A membership's name reference was not declared in the same request.
    #[error("membership references property '{0}' which is not declared in this request for collection '{1}'")]
    UnresolvedPropertyRef(
        /// The unresolved property name.
        String,
        /// The model collection label containing the membership.
        String,
    ),
    /// A descriptor referenced a minting model absent from the request and catalog.
    #[error("descriptor references collection '{0}' which is neither declared in this request nor present in the catalog")]
    UnknownMintingCollection(
        /// The unknown minting-model collection label.
        String,
    ),
    /// An explicit property binding named an unknown catalog entity.
    #[error("explicit property id {property} does not exist in the catalog; explicit binding never mints")]
    ExplicitIdNotFound {
        /// The requested property entity.
        property: EntityId,
    },
    /// An explicit model binding named an unknown catalog entity.
    #[error("explicit model id {model} does not exist in the catalog; explicit binding never mints")]
    ExplicitModelIdNotFound {
        /// The requested model entity.
        model: EntityId,
    },
    /// An explicit model binding conflicts with the catalog's model label.
    #[error("explicit model id {model} is bound to label '{found_label}'; binder declares '{label}'")]
    ExplicitModelIdMismatch {
        /// The explicitly bound model entity.
        model: EntityId,
        /// The model label already stored in the catalog.
        found_label: String,
        /// The model label declared by the requester.
        label: String,
    },
    /// A declaration conflicts with an existing property's canonical backend or type.
    #[error(
        "property '{name}' in '{collection}' is canonically ({found_backend}, {found_value_type}); this binary declares ({backend}, {value_type}), which is not castable to/from the canonical type (backend must match; value types must be mutually castable per Value::cast_to). The canonical type is fixed at allocation; changing it is a deliberate migration (#303), never a model-struct edit"
    )]
    NonCastable {
        /// The property's minting-model collection label.
        collection: String,
        /// The property's registered name.
        name: String,
        /// The canonical backend already stored in the catalog.
        found_backend: String,
        /// The canonical value type already stored in the catalog.
        found_value_type: String,
        /// The backend declared by the requester.
        backend: String,
        /// The value type declared by the requester.
        value_type: String,
    },
    /// The policy agent denied the resolved registration plan, or the
    /// context could not act as a single principal.
    #[error("registration refused by policy: {0}")]
    PolicyDenied(
        /// The refusal, from either origin: the policy agent's verdict
        /// on the resolved plan, or the credential source's own refusal
        /// when it holds no session or several and so cannot name the
        /// single principal a registration acts as.
        #[from]
        AccessDenied,
    ),
    /// Committing the catalog events failed.
    #[error(transparent)]
    Mutation(
        /// The durable mutation failure.
        #[from]
        MutationError,
    ),
    /// Reading catalog or storage state failed.
    #[error(transparent)]
    Retrieval(
        /// The underlying catalog or storage read failure.
        #[from]
        RetrievalError,
    ),
}

/// Read paths propagate ensure failures as retrieval errors; the Retrieval
/// variant unwraps to its inner error rather than double-wrapping.
impl From<RegistrationError> for crate::error::RetrievalError {
    fn from(error: RegistrationError) -> Self {
        match error {
            RegistrationError::Retrieval(inner) => inner,
            other => crate::error::RetrievalError::Other(other.to_string()),
        }
    }
}

/// Write paths (Transaction::create) propagate ensure failures as mutation
/// errors, keeping the typed error as the source.
impl From<RegistrationError> for crate::error::MutationError {
    fn from(error: RegistrationError) -> Self { crate::error::MutationError::General(Box::new(error)) }
}

/// What a RegisterSchema request will ACTUALLY do, resolved by the
/// registration executor under the allocation mutex and handed to
/// [`crate::policy::PolicyAgent::check_schema_registration`] before any
/// event is emitted, so an agent can judge real creations and metadata
/// changes without performing its own catalog lookups. Core-side only;
/// never crosses the wire.
#[derive(Debug, Default)]
pub struct RegistrationPlan {
    /// Model entities this request will CREATE, with their would-be
    /// allocated ids (minted, not yet committed).
    pub creates_models: Vec<(EntityId, RegisterModel)>,
    /// Property entities this request will CREATE, with their would-be
    /// allocated ids.
    pub creates_properties: Vec<(EntityId, RegisterProperty)>,
    /// Model-property memberships this request will CREATE, fully resolved.
    pub creates_memberships: Vec<PlannedModelPropertyMembership>,
    /// Metadata follow-ups this request will write on EXISTING entities
    /// (display-name changes including rename-hint applications, target
    /// retargets, membership `optional` flips).
    pub updates: Vec<PlannedUpdate>,
    /// Definitions that resolved to existing entities with no changes:
    /// pure no-ops, listed for context.
    pub existing: Vec<EntityId>,
}

impl RegistrationPlan {
    /// Whether the plan writes anything at all (a re-registration of
    /// unchanged definitions is a pure no-op and skips the policy verb).
    pub fn is_noop(&self) -> bool {
        self.creates_models.is_empty()
            && self.creates_properties.is_empty()
            && self.creates_memberships.is_empty()
            && self.updates.is_empty()
    }
}

/// A model-property membership creation in a [`RegistrationPlan`], resolved
/// to ids.
#[derive(Debug, Clone)]
pub struct PlannedModelPropertyMembership {
    /// The durable identity assigned to the membership entity.
    pub id: EntityId,
    /// The model receiving the property.
    pub model: EntityId,
    /// The property admitted to the model.
    pub property: EntityId,
    /// Whether entities of the model may omit the property.
    pub optional: bool,
}

/// A metadata follow-up on an existing catalog entity, in a
/// [`RegistrationPlan`].
#[derive(Debug, Clone)]
pub struct PlannedUpdate {
    /// Which catalog collection the entity lives in.
    pub collection: crate::ModelId,
    /// The durable catalog entity to update.
    pub entity: EntityId,
    /// The system field whose metadata value changes.
    pub field: String,
    /// The current catalog value, when one exists.
    pub from: Option<crate::value::Value>,
    /// The requested catalog value. `None` means the field will be cleared.
    pub to: Option<crate::value::Value>,
}

impl<SE, PA> super::catalog::CatalogManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Execute a RegisterSchema request as the system's allocator: upsert
    /// every model by its label and every nested property within its
    /// model's membership set, allocate fresh ids for misses, emit ordinary
    /// creation events and difference-only follow-ups through the
    /// policy-checked pipeline, and return the resolved tree.
    pub async fn register_schema(
        &self,
        cdata: &PA::ContextData,
        models: Vec<RegisterModel>,
    ) -> Result<Vec<RegisteredModel>, RegistrationError> {
        let node = self.node().ok_or(RegistrationError::SystemNotReady)?;
        if !node.durable {
            return Err(RegistrationError::NotDurable);
        }
        if node.system.root().is_none() {
            return Err(RegistrationError::SystemNotReady);
        }
        let registration_validity = self.registration_validity().ok_or(RegistrationError::SystemNotReady)?;
        // Labels are schema lookup keys, not runtime model identities.
        // Reserved collections are refused before policy is even asked: the
        // catalog and system collections route by name and have no catalog
        // model entities of their own, so a registration naming one -- as a
        // model label or a property's target -- could only route ordinary
        // traffic into a protected collection.
        {
            let mut named: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
            for m in &models {
                named.insert(m.label.as_str());
                for p in &m.properties {
                    if let Some(target) = &p.target_label {
                        named.insert(target.as_str());
                    }
                }
            }
            for label in &named {
                if label.starts_with(crate::schema::RESERVED_COLLECTION_PREFIX) {
                    return Err(RegistrationError::ReservedCollection(label.to_string()));
                }
            }
        }
        // The catalog map is the executor's primary lookup source; wait for
        // the warm so the common case looks up against a full map. The map
        // is NOT trusted alone on a miss: every miss double-checks durable
        // storage before minting (the *_lookup_checked helpers), so a
        // lagging or cold map can never fork identity.
        if !self.wait_catalog_ready_if_current(&registration_validity).await {
            return Err(RegistrationError::SystemNotReady);
        }
        let _registration_lease = registration_validity.try_acquire().ok_or(RegistrationError::SystemNotReady)?;

        // Executor discipline: the whole upsert -- lookups, allocation,
        // commit, and the synchronous map update -- serializes on the
        // allocator mutex, so consecutive registrations observe each other.
        let _allocator = self.lock_allocator().await;

        let mut plan = RegistrationPlan::default();
        // Creation events carry the FULL definition state; follow-ups carry
        // only fields that differ, parented at the entity's current head so
        // LWW recency, rather than a concurrent tiebreak, decides.
        let mut events: Vec<Attested<proto::Event>> = Vec::new();
        let mut push = |event: proto::Event| events.push(Attested::opt(event, None));

        // -- pass 1: model shells, so nested target references and
        //    same-request cross-references resolve ---------------------------
        let mut model_ids: BTreeMap<String, (EntityId, usize)> = BTreeMap::new();
        let mut out_models: Vec<RegisteredModel> = Vec::new();
        for m in &models {
            // A duplicate label in ONE request must not re-mint: the first
            // occurrence resolves the model; later occurrences reuse it (their
            // property entries still attach below).
            if model_ids.contains_key(&m.label) {
                continue;
            }
            let (model_id, resolved_name) = match m.explicit_id {
                Some(id) => {
                    // Explicit binding: verify, never mint, never mutate the
                    // bound entity's fields; the catalog's display name stands.
                    let values = self.verify_explicit_model_binding(id, m).await?;
                    let name = string_field(&values, "name").unwrap_or_else(|| m.label.clone());
                    plan.existing.push(id);
                    (id, name)
                }
                None => match self.model_lookup_checked(&m.label).await? {
                    Some(def) => {
                        // Display names follow the most recent registration;
                        // emit only on difference.
                        if def.name != m.name {
                            let (_, head) = self
                                .catalog_entity_snapshot(def.id, &model_collection())
                                .await?
                                .ok_or_else(|| RetrievalError::Other(format!("catalog map holds model {} absent from storage", def.id)))?;
                            plan.updates.push(PlannedUpdate {
                                collection: model_collection(),
                                entity: def.id,
                                field: "name".into(),
                                from: Some(Value::String(def.name.clone())),
                                to: Some(Value::String(m.name.clone())),
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
                            vec![("label", Value::String(m.label.clone())), ("name", Value::String(m.name.clone()))],
                        ));
                        (id, m.name.clone())
                    }
                },
            };
            model_ids.insert(m.label.clone(), (model_id, out_models.len()));
            out_models.push(RegisteredModel { id: model_id, label: m.label.clone(), name: resolved_name, properties: Vec::new() });
        }

        // Resolve a label reference to a model id, allocating a stub model
        // on full miss (target-model references resolve executor-side).
        macro_rules! resolve_model {
            ($label:expr) => {{
                let l: &str = $label;
                match model_ids.get(l) {
                    Some((id, _)) => *id,
                    None => match self.model_lookup_checked(l).await? {
                        Some(def) => {
                            model_ids.insert(l.to_string(), (def.id, out_models.len()));
                            out_models.push(RegisteredModel { id: def.id, label: def.label, name: def.name, properties: Vec::new() });
                            def.id
                        }
                        None => {
                            let id = EntityId::new();
                            let stub =
                                RegisterModel { label: l.to_string(), name: l.to_string(), explicit_id: None, properties: Vec::new() };
                            plan.creates_models.push((id, stub));
                            push(creation(
                                model_collection(),
                                id,
                                vec![("label", Value::String(l.to_string())), ("name", Value::String(l.to_string()))],
                            ));
                            model_ids.insert(l.to_string(), (id, out_models.len()));
                            out_models.push(RegisteredModel { id, label: l.to_string(), name: l.to_string(), properties: Vec::new() });
                            id
                        }
                    },
                }
            }};
        }

        // -- pass 2: each model entry's properties. Nesting IS the
        //    membership assertion: every entry ensures a (model, property)
        //    membership, whether the property is minted here, found in the
        //    model's membership set by name, or explicitly shared by id. ----
        let mut property_ids: BTreeMap<(EntityId, String), (EntityId, String, String)> = BTreeMap::new();
        let mut membership_seen: std::collections::BTreeSet<(EntityId, EntityId)> = std::collections::BTreeSet::new();
        for m in &models {
            let (model_id, out_index) = *model_ids.get(&m.label).expect("pass 1 resolved every model label");
            for p in &m.properties {
                // Duplicate (model, name) in one request: the first
                // occurrence fixes the resolution; a later duplicate
                // coalesces onto it under the same compatibility bar as a
                // catalog hit.
                if let Some((_, canon_backend, canon_vt)) = property_ids.get(&(model_id, p.name.clone())) {
                    if p.backend != *canon_backend || !value_types_compatible(canon_vt, &p.value_type) {
                        return Err(RegistrationError::NonCastable {
                            collection: m.label.clone(),
                            name: p.name.clone(),
                            found_backend: canon_backend.clone(),
                            found_value_type: canon_vt.clone(),
                            backend: p.backend.clone(),
                            value_type: p.value_type.clone(),
                        });
                    }
                    continue;
                }

                if let Some(id) = p.explicit_id {
                    // Explicit binding: verify (backend, value_type), never
                    // mint the property; the bound entity's name and metadata
                    // are authoritative. The nested position still asserts
                    // THIS model's membership, which is how a property minted
                    // elsewhere is intentionally shared.
                    let values = self.verify_explicit_binding(id, &m.label, p).await?;
                    plan.existing.push(id);
                    let backend = string_field(&values, "backend").unwrap_or_else(|| p.backend.clone());
                    let value_type = string_field(&values, "value_type").unwrap_or_else(|| p.value_type.clone());
                    let membership_id = self.ensure_membership(&mut plan, &mut push, model_id, id, p.optional).await?;
                    property_ids.insert((model_id, p.name.clone()), (id, backend.clone(), value_type.clone()));
                    out_models[out_index].properties.push(RegisteredProperty {
                        id,
                        membership_id,
                        name: string_field(&values, "name").unwrap_or_else(|| p.name.clone()),
                        backend,
                        value_type,
                        target_model: entity_id_field(&values, "target_model"),
                        minted_for: entity_id_field(&values, "minted_for"),
                        optional: p.optional,
                    });
                    continue;
                }

                // Resolve the target-model reference first so both the
                // create and the diff paths can use it.
                let target = match &p.target_label {
                    Some(tl) => Some(resolve_model!(tl)),
                    None => None,
                };

                // Lookup scope is the model's MEMBERSHIP SET by current name
                // (a shared property resolves here regardless of where it
                // was minted; minted_for is provenance, never a key), with
                // the rename hint consulted only when the current name
                // misses.
                let current = self.member_property_lookup_checked(&model_id, &p.name).await?;
                let renamed = match (&current, &p.renamed_from) {
                    (None, Some(old)) => self.member_property_lookup_checked(&model_id, old).await?,
                    _ => None,
                };

                // The canonical-type compatibility gate: a hit never mutates
                // (backend, value_type) and never forks a second identity.
                // Refuses loudly on a different backend or a non-castable
                // type pair; a castable drift is admitted (the binary writes
                // and reads through the cast) and the response carries the
                // CANONICAL types so the requester's map holds its cast
                // target.
                let canonical = current.as_ref().or(renamed.as_ref()).map(|(def, _)| (def.backend.clone(), def.value_type.clone()));
                if let Some((def, _)) = current.as_ref().or(renamed.as_ref()) {
                    check_property_compat(def, &m.label, p)?;
                }

                let (property_id, membership_id) = match (&current, &renamed) {
                    (Some((def, membership)), _) => {
                        // Plain hit: name matches by construction; only the
                        // target reference can differ.
                        let mut fields: Vec<(&str, Option<Value>)> = Vec::new();
                        if def.target_model != target {
                            let target_value = target.map(Value::EntityId);
                            plan.updates.push(PlannedUpdate {
                                collection: property_collection(),
                                entity: def.id,
                                field: "target_model".into(),
                                from: def.target_model.map(Value::EntityId),
                                to: target_value.clone(),
                            });
                            fields.push(("target_model", target_value));
                        }
                        if fields.is_empty() {
                            plan.existing.push(def.id);
                        } else {
                            let (_, head) = self.catalog_entity_snapshot(def.id, &property_collection()).await?.ok_or_else(|| {
                                RetrievalError::Other(format!("catalog map holds property {} absent from storage", def.id))
                            })?;
                            push(follow_up_patch(property_collection(), def.id, head, fields));
                        }
                        let membership_id = self.ensure_membership_from(&mut plan, &mut push, membership.clone(), p.optional).await?;
                        (def.id, membership_id)
                    }
                    (None, Some((def, membership))) => {
                        // The rename hint applies: update `name` on the
                        // existing lineage, plus any target change, in one
                        // follow-up.
                        let mut fields: Vec<(&str, Option<Value>)> = vec![("name", Some(Value::String(p.name.clone())))];
                        plan.updates.push(PlannedUpdate {
                            collection: property_collection(),
                            entity: def.id,
                            field: "name".into(),
                            from: Some(Value::String(def.name.clone())),
                            to: Some(Value::String(p.name.clone())),
                        });
                        if def.target_model != target {
                            let target_value = target.map(Value::EntityId);
                            plan.updates.push(PlannedUpdate {
                                collection: property_collection(),
                                entity: def.id,
                                field: "target_model".into(),
                                from: def.target_model.map(Value::EntityId),
                                to: target_value.clone(),
                            });
                            fields.push(("target_model", target_value));
                        }
                        let (_, head) = self
                            .catalog_entity_snapshot(def.id, &property_collection())
                            .await?
                            .ok_or_else(|| RetrievalError::Other(format!("catalog map holds property {} absent from storage", def.id)))?;
                        push(follow_up_patch(property_collection(), def.id, head, fields));
                        let membership_id = self.ensure_membership_from(&mut plan, &mut push, membership.clone(), p.optional).await?;
                        (def.id, membership_id)
                    }
                    (None, None) => {
                        // Miss: allocate the property AND its membership. The
                        // creation events carry the full definition state.
                        let id = EntityId::new();
                        plan.creates_properties.push((id, p.clone()));
                        let mut fields: Vec<(&str, Value)> = vec![
                            ("minted_for", Value::EntityId(model_id)),
                            ("name", Value::String(p.name.clone())),
                            ("backend", Value::String(p.backend.clone())),
                            ("value_type", Value::String(p.value_type.clone())),
                        ];
                        if let Some(t) = target {
                            fields.push(("target_model", Value::EntityId(t)));
                        }
                        push(creation(property_collection(), id, fields));
                        let membership_id = self.ensure_membership(&mut plan, &mut push, model_id, id, p.optional).await?;
                        (id, membership_id)
                    }
                };
                membership_seen.insert((model_id, property_id));

                let (backend, value_type) = canonical.unwrap_or_else(|| (p.backend.clone(), p.value_type.clone()));
                let minted_for = match (&current, &renamed) {
                    (Some((def, _)), _) | (None, Some((def, _))) => def.minted_for,
                    (None, None) => Some(model_id),
                };
                property_ids.insert((model_id, p.name.clone()), (property_id, backend.clone(), value_type.clone()));
                out_models[out_index].properties.push(RegisteredProperty {
                    id: property_id,
                    membership_id,
                    name: p.name.clone(),
                    backend,
                    value_type,
                    target_model: target,
                    minted_for,
                    optional: p.optional,
                });
            }
        }
        let _ = membership_seen;

        // A re-registration of unchanged definitions is a pure no-op:
        // nothing to gate, nothing to commit, nothing to relay -- but the
        // requester still gets the full resolved tree.
        if plan.is_noop() {
            return Ok(out_models);
        }

        // The exists-aware policy gate judges the resolved plan before
        // anything is emitted; refusal fails the request before any write.
        // Underneath, check_event still gates each event individually inside
        // the commit pipeline, and the batch is NOT transactional: a
        // mid-batch event denial leaves the earlier catalog events durable
        // (maintainer ruling 2026-07-06: registration does not need to be
        // atomic; #313 tracks the transactional upgrade). Identity survives
        // such partials because every allocator lookup double-checks storage
        // on a map miss, so a retry converges on the stored ids instead of
        // re-minting.
        node.policy_agent.check_schema_registration(&node, cdata, &plan)?;

        // The ordinary remote-commit pipeline: policy check (check_event),
        // attest, persist, apply, reactor notify.
        node.commit_remote_transaction(cdata, TransactionId::new(), events).await?;

        // Synchronous map upsert BEFORE the allocator mutex releases: the
        // next registration in line must observe these allocations even if
        // the reactor has not delivered them yet.
        self.upsert_registered(&out_models);

        Ok(out_models)
    }

    /// Ensure the (model, property) membership exists with `optional`: reuse
    /// and difference-patch a stored one, or mint. Returns the membership id.
    async fn ensure_membership(
        &self,
        plan: &mut RegistrationPlan,
        push: &mut impl FnMut(proto::Event),
        model: EntityId,
        property: EntityId,
        optional: bool,
    ) -> Result<EntityId, RegistrationError> {
        match self.membership_lookup_checked(&model, &property).await? {
            Some(def) => self.ensure_membership_from(plan, push, def, optional).await,
            None => {
                let id = EntityId::new();
                plan.creates_memberships.push(PlannedModelPropertyMembership { id, model, property, optional });
                push(creation(
                    model_property_collection(),
                    id,
                    vec![("model", Value::EntityId(model)), ("property", Value::EntityId(property)), ("optional", Value::Bool(optional))],
                ));
                Ok(id)
            }
        }
    }

    /// Difference-patch an already-resolved membership's `optional` flag.
    async fn ensure_membership_from(
        &self,
        plan: &mut RegistrationPlan,
        push: &mut impl FnMut(proto::Event),
        def: super::catalog::ModelPropertyMembershipDef,
        optional: bool,
    ) -> Result<EntityId, RegistrationError> {
        if def.optional != Some(optional) {
            let (_, head) = self
                .catalog_entity_snapshot(def.id, &model_property_collection())
                .await?
                .ok_or_else(|| RetrievalError::Other(format!("catalog map holds membership {} absent from storage", def.id)))?;
            plan.updates.push(PlannedUpdate {
                collection: model_property_collection(),
                entity: def.id,
                field: "optional".into(),
                from: def.optional.map(Value::Bool),
                to: Some(Value::Bool(optional)),
            });
            push(follow_up(model_property_collection(), def.id, head, vec![("optional", Value::Bool(optional))]));
        } else {
            plan.existing.push(def.id);
        }
        Ok(def.id)
    }

    /// Allocator lookup for a model: the catalog map first, then durable
    /// storage on a miss. The in-memory map can lag
    /// durable truth -- a partial-commit abort skips the post-commit fold,
    /// and non-sled engines warm lazily (#310) -- and minting from a cold
    /// map would fork identity for a key that already exists. A storage hit
    /// is folded into the map so the rest of the request (and the next one)
    /// sees it; ordinary first sightings miss both and pay one bounded
    /// fetch under the allocator mutex.
    async fn model_lookup_checked(&self, label: &str) -> Result<Option<super::catalog::ModelDef>, RetrievalError> {
        if let Some(def) = self.model_by_label(label) {
            return Ok(Some(def));
        }
        let Some((id, values)) = self.catalog_row_by_key(model_collection(), field_eq_str("label", label)).await? else {
            return Ok(None);
        };
        let def = super::catalog::ModelDef {
            id,
            label: label.to_string(),
            name: string_field(&values, "name").unwrap_or_else(|| label.to_string()),
        };
        self.upsert_registered(&[RegisteredModel { id: def.id, label: def.label.clone(), name: def.name.clone(), properties: Vec::new() }]);
        Ok(Some(def))
    }

    /// Allocator lookup for a property by name WITHIN a model's membership
    /// set: map first, storage on a miss (see
    /// [`Self::model_lookup_checked`]). Membership is the scope -- a shared
    /// property resolves regardless of where it was minted -- so the storage
    /// path walks the model's membership rows and matches each bound
    /// property's current name. Returns the property with the membership
    /// that binds it.
    async fn member_property_lookup_checked(
        &self,
        model: &EntityId,
        name: &str,
    ) -> Result<Option<(super::catalog::PropertyDef, super::catalog::ModelPropertyMembershipDef)>, RetrievalError> {
        if let Some(def) = self.property_by_name(model, name) {
            if let Some(membership) = self.membership(model, &def.id) {
                return Ok(Some((def, membership)));
            }
        }
        let node = self.node().ok_or_else(|| RetrievalError::Other("node dropped during catalog lookup".to_owned()))?;
        let selection = ankql::ast::Selection { predicate: field_eq_id("model", *model), order_by: None, limit: None };
        let mut rows: Vec<proto::Attested<proto::EntityState>> =
            node.collections.get(&catalog_collection_id(model_property_collection())).await?.fetch_states(&selection).await?;
        // Lowest membership id first, so repeated calls are deterministic
        // even over historical duplicates.
        rows.sort_by_key(|state| state.payload.entity_id);
        for row in rows {
            let membership_id = row.payload.entity_id;
            let Some(buffer) = row.payload.state.state_buffers.0.get("lww") else { continue };
            let values = LWWBackend::from_state_buffer(buffer)?.property_values();
            let Some(property_id) = entity_id_field(&values, "property") else { continue };
            let Some(prop_values) = self.catalog_entity_values(property_id, &property_collection()).await? else { continue };
            if string_field(&prop_values, "name").as_deref() != Some(name) {
                continue;
            }
            let def = super::catalog::PropertyDef {
                id: property_id,
                minted_for: entity_id_field(&prop_values, "minted_for"),
                name: name.to_string(),
                backend: string_field(&prop_values, "backend").unwrap_or_default(),
                value_type: string_field(&prop_values, "value_type").unwrap_or_default(),
                target_model: entity_id_field(&prop_values, "target_model"),
            };
            let membership = super::catalog::ModelPropertyMembershipDef {
                id: membership_id,
                model: *model,
                property: property_id,
                optional: bool_field(&values, "optional"),
            };
            return Ok(Some((def, membership)));
        }
        Ok(None)
    }

    /// Allocator lookup for a membership by (model, property): map first,
    /// storage on a miss (see [`Self::model_lookup_checked`]).
    async fn membership_lookup_checked(
        &self,
        model: &EntityId,
        property: &EntityId,
    ) -> Result<Option<super::catalog::ModelPropertyMembershipDef>, RetrievalError> {
        if let Some(def) = self.membership(model, property) {
            return Ok(Some(def));
        }
        let predicate = and(field_eq_id("model", *model), field_eq_id("property", *property));
        let Some((id, values)) = self.catalog_row_by_key(model_property_collection(), predicate).await? else {
            return Ok(None);
        };
        let optional = bool_field(&values, "optional");
        // No map fold here: memberships fold with their full registered tree
        // (a flag-less row is TREATED as optional, never defaulted; the
        // executor's diff arm emits the repairing follow-up either way).
        Ok(Some(super::catalog::ModelPropertyMembershipDef { id, model: *model, property: *property, optional }))
    }

    /// Fetch the catalog row matching `predicate` straight from durable
    /// storage (no map, no policy: allocator-internal, under the mutex).
    /// Returns the lowest entity id on multiple matches so repeated calls
    /// are deterministic even over historical duplicates.
    async fn catalog_row_by_key(
        &self,
        collection: ModelId,
        predicate: ankql::ast::Predicate,
    ) -> Result<Option<(EntityId, BTreeMap<String, Option<Value>>)>, RetrievalError> {
        let selection = ankql::ast::Selection { predicate, order_by: None, limit: None };
        let mut best: Option<(EntityId, BTreeMap<String, Option<Value>>)> = None;
        let node = self.node().ok_or_else(|| RetrievalError::Other("node dropped during catalog lookup".to_owned()))?;
        for state in node.collections.get(&catalog_collection_id(collection)).await?.fetch_states(&selection).await? {
            let id = state.payload.entity_id;
            if best.as_ref().is_some_and(|(b, _)| *b <= id) {
                continue;
            }
            let Some(buffer) = state.payload.state.state_buffers.0.get("lww") else {
                continue;
            };
            best = Some((id, LWWBackend::from_state_buffer(buffer)?.property_values()));
        }
        Ok(best)
    }

    /// An explicit binding references a definition authored
    /// elsewhere. Absence is a hard failure (cold start), and a
    /// (backend, value_type) mismatch means the definition was retyped:
    /// breaking for binders BY DESIGN. Returns the bound entity's current
    /// values for response building.
    async fn verify_explicit_binding(
        &self,
        id: EntityId,
        model_label: &str,
        p: &RegisterProperty,
    ) -> Result<BTreeMap<String, Option<Value>>, RegistrationError> {
        let Some(values) = self.catalog_entity_values(id, &property_collection()).await? else {
            return Err(RegistrationError::ExplicitIdNotFound { property: id });
        };
        let get_string = |field: &str| match values.get(field) {
            Some(Some(Value::String(s))) => s.clone(),
            _ => String::new(),
        };
        let (found_backend, found_value_type) = (get_string("backend"), get_string("value_type"));
        // Same compatibility bar as the name-keyed upsert: the backend must match, and a drifted
        // value_type is admitted only when mutually castable with the
        // canonical one. The binding never mutates the bound definition.
        if found_backend != p.backend || !value_types_compatible(&found_value_type, &p.value_type) {
            return Err(RegistrationError::NonCastable {
                collection: model_label.to_string(),
                name: p.name.clone(),
                found_backend,
                found_value_type,
                backend: p.backend.clone(),
                value_type: p.value_type.clone(),
            });
        }
        Ok(values)
    }

    /// An explicit model binding references a model entity
    /// authored elsewhere. Absence is a hard failure (never mints), and a
    /// collection mismatch means the binder points at the wrong contract.
    /// Returns the bound entity's current values for response building.
    async fn verify_explicit_model_binding(
        &self,
        id: EntityId,
        m: &RegisterModel,
    ) -> Result<BTreeMap<String, Option<Value>>, RegistrationError> {
        let Some((values, _)) = self.catalog_entity_snapshot(id, &model_collection()).await? else {
            return Err(RegistrationError::ExplicitModelIdNotFound { model: id });
        };
        let found_label = string_field(&values, "label").unwrap_or_default();
        if found_label != m.label {
            return Err(RegistrationError::ExplicitModelIdMismatch { model: id, found_label, label: m.label.clone() });
        }
        Ok(values)
    }

    /// Read a catalog entity's LWW values and head straight from storage
    /// (catalog entities are system models: raw backend access, never a
    /// View). `None` when the entity does not exist yet.
    async fn catalog_entity_snapshot(
        &self,
        id: EntityId,
        expected_model: &ModelId,
    ) -> Result<Option<(BTreeMap<String, Option<Value>>, proto::Clock)>, RetrievalError> {
        let node = self.node().ok_or_else(|| RetrievalError::Other("node dropped during catalog lookup".to_owned()))?;
        let state = match node.collections.get(&catalog_collection_id(*expected_model)).await?.get_state(id).await {
            Ok(state) => state,
            Err(RetrievalError::EntityNotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        // Per-collection storage already scopes the read; keep the routing
        // check anyway so a mis-filed id reads as absent rather than as a
        // definition of the wrong kind.
        if state.payload.collection != catalog_collection_id(*expected_model) {
            return Ok(None);
        }
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
        id: EntityId,
        expected_model: &ModelId,
    ) -> Result<Option<BTreeMap<String, Option<Value>>>, RetrievalError> {
        Ok(self.catalog_entity_snapshot(id, expected_model).await?.map(|(values, _)| values))
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
pub(crate) fn value_types_compatible(canonical: &str, declared: &str) -> bool {
    canonical == declared
        || match (ValueType::from_property_str(canonical), ValueType::from_property_str(declared)) {
            (Some(a), Some(b)) => ValueType::mutually_castable(a, b),
            _ => false,
        }
}

/// The canonical-type compatibility gate
/// for a name-keyed upsert hit. Never mutates the found definition.
fn check_property_compat(def: &super::catalog::PropertyDef, model_label: &str, p: &RegisterProperty) -> Result<(), RegistrationError> {
    if def.backend != p.backend || !value_types_compatible(&def.value_type, &p.value_type) {
        return Err(RegistrationError::NonCastable {
            collection: model_label.to_string(),
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
             The canonical type is fixed at allocation; changing it is a deliberate migration (#303)",
            p.name,
            model_label,
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
//
// These build the RESOLVED form directly. The allocator reads catalog rows
// straight from storage (`catalog_row_by_key`), bypassing catalog-backed name
// resolution because these lookups bootstrap the catalog that resolution
// would read. A storage engine can only address a property by
// identity and rejects anything else (`ankql::ast::Selection::check`), so these
// predicates must arrive resolved on their own. Catalog collections are frozen
// and mint no property-definition ids, so their fields are `System` properties,
// named through the same `system_property` decision the systemize pass uses.

fn field_eq(field: &str, value: ankql::ast::Literal) -> ankql::ast::Predicate {
    ankql::ast::Predicate::Comparison {
        left: Box::new(ankql::ast::Expr::Path(ankql::ast::PathExpr::simple(field))),
        operator: ankql::ast::ComparisonOperator::Equal,
        right: Box::new(ankql::ast::Expr::Literal(value)),
    }
}

fn field_eq_str(field: &str, value: &str) -> ankql::ast::Predicate { field_eq(field, ankql::ast::Literal::String(value.to_string())) }

fn field_eq_id(field: &str, id: EntityId) -> ankql::ast::Predicate { field_eq(field, ankql::ast::Literal::EntityId(id.to_ulid())) }

fn and(a: ankql::ast::Predicate, b: ankql::ast::Predicate) -> ankql::ast::Predicate { ankql::ast::Predicate::And(Box::new(a), Box::new(b)) }

fn entity_id_field(values: &BTreeMap<String, Option<Value>>, field: &str) -> Option<EntityId> {
    match values.get(field) {
        Some(Some(Value::EntityId(id))) => Some(*id),
        _ => None,
    }
}

/// The storage collection for a catalog model: the event's routing
/// materialization in this write-only phase, always derived from the same
/// model the membership operation asserts.
fn catalog_collection_id(model: ModelId) -> proto::CollectionId {
    match model {
        ModelId::System(system) => proto::CollectionId::fixed_name(crate::schema::system_collection_label(system)),
        ModelId::EntityId(_) => unreachable!("catalog collections are system models"),
    }
}

/// A creation event: full definition state, empty parent clock. Ordinary
/// in every respect.
/// The membership operation is the authority for the entity's model; the
/// event's collection field is the routing materialization of the same
/// fact.
fn creation(model: ModelId, entity_id: EntityId, fields: Vec<(&str, Value)>) -> proto::Event {
    let mut event = follow_up(model, entity_id, proto::Clock::default(), fields);
    event.operations.push(Operation::Membership(Membership::Add(model)));
    event
}

/// A follow-up event carrying changed metadata, parented at the entity's
/// current head. It must descend the metadata it supersedes so LWW recency
/// decides, not the concurrent tiebreak.
fn follow_up(model: ModelId, entity_id: EntityId, parent: proto::Clock, fields: Vec<(&str, Value)>) -> proto::Event {
    follow_up_patch(model, entity_id, parent, fields.into_iter().map(|(name, value)| (name, Some(value))).collect())
}

/// A metadata follow-up that may clear fields as well as replace them.
fn follow_up_patch(model: ModelId, entity_id: EntityId, parent: proto::Clock, fields: Vec<(&str, Option<Value>)>) -> proto::Event {
    let backend = LWWBackend::new();
    for (name, value) in fields {
        let property = SystemProperty::from_name(name).expect("catalog event fields are closed SystemProperty variants");
        backend.set(property.to_string(), value);
    }
    let operations = backend.to_operations().expect("LWW encoding of scalar values is infallible").expect("fields are non-empty");
    proto::Event {
        collection: catalog_collection_id(model),
        entity_id,
        operations: OperationSet::from_backends(BTreeMap::from([("lww".to_string(), operations)])),
        parent,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selection(predicate: ankql::ast::Predicate) -> ankql::ast::Selection {
        ankql::ast::Selection { predicate, order_by: None, limit: None }
    }
}
