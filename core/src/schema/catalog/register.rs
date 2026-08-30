use std::collections::BTreeMap;

use ankql::ast::{Parsed, Resolved, Selection};
use ankurah_core_types::{Value, ValueType};
use ankurah_proto::{self as proto, EntityId, ModelId, PropertyId, RegisterModel, RegisterProperty, RegisteredModel, RegisteredProperty};

use super::resolver::{resolve_selection, resolve_without_model, DescriptorResolver};
use super::{CatalogManager, SysModelPropertyRow, SysModelRow, SysModelRowView, SysPropertyRow};
use crate::context::Context;
use crate::error::RetrievalError;
use crate::model::View;
use crate::node::Node;
use crate::policy::PolicyAgent;
use crate::schema::compiled::parse_explicit_id;
use crate::schema::registration::{PlannedModelPropertyMembership, PlannedUpdate, RegistrationError, RegistrationPlan};
use crate::schema::{model_collection, model_property_collection, property_collection, ModelStructDescriptor, SchemaEpoch, StructProperty};
use crate::storage::StorageEngine;
use crate::transaction::Transaction;

/// The two sides of one registration, on whichever party is registering: the
/// definition to register (read once at entry) and the destination its
/// resolution lands in (written exactly once, on success). A compiled
/// declaration registering locally is [`DescriptorRegistrant`] -- its
/// resolution binds the descriptor's cells; a wire request served for a
/// remote requester is [`WireRegistrant`] -- its resolution accumulates as
/// the `SchemaRegistered` response.
pub trait Registrant {
    /// The definition to register, in the request vocabulary: one model with
    /// its properties nested, id-free except explicit bindings.
    fn definition(&self) -> RegisterModel;
    /// Receive the resolved definition -- each property echoing its request
    /// entry's `build_id`. Called exactly once, after the registration
    /// transaction commits (or when the upsert proves a pure no-op).
    fn bind(&mut self, model: RegisteredModel) -> Result<(), RegistrationError>;
}

/// A compiled declaration registering itself: the definition is the
/// descriptor's request form, and the resolution binds the descriptor's
/// cells under the caller-snapshotted epoch.
pub struct DescriptorRegistrant {
    pub schema: &'static ModelStructDescriptor,
    pub epoch: SchemaEpoch,
}

impl Registrant for DescriptorRegistrant {
    fn definition(&self) -> RegisterModel { RegisterModel::from(self.schema) }
    fn bind(&mut self, model: RegisteredModel) -> Result<(), RegistrationError> { bind_registered(self.schema, &model, self.epoch) }
}

/// A wire request being served for a remote requester: the definition is the
/// request itself, and the resolution accumulates as the response the
/// requester binds its own cells from.
pub struct WireRegistrant {
    request: RegisterModel,
    response: Option<RegisteredModel>,
}

impl WireRegistrant {
    pub fn new(request: RegisterModel) -> Self { Self { request, response: None } }
    /// The accumulated `SchemaRegistered` payload; `Some` exactly when the
    /// registration succeeded.
    pub fn into_response(self) -> Option<RegisteredModel> { self.response }
}

impl Registrant for WireRegistrant {
    fn definition(&self) -> RegisterModel { self.request.clone() }
    fn bind(&mut self, model: RegisteredModel) -> Result<(), RegistrationError> {
        self.response = Some(model);
        Ok(())
    }
}

/// The node-facing half of the manager: registration, compiled-declaration
/// binding, and descriptor-based selection resolution. Generic over the node
/// because registration acts as a principal and may cross the wire; the
/// manager itself stays erased.
impl CatalogManager {
    /// Register one model as the system's allocator, as the principal `cdata`
    /// names: upsert the model by its label and every nested property within
    /// its membership set, mint ids for misses, and write the differences
    /// through one transaction. The registrant supplies the definition and
    /// receives the resolution ([`Registrant`]); this one method serves both
    /// the RegisterSchema RPC and local first use on the durable node.
    pub async fn register_schema<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        cdata: &PA::ContextData,
        registrant: &mut impl Registrant,
    ) -> Result<(), RegistrationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        if !node.durable {
            return Err(RegistrationError::NotDurable);
        }
        // Every catalog entity this allocator mints binds the system root into
        // its id, so the root must already exist.
        if node.system.root_id().is_none() {
            return Err(RegistrationError::SystemNotReady);
        }
        let model = registrant.definition();
        // Reserved collections route by name and have no catalog model entities
        // of their own, so a registration naming one -- as the model label or as
        // a property's target -- is refused before policy is even asked.
        for label in std::iter::once(&model.label).chain(model.properties.iter().filter_map(|p| p.target_label.as_ref())) {
            if label.starts_with(crate::schema::RESERVED_COLLECTION_PREFIX) {
                return Err(RegistrationError::ReservedCollection(label.clone()));
            }
        }
        // The signals are the executor's only lookup source, so wait for their
        // first read before consulting them.
        self.wait_ready().await?;

        // Executor discipline: the whole upsert -- lookups, minting, and the
        // commit (whose reactor notification updates the catalog queries before
        // it returns) -- serializes here, so consecutive registrations observe
        // each other through the signals.
        let _allocator = self.allocator.lock().await;
        let ctx = Context::new(Node::clone(node), cdata.clone());
        let privileged = node.privileged_context();

        let trx = privileged.begin();
        let mut plan = RegistrationPlan::default();

        // -- the model shell ------------------------------------------------
        let (model_id, name) = match model.explicit_id {
            // Explicit binding: verify, never mint, never mutate the bound
            // entity's fields; the catalog's display name stands.
            Some(id) => {
                let row = self.model_row(&ctx, id).await?.ok_or(RegistrationError::ExplicitModelIdNotFound { model: id })?;
                if row.label != model.label {
                    return Err(RegistrationError::ExplicitModelIdMismatch {
                        model: id,
                        found_label: row.label,
                        label: model.label.clone(),
                    });
                }
                plan.existing.push(id);
                (id, row.name)
            }
            None => match self.model_by_label(&model.label) {
                Some((id, row)) => {
                    // Display names follow the most recent registration;
                    // write only on difference.
                    if row.name == model.name {
                        plan.existing.push(id);
                    } else {
                        plan.updates.push(planned(model_collection(), id, "name", string(&row.name), string(&model.name)));
                        trx.get::<SysModelRow>(&id).await?.name()?.set(&model.name)?;
                    }
                    (id, model.name.clone())
                }
                None => {
                    let id = trx.create(&SysModelRow { label: model.label.clone(), name: model.name.clone() }).await?.id();
                    plan.creates_models.push((id, model.clone()));
                    (id, model.name.clone())
                }
            },
        };

        // -- the properties. Nesting IS the membership assertion: every entry
        //    ensures a (model, property) membership, whether the property is
        //    minted here, found in the model's membership set by name, or
        //    explicitly shared by id. ----------------------------------------
        // Target-model references resolved so far, seeded with the model
        // itself (self-references) and extended by each stub mint, since a
        // mint inside this transaction is invisible to the signals until
        // commit.
        let mut targets: BTreeMap<String, EntityId> = BTreeMap::from([(model.label.clone(), model_id)]);
        // Duplicate names in one request: the first occurrence fixes the
        // resolution and a later one coalesces onto it, under the same
        // compatibility bar as a catalog hit.
        let mut property_ids: BTreeMap<String, (EntityId, String, String)> = BTreeMap::new();
        let mut properties: Vec<RegisteredProperty> = Vec::with_capacity(model.properties.len());

        for p in &model.properties {
            if let Some((_, backend, value_type)) = property_ids.get(&p.name) {
                if p.backend != *backend || !value_types_compatible(value_type, &p.value_type) {
                    return Err(non_castable(&model.label, p, backend, value_type));
                }
                continue;
            }

            let registered = match p.explicit_id {
                // Explicit binding: verify (backend, value_type), never mint
                // the property; the bound entity's name and metadata are
                // authoritative. The nested position still asserts THIS
                // model's membership, which is how a property minted
                // elsewhere is intentionally shared.
                Some(id) => {
                    let row = self.property_by_id(&id).ok_or(RegistrationError::ExplicitIdNotFound { property: id })?;
                    if row.backend != p.backend || !value_types_compatible(&row.value_type, &p.value_type) {
                        return Err(non_castable(&model.label, p, &row.backend, &row.value_type));
                    }
                    plan.existing.push(id);
                    RegisteredProperty {
                        build_id: p.build_id,
                        id,
                        membership_id: self.ensure_membership(&trx, &mut plan, model_id, id, p.optional).await?,
                        name: row.name,
                        backend: row.backend,
                        value_type: row.value_type,
                        target_model: row.target_model,
                        minted_for: row.minted_for,
                        optional: p.optional,
                    }
                }
                None => {
                    // Resolve the target-model reference first, so both the
                    // mint and the difference paths can use it.
                    let target = match &p.target_label {
                        Some(label) => Some(self.resolve_target(&trx, &mut plan, &mut targets, label).await?),
                        None => None,
                    };
                    // Lookup scope is the model's MEMBERSHIP SET by current
                    // name (a shared property resolves here regardless of
                    // where it was minted; minted_for is provenance, never a
                    // key), with the rename hint consulted only when the
                    // current name misses.
                    let found = match self.member_property(&model_id, &p.name)? {
                        Some(hit) => Some(hit),
                        None => match &p.renamed_from {
                            Some(old) => self.member_property(&model_id, old)?,
                            None => None,
                        },
                    };
                    match found {
                        // A hit never mutates (backend, value_type) and never
                        // forks a second identity. A castable drift is
                        // admitted (the binary writes and reads through the
                        // cast) and the response carries the CANONICAL types,
                        // so the requester holds its cast target. The hit
                        // differs at most in its display name (the rename hint
                        // applied) and its target reference.
                        Some(((id, row), (membership, optional))) => {
                            check_property_compat(&row, &model.label, p)?;
                            let rename = row.name != p.name;
                            let retarget = row.target_model != target;
                            if !rename && !retarget {
                                plan.existing.push(id);
                            } else {
                                let mutable = trx.get::<SysPropertyRow>(&id).await?;
                                if rename {
                                    plan.updates.push(planned(property_collection(), id, "name", string(&row.name), string(&p.name)));
                                    mutable.name()?.set(&p.name)?;
                                }
                                if retarget {
                                    let update =
                                        planned(property_collection(), id, "target_model", entity(row.target_model), entity(target));
                                    plan.updates.push(update);
                                    mutable.target_model()?.set(&target)?;
                                }
                            }
                            RegisteredProperty {
                                build_id: p.build_id,
                                id,
                                membership_id: self.set_membership_optional(&trx, &mut plan, membership, optional, p.optional).await?,
                                name: p.name.clone(),
                                backend: row.backend,
                                value_type: row.value_type,
                                target_model: target,
                                minted_for: row.minted_for,
                                optional: p.optional,
                            }
                        }
                        // Miss: mint the property AND its membership.
                        None => {
                            let id = trx
                                .create(&SysPropertyRow {
                                    name: p.name.clone(),
                                    backend: p.backend.clone(),
                                    value_type: p.value_type.clone(),
                                    minted_for: Some(model_id),
                                    target_model: target,
                                })
                                .await?
                                .id();
                            plan.creates_properties.push((id, p.clone()));
                            RegisteredProperty {
                                build_id: p.build_id,
                                id,
                                membership_id: self.ensure_membership(&trx, &mut plan, model_id, id, p.optional).await?,
                                name: p.name.clone(),
                                backend: p.backend.clone(),
                                value_type: p.value_type.clone(),
                                target_model: target,
                                minted_for: Some(model_id),
                                optional: p.optional,
                            }
                        }
                    }
                }
            };

            property_ids.insert(p.name.clone(), (registered.id, registered.backend.clone(), registered.value_type.clone()));
            properties.push(registered);
        }

        let out = RegisteredModel { id: model_id, label: model.label.clone(), name, properties };

        // A re-registration of unchanged definitions is a pure no-op: nothing
        // to gate and nothing to commit -- but the registrant still receives
        // the full resolved definition.
        if plan.is_noop() {
            return registrant.bind(out);
        }
        // The exists-aware policy gate judges the resolved plan before
        // anything is committed; refusal fails the request before any write,
        // and it is the agent's only voice here (the commit runs under the
        // privileged context).
        ctx.check_schema_registration(&plan)?;
        trx.commit().await?;
        // Only a committed registration resolves: the registrant's cells or
        // response must never publish identities the catalog did not keep.
        registrant.bind(out)
    }

    /// Admit a compiled declaration for use -- a mutation, a predicate, a
    /// typed read: bind it from what the catalog already proves, and reach
    /// the allocator only for a shape the catalog cannot prove. A policy or
    /// executor refusal is strict. Returns the identity WITH the snapshot
    /// epoch, so one logical operation (ensure, field resolution, entity
    /// stamp) observes exactly one epoch. What it registers is the DECLARED
    /// delta: a field added since the model was registered registers that
    /// field alone, against the model already there.
    pub async fn ensure_schema_for_use<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        cdata: &PA::ContextData,
        schema: &'static ModelStructDescriptor,
    ) -> Result<(proto::ModelId, SchemaEpoch), RegistrationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // A built-in is already resolved at every epoch, including the
        // bootstrap epoch a pre-system node stamps its entities with.
        if let Some(system) = schema.system {
            return Ok((proto::ModelId::System(system), node.system.schema_epoch().unwrap_or(SchemaEpoch::BOOTSTRAP)));
        }
        let epoch = node.system.schema_epoch().ok_or(RegistrationError::SystemNotReady)?;
        if schema.resolved.get(epoch).is_none() && !self.bind_compatible_schema(schema, epoch) {
            match self.register_at(node, cdata, schema, epoch).await {
                Ok(()) => {}
                Err(error @ RegistrationError::NoDurablePeer(_)) if self.bind_compatible_schema(schema, epoch) => {
                    tracing::warn!(
                        "schema reassertion for fully bound collection '{}' has no durable peer; proceeding with proven canonical identities: {}",
                        schema.label,
                        error
                    );
                }
                // A known label whose compiled shape could not be proven bound is
                // an unconfirmed schema, not an unregistered collection.
                Err(RegistrationError::NoDurablePeer(label)) if self.model_by_label(schema.label).is_some() => {
                    return Err(RegistrationError::UnconfirmedSchema(label));
                }
                Err(error) => return Err(error),
            }
        }
        schema.resolved.get(epoch).map(|model| (model, epoch)).ok_or_else(|| {
            RegistrationError::Retrieval(RetrievalError::Other(format!(
                "binding of '{}' did not retain its exact model identity",
                schema.label
            )))
        })
    }

    /// Register `schema` with the durable allocator and bind the response's
    /// cells under the caller-snapshotted epoch. A durable node executes the
    /// registration itself; an ephemeral one forwards it to a durable peer;
    /// an ephemeral with NO durable peer fails (only the durable allocator
    /// may mint ids). Every error path resolves nothing, so a later attempt
    /// retries. The epoch keying is also the whole reset guard: a response
    /// landing after a reset binds cells tagged with the OLD epoch, which
    /// nothing reads.
    async fn register_at<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        cdata: &PA::ContextData,
        schema: &'static ModelStructDescriptor,
        epoch: SchemaEpoch,
    ) -> Result<(), RegistrationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        if node.durable {
            return self.register_schema(node, cdata, &mut DescriptorRegistrant { schema, epoch }).await;
        }
        // Ephemeral: forward to a connected durable peer. There is no offline
        // registration queue because only the durable allocator may mint ids.
        let request = proto::RegisterModel::from(schema);
        let Some(peer) = node.get_durable_peers().first().copied() else {
            return Err(RegistrationError::NoDurablePeer(schema.label.to_string()));
        };
        let body = proto::NodeRequestBody::RegisterSchema { model: request };
        let response =
            node.request(peer, cdata, body).await.map_err(|e| RegistrationError::Retrieval(RetrievalError::Other(format!("{e:?}"))))?;
        let model = match response {
            proto::NodeResponseBody::SchemaRegistered { model } => model,
            proto::NodeResponseBody::Error(e) => return Err(RegistrationError::Retrieval(RetrievalError::Other(e))),
            other => {
                return Err(RegistrationError::Retrieval(RetrievalError::Other(format!("unexpected response to RegisterSchema: {other}"))))
            }
        };
        bind_registered(schema, &model, epoch)
    }

    /// Bind the descriptor's cells from a catalog-proven binding, under the
    /// epoch the gate snapshotted at entry. The first entry per epoch is
    /// final: a reset racing this gate leaves entries tagged with the old
    /// epoch, where nothing ever reads them. Field cells first, the model's
    /// last -- the model cell is the publication point the registration
    /// gate's fast path probes, so it must not become visible while any
    /// field cell is still empty.
    fn bind_compatible_schema(&self, schema: &'static ModelStructDescriptor, epoch: SchemaEpoch) -> bool {
        let Some((model, pairs)) = self.compatible_binding(schema) else { return false };
        for (field, id) in pairs {
            field.resolved.set(epoch, PropertyId::EntityId(id));
        }
        schema.resolved.set(epoch, ModelId::EntityId(model));
        true
    }

    /// The binding an already-populated catalog proves for this compiled
    /// declaration, as (model, field-by-field identities): ordinary fields
    /// resolve by current name within the model's membership set (the
    /// allocator's scope; an ambiguous name fails the proof), an explicit
    /// model id must be the label's live model, and every field needs a
    /// compatible immutable backend/type pair.
    fn compatible_binding(&self, schema: &'static ModelStructDescriptor) -> Option<(EntityId, Vec<(&'static StructProperty, EntityId)>)> {
        let (label_model, _) = self.model_by_label(schema.label)?;
        let model = match schema.explicit_id {
            Some(id) => {
                let id = parse_explicit_id(id);
                if label_model != id {
                    return None;
                }
                id
            }
            None => label_model,
        };

        let mut fields = Vec::with_capacity(schema.properties.len());
        for field in schema.properties {
            let id = match field.explicit_id {
                Some(id) => parse_explicit_id(id),
                None => match self.resolve(&proto::ModelId::EntityId(model), field.name)? {
                    PropertyId::EntityId(id) => id,
                    _ => return None,
                },
            };
            if self.membership(&model, &id).is_none() {
                return None;
            }
            let def = self.property_by_id(&id)?;
            if def.backend != field.backend || !value_types_compatible(&def.value_type, field.value_type) {
                return None;
            }
            fields.push((field, id));
        }
        Some((model, fields))
    }

    /// Bind a compiled declaration to this system's durable identities from
    /// what the catalog already holds, answering with the model identity and
    /// the epoch the binding holds under.
    ///
    /// It defines nothing. Either the catalog already proves this exact
    /// compiled shape, in which case the WHOLE declaration binds here --
    /// every field's cell, not just the names a query happens to mention, so
    /// an accessor on an unqueried field resolves too -- or it does not, and
    /// this call says so rather than handing back an empty result. Which of
    /// the two it is decides whether a read may judge its query on the spot;
    /// healing what the catalog cannot prove is
    /// [`Self::ensure_schema_for_use`]'s job.
    fn bind_descriptor<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        schema: &'static ModelStructDescriptor,
    ) -> Result<(ModelId, SchemaEpoch), RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // A built-in's identities are pinned at compile time and valid at
        // every epoch, so its rows are readable on a node with no system.
        if let Some(system) = schema.system {
            return Ok((ModelId::System(system), node.system.schema_epoch().unwrap_or(SchemaEpoch::BOOTSTRAP)));
        }
        let epoch = node
            .system
            .schema_epoch()
            .ok_or_else(|| RetrievalError::Other("no system is ready; a typed read cannot resolve its model".to_string()))?;
        if schema.resolved.get(epoch).is_none() {
            self.bind_compatible_schema(schema, epoch);
        }
        let model = schema.resolved.get(epoch).ok_or_else(|| RetrievalError::UnboundDeclaration { label: schema.label.to_string() })?;
        Ok((model, epoch))
    }

    /// Resolve a selection under a compiled declaration: field names bind
    /// through the descriptor's cells at this node's current epoch.
    pub(crate) fn resolve_selection_with_descriptor<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        schema: &'static ModelStructDescriptor,
        selection: Selection<Parsed>,
    ) -> Result<Selection<Resolved>, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // Bind FIRST, whatever the selection says. The views this query
        // returns read their identities off these cells, so a selection that
        // happens to name no property (`true`, a limit-only page) must not
        // leave the declaration unbound and every accessor broken.
        let (model, epoch) = self.bind_descriptor(node, schema)?;
        if let Some(resolved) = resolve_without_model(&selection) {
            return Ok(resolved);
        }
        let resolver = DescriptorResolver { schema, epoch, catalog: self };
        resolve_selection(&model, &resolver, selection).map_err(|error| RetrievalError::Other(error.to_string()))
    }

    /// Resolve a property's target-model label to a model id: a reference
    /// this request already resolved (including the model itself), the
    /// catalog's model for that label, or a stub minted here on a full miss.
    /// The memo extends with each mint, because a mint inside this
    /// transaction is invisible to the signals until commit.
    async fn resolve_target(
        &self,
        trx: &Transaction,
        plan: &mut RegistrationPlan,
        targets: &mut BTreeMap<String, EntityId>,
        target: &str,
    ) -> Result<EntityId, RegistrationError> {
        if let Some(id) = targets.get(target) {
            return Ok(*id);
        }
        let id = match self.model_by_label(target) {
            Some((id, _)) => id,
            None => {
                let id = trx.create(&SysModelRow { label: target.to_string(), name: target.to_string() }).await?.id();
                // Executor-synthesized stub: no compiled declaration stands
                // behind it, so no build identity either.
                let stub = RegisterModel {
                    label: target.to_string(),
                    name: target.to_string(),
                    explicit_id: None,
                    build_id: [0u8; 16],
                    properties: Vec::new(),
                };
                plan.creates_models.push((id, stub));
                id
            }
        };
        targets.insert(target.to_string(), id);
        Ok(id)
    }

    /// The property bound to `model` under `name`, with the membership binding
    /// it and that membership's `optional` flag. A name two properties answer to
    /// refuses, rather than picking one of them or minting a third.
    fn member_property(
        &self,
        model: &EntityId,
        name: &str,
    ) -> Result<Option<((EntityId, SysPropertyRow), (EntityId, bool))>, RegistrationError> {
        let resolved = self.try_resolve(&proto::ModelId::EntityId(*model), name).map_err(|e| RetrievalError::Other(e.to_string()))?;
        let Some(PropertyId::EntityId(property)) = resolved else { return Ok(None) };
        let (Some(row), Some((membership, membership_row))) = (self.property_by_id(&property), self.membership(model, &property)) else {
            return Ok(None);
        };
        Ok(Some(((property, row), (membership, membership_row.optional))))
    }

    /// Ensure the (model, property) membership exists with `optional`: reuse and
    /// difference-patch a stored one, or mint. Returns the membership id.
    async fn ensure_membership(
        &self,
        trx: &Transaction,
        plan: &mut RegistrationPlan,
        model: EntityId,
        property: EntityId,
        optional: bool,
    ) -> Result<EntityId, RegistrationError> {
        match self.membership(&model, &property) {
            Some((id, row)) => self.set_membership_optional(trx, plan, id, row.optional, optional).await,
            None => {
                let id = trx.create(&SysModelPropertyRow { model, property, optional }).await?.id();
                plan.creates_memberships.push(PlannedModelPropertyMembership { id, model, property, optional });
                Ok(id)
            }
        }
    }

    /// Difference-patch an already-resolved membership's `optional` flag.
    async fn set_membership_optional(
        &self,
        trx: &Transaction,
        plan: &mut RegistrationPlan,
        membership: EntityId,
        current: bool,
        optional: bool,
    ) -> Result<EntityId, RegistrationError> {
        if current == optional {
            plan.existing.push(membership);
        } else {
            plan.updates.push(planned(
                model_property_collection(),
                membership,
                "optional",
                Some(Value::Bool(current)),
                Some(Value::Bool(optional)),
            ));
            trx.get::<SysModelPropertyRow>(&membership).await?.optional()?.set(&optional)?;
        }
        Ok(membership)
    }

    /// A model row by catalog identity. The signals index models by label, so an
    /// explicit binding -- the one lookup keyed by id -- reads the row itself.
    async fn model_row(&self, ctx: &Context, id: EntityId) -> Result<Option<SysModelRow>, RegistrationError> {
        match ctx.get::<SysModelRowView>(id).await {
            // An entity can exist under this id without being a model row
            // (ids are node-global, so a resident entity answers a get in
            // any collection). One that does not parse as a model row is no
            // model definition: refuse it exactly like an absent id.
            Ok(view) => Ok(view.to_model().ok()),
            Err(RetrievalError::EntityNotFound(_)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

/// Bind the descriptor's cells from the allocator's response, each field
/// correlated by its `build_id` echo -- immune to display-name drift. Field
/// cells first, the model's last: the model cell is the publication point
/// the registration gate's fast path probes, so it must not become visible
/// while any field cell is still empty.
pub(crate) fn bind_registered(
    schema: &'static ModelStructDescriptor,
    model: &RegisteredModel,
    epoch: SchemaEpoch,
) -> Result<(), RegistrationError> {
    let incomplete = || {
        RegistrationError::Retrieval(RetrievalError::Other(format!(
            "registration of '{}' succeeded without a complete compatible catalog binding",
            schema.label
        )))
    };
    if model.label != schema.label {
        return Err(incomplete());
    }
    if schema.explicit_id.is_some_and(|id| parse_explicit_id(id) != model.id) {
        return Err(incomplete());
    }
    let mut pairs = Vec::with_capacity(schema.properties.len());
    for field in schema.properties {
        let Some(property) = model.properties.iter().find(|p| p.build_id == field.build_id) else {
            return Err(incomplete());
        };
        if field.explicit_id.is_some_and(|id| parse_explicit_id(id) != property.id) {
            return Err(incomplete());
        }
        if property.backend != field.backend || !value_types_compatible(&property.value_type, field.value_type) {
            return Err(incomplete());
        }
        pairs.push((field, property.id));
    }
    for (field, id) in pairs {
        field.resolved.set(epoch, PropertyId::EntityId(id));
    }
    schema.resolved.set(epoch, ModelId::EntityId(model.id));
    Ok(())
}

fn planned(collection: crate::ModelId, entity: EntityId, field: &str, from: Option<Value>, to: Option<Value>) -> PlannedUpdate {
    PlannedUpdate { collection, entity, field: field.to_string(), from, to }
}

fn string(value: &str) -> Option<Value> { Some(Value::String(value.to_string())) }

fn entity(id: Option<EntityId>) -> Option<Value> { id.map(Value::EntityId) }

fn non_castable(model_label: &str, p: &RegisterProperty, found_backend: &str, found_value_type: &str) -> RegistrationError {
    RegistrationError::NonCastable {
        collection: model_label.to_string(),
        name: p.name.clone(),
        found_backend: found_backend.to_string(),
        found_value_type: found_value_type.to_string(),
        backend: p.backend.clone(),
        value_type: p.value_type.clone(),
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

/// The canonical-type compatibility gate for a name-keyed upsert hit. Never
/// mutates the found definition.
fn check_property_compat(def: &SysPropertyRow, model_label: &str, p: &RegisterProperty) -> Result<(), RegistrationError> {
    if def.backend != p.backend || !value_types_compatible(&def.value_type, &p.value_type) {
        return Err(non_castable(model_label, p, &def.backend, &def.value_type));
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
