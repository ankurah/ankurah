use std::collections::BTreeMap;

use ankql::ast::{Parsed, Resolved, Selection};
use ankurah_core_types::Value;
use ankurah_proto::{EntityId, ModelId, PropertyId, RegisterModel, RegisterProperty, RegisteredModel, RegisteredProperty};

use super::resolver::{resolve_selection, resolve_without_model, DescriptorResolver};
use super::{SysModelPropertyRow, SysModelRow, SysPropertyRow};
use crate::internal::prelude::*;
use crate::schema::compiled::parse_explicit_id;
use crate::schema::registration::{PlannedModelPropertyMembership, PlannedUpdate, RegistrationError, RegistrationPlan};
use crate::schema::{model_collection, model_property_collection, property_collection, StructProperty};

pub trait Registrant {
    fn definition(&self) -> RegisterModel;
    fn bind(&mut self, model: RegisteredModel) -> Result<(), RegistrationError>;
}

pub struct DescriptorRegistrant {
    pub schema: &'static ModelStructDescriptor,
    pub epoch: SchemaEpoch,
}

impl Registrant for DescriptorRegistrant {
    fn definition(&self) -> RegisterModel { RegisterModel::from(self.schema) }

    fn bind(&mut self, model: RegisteredModel) -> Result<(), RegistrationError> { bind_registered(self.schema, &model, self.epoch) }
}

pub struct WireRegistrant {
    request: RegisterModel,
    response: Option<RegisteredModel>,
}

impl WireRegistrant {
    pub fn new(request: RegisterModel) -> Self { Self { request, response: None } }

    pub fn into_response(self) -> Option<RegisteredModel> { self.response }
}

impl Registrant for WireRegistrant {
    fn definition(&self) -> RegisterModel { self.request.clone() }

    fn bind(&mut self, model: RegisteredModel) -> Result<(), RegistrationError> {
        self.response = Some(model);
        Ok(())
    }
}

/// Registration and descriptor binding over the type-erased catalog projection.
impl CatalogManager {
    /// Upsert one model and its nested properties as the system allocator.
    /// Returns only after commit, or immediately for a proven no-op.
    pub(crate) async fn register_schema<SE, PA>(
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
        if node.system.root_id().is_none() {
            return Err(RegistrationError::SystemNotReady);
        }
        let model = registrant.definition();
        for label in std::iter::once(&model.label).chain(model.properties.iter().filter_map(|p| p.target_label.as_ref())) {
            if label.starts_with(crate::schema::RESERVED_COLLECTION_PREFIX) {
                return Err(RegistrationError::ReservedCollection(label.clone()));
            }
        }
        self.wait_ready().await?;

        let _allocator = self.allocator.lock().await;
        let privileged = node.privileged_context();

        let trx = privileged.begin();
        let mut plan = RegistrationPlan::default();

        let (model_id, name) = match model.explicit_id {
            Some(id) => {
                let row = self.model_by_id(&id).ok_or(RegistrationError::ExplicitModelIdNotFound { model: id })?;
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

        let mut targets: BTreeMap<String, EntityId> = BTreeMap::from([(model.label.clone(), model_id)]);
        let mut declared: BTreeMap<String, RegisterProperty> = BTreeMap::new();
        let mut registered_ids: BTreeMap<EntityId, String> = BTreeMap::new();
        let mut properties: Vec<RegisteredProperty> = Vec::with_capacity(model.properties.len());

        for p in &model.properties {
            if let Some(first) = declared.get(&p.name) {
                if !same_declaration(first, p) {
                    return Err(RegistrationError::ConflictingDuplicateProperty { collection: model.label.clone(), name: p.name.clone() });
                }
                continue;
            }

            let registered = match p.explicit_id {
                Some(id) => {
                    let row = self.property_by_id(&id).ok_or(RegistrationError::ExplicitIdNotFound { property: id })?;
                    if row.backend != p.backend || row.value_type != p.value_type {
                        return Err(incompatible_property(&model.label, p, &row.backend, &row.value_type));
                    }
                    if self.member_property(&model_id, &row.name)?.is_some_and(|((property, _), _)| property != id) {
                        return Err(RegistrationError::PropertyNameTaken { collection: model.label.clone(), name: row.name });
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
                    let target = match &p.target_label {
                        Some(label) => Some(self.resolve_target(&trx, &mut plan, &mut targets, label).await?),
                        None => None,
                    };
                    let found = match self.member_property(&model_id, &p.name)? {
                        Some(hit) => Some(hit),
                        None => match &p.renamed_from {
                            Some(old) => self.member_property(&model_id, old)?,
                            None => None,
                        },
                    };
                    match found {
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

            if registered_ids.insert(registered.id, p.name.clone()).is_some() {
                return Err(RegistrationError::ConflictingDuplicateProperty { collection: model.label.clone(), name: registered.name });
            }
            declared.insert(p.name.clone(), p.clone());
            properties.push(registered);
        }

        let out = RegisteredModel { id: model_id, label: model.label.clone(), name, properties };

        if plan.is_noop() {
            return registrant.bind(out);
        }
        node.policy_agent.check_schema_registration(node, cdata, &plan)?;
        trx.commit().await?;
        registrant.bind(out)
    }

    /// Bind a compiled declaration from catalog truth, registering any delta.
    /// Returns the model id with the epoch that was checked.
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
                Err(RegistrationError::NoDurablePeer(label)) if self.model_by_label(schema.label).is_some() => {
                    return Err(RegistrationError::UnconfirmedSchema(label));
                }
                Err(error) => return Err(error),
            }
        }
        let model = schema.resolved.get(epoch).ok_or_else(|| {
            RegistrationError::Retrieval(RetrievalError::Other(format!(
                "binding of '{}' did not retain its exact model identity",
                schema.label
            )))
        })?;
        if node.system.schema_epoch() != Some(epoch) {
            return Err(RegistrationError::SystemChanged);
        }
        Ok((model, epoch))
    }

    /// Register through the local allocator or a durable peer, then bind the
    /// response under the epoch captured by the caller.
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
        // Only a durable allocator may mint identities.
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

    /// Bind a catalog-proven descriptor, publishing its model cell last.
    fn bind_compatible_schema(&self, schema: &'static ModelStructDescriptor, epoch: SchemaEpoch) -> bool {
        let Some((model, pairs)) = self.compatible_binding(schema) else { return false };
        for (field, id) in pairs {
            field.resolved.set(epoch, PropertyId::EntityId(id));
        }
        schema.resolved.set(epoch, ModelId::EntityId(model));
        true
    }

    /// Return the complete catalog binding when every declared field agrees.
    fn compatible_binding(&self, schema: &'static ModelStructDescriptor) -> Option<(EntityId, Vec<(&'static StructProperty, EntityId)>)> {
        let (label_model, model_row) = self.model_by_label(schema.label)?;
        let model = match schema.explicit_id {
            Some(id) => {
                let id = parse_explicit_id(id);
                if label_model != id {
                    return None;
                }
                id
            }
            None => {
                if model_row.name != schema.name {
                    return None;
                }
                label_model
            }
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
            let (_, membership) = self.membership(&model, &id)?;
            if membership.optional != field.optional {
                return None;
            }
            let def = self.property_by_id(&id)?;
            if def.backend != field.backend || def.value_type != field.value_type {
                return None;
            }
            if field.explicit_id.is_none() {
                let target = match field.target_label {
                    Some(label) => Some(self.model_by_label(label)?.0),
                    None => None,
                };
                if def.target_model != target {
                    return None;
                }
            }
            fields.push((field, id));
        }
        Some((model, fields))
    }

    /// Bind a complete compiled declaration from current catalog truth.
    pub(crate) fn bind_descriptor(
        &self,
        epoch: Option<SchemaEpoch>,
        schema: &'static ModelStructDescriptor,
    ) -> Result<(ModelId, SchemaEpoch), RetrievalError> {
        if let Some(system) = schema.system {
            return Ok((ModelId::System(system), epoch.unwrap_or(SchemaEpoch::BOOTSTRAP)));
        }
        let epoch = epoch.ok_or_else(|| RetrievalError::UnboundDeclaration { label: schema.label.to_string() })?;
        if schema.resolved.get(epoch).is_none() {
            self.bind_compatible_schema(schema, epoch);
        }
        let model = schema.resolved.get(epoch).ok_or_else(|| RetrievalError::UnboundDeclaration { label: schema.label.to_string() })?;
        Ok((model, epoch))
    }

    /// Resolve a selection under a compiled declaration: field names bind
    /// through the descriptor's cells at this node's current epoch.
    pub(crate) fn resolve_selection_with_descriptor(
        &self,
        epoch: Option<SchemaEpoch>,
        schema: &'static ModelStructDescriptor,
        selection: Selection<Parsed>,
    ) -> Result<Selection<Resolved>, RetrievalError> {
        // Views need every descriptor cell, even when the query names none.
        let (model, epoch) = self.bind_descriptor(epoch, schema)?;
        if let Some(resolved) = resolve_without_model(&selection) {
            return Ok(resolved);
        }
        let resolver = DescriptorResolver { schema, epoch, catalog: self };
        resolve_selection(&model, &resolver, selection).map_err(|error| RetrievalError::Other(error.to_string()))
    }

    /// Resolve a target label, minting and memoizing a stub model on miss.
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
}

/// Bind a registration response by build id, publishing the model cell last.
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
    if schema.explicit_id.is_none() && model.name != schema.name {
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
        if field.explicit_id.is_none() && property.name != field.name
            || property.backend != field.backend
            || property.value_type != field.value_type
            || property.optional != field.optional
        {
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

fn same_declaration(left: &RegisterProperty, right: &RegisterProperty) -> bool {
    left.name == right.name
        && left.renamed_from == right.renamed_from
        && left.backend == right.backend
        && left.value_type == right.value_type
        && left.target_label == right.target_label
        && left.explicit_id == right.explicit_id
        && left.build_id == right.build_id
        && left.optional == right.optional
}

fn incompatible_property(model_label: &str, p: &RegisterProperty, found_backend: &str, found_value_type: &str) -> RegistrationError {
    RegistrationError::IncompatibleProperty {
        collection: model_label.to_string(),
        name: p.name.clone(),
        found_backend: found_backend.to_string(),
        found_value_type: found_value_type.to_string(),
        backend: p.backend.clone(),
        value_type: p.value_type.clone(),
    }
}

/// The canonical-type compatibility gate for a name-keyed upsert hit. Never
/// mutates the found definition.
fn check_property_compat(def: &SysPropertyRow, model_label: &str, p: &RegisterProperty) -> Result<(), RegistrationError> {
    if def.backend != p.backend || def.value_type != p.value_type {
        return Err(incompatible_property(model_label, p, &def.backend, &def.value_type));
    }
    Ok(())
}
