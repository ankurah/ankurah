use std::collections::{BTreeMap, BTreeSet};

use ankurah_core_types::Value;
use ankurah_proto::{self as proto, EntityId, PropertyId};

use super::{CatalogManager, SysModelPropertyRow, SysModelRow, SysPropertyRow};
use crate::error::RetrievalError;
use crate::node::Node;
use crate::policy::{AccessDenied, PolicyAgent};
use crate::schema::registration::{PlannedModelPropertyMembership, PlannedUpdate, RegistrationError, RegistrationPlan};
use crate::schema::{model_collection, model_property_collection, property_collection};
use crate::storage::StorageEngine;
use crate::transaction::Transaction;

/// Registration authority; credential errors do not prevent local resolution.
pub(crate) enum RegistrationAuth<CD> {
    Credential(CD),
    Unavailable(AccessDenied),
    Privileged,
}

/// A property's declaration within a model, before catalog resolution.
pub(crate) trait RegistrantProperty {
    /// Correlates this declaration with its entry in the registration response.
    fn build_id(&self) -> [u8; 16];
    /// The requested property name within the model.
    fn name(&self) -> &str;
    /// A previous name to look up when the requested name is absent.
    fn renamed_from(&self) -> Option<&str>;
    /// The required property backend, such as `lww` or `yrs`.
    fn backend(&self) -> &str;
    /// The required language-independent value type.
    fn value_type(&self) -> &str;
    /// The model label a reference-valued property points to.
    fn target_label(&self) -> Option<&str>;
    /// An existing property to bind by identity rather than by name.
    fn explicit_id(&self) -> Option<EntityId>;
    /// Whether this model allows the property to be absent.
    fn optional(&self) -> bool;
}

/// A model declaration and an accumulator for its resolved catalog rows.
pub(crate) trait Registrant {
    type Property: RegistrantProperty;

    /// Per-build declaration identity carried in the registration request.
    fn build_id(&self) -> [u8; 16];
    /// The model's registration lookup key, distinct from its display name.
    fn label(&self) -> &str;
    /// The requested display name of the model.
    fn name(&self) -> &str;
    /// An existing model to bind by identity; explicit binding never creates a model.
    fn explicit_id(&self) -> Option<EntityId>;
    /// Property declarations in the order used by `set_property`.
    fn properties(&self) -> impl ExactSizeIterator<Item = &Self::Property>;
    /// Accumulate the resolved model identity and row.
    fn set_model(&mut self, id: EntityId, model: SysModelRow) -> Result<(), RegistrationError>;
    /// Accumulate a property's identity, row, and membership at its declaration index.
    fn set_property(
        &mut self,
        index: usize,
        id: EntityId,
        property: SysPropertyRow,
        membership_id: EntityId,
        optional: bool,
    ) -> Result<(), RegistrationError>;
    /// Finalize the complete binding or response after all rows have been supplied.
    fn finish(&mut self) -> Result<(), RegistrationError>;
}

/// Populate from the local catalog, registering here or through a durable peer when needed.
/// Wait for node readiness; registration authority is needed only if local resolution cannot satisfy the declaration.
pub(super) async fn resolve_or_register<SE, PA, R>(
    catalog: &CatalogManager,
    node: &Node<SE, PA>,
    registrant: &mut R,
    auth: RegistrationAuth<PA::ContextData>,
) -> Result<(), RegistrationError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    R: Registrant,
{
    node.wait_ready().await?;
    match resolve_local(catalog, registrant) {
        Ok(()) => return Ok(()),
        Err(RegistrationError::Retrieval(RetrievalError::UnboundDeclaration { .. })) => {}
        Err(error) => return Err(error),
    }

    let principal = match auth {
        RegistrationAuth::Credential(credential) => Some(credential),
        RegistrationAuth::Unavailable(source) => {
            return Err(RegistrationError::PolicyDenied { collection: registrant.label().to_string(), source });
        }
        RegistrationAuth::Privileged => None,
    };
    let result = if node.durable {
        register(catalog, node, principal.as_ref(), registrant).await
    } else {
        match principal.as_ref() {
            Some(principal) => register_remote(node, principal, registrant).await,
            None => Err(RegistrationError::NotDurable),
        }
    };
    match result {
        Err(RegistrationError::NoDurablePeer(label)) => match resolve_local(catalog, registrant) {
            Ok(()) => Ok(()),
            Err(RegistrationError::Retrieval(RetrievalError::UnboundDeclaration { .. })) => {
                if catalog.model_by_label(&label)?.is_some() {
                    Err(RegistrationError::UnconfirmedSchema(label))
                } else {
                    Err(RegistrationError::NoDurablePeer(label))
                }
            }
            Err(error) => Err(error),
        },
        other => other,
    }
}

/// Upsert the declaration on a durable node in one transaction, then populate the registrant.
/// The allocator lock covers lookup through commit so concurrent registrations reuse identities.
/// When a principal is supplied, policy checks the complete change plan before commit.
async fn register<SE, PA, R>(
    catalog: &CatalogManager,
    node: &Node<SE, PA>,
    principal: Option<&PA::ContextData>,
    registrant: &mut R,
) -> Result<(), RegistrationError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    R: Registrant,
{
    let label = registrant.label().to_string();
    let name = registrant.name().to_string();
    let explicit_id = registrant.explicit_id();

    let _allocator = catalog.allocator.lock().await;
    let transaction = node.privileged_context().begin();
    let mut plan = RegistrationPlan::default();

    let (model_id, model_row) = match explicit_id {
        Some(id) => {
            let row = catalog.model_by_id(&id)?.ok_or(RegistrationError::ExplicitModelIdNotFound { model: id })?;
            if row.label != label {
                return Err(RegistrationError::ExplicitModelIdMismatch { model: id, found_label: row.label, label: label.clone() });
            }
            plan.existing.push(id);
            (id, row)
        }
        None => match catalog.model_by_label(&label)? {
            Some((id, mut row)) => {
                if row.name == name {
                    plan.existing.push(id);
                } else {
                    plan.updates.push(planned(model_collection(), id, "name", string(&row.name), string(&name)));
                    transaction.get::<SysModelRow>(&id).await?.name()?.set(&name)?;
                    row.name = name.clone();
                }
                (id, row)
            }
            None => {
                let row = SysModelRow { label: label.clone(), name: name.clone() };
                let id = transaction.create(&row).await?.id();
                plan.creates_models.push((id, row.clone()));
                (id, row)
            }
        },
    };

    let mut targets = BTreeMap::from([(label.clone(), model_id)]);
    let registered_properties = {
        let properties = registrant.properties();
        let mut registered_properties = Vec::with_capacity(properties.len());
        let mut declared = BTreeSet::new();
        let mut registered_ids = BTreeSet::new();

        for (index, property) in properties.enumerate() {
            if !declared.insert(property.name()) {
                continue;
            }

            let (id, row, membership_id) = match property.explicit_id() {
                Some(id) => {
                    let row = catalog.property_by_id(&id)?.ok_or(RegistrationError::ExplicitIdNotFound { property: id })?;
                    check_property_compat(&row, &label, property)?;
                    if member_property(catalog, model_id, &row.name)?.is_some_and(|((property, _), _)| property != id) {
                        return Err(RegistrationError::PropertyNameTaken { collection: label.clone(), name: row.name });
                    }
                    plan.existing.push(id);
                    let membership_id = ensure_membership(catalog, &transaction, &mut plan, model_id, id, property.optional()).await?;
                    (id, row, membership_id)
                }
                None => {
                    let target = match property.target_label() {
                        Some(target) => Some(resolve_target(catalog, &transaction, &mut plan, &mut targets, target).await?),
                        None => None,
                    };
                    let found = match member_property(catalog, model_id, property.name())? {
                        Some(hit) => Some(hit),
                        None => match property.renamed_from() {
                            Some(old) => member_property(catalog, model_id, old)?,
                            None => None,
                        },
                    };
                    match found {
                        Some(((id, mut row), (membership, optional))) => {
                            check_property_compat(&row, &label, property)?;
                            let rename = row.name != property.name();
                            let retarget = row.target_model != target;
                            if !rename && !retarget {
                                plan.existing.push(id);
                            } else {
                                let mutable = transaction.get::<SysPropertyRow>(&id).await?;
                                if rename {
                                    let name = property.name().to_string();
                                    plan.updates.push(planned(property_collection(), id, "name", string(&row.name), string(&name)));
                                    mutable.name()?.set(&name)?;
                                    row.name = name;
                                }
                                if retarget {
                                    plan.updates.push(planned(
                                        property_collection(),
                                        id,
                                        "target_model",
                                        entity(row.target_model),
                                        entity(target),
                                    ));
                                    mutable.target_model()?.set(&target)?;
                                    row.target_model = target;
                                }
                            }
                            let membership_id =
                                set_membership_optional(&transaction, &mut plan, membership, optional, property.optional()).await?;
                            (id, row, membership_id)
                        }
                        None => {
                            let row = SysPropertyRow {
                                name: property.name().to_string(),
                                backend: property.backend().to_string(),
                                value_type: property.value_type().to_string(),
                                minted_for: Some(model_id),
                                target_model: target,
                            };
                            let id = transaction.create(&row).await?.id();
                            plan.creates_properties.push((id, row.clone()));
                            let membership_id =
                                ensure_membership(catalog, &transaction, &mut plan, model_id, id, property.optional()).await?;
                            (id, row, membership_id)
                        }
                    }
                }
            };

            if !registered_ids.insert(id) {
                return Err(RegistrationError::ConflictingDuplicateProperty { collection: label.clone(), name: row.name });
            }
            registered_properties.push((index, id, row, membership_id, property.optional()));
        }
        registered_properties
    };

    if !plan.is_noop() {
        if let Some(principal) = principal {
            node.policy_agent
                .check_schema_registration(node, principal, &plan)
                .map_err(|source| RegistrationError::PolicyDenied { collection: label.clone(), source })?;
        }
        transaction.commit().await?;
    }
    populate(registrant, model_id, model_row, registered_properties)
}

/// Register through a durable peer and populate the registrant from its response.
async fn register_remote<SE, PA, R>(node: &Node<SE, PA>, cdata: &PA::ContextData, registrant: &mut R) -> Result<(), RegistrationError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    R: Registrant,
{
    let Some(peer) = node.get_durable_peers().first().copied() else {
        return Err(RegistrationError::NoDurablePeer(registrant.label().to_string()));
    };
    let body = proto::NodeRequestBody::RegisterSchema { model: declaration(registrant) };
    let response = node.request(peer, cdata, body).await.map_err(RetrievalError::from)?;
    let registered = match response {
        proto::NodeResponseBody::SchemaRegistered { model } => model,
        proto::NodeResponseBody::Error(error) => return Err(RetrievalError::Other(error).into()),
        other => return Err(RetrievalError::Other(format!("unexpected response to RegisterSchema: {other}")).into()),
    };
    accept(registrant, &registered)
}

/// Build the wire request from the registrant's declaration.
fn declaration<R: Registrant>(registrant: &R) -> proto::RegisterModel {
    proto::RegisterModel {
        label: registrant.label().to_string(),
        name: registrant.name().to_string(),
        explicit_id: registrant.explicit_id(),
        build_id: registrant.build_id(),
        properties: registrant
            .properties()
            .map(|property| proto::RegisterProperty {
                name: property.name().to_string(),
                renamed_from: property.renamed_from().map(str::to_string),
                backend: property.backend().to_string(),
                value_type: property.value_type().to_string(),
                target_label: property.target_label().map(str::to_string),
                explicit_id: property.explicit_id(),
                build_id: property.build_id(),
                optional: property.optional(),
            })
            .collect(),
    }
}

/// Match response properties to declaration entries by build ID, then populate the registrant.
pub(crate) fn accept<R: Registrant>(registrant: &mut R, registered: &proto::RegisteredModel) -> Result<(), RegistrationError> {
    let mut indices = BTreeMap::new();
    for (index, property) in registrant.properties().enumerate() {
        if indices.insert(property.build_id(), index).is_some() {
            return Err(incomplete(registrant.label()));
        }
    }
    let mut seen = BTreeSet::new();
    let mut properties = Vec::with_capacity(registered.properties.len());
    for property in &registered.properties {
        let Some(index) = indices.get(&property.build_id).copied() else {
            return Err(incomplete(registrant.label()));
        };
        if !seen.insert(property.build_id) {
            return Err(incomplete(registrant.label()));
        }
        properties.push((
            index,
            property.id,
            SysPropertyRow {
                name: property.name.clone(),
                backend: property.backend.clone(),
                value_type: property.value_type.clone(),
                target_model: property.target_model,
                minted_for: property.minted_for,
            },
            property.membership_id,
            property.optional,
        ));
    }
    populate(registrant, registered.id, SysModelRow { label: registered.label.clone(), name: registered.name.clone() }, properties)
}

/// Supply property and model rows to the registrant, then finalize its binding or response.
fn populate<R: Registrant>(
    registrant: &mut R,
    model_id: EntityId,
    model: SysModelRow,
    properties: Vec<(usize, EntityId, SysPropertyRow, EntityId, bool)>,
) -> Result<(), RegistrationError> {
    for (index, id, property, membership_id, optional) in properties {
        registrant.set_property(index, id, property, membership_id, optional)?;
    }
    registrant.set_model(model_id, model)?;
    registrant.finish()
}

/// Populate the registrant only if its full declaration matches the local catalog.
/// Return `UnboundDeclaration` when synchronization or registration is needed; perform neither here.
pub(super) fn resolve_local<R: Registrant>(catalog: &CatalogManager, registrant: &mut R) -> Result<(), RegistrationError> {
    validate(registrant)?;
    let unbound = || RegistrationError::Retrieval(RetrievalError::UnboundDeclaration { label: registrant.label().to_string() });
    let (label_model, model_row) = catalog.model_by_label(registrant.label())?.ok_or_else(unbound)?;
    let model = match registrant.explicit_id() {
        Some(id) => (label_model == id).then_some(id).ok_or_else(unbound)?,
        None => (model_row.name == registrant.name()).then_some(label_model).ok_or_else(unbound)?,
    };

    let mut properties = Vec::with_capacity(registrant.properties().len());
    let mut declared = BTreeSet::new();
    let mut registered = BTreeSet::new();
    for (index, field) in registrant.properties().enumerate() {
        if !declared.insert(field.name()) {
            continue;
        }
        let id = match field.explicit_id() {
            Some(id) => id,
            None => match catalog
                .try_resolve(&proto::ModelId::EntityId(model), field.name())
                .map_err(RetrievalError::from)?
                .ok_or_else(unbound)?
            {
                PropertyId::EntityId(id) => id,
                _ => return Err(unbound()),
            },
        };
        if !registered.insert(id) {
            return Err(unbound());
        }
        let (membership_id, membership) = catalog.membership(&model, &id)?.ok_or_else(unbound)?;
        if membership.optional != field.optional() {
            return Err(unbound());
        }
        let property = catalog.property_by_id(&id)?.ok_or_else(unbound)?;
        if property.backend != field.backend() || property.value_type != field.value_type() {
            return Err(unbound());
        }
        if field.explicit_id().is_none() {
            let target = match field.target_label() {
                Some(label) => Some(catalog.model_by_label(label)?.ok_or_else(unbound)?.0),
                None => None,
            };
            if property.target_model != target {
                return Err(unbound());
            }
        }
        properties.push((index, id, property, membership_id, membership.optional));
    }
    populate(registrant, model, model_row, properties)
}

/// Reject reserved model/target labels and conflicting declarations of the same property name.
fn validate<R: Registrant>(registrant: &R) -> Result<(), RegistrationError> {
    for label in std::iter::once(registrant.label()).chain(registrant.properties().filter_map(RegistrantProperty::target_label)) {
        if label.starts_with(crate::schema::RESERVED_COLLECTION_PREFIX) {
            return Err(RegistrationError::ReservedCollection(label.to_string()));
        }
    }
    let mut declared = BTreeMap::new();
    for property in registrant.properties() {
        if let Some(first) = declared.insert(property.name(), property) {
            if !same_declaration(first, property) {
                return Err(RegistrationError::ConflictingDuplicateProperty {
                    collection: registrant.label().to_string(),
                    name: property.name().to_string(),
                });
            }
        }
    }
    Ok(())
}

fn incomplete(label: &str) -> RegistrationError {
    RetrievalError::Other(format!("registration of '{label}' succeeded without a complete compatible catalog binding")).into()
}

/// Find or create the model named by a reference property.
/// The request-local cache includes models created in this transaction, which the catalog cannot see yet.
async fn resolve_target(
    catalog: &CatalogManager,
    transaction: &Transaction,
    plan: &mut RegistrationPlan,
    targets: &mut BTreeMap<String, EntityId>,
    target: &str,
) -> Result<EntityId, RegistrationError> {
    if let Some(id) = targets.get(target) {
        return Ok(*id);
    }
    let id = match catalog.model_by_label(target)? {
        Some((id, _)) => id,
        None => {
            let row = SysModelRow { label: target.to_string(), name: target.to_string() };
            let id = transaction.create(&row).await?.id();
            plan.creates_models.push((id, row));
            id
        }
    };
    targets.insert(target.to_string(), id);
    Ok(id)
}

/// Resolve a model-scoped property name and return its row, membership ID, and optionality.
fn member_property(
    catalog: &CatalogManager,
    model: EntityId,
    name: &str,
) -> Result<Option<((EntityId, SysPropertyRow), (EntityId, bool))>, RegistrationError> {
    let resolved = catalog.try_resolve(&proto::ModelId::EntityId(model), name)?;
    let Some(PropertyId::EntityId(property)) = resolved else { return Ok(None) };
    let (Some(row), Some((membership, membership_row))) = (catalog.property_by_id(&property)?, catalog.membership(&model, &property)?)
    else {
        return Ok(None);
    };
    Ok(Some(((property, row), (membership, membership_row.optional))))
}

/// Create a model-property membership if missing, or update its optionality.
async fn ensure_membership(
    catalog: &CatalogManager,
    transaction: &Transaction,
    plan: &mut RegistrationPlan,
    model: EntityId,
    property: EntityId,
    optional: bool,
) -> Result<EntityId, RegistrationError> {
    match catalog.membership(&model, &property)? {
        Some((id, row)) => set_membership_optional(transaction, plan, id, row.optional, optional).await,
        None => {
            let id = transaction.create(&SysModelPropertyRow { model, property, optional }).await?.id();
            plan.creates_memberships.push(PlannedModelPropertyMembership { id, model, property, optional });
            Ok(id)
        }
    }
}

/// Record an unchanged membership or stage an optionality change in the transaction and policy plan.
async fn set_membership_optional(
    transaction: &Transaction,
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
        transaction.get::<SysModelPropertyRow>(&membership).await?.optional()?.set(&optional)?;
    }
    Ok(membership)
}

/// Describe a catalog field change for the registration policy check.
fn planned(collection: crate::ModelId, entity: EntityId, field: &str, from: Option<Value>, to: Option<Value>) -> PlannedUpdate {
    PlannedUpdate { collection, entity, field: field.to_string(), from, to }
}

fn string(value: &str) -> Option<Value> { Some(Value::String(value.to_string())) }

fn entity(id: Option<EntityId>) -> Option<Value> { id.map(Value::EntityId) }

/// Compare registration metadata; build IDs correlate responses but do not affect equivalence.
fn same_declaration<P: RegistrantProperty>(left: &P, right: &P) -> bool {
    left.name() == right.name()
        && left.renamed_from() == right.renamed_from()
        && left.backend() == right.backend()
        && left.value_type() == right.value_type()
        && left.target_label() == right.target_label()
        && left.explicit_id() == right.explicit_id()
        && left.optional() == right.optional()
}

fn incompatible_property<P: RegistrantProperty>(
    model_label: &str,
    property: &P,
    found_backend: &str,
    found_value_type: &str,
) -> RegistrationError {
    RegistrationError::IncompatibleProperty {
        collection: model_label.to_string(),
        name: property.name().to_string(),
        found_backend: found_backend.to_string(),
        found_value_type: found_value_type.to_string(),
        backend: property.backend().to_string(),
        value_type: property.value_type().to_string(),
    }
}

/// Require the declared backend and value type to match the property's canonical pair exactly.
fn check_property_compat<P: RegistrantProperty>(
    definition: &SysPropertyRow,
    model_label: &str,
    property: &P,
) -> Result<(), RegistrationError> {
    if definition.backend != property.backend() || definition.value_type != property.value_type() {
        return Err(incompatible_property(model_label, property, &definition.backend, &definition.value_type));
    }
    Ok(())
}
