//! The durable allocator for `RegisterSchema`.
//!
//! Here, **registration** means the durable node's complete transition from
//! a language-neutral declaration to resolved catalog identities. The input
//! is a set of `RegisterModel` descriptors plus current catalog state; the
//! output is a `SchemaRegistered` tree containing every resolved id. This
//! module owns lookup-key semantics, compatibility checks, and allocation;
//! it persists the resulting rows through the catalog's ordinary typed
//! Models and Mutables under a local SystemRoot context. The per-node feed,
//! reset epoch, compiled-binding latch, and ephemeral forwarding path belong
//! to [`super::catalog`].
//!
//! Registration is an upsert: models resolve by source label, properties by
//! current name within a model's membership set, and memberships by
//! `(model, property)`. A miss allocates a fresh `EntityId`; a hit keeps its
//! identity. The whole lookup/allocate/commit/fold transaction serializes on
//! the catalog manager's allocator mutex. The SystemRoot transaction notifies
//! the typed catalog LiveQueries synchronously before commit returns, so the
//! projection observes the new identities before the mutex is released.
//!
//! Remote request authentication decides whether a principal may submit
//! schema descriptors. Materializing an accepted request is trusted system
//! work and therefore bypasses the application's PolicyAgent. Idempotence is
//! the upsert's: a repeat registration finds every key, emits zero events,
//! and returns the same ids.
//!
//! A property's (backend, value_type) is CANONICAL: fixed at allocation and
//! never changed by registration. A hit
//! whose descriptor declares a different value_type is admitted only when the
//! two types are mutually castable per `Value::cast_to` (the binary writes
//! and reads through the cast); a different backend, or a non-castable type
//! pair, refuses the registration loudly. Changing a canonical type is a
//! deliberate migration (#303), never a model-struct edit.
//!
use std::collections::BTreeMap;

use ankurah_proto::{EntityId, RegisterModel, RegisterProperty, RegisteredModel, RegisteredProperty};

use crate::context::Context;
use crate::error::{MutationError, RetrievalError};
use crate::model::View;
use crate::policy::PolicyAgent;
use crate::schema::catalog::rows::{
    SysModelPropertyRow, SysModelPropertyRowView, SysModelRow, SysModelRowView, SysPropertyRow, SysPropertyRowView,
};
use crate::storage::StorageEngine;
use ankurah_core_types::ValueType;

use ankql::ast::Selection;

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
    /// SystemRoot operates only on built-in system models; it never
    /// registers application models.
    #[error("SystemRoot cannot register application model '{0}'")]
    SystemRootApplicationModel(String),
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

impl<SE, PA> super::catalog::CatalogManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Resolve and persist a RegisterSchema request through the ordinary
    /// typed Model/Mutable transaction path under local SystemRoot authority.
    pub async fn register_schema(&self, models: Vec<RegisterModel>) -> Result<Vec<RegisteredModel>, RegistrationError> {
        let node = self.node().ok_or(RegistrationError::SystemNotReady)?;
        if !node.durable {
            return Err(RegistrationError::NotDurable);
        }
        if node.system.root().is_none() {
            return Err(RegistrationError::SystemNotReady);
        }
        let registration_validity = self.registration_validity().ok_or(RegistrationError::SystemNotReady)?;
        // Catalog and system collections route by built-in identity and can
        // never be declared by an application descriptor.
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
        if !self.wait_catalog_ready_if_current(&registration_validity).await {
            return Err(RegistrationError::SystemNotReady);
        }
        let _registration_lease = registration_validity.try_acquire().ok_or(RegistrationError::SystemNotReady)?;
        let _allocator = self.lock_allocator().await;
        let context = Context::new_system_root(node)?;
        let trx = context.begin();
        let mut changed = false;

        // Pass 1 resolves model shells so target-model references in pass 2
        // can point at models declared later in the same request.
        let mut model_ids: BTreeMap<String, (EntityId, usize)> = BTreeMap::new();
        let mut out_models: Vec<RegisteredModel> = Vec::new();
        for m in &models {
            if model_ids.contains_key(&m.label) {
                continue;
            }
            let (model_id, resolved_name) = match m.explicit_id {
                Some(id) => {
                    let def = self.verify_explicit_model_binding(&context, id, m).await?;
                    (id, def.name)
                }
                None => match self.model_lookup_checked(&context, &m.label).await? {
                    Some(def) => {
                        if def.name != m.name {
                            trx.get::<SysModelRow>(&def.id).await?.name().set(&m.name).map_err(MutationError::from)?;
                            changed = true;
                        }
                        (def.id, m.name.clone())
                    }
                    None => {
                        let id = trx.create(&SysModelRow { label: m.label.clone(), name: m.name.clone() }).await?.id();
                        changed = true;
                        (id, m.name.clone())
                    }
                },
            };
            model_ids.insert(m.label.clone(), (model_id, out_models.len()));
            out_models.push(RegisteredModel { id: model_id, label: m.label.clone(), name: resolved_name, properties: Vec::new() });
        }

        // Resolve a target label, creating its catalog model shell if the
        // target was not separately declared.
        macro_rules! resolve_model {
            ($label:expr) => {{
                let l: &str = $label;
                match model_ids.get(l) {
                    Some((id, _)) => *id,
                    None => match self.model_lookup_checked(&context, l).await? {
                        Some(def) => {
                            model_ids.insert(l.to_string(), (def.id, out_models.len()));
                            out_models.push(RegisteredModel { id: def.id, label: def.label, name: def.name, properties: Vec::new() });
                            def.id
                        }
                        None => {
                            let id = trx.create(&SysModelRow { label: l.to_string(), name: l.to_string() }).await?.id();
                            changed = true;
                            model_ids.insert(l.to_string(), (id, out_models.len()));
                            out_models.push(RegisteredModel { id, label: l.to_string(), name: l.to_string(), properties: Vec::new() });
                            id
                        }
                    },
                }
            }};
        }

        let mut property_ids: BTreeMap<(EntityId, String), (EntityId, String, String)> = BTreeMap::new();
        let mut membership_ids: BTreeMap<(EntityId, EntityId), EntityId> = BTreeMap::new();
        for m in &models {
            let (model_id, out_index) = *model_ids.get(&m.label).expect("pass 1 resolved every model label");
            for p in &m.properties {
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
                    let def = self.verify_explicit_binding(&context, id, &m.label, p).await?;
                    let backend = def.backend.clone();
                    let value_type = def.value_type.clone();
                    let membership_id =
                        self.ensure_membership(&context, &trx, &mut membership_ids, &mut changed, model_id, id, p.optional).await?;
                    property_ids.insert((model_id, p.name.clone()), (id, backend.clone(), value_type.clone()));
                    out_models[out_index].properties.push(RegisteredProperty {
                        id,
                        membership_id,
                        name: if def.name.is_empty() { p.name.clone() } else { def.name },
                        backend,
                        value_type,
                        target_model: def.target_model,
                        minted_for: def.minted_for,
                        optional: p.optional,
                    });
                    continue;
                }

                let target = match &p.target_label {
                    Some(tl) => Some(resolve_model!(tl)),
                    None => None,
                };

                let current = self.member_property_lookup_checked(&context, &model_id, &p.name).await?;
                let renamed = match (&current, &p.renamed_from) {
                    (None, Some(old)) => self.member_property_lookup_checked(&context, &model_id, old).await?,
                    _ => None,
                };
                let canonical = current.as_ref().or(renamed.as_ref()).map(|(def, _)| (def.backend.clone(), def.value_type.clone()));
                if let Some((def, _)) = current.as_ref().or(renamed.as_ref()) {
                    check_property_compat(def, &m.label, p)?;
                }

                let (property_id, membership_id) = match (&current, &renamed) {
                    (Some((def, membership)), _) => {
                        if def.target_model != target {
                            trx.get::<SysPropertyRow>(&def.id).await?.target_model().set(&target).map_err(MutationError::from)?;
                            changed = true;
                        }
                        let membership_id = self.ensure_membership_from(&trx, &mut changed, membership.clone(), p.optional).await?;
                        membership_ids.insert((model_id, def.id), membership_id);
                        (def.id, membership_id)
                    }
                    (None, Some((def, membership))) => {
                        let property = trx.get::<SysPropertyRow>(&def.id).await?;
                        property.name().set(&p.name).map_err(MutationError::from)?;
                        if def.target_model != target {
                            property.target_model().set(&target).map_err(MutationError::from)?;
                        }
                        changed = true;
                        let membership_id = self.ensure_membership_from(&trx, &mut changed, membership.clone(), p.optional).await?;
                        membership_ids.insert((model_id, def.id), membership_id);
                        (def.id, membership_id)
                    }
                    (None, None) => {
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
                        changed = true;
                        let membership_id =
                            self.ensure_membership(&context, &trx, &mut membership_ids, &mut changed, model_id, id, p.optional).await?;
                        (id, membership_id)
                    }
                };

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

        if changed {
            trx.commit().await?;
        } else {
            trx.rollback();
        }

        Ok(out_models)
    }

    async fn ensure_membership(
        &self,
        context: &Context,
        trx: &crate::transaction::Transaction,
        request_memberships: &mut BTreeMap<(EntityId, EntityId), EntityId>,
        changed: &mut bool,
        model: EntityId,
        property: EntityId,
        optional: bool,
    ) -> Result<EntityId, RegistrationError> {
        if let Some(id) = request_memberships.get(&(model, property)) {
            return Ok(*id);
        }
        let id = match self.membership_lookup_checked(context, &model, &property).await? {
            Some(def) => self.ensure_membership_from(trx, changed, def, optional).await?,
            None => {
                *changed = true;
                trx.create(&SysModelPropertyRow { model, property, optional }).await?.id()
            }
        };
        request_memberships.insert((model, property), id);
        Ok(id)
    }

    async fn ensure_membership_from(
        &self,
        trx: &crate::transaction::Transaction,
        changed: &mut bool,
        def: super::catalog::ModelPropertyMembershipDef,
        optional: bool,
    ) -> Result<EntityId, RegistrationError> {
        if def.optional != Some(optional) {
            trx.get::<SysModelPropertyRow>(&def.id).await?.optional().set(&optional).map_err(MutationError::from)?;
            *changed = true;
        }
        Ok(def.id)
    }

    async fn model_lookup_checked(&self, context: &Context, label: &str) -> Result<Option<super::catalog::ModelDef>, RetrievalError> {
        if let Some(def) = self.model_by_label(label) {
            return Ok(Some(def));
        }
        let mut rows =
            context.fetch::<SysModelRowView>(Selection { predicate: field_eq_str("label", label), order_by: None, limit: None }).await?;
        rows.sort_by_key(|row| row.id());
        rows.first().map(model_def).transpose()
    }

    async fn member_property_lookup_checked(
        &self,
        context: &Context,
        model: &EntityId,
        name: &str,
    ) -> Result<Option<(super::catalog::PropertyDef, super::catalog::ModelPropertyMembershipDef)>, RetrievalError> {
        if let Some(def) = self.property_by_name(model, name) {
            if let Some(membership) = self.membership(model, &def.id) {
                return Ok(Some((def, membership)));
            }
        }
        let mut rows = context
            .fetch::<SysModelPropertyRowView>(Selection { predicate: field_eq_id("model", *model), order_by: None, limit: None })
            .await?;
        rows.sort_by_key(|row| row.id());
        for row in rows {
            let Ok(membership) = membership_def(&row) else { continue };
            let property = match catalog_row_by_id::<SysPropertyRowView>(context, membership.property).await? {
                Some(row) => property_def(&row)?,
                None => continue,
            };
            if property.name != name {
                continue;
            }
            return Ok(Some((property, membership)));
        }
        Ok(None)
    }

    async fn membership_lookup_checked(
        &self,
        context: &Context,
        model: &EntityId,
        property: &EntityId,
    ) -> Result<Option<super::catalog::ModelPropertyMembershipDef>, RetrievalError> {
        if let Some(def) = self.membership(model, property) {
            return Ok(Some(def));
        }
        let predicate = and(field_eq_id("model", *model), field_eq_id("property", *property));
        let mut rows = context.fetch::<SysModelPropertyRowView>(Selection { predicate, order_by: None, limit: None }).await?;
        rows.sort_by_key(|row| row.id());
        rows.first().map(membership_def).transpose()
    }

    async fn verify_explicit_binding(
        &self,
        context: &Context,
        id: EntityId,
        model_label: &str,
        p: &RegisterProperty,
    ) -> Result<super::catalog::PropertyDef, RegistrationError> {
        let Some(row) = catalog_row_by_id::<SysPropertyRowView>(context, id).await? else {
            return Err(RegistrationError::ExplicitIdNotFound { property: id });
        };
        let def = property_def(&row)?;
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
        Ok(def)
    }

    async fn verify_explicit_model_binding(
        &self,
        context: &Context,
        id: EntityId,
        m: &RegisterModel,
    ) -> Result<super::catalog::ModelDef, RegistrationError> {
        let Some(row) = catalog_row_by_id::<SysModelRowView>(context, id).await? else {
            return Err(RegistrationError::ExplicitModelIdNotFound { model: id });
        };
        let def = model_def(&row)?;
        if def.label != m.label {
            return Err(RegistrationError::ExplicitModelIdMismatch { model: id, found_label: def.label, label: m.label.clone() });
        }
        Ok(def)
    }
}

fn row_error(kind: &str, id: EntityId, error: crate::property::PropertyError) -> RetrievalError {
    RetrievalError::Other(format!("malformed {kind} catalog row {id}: {error}"))
}

fn model_def(row: &SysModelRowView) -> Result<super::catalog::ModelDef, RetrievalError> {
    let label = row.label().map_err(|error| row_error("model", row.id(), error))?;
    let name = row.name().unwrap_or_else(|_| label.clone());
    Ok(super::catalog::ModelDef { id: row.id(), label, name })
}

fn property_def(row: &SysPropertyRowView) -> Result<super::catalog::PropertyDef, RetrievalError> {
    Ok(super::catalog::PropertyDef {
        id: row.id(),
        minted_for: row.minted_for().map_err(|error| row_error("property", row.id(), error))?,
        name: row.name().map_err(|error| row_error("property", row.id(), error))?,
        backend: row.backend().map_err(|error| row_error("property", row.id(), error))?,
        value_type: row.value_type().map_err(|error| row_error("property", row.id(), error))?,
        target_model: row.target_model().map_err(|error| row_error("property", row.id(), error))?,
    })
}

fn membership_def(row: &SysModelPropertyRowView) -> Result<super::catalog::ModelPropertyMembershipDef, RetrievalError> {
    Ok(super::catalog::ModelPropertyMembershipDef {
        id: row.id(),
        model: row.model().map_err(|error| row_error("membership", row.id(), error))?,
        property: row.property().map_err(|error| row_error("membership", row.id(), error))?,
        optional: row.optional().ok(),
    })
}

/// Fetch one typed catalog row by id through the collection scan rather than
/// accepting a state solely because a storage backend has that globally
/// unique id in another collection. Explicit bindings must prove both the id
/// and the catalog collection they claim.
async fn catalog_row_by_id<R: View>(context: &Context, id: EntityId) -> Result<Option<R>, RetrievalError> {
    let rows = context.fetch::<R>(Selection { predicate: field_eq_id("id", id), order_by: None, limit: Some(1) }).await?;
    Ok(rows.into_iter().find(|row| row.id() == id))
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

// -- allocator lookup predicate builders -------------------------------------
//
// Catalog collections use built-in System properties, so these predicates
// are already identity-resolved before they enter the normal typed Context
// fetch path.

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
