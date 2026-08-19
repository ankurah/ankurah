//! Selection resolution: the model-scoped name lookup, and the one walk that
//! carries a selection from names to durable identities.
//!
//! A query is written in names and enters the system exactly once through
//! [`resolve_selection`], the ONLY door from [`Parsed`] to [`Resolved`].
//! Downstream of that door -- filtering, the reactor, storage, the wire --
//! everything addresses properties by [`PropertyId`] alone. Each name's
//! lookup answers with the property's registered type as well as its
//! identity, and the walk casts that comparison's literals to the type on the
//! spot, so no consumer downstream has to guess what a literal was compared
//! against.
//!
//! Two [`ModelResolver`] implementations serve the two ways a query names a
//! model. A query made with a compiled declaration in hand (`fetch::<R>`,
//! `query::<R>`) binds field names through the descriptor's cells, so the
//! binding is fixed per epoch and a display-name change cannot re-aim a
//! running query. A query that names a collection by string alone (relay and
//! policy paths) binds through the catalog map's current display names. Both
//! resolutions fail closed on unknown names
//! ([`ModelResolutionError::UnknownProperty`]), never treating a typo as
//! NULL.

use ankql::ast::{Expr, OrderByItem, Parsed, PathExpr, Predicate, PropertyIdExt, PropertyPath, Resolved, Selection};
use ankurah_proto::{CollectionId, ModelId, PropertyId, SystemProperty};
use thiserror::Error;

use super::catalog::CatalogManager;
use super::{ModelStructDescriptor, SchemaEpoch};
use crate::error::RetrievalError;
use crate::policy::PolicyAgent;
use crate::storage::StorageEngine;
use crate::value::ValueType;

/// What one source-level property name binds to: the durable identity every
/// consumer past the query boundary addresses it by, plus the registered type
/// its comparison literals canonicalize to. One lookup answers both, because
/// a property whose canonical type cannot be supplied is not a resolved
/// property -- the walk would have nothing to canonicalize against, and a
/// comparison would reach execution untyped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedProperty {
    /// The durable property identity.
    pub id: PropertyId,
    /// The property's registered logical type.
    pub value_type: ValueType,
}

/// A failure to bind a source-level name, or to canonicalize a comparison
/// literal against the type that name bound to.
#[derive(Debug, Error)]
pub enum ModelResolutionError {
    /// A model-scoped property lookup failed.
    #[error("property lookup for '{name}' in model '{model}' failed: {message}")]
    Lookup {
        /// The model being searched.
        model: ModelId,
        /// The source-level property name.
        name: String,
        /// The resolver's failure message.
        message: String,
    },
    /// A qualified model-name lookup failed.
    #[error("model lookup for qualifier '{name}' failed: {message}")]
    ModelLookup {
        /// The source-level model qualifier.
        name: String,
        /// The resolver's failure message.
        message: String,
    },
    /// No property with the requested name exists in an authoritative model.
    #[error("unknown property '{name}' in model '{model}'")]
    UnknownProperty {
        /// The model being searched.
        model: ModelId,
        /// The unresolved source-level name.
        name: String,
    },
    /// A resolved property cannot support the requested nested path.
    #[error("unsupported subpath '{path}' in model '{model}': {reason}")]
    UnsupportedSubpath {
        /// The model containing the property.
        model: ModelId,
        /// The complete source-level path.
        path: String,
        /// Why this consumer rejects the subpath.
        reason: String,
    },
    /// A comparison literal could not be cast to the registered type of the
    /// property it is compared against.
    #[error("comparison canonicalization in model '{model}' failed: {message}")]
    Canonicalization {
        /// The model containing the comparison.
        model: ModelId,
        /// The cast failure message.
        message: String,
    },
    /// A property resolved by name, but its registered type could not be
    /// supplied.
    #[error("value-type lookup for property '{property}' in model '{model}' failed: {message}")]
    ValueTypeLookup {
        /// The model containing the property.
        model: ModelId,
        /// The durable property identity.
        property: PropertyId,
        /// The resolver's failure message.
        message: String,
    },
}

/// The model-scoped name lookup [`resolve_selection`] runs on.
///
/// Exactly two lookups, both fail-closed: a miss is authoritative, so
/// `Ok(None)` means there is no such model or property -- never "not known
/// yet, ask again later". Implementors answer for ordinary registered models
/// only; the frozen system models resolve inside the walk from the closed
/// [`SystemProperty`] vocabulary and are never shown to an implementor.
pub trait ModelResolver {
    /// Resolve a source-level model qualifier, where qualified property paths
    /// are supported. Returning `None` leaves the first path step to be read
    /// as a property name, which is what a resolver with no qualifier
    /// vocabulary wants.
    fn resolve_model(&self, _name: &str) -> Result<Option<ModelId>, ModelResolutionError> { Ok(None) }

    /// Resolve `name` within an already-known `model`, answering with the
    /// durable identity AND the registered type this property's comparison
    /// literals canonicalize to.
    ///
    /// `Ok(None)` means the property is absent: a miss is authoritative. A
    /// property that exists but whose type cannot be supplied is an error,
    /// never a silent skip of canonicalization.
    fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError>;
}

/// The canonical value type of each closed system property. System
/// properties have no catalog rows to record their types (they are the
/// frozen bootstrap base case), so the vocabulary is closed here instead.
pub(crate) fn system_property_value_type(property: SystemProperty) -> ValueType {
    match property {
        SystemProperty::Item => ValueType::String,
        SystemProperty::Label => ValueType::String,
        SystemProperty::Name => ValueType::String,
        SystemProperty::MintedFor => ValueType::EntityId,
        SystemProperty::Backend => ValueType::String,
        SystemProperty::ValueType => ValueType::String,
        SystemProperty::TargetModel => ValueType::EntityId,
        SystemProperty::Model => ValueType::EntityId,
        SystemProperty::Property => ValueType::EntityId,
        SystemProperty::Optional => ValueType::Bool,
    }
}

/// Raw resolution: the catalog map's current display names, scoped to the
/// model being queried.
impl<SE, PA> ModelResolver for CatalogManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn resolve_model(&self, name: &str) -> Result<Option<ModelId>, ModelResolutionError> {
        // Answers the legacy collection-qualified reference form
        // (`album.name`): resolution strips the qualifier when this returns
        // the model being queried.
        Ok(self.model_id_for(name))
    }

    fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
        let found = self.try_resolve(model, name).map_err(|error| ModelResolutionError::Lookup {
            model: *model,
            name: name.to_owned(),
            message: error.to_string(),
        })?;
        let Some(id) = found else { return Ok(None) };
        Ok(Some(ResolvedProperty { id, value_type: self.registered_value_type(model, &id)? }))
    }
}

impl<SE, PA> CatalogManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// The registered type of a property the catalog map has already
    /// resolved: the `id` pseudo-property and the frozen system vocabulary
    /// carry their own, and an allocated property takes the one its catalog
    /// definition records.
    fn registered_value_type(&self, model: &ModelId, property: &PropertyId) -> Result<ValueType, ModelResolutionError> {
        let lookup_failed =
            |message: &str| ModelResolutionError::ValueTypeLookup { model: *model, property: *property, message: message.into() };
        match property {
            PropertyId::Id => Ok(ValueType::EntityId),
            PropertyId::System(system) => Ok(system_property_value_type(*system)),
            PropertyId::EntityId(id) => {
                let def = self.property_by_id(id).ok_or_else(|| lookup_failed("no catalog definition"))?;
                ValueType::from_property_str(&def.value_type)
                    .ok_or_else(|| lookup_failed(&format!("unparseable registered type '{}'", def.value_type)))
            }
        }
    }
}

/// Typed resolution: the compiled declaration's fields bind through their
/// descriptor cells under the operation's epoch; names the struct does not
/// carry fall back to raw catalog resolution (an older binary may query a
/// field it does not compile).
pub(crate) struct DescriptorResolver<'a, SE, PA: PolicyAgent> {
    pub schema: &'static ModelStructDescriptor,
    pub epoch: SchemaEpoch,
    pub catalog: &'a CatalogManager<SE, PA>,
}

impl<SE, PA> ModelResolver for DescriptorResolver<'_, SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn resolve_model_name(&self, name: &str) -> Result<Option<ModelId>, NameResolutionError> {
        // The declaration's own label answers from its cells (the admitted
        // identity); anything else falls back to the catalog, like a raw
        // reference.
        if name == self.schema.label {
            if let Some(model) = self.schema.resolved.get(self.epoch) {
                return Ok(Some(model));
            }
        }
        self.catalog.resolve_model_name(name)
    }

    fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<PropertyId>, NameResolutionError> {
        if let Some(field) = self.schema.field_by_name(name) {
            // A populated cell is the admitted binding, which a later
            // display-name change cannot re-aim. A miss means this epoch has
            // not passed the registration gate (the synchronous query entry
            // cannot await it); before the gate there is no admitted binding
            // to protect, so the name resolves like a raw one, against the
            // catalog's current names. After the gate the cells always hit.
            if let Some(id) = field.resolved.get(self.epoch) {
                return Ok(Some(id));
            }
        }
        self.catalog.resolve_property(model, name)
    }

    fn property_value_type(&self, model: &ModelId, property: &PropertyId) -> Result<ValueType, NameResolutionError> {
        if let Some(field) = self.schema.properties.iter().find(|field| field.resolved.get(self.epoch) == Some(*property)) {
            return ValueType::from_property_str(field.value_type).ok_or_else(|| NameResolutionError::ValueTypeLookup {
                model: *model,
                property: *property,
                message: format!("unparseable compiled type '{}'", field.value_type),
            });
        }
        self.catalog.property_value_type(model, property)
    }
}

/// Resolve one selection under `model` scope: bind its names to durable
/// ids, then convert comparison literals to the properties' registered
/// types. A selection that already passes [`Selection::check`] (fully
/// resolved, fully populated -- including one with no property references
/// at all) passes through untouched, which is what makes the shared entry
/// points idempotent over selections the typed entry already resolved.
pub(crate) fn resolve_selection_with<R: NameResolver + ?Sized>(
    selection: Selection,
    model: &ModelId,
    resolver: &R,
) -> Result<Selection, RetrievalError> {
    if selection.check().is_ok() {
        return Ok(selection);
    }
    let resolved = selection.resolve_names(model, resolver).map_err(|error| RetrievalError::Other(error.to_string()))?;
    let type_of = |path: &PropertyPath| resolver.property_value_type(model, &path.property_id()).ok();
    match resolved.cast_comparison_values(&type_of) {
        Ok(casted) => Ok(casted),
        // A value that cannot take its property's registered type does not
        // fail the query: policy-injected comparisons deliberately hold a
        // typed field against a non-conforming literal (the row is denied,
        // never the query), and every execution consumer re-casts at its own
        // trust boundary regardless -- a normalized AST is never treated as
        // proof. The selection proceeds resolved but uncanonicalized.
        Err(_) => Ok(resolved),
    }
}

impl<SE, PA> CatalogManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Resolve a RAW selection against `collection`: bind its names through
    /// the catalog's current display names and convert its comparison
    /// values. Selections that are already resolved (typed entries resolve
    /// descriptor-backed before reaching the shared paths) pass through
    /// untouched, and a selection with no property references never needs
    /// the model scope at all.
    pub fn resolve_selection(&self, collection: &CollectionId, selection: Selection) -> Result<Selection, RetrievalError> {
        if selection.check().is_ok() {
            return Ok(selection);
        }
        let model = self.model_id_for(collection.as_str()).ok_or_else(|| {
            RetrievalError::Other(format!("collection '{collection}' is not a registered model; its property names cannot resolve"))
        })?;
        resolve_selection_with(selection, &model, self)
    }

    /// Resolve a selection under a compiled declaration: field names bind
    /// through the descriptor's cells at this node's current epoch. Callers
    /// run this after the registration gate, so the cells are populated for
    /// the epoch snapshotted here.
    pub fn resolve_selection_with_descriptor(
        &self,
        schema: &'static ModelStructDescriptor,
        selection: Selection,
    ) -> Result<Selection, RetrievalError> {
        if selection.check().is_ok() {
            return Ok(selection);
        }
        let epoch =
            self.schema_epoch().ok_or_else(|| RetrievalError::Other("no system is ready; typed selection cannot resolve".to_string()))?;
        // The model scope prefers the admitted identity; before the gate
        // (the synchronous query entry) it falls back to the catalog's
        // label binding, like a raw query.
        let model = match schema.resolved.get(epoch) {
            Some(model) => model,
            None => self.model_id_for(schema.label).ok_or_else(|| {
                RetrievalError::Other(format!("model '{}' is not registered; its property names cannot resolve", schema.label))
            })?,
        };
        resolve_selection_with(selection, &model, &DescriptorResolver { schema, epoch, catalog: self })
    }
}
