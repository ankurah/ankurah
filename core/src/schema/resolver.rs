//! Selection resolution: binds the property names in a parsed selection to
//! durable identities, exactly once, where a query enters the system.
//! Downstream of that point -- filtering, the reactor, storage --
//! everything addresses properties by [`PropertyId`] alone.
//!
//! Two [`NameResolver`] implementations serve the two ways a query names a
//! model. A query made with a compiled declaration in hand (`fetch::<R>`)
//! binds field names through the descriptor's cells, so the binding is fixed
//! per epoch and a display-name change cannot re-aim a running query. A query
//! that names a collection by string alone (relay and policy paths) binds
//! through the catalog map's current display names. Both resolutions fail
//! closed on unknown names (`NameResolutionError::UnknownProperty`), never
//! treating a typo as NULL.

use ankql::ast::{PropertyId, PropertyPath, Selection};
use ankql::{NameResolutionError, NameResolver};
use ankurah_proto::{CollectionId, ModelId, SystemProperty};

use super::catalog::CatalogManager;
use super::{ModelStructDescriptor, SchemaEpoch};
use crate::error::RetrievalError;
use crate::policy::PolicyAgent;
use crate::storage::StorageEngine;
use crate::value::ValueType;

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
impl<SE, PA> NameResolver for CatalogManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn is_ready(&self, _model: &ModelId) -> bool {
        // The map is authoritative only once the durable warm (or, later,
        // the catalog subscription) has published it; before that, an
        // absent name is indistinguishable from metadata that has not
        // arrived, and resolution reports not-ready instead of unknown.
        self.is_catalog_ready()
    }

    fn resolve_model_name(&self, name: &str) -> Result<Option<ModelId>, NameResolutionError> {
        // Answers the legacy collection-qualified reference form
        // (`album.name`): resolution strips the qualifier when this returns
        // the model being queried.
        Ok(self.model_id_for(name))
    }

    fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<PropertyId>, NameResolutionError> {
        self.try_resolve(model, name).map_err(|error| NameResolutionError::Lookup {
            model: *model,
            name: name.to_owned(),
            message: error.to_string(),
        })
    }

    fn property_value_type(&self, model: &ModelId, property: &PropertyId) -> Result<ValueType, NameResolutionError> {
        let lookup_failed =
            |message: &str| NameResolutionError::ValueTypeLookup { model: *model, property: *property, message: message.into() };
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

impl<SE, PA> NameResolver for DescriptorResolver<'_, SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn is_ready(&self, model: &ModelId) -> bool { self.catalog.is_ready(model) }

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
