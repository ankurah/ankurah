use ankql::ast::PropertyId;
use ankurah_proto::ModelId;
use anyhow::Result;

use crate::value::ValueType;

/// Read-only access to the durable metadata catalog, addressed exclusively by
/// durable model/property identities. Human names are returned as metadata or
/// accepted at the compile/schema binding boundary; they are never runtime
/// storage addresses.
pub trait CatalogResolver: Send + Sync {
    /// Whether a missing property lookup is authoritative for this model.
    /// Catalog-backed implementations remain unready while their metadata is
    /// only a partial asynchronous snapshot.
    fn model_properties_ready(&self, _model: &ModelId) -> bool { true }

    /// Resolve a source/schema model name to its durable identity. Query
    /// resolution uses this only to recognize an optional model qualifier;
    /// runtime storage remains addressed by the already-known `ModelId`.
    fn resolve_model(&self, _name: &str) -> Result<Option<ModelId>> { Ok(None) }

    /// Resolve a source/schema property name inside an already-known model.
    fn resolve_model_property(&self, model: &ModelId, property_name: &str) -> Result<Option<PropertyId>>;

    /// Enumerate the durable properties projected by a model.
    ///
    /// Storage engines use this when refreshing a model materialization from
    /// canonical entity state. The result excludes the `id` pseudo-property,
    /// which every engine handles as a fixed primary-key projection.
    fn model_properties(&self, model: &ModelId) -> Result<Vec<PropertyId>>;

    /// The registered model name. Not guaranteed to be unique. SQL/IndexedDB engines use this only
    /// to seed a private materialization-name assignment on first use.
    fn model_name(&self, model: &ModelId) -> Result<String>;

    /// The registered name of a property. All properties have a name. Not guaranteed to be unique.
    /// Storage engines use this only to seed durable physical-name maps.
    fn property_name(&self, id: &PropertyId) -> Result<String>;

    /// The canonical catalog value type for a durable property identity.
    fn property_value_type(&self, property: &PropertyId) -> Result<ValueType>;
}

// A generic `impl<T: CatalogResolver> ankql::NameResolver for T` is not
// coherent because `NameResolver` belongs to another crate and `T` is an
// uncovered type parameter. The local trait object is the blanket bridge:
// every CatalogResolver implementation coerces to this one implementation.
impl ankql::NameResolver for dyn CatalogResolver + '_ {
    fn is_ready(&self, model: &ModelId) -> bool { self.model_properties_ready(model) }

    fn resolve_model_name(&self, name: &str) -> std::result::Result<Option<ModelId>, ankql::NameResolutionError> {
        self.resolve_model(name)
            .map_err(|error| ankql::NameResolutionError::ModelLookup { name: name.to_owned(), message: error.to_string() })
    }

    fn resolve_property(&self, model: &ModelId, name: &str) -> std::result::Result<Option<PropertyId>, ankql::NameResolutionError> {
        self.resolve_model_property(model, name).map_err(|error| ankql::NameResolutionError::Lookup {
            model: *model,
            name: name.to_owned(),
            message: error.to_string(),
        })
    }

    fn property_value_type(&self, model: &ModelId, property: &PropertyId) -> std::result::Result<ValueType, ankql::NameResolutionError> {
        CatalogResolver::property_value_type(self, property).map_err(|error| ankql::NameResolutionError::ValueTypeLookup {
            model: *model,
            property: *property,
            message: error.to_string(),
        })
    }
}
