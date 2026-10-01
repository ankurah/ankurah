use ankql::ast::{Parsed, Predicate, Resolved};
use ankurah_core::schema::catalog::CatalogManager;
use ankurah_core::schema::resolver::{ModelResolver, ResolvedProperty};
use ankurah_proto::{ModelId, PropertyId};
use ankurah_core_types::ValueType;

/// Catalog lookup during policy authoring, never during enforcement.
pub trait PolicyCatalog: Send + Sync {
    fn model_labels(&self) -> Vec<(ModelId, String)>;
    fn property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, String>;
    fn property_type(&self, model: &ModelId, property: &PropertyId) -> Result<ValueType, String>;
    fn resolve_predicate(&self, model: &ModelId, predicate: Predicate<Parsed>) -> Result<Predicate<Resolved>, String>;
}

impl PolicyCatalog for CatalogManager {
    fn model_labels(&self) -> Vec<(ModelId, String)> { self.model_labels() }
    fn property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, String> {
        self.resolve_property(model, name).map_err(|error| error.to_string())
    }

    fn property_type(&self, model: &ModelId, property: &PropertyId) -> Result<ValueType, String> {
        self.registered_value_type(model, property).map_err(|error| error.to_string())
    }

    fn resolve_predicate(&self, model: &ModelId, predicate: Predicate<Parsed>) -> Result<Predicate<Resolved>, String> {
        self.resolve_selection(model, predicate.into()).map(|selection| selection.predicate).map_err(|error| error.to_string())
    }
}
