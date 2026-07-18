use ankurah_proto::{EntityId, ModelId};

/// Read-only access to the durable metadata catalog.
///
/// Entities use this interface to bind display names to stable property ids;
/// storage and wire-envelope paths also use it for reverse property names,
/// canonical value types, and model ids. The catalog remains the sole owner
/// of this metadata; consumers receive a weak trait object so they cannot keep
/// a node alive.
pub trait CatalogResolver: Send + Sync {
    /// The property-definition id for `name` in `collection`, if the catalog
    /// knows it. `None` for a system/catalog field or a user field not yet
    /// registered, which a synchronous accessor may stage transiently by name.
    fn resolve(&self, collection: &str, name: &str) -> Option<EntityId>;

    /// The display name of a property-definition id, if the catalog knows it.
    /// Storage engines use this only to seed durable physical-name maps.
    fn name_for(&self, id: &EntityId) -> Option<String>;

    /// The model-definition id for `collection`, if the catalog knows it.
    /// Built-in collections are answered by the static `WellKnown` arm before
    /// callers consult this resolver; catalog-backed models use `Entity`.
    fn model_id_for(&self, collection: &str) -> Option<ModelId> {
        let _ = collection;
        None
    }

    /// The canonical catalog value type for a property-definition id.
    fn canonical_value_type(&self, id: &EntityId) -> Option<String> {
        let _ = id;
        None
    }
}
