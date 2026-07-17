use ankql::ast::PropertyId;
use ankurah_core::selection::filter::Filterable;
use ankurah_proto::{CollectionId, EntityId};
use ankurah_storage_common::filtering::HasEntityId;

/// Projected property values for one entity, used for filtering and sorting.
/// Contains pre-materialized values extracted from the collection tree, keyed by
/// durable [`PropertyId`] (NOT physical column) so the in-memory post-filter and
/// sort read by resolved identity exactly like the entity's own evaluation.
#[derive(Debug)]
pub struct ProjectedEntity {
    pub(crate) id: EntityId,
    pub(crate) collection: CollectionId,
    pub(crate) map: std::collections::BTreeMap<PropertyId, ankurah_core::value::Value>,
}

impl Filterable for ProjectedEntity {
    fn collection(&self) -> &str { self.collection.as_str() }

    /// Read by name: the `id` pseudo-property and system properties. A registered
    /// property is addressed by id via [`Filterable::value_by_id`], not here.
    fn value(&self, name: &str) -> Option<ankurah_core::value::Value> {
        if name == "id" {
            return Some(ankurah_core::value::Value::EntityId(self.id));
        }
        self.map.get(&PropertyId::System { name: name.to_string() }).cloned()
    }

    /// Read a registered property by its stable id -- the identity form a
    /// resolved `Expr::PropertyIdentifier` evaluates through. A miss is absent
    /// (NULL); there is no fallback to a name.
    fn value_by_id(&self, property_id: EntityId) -> Option<ankurah_core::value::Value> {
        self.map.get(&PropertyId::EntityId(property_id.to_ulid())).cloned()
    }
}

impl HasEntityId for ProjectedEntity {
    fn entity_id(&self) -> EntityId { self.id }
}
