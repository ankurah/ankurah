use ankurah_core::selection::filter::Filterable;
use ankurah_proto::{CollectionId, EntityId};
use ankurah_storage_common::filtering::HasEntityId;

/// Projected property values for one entity, used for filtering and sorting.
/// Contains pre-materialized values extracted from the collection tree.
#[derive(Debug)]
pub struct ProjectedEntity {
    pub(crate) id: EntityId,
    pub(crate) collection: CollectionId,
    pub(crate) map: std::collections::BTreeMap<String, ankurah_core::value::Value>,
}

impl Filterable for ProjectedEntity {
    fn collection(&self) -> &str { self.collection.as_str() }
    fn value(&self, name: &str) -> Option<ankurah_core::value::Value> {
        if name == "id" {
            return Some(ankurah_core::value::Value::EntityId(self.id));
        }
        self.map.get(name).cloned()
    }
}

impl HasEntityId for ProjectedEntity {
    fn entity_id(&self) -> EntityId { self.id }
}
