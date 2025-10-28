use ankurah_core::selection::filter::Filterable;
use ankurah_proto::{CollectionId, EntityId};

// Lightweight filterable over materialized values
#[derive(Debug)]
pub struct MatEntity {
    pub(crate) id: EntityId,
    pub(crate) collection: CollectionId,
    pub(crate) map: std::collections::BTreeMap<String, ankurah_core::value::Value>,
}
impl Filterable for MatEntity {
    fn collection(&self) -> &str { self.collection.as_str() }
    fn value(&self, name: &str) -> Option<ankurah_core::value::Value> {
        if name == "id" {
            return Some(ankurah_core::value::Value::EntityId(self.id));
        }
        self.map.get(name).cloned()
    }
}

// Temporary wrapper to make Result<(IVec, MatEntity), RetrievalError> implement Filterable
// This allows our iterators to work with GetPropertyValueStream until we implement proper error handling
#[derive(Debug)]
pub struct MatRow {
    pub id: EntityId,
    pub mat: MatEntity,
}

impl Filterable for MatRow {
    fn collection(&self) -> &str { self.mat.collection() }
    fn value(&self, name: &str) -> Option<ankurah_core::value::Value> {
        if name == "id" {
            Some(ankurah_core::value::Value::EntityId(self.id))
        } else {
            self.mat.value(name)
        }
    }
}

impl ankurah_storage_common::filtering::HasEntityId for MatRow {
    fn entity_id(&self) -> EntityId { self.id }
}

// TODO: We'll need to handle Result<(IVec, MatEntity), RetrievalError> in the iterator logic
// instead of trying to make it implement Filterable directly (orphan rules prevent this)
