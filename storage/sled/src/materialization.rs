use ankql::selection::filter::Filterable;
use ankurah_core::error::RetrievalError;
use ankurah_proto::{CollectionId, EntityId};

// Lightweight filterable over materialized values
pub struct MatEntity {
    pub(crate) id: EntityId,
    pub(crate) collection: CollectionId,
    pub(crate) map: std::collections::BTreeMap<String, ankurah_core::property::PropertyValue>,
}
impl Filterable for MatEntity {
    fn collection(&self) -> &str { self.collection.as_str() }
    fn value(&self, name: &str) -> Option<String> {
        if name == "id" {
            return Some(self.id.to_base64());
        }
        self.map.get(name).and_then(|v| match v {
            ankurah_core::property::PropertyValue::String(s) => Some(s.clone()),
            ankurah_core::property::PropertyValue::I16(x) => Some(x.to_string()),
            ankurah_core::property::PropertyValue::I32(x) => Some(x.to_string()),
            ankurah_core::property::PropertyValue::I64(x) => Some(x.to_string()),
            ankurah_core::property::PropertyValue::Bool(b) => Some(b.to_string()),
            ankurah_core::property::PropertyValue::Object(bytes) | ankurah_core::property::PropertyValue::Binary(bytes) => {
                Some(String::from_utf8_lossy(bytes).to_string())
            }
        })
    }
}

// Temporary wrapper to make Result<(IVec, MatEntity), RetrievalError> implement Filterable
// This allows our iterators to work with GetPropertyValueStream until we implement proper error handling
pub struct MatRow {
    pub id: EntityId,
    pub mat: MatEntity,
}

impl Filterable for MatRow {
    fn collection(&self) -> &str { self.mat.collection() }
    fn value(&self, name: &str) -> Option<String> { self.mat.value(name) }
}

impl ankurah_storage_common::filtering::HasEntityId for MatRow {
    fn entity_id(&self) -> EntityId { self.id }
}

// TODO: We'll need to handle Result<(IVec, MatEntity), RetrievalError> in the iterator logic
// instead of trying to make it implement Filterable directly (orphan rules prevent this)
